from __future__ import annotations

import asyncio
from collections import defaultdict
import contextlib
from datetime import datetime, timezone
import logging
from typing import Any

import discord
from discord import app_commands
from discord.ext import commands

from pipeline.config import bootstrap_runtime_env, load_settings
from pipeline.db import (
    db_connection,
    get_publish_job_record,
    list_metricool_jobs_needing_review_post,
    list_metricool_review_jobs,
    set_metricool_job_review_approval,
    set_metricool_job_review_message_refs,
    set_metricool_job_review_regenerating,
    update_publish_job_request_payload,
)
from pipeline.review_regeneration import regenerate_metricool_publish_job
from pipeline.review_state import REVIEW_STATUS_PENDING, utc_now_iso


LOGGER = logging.getLogger("wj_discord_bot")


def _safe_int(raw_value: str) -> int | None:
    try:
        return int(str(raw_value or "").strip())
    except (TypeError, ValueError):
        return None


def _truncate(value: str, max_len: int) -> str:
    text = str(value or "").strip()
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"


def _iso_to_human(value: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return "unscheduled"
    try:
        dt = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return normalized
    utc_value = dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    local_value = dt.astimezone().strftime("%Y-%m-%d %I:%M %p %Z")
    return f"{utc_value} ({local_value})"


class RegenerateModal(discord.ui.Modal, title="Regenerate Draft"):
    edit_notes = discord.ui.TextInput(
        label="What should change?",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=1000,
        placeholder="Example: Tighter hook, simplify sentence two, stronger factual close.",
    )

    def __init__(self, *, bot: "ReviewBot", job_id: str) -> None:
        super().__init__(timeout=300)
        self.bot = bot
        self.job_id = job_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await self.bot.handle_regenerate(
            interaction=interaction,
            job_id=self.job_id,
            edit_notes=str(self.edit_notes.value or "").strip(),
        )


class ReviewActionsView(discord.ui.View):
    def __init__(self, *, bot: "ReviewBot", job_id: str) -> None:
        super().__init__(timeout=None)
        self.bot = bot
        self.job_id = job_id

    @discord.ui.button(label="Approve", style=discord.ButtonStyle.success)
    async def approve(
        self,
        interaction: discord.Interaction,
        _button: discord.ui.Button,
    ) -> None:
        await self.bot.handle_approval(interaction=interaction, job_id=self.job_id, approved=True)

    @discord.ui.button(label="Reject", style=discord.ButtonStyle.danger)
    async def reject(
        self,
        interaction: discord.Interaction,
        _button: discord.ui.Button,
    ) -> None:
        await self.bot.handle_approval(interaction=interaction, job_id=self.job_id, approved=False)

    @discord.ui.button(label="Regenerate", style=discord.ButtonStyle.primary)
    async def regenerate(
        self,
        interaction: discord.Interaction,
        _button: discord.ui.Button,
    ) -> None:
        if not await self.bot.ensure_interaction_allowed(interaction):
            return
        await interaction.response.send_modal(RegenerateModal(bot=self.bot, job_id=self.job_id))


class ReviewCog(commands.Cog):
    def __init__(self, bot: "ReviewBot") -> None:
        self.bot = bot

    @app_commands.command(name="queue", description="Show Metricool review queue")
    async def queue(self, interaction: discord.Interaction) -> None:
        await self.bot.handle_queue(interaction)


class ReviewBot(commands.Bot):
    def __init__(self) -> None:
        bootstrap_runtime_env()
        settings = load_settings()
        intents = discord.Intents.default()
        intents.guilds = True
        super().__init__(
            command_prefix="!",
            intents=intents,
            application_id=_safe_int(settings.discord_application_id),
        )
        self.settings = settings
        self._job_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._poll_task: asyncio.Task[None] | None = None

    def _with_db(self, fn: Any) -> Any:
        with db_connection(self.settings.supabase_db_url) as conn:
            return fn(conn)

    async def setup_hook(self) -> None:
        await self.add_cog(ReviewCog(self))
        guild_id = _safe_int(self.settings.discord_guild_id)
        if guild_id:
            guild = discord.Object(id=guild_id)
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
        else:
            await self.tree.sync()
        self._poll_task = asyncio.create_task(self._review_poll_loop(), name="discord-review-poll")

    async def close(self) -> None:
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._poll_task
        await super().close()

    async def on_ready(self) -> None:
        LOGGER.info("Discord review bot connected user=%s", self.user)

    async def ensure_interaction_allowed(self, interaction: discord.Interaction) -> bool:
        if self.settings.discord_allow_all_members:
            return True
        member = interaction.user
        if isinstance(member, discord.Member) and (
            member.guild_permissions.manage_guild or member.guild_permissions.manage_messages
        ):
            return True
        if not interaction.response.is_done():
            await interaction.response.send_message(
                "You do not have permission to approve/reject/regenerate drafts.",
                ephemeral=True,
            )
        else:
            await interaction.followup.send(
                "You do not have permission to approve/reject/regenerate drafts.",
                ephemeral=True,
            )
        return False

    async def _resolve_review_channel(self) -> discord.TextChannel | None:
        channel_id = _safe_int(self.settings.discord_review_channel_id)
        if not channel_id:
            return None
        cached = self.get_channel(channel_id)
        if isinstance(cached, discord.TextChannel):
            return cached
        try:
            fetched = await self.fetch_channel(channel_id)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to fetch review channel id=%s error=%s", channel_id, exc)
            return None
        return fetched if isinstance(fetched, discord.TextChannel) else None

    async def _review_poll_loop(self) -> None:
        await self.wait_until_ready()
        while not self.is_closed():
            try:
                await self.post_pending_reviews()
            except Exception:  # noqa: BLE001
                LOGGER.exception("Review polling iteration failed")
            await asyncio.sleep(self.settings.discord_poll_seconds)

    async def post_pending_reviews(self) -> None:
        channel = await self._resolve_review_channel()
        if channel is None:
            LOGGER.warning("DISCORD_REVIEW_CHANNEL_ID is missing or invalid; cannot post reviews")
            return

        jobs = await asyncio.to_thread(
            lambda: self._with_db(
                lambda conn: list_metricool_jobs_needing_review_post(
                    conn,
                    persona_key=self.settings.persona_key,
                    limit=10,
                )
            )
        )
        if not jobs:
            return

        for job in jobs:
            job_id = str(job.get("id") or "").strip()
            if not job_id:
                continue
            payload = job.get("request_payload")
            if not isinstance(payload, dict):
                payload = {}
            message = await channel.send(
                content=self._build_review_message(job=job, payload=payload),
                view=ReviewActionsView(bot=self, job_id=job_id),
                allowed_mentions=discord.AllowedMentions.none(),
            )
            thread_id = ""
            try:
                thread = await message.create_thread(
                    name=_truncate(f"Review: {str(job.get('title') or 'Untitled')}", 90),
                    auto_archive_duration=1440,
                )
                thread_id = str(thread.id)
                await thread.send("Use the message buttons above to approve, reject, or regenerate this draft.")
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Failed creating review thread for job_id=%s error=%s", job_id, exc)
            await asyncio.to_thread(
                lambda: self._with_db(
                    lambda conn: set_metricool_job_review_message_refs(
                        conn,
                        job_id=job_id,
                        persona_key=self.settings.persona_key,
                        review_channel_id=str(channel.id),
                        review_message_id=str(message.id),
                        review_thread_id=thread_id,
                    )
                )
            )

    def _build_review_message(self, *, job: dict[str, Any], payload: dict[str, Any]) -> str:
        title = _truncate(str(job.get("title") or "Untitled"), 120)
        video_url = str(payload.get("media_url") or "").strip()
        publish_at = _iso_to_human(str(payload.get("desired_publish_at") or ""))
        providers = ",".join(self.settings.metricool_target_platforms) or "metricool-defaults"
        lines = [
            f"**Draft Ready for Review**",
            f"Job ID: `{job.get('id')}`",
            f"Title: {title}",
            f"Publish Time: {publish_at}",
            f"Providers: {providers}",
            f"Video URL: {video_url or 'missing'}",
        ]
        return "\n".join(lines)

    async def handle_queue(self, interaction: discord.Interaction) -> None:
        jobs = await asyncio.to_thread(
            lambda: self._with_db(
                lambda conn: list_metricool_review_jobs(
                    conn,
                    persona_key=self.settings.persona_key,
                    limit=20,
                )
            )
        )
        if not jobs:
            await interaction.response.send_message("No Metricool drafts in review queue.", ephemeral=True)
            return

        guild_id = interaction.guild_id or _safe_int(self.settings.discord_guild_id) or 0
        lines: list[str] = []
        for idx, job in enumerate(jobs, start=1):
            payload = job.get("request_payload")
            if not isinstance(payload, dict):
                payload = {}
            approval_status = str(job.get("approval_status") or REVIEW_STATUS_PENDING)
            desired_publish_at = _iso_to_human(str(payload.get("desired_publish_at") or ""))
            thread_id = str(payload.get("review_thread_id") or "").strip()
            thread_link = (
                f"https://discord.com/channels/{guild_id}/{thread_id}"
                if guild_id and thread_id
                else "not-created"
            )
            title = _truncate(str(job.get("title") or "Untitled"), 70)
            lines.append(
                f"{idx}. [{approval_status}] {title} | publish={desired_publish_at} | thread={thread_link}"
            )

        message = "\n".join(lines)
        if len(message) > 1800:
            message = "\n".join(lines[:10])
            message += "\n…truncated"
        await interaction.response.send_message(message, ephemeral=True)

    def _actor_label(self, user: discord.abc.User) -> str:
        username = getattr(user, "name", "unknown")
        user_id = getattr(user, "id", 0)
        return f"{username} ({user_id})"

    async def _send_thread_update(self, *, job_id: str, body: str) -> None:
        record = await asyncio.to_thread(
            lambda: self._with_db(
                lambda conn: get_publish_job_record(
                    conn,
                    job_id=job_id,
                    persona_key=self.settings.persona_key,
                )
            )
        )
        if not record:
            return
        payload = record.get("request_payload")
        if not isinstance(payload, dict):
            return
        thread_id = _safe_int(str(payload.get("review_thread_id") or ""))
        await self._send_thread_update_by_id(thread_id=thread_id, body=body, job_id=job_id)

    async def _send_thread_update_by_id(self, *, thread_id: int | None, body: str, job_id: str) -> None:
        if not thread_id:
            return
        channel = self.get_channel(thread_id)
        if channel is None:
            try:
                channel = await self.fetch_channel(thread_id)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Unable to fetch review thread job_id=%s thread_id=%s error=%s", job_id, thread_id, exc)
                return
        if isinstance(channel, discord.Thread):
            await channel.send(body, allowed_mentions=discord.AllowedMentions.none())

    async def handle_approval(self, *, interaction: discord.Interaction, job_id: str, approved: bool) -> None:
        if not await self.ensure_interaction_allowed(interaction):
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        lock = self._job_locks[job_id]
        actor = self._actor_label(interaction.user)
        async with lock:
            success = await asyncio.to_thread(
                lambda: self._with_db(
                    lambda conn: set_metricool_job_review_approval(
                        conn,
                        job_id=job_id,
                        persona_key=self.settings.persona_key,
                        actor=actor,
                        approved=approved,
                    )
                )
            )
        if not success:
            await interaction.followup.send(
                "Action could not be applied. The job may already be published or locked by another action.",
                ephemeral=True,
            )
            return
        action = "approved" if approved else "rejected"
        await self._send_thread_update(job_id=job_id, body=f"{action.title()} by {actor}.")
        await interaction.followup.send(f"Job `{job_id}` {action}.", ephemeral=True)

    async def handle_regenerate(
        self,
        *,
        interaction: discord.Interaction,
        job_id: str,
        edit_notes: str,
    ) -> None:
        if not await self.ensure_interaction_allowed(interaction):
            return
        if not edit_notes.strip():
            await interaction.response.send_message("Edit notes are required.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        lock = self._job_locks[job_id]
        actor = self._actor_label(interaction.user)
        previous_record = await asyncio.to_thread(
            lambda: self._with_db(
                lambda conn: get_publish_job_record(
                    conn,
                    job_id=job_id,
                    persona_key=self.settings.persona_key,
                )
            )
        )
        previous_payload = previous_record.get("request_payload") if isinstance(previous_record, dict) else {}
        previous_thread_id = _safe_int(
            str(previous_payload.get("review_thread_id") if isinstance(previous_payload, dict) else "")
        )

        async with lock:
            marked = await asyncio.to_thread(
                lambda: self._with_db(
                    lambda conn: set_metricool_job_review_regenerating(
                        conn,
                        job_id=job_id,
                        persona_key=self.settings.persona_key,
                        actor=actor,
                        edit_notes=edit_notes,
                    )
                )
            )
            if not marked:
                await interaction.followup.send(
                    "Could not start regeneration. The job may already be published or locked.",
                    ephemeral=True,
                )
                return
            await self._send_thread_update(
                job_id=job_id,
                body=f"Regeneration started by {actor}. Notes: {edit_notes}",
            )
            try:
                regen_result = await asyncio.to_thread(
                    lambda: self._with_db(
                        lambda conn: regenerate_metricool_publish_job(
                            conn,
                            settings=self.settings,
                            job_id=job_id,
                            edit_notes=edit_notes,
                            actor=actor,
                        )
                    )
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("Regeneration failed for job_id=%s", job_id)
                await asyncio.to_thread(
                    lambda: self._with_db(
                        lambda conn: update_publish_job_request_payload(
                            conn,
                            job_id=job_id,
                            persona_key=self.settings.persona_key,
                            payload_patch={
                                "approval_status": REVIEW_STATUS_PENDING,
                                "approval_by": actor,
                                "approval_at": utc_now_iso(),
                            },
                        )
                    )
                )
                await self._send_thread_update(
                    job_id=job_id,
                    body=f"Regeneration failed: {exc}",
                )
                await self._send_thread_update_by_id(
                    thread_id=previous_thread_id,
                    body=f"Regeneration failed for job `{job_id}`: {exc}",
                    job_id=job_id,
                )
                await interaction.followup.send(
                    f"Regeneration failed for job `{job_id}`: {exc}",
                    ephemeral=True,
                )
                return

        await interaction.followup.send(
            f"Regeneration complete for job `{job_id}`. A fresh review post will appear shortly.",
            ephemeral=True,
        )
        await self._send_thread_update_by_id(
            thread_id=previous_thread_id,
            body=(
                f"Regeneration completed by {actor}. "
                "A new pending review message is being posted for approval."
            ),
            job_id=str(regen_result.get("job_id") or job_id),
        )


def _validate_discord_settings(bot: ReviewBot) -> None:
    required = {
        "DISCORD_BOT_TOKEN": bot.settings.discord_bot_token,
        "DISCORD_APPLICATION_ID": bot.settings.discord_application_id,
        "DISCORD_GUILD_ID": bot.settings.discord_guild_id,
        "DISCORD_REVIEW_CHANNEL_ID": bot.settings.discord_review_channel_id,
    }
    missing = [key for key, value in required.items() if not str(value or "").strip()]
    if missing:
        raise ValueError(f"Missing Discord configuration: {', '.join(missing)}")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    bot = ReviewBot()
    _validate_discord_settings(bot)
    bot.run(bot.settings.discord_bot_token)


if __name__ == "__main__":
    main()
