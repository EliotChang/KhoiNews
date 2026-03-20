from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from typing import Any

from pipeline.article_media import MediaCandidate
from pipeline.config import Settings
from pipeline.content_gen import generate_content_pack
from pipeline.db import (
    get_publish_job_record,
    get_source_post_by_id,
    update_publish_job_request_payload,
    upsert_content_asset,
    upsert_video_asset,
    upsert_voice_asset,
)
from pipeline.publish import MediaPublishPayload, enqueue_publish_jobs_for_post
from pipeline.review_state import review_patch_after_regeneration
from pipeline.video_gen import generate_fish_lipsync_video
from pipeline.voice_gen import generate_elevenlabs_voice


def _ensure_conn_alive(conn: Any) -> None:
    if hasattr(conn, "ensure_alive"):
        conn.ensure_alive()


def _is_mp4_url(url: str) -> bool:
    return str(url or "").strip().lower().split("?", 1)[0].endswith(".mp4")


def _build_regen_prompt_modifier(edit_notes: str) -> str:
    cleaned = " ".join(edit_notes.split()).strip()
    if not cleaned:
        return ""
    return (
        "Reviewer-requested edits for this regeneration: "
        f"{cleaned}\n"
        "Apply these notes while preserving factual neutrality and source-grounded claims."
    )


def _safe_media_candidates_from_payload(payload: dict[str, Any]) -> tuple[list[MediaCandidate], str | None]:
    candidates: list[MediaCandidate] = []
    raw_media_url = str(payload.get("media_url") or "").strip()
    raw_media_type = str(payload.get("media_type") or "").strip().lower()
    if raw_media_url:
        media_type = raw_media_type if raw_media_type in {"image", "video"} else "image"
        candidates.append(
            MediaCandidate(
                media_type=media_type,
                media_url=raw_media_url,
                selection_reason="review_regeneration_existing_media",
                priority=-1,
            )
        )
    thumbnail_url = str(payload.get("thumbnail_url") or "").strip()
    if thumbnail_url and thumbnail_url != raw_media_url:
        candidates.append(
            MediaCandidate(
                media_type="image",
                media_url=thumbnail_url,
                selection_reason="review_regeneration_thumbnail",
                priority=0,
            )
        )
    post_image_url = raw_media_url if raw_media_type == "image" else None
    return candidates, post_image_url


def regenerate_metricool_publish_job(
    conn: Any,
    *,
    settings: Settings,
    job_id: str,
    edit_notes: str,
    actor: str,
) -> dict[str, Any]:
    job = get_publish_job_record(conn, job_id=job_id, persona_key=settings.persona_key)
    if not job:
        raise ValueError(f"Publish job not found: {job_id}")
    if str(job.get("platform") or "") != "metricool":
        raise ValueError(f"Unsupported regeneration platform: {job.get('platform')}")

    old_payload = job.get("request_payload")
    if not isinstance(old_payload, dict):
        old_payload = {}

    source_post = get_source_post_by_id(conn, post_id=str(job.get("post_id")))
    if source_post is None:
        raise ValueError(f"Source post missing for publish job: {job_id}")

    content = generate_content_pack(
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        aws_region=settings.aws_region,
        model_name=settings.anthropic_model,
        title=source_post.title,
        description=source_post.description,
        article_url=source_post.link,
        script_target_seconds=settings.content_script_target_seconds,
        script_target_words=settings.content_script_target_words,
        script_max_words_buffer=settings.content_script_max_words_buffer,
        script_min_words=settings.content_script_min_words,
        script_min_facts=settings.content_script_min_facts,
        script_min_sentences=settings.content_script_min_sentences,
        script_max_sentences=settings.content_script_max_sentences,
        experiment_prompt_modifier=_build_regen_prompt_modifier(edit_notes),
    )
    _ensure_conn_alive(conn)
    upsert_content_asset(conn, post_id=str(job.get("post_id")), content=content)

    voice = generate_elevenlabs_voice(
        api_key=settings.elevenlabs_api_key,
        voice_id=settings.elevenlabs_voice_id,
        text=content.script_10s,
        post_id=str(job.get("post_id")),
        supabase_url=settings.supabase_url,
        supabase_service_role_key=settings.supabase_service_role_key,
        supabase_voice_bucket=settings.supabase_voice_bucket,
        timeout_seconds=settings.request_timeout_seconds,
        model_id=settings.elevenlabs_tts_model_id,
        voice_stability=settings.elevenlabs_voice_stability,
        voice_similarity_boost=settings.elevenlabs_voice_similarity_boost,
        apply_text_normalization=settings.elevenlabs_apply_text_normalization,
    )
    _ensure_conn_alive(conn)
    upsert_voice_asset(
        conn,
        post_id=str(job.get("post_id")),
        voice_id=settings.elevenlabs_voice_id,
        voice_result=voice,
    )
    if not voice.audio_url:
        raise ValueError(f"Voice regeneration failed: {voice.error or 'unknown error'}")

    media_candidates, post_image_url = _safe_media_candidates_from_payload(old_payload)
    date_label = (
        source_post.published_at.astimezone().strftime("%B %d, %Y")
        if source_post.published_at
        else datetime.now().astimezone().strftime("%B %d, %Y")
    )
    video = generate_fish_lipsync_video(
        settings=settings,
        post_id=str(job.get("post_id")),
        audio_url=voice.audio_url,
        post_image_url=post_image_url,
        audio_duration_sec=voice.audio_duration_sec,
        post_title=content.video_title_short,
        date_label=date_label,
        script_text=content.script_10s,
        media_candidates=media_candidates,
        voice_alignment=voice.alignment,
    )
    _ensure_conn_alive(conn)
    upsert_video_asset(
        conn,
        post_id=str(job.get("post_id")),
        template_name="fish_lipsync",
        video_result=video,
    )

    publish_media: MediaPublishPayload | None = None
    if video.status == "generated" and video.video_url:
        publish_media = MediaPublishPayload(
            media_type="video",
            media_url=video.video_url,
            selection_reason="fish_lipsync_render;review_regeneration",
        )
    else:
        old_media_url = str(old_payload.get("media_url") or "").strip()
        old_media_type = str(old_payload.get("media_type") or "").strip().lower()
        if old_media_type == "video" and _is_mp4_url(old_media_url):
            publish_media = MediaPublishPayload(
                media_type="video",
                media_url=old_media_url,
                selection_reason="review_regeneration_fallback_existing_video",
            )
    if publish_media is None:
        raise ValueError("Regeneration did not produce a publishable MP4 video")

    desired_publish_at = str(old_payload.get("desired_publish_at") or "").strip() or None
    platform_settings = replace(settings, publish_platforms=["metricool"])
    enqueued = enqueue_publish_jobs_for_post(
        conn,
        settings=platform_settings,
        post=source_post,
        post_id=str(job.get("post_id")),
        content=content,
        media=publish_media,
        voice=voice,
        desired_publish_at=desired_publish_at,
        video_duration_sec=video.video_duration_sec,
    )
    if not enqueued:
        raise ValueError("Regeneration completed but no Metricool publish job was enqueued")

    refreshed_job_id = str(enqueued[0].id)
    _ensure_conn_alive(conn)
    update_publish_job_request_payload(
        conn,
        job_id=refreshed_job_id,
        persona_key=settings.persona_key,
        payload_patch=review_patch_after_regeneration(
            previous_payload=old_payload,
            actor=actor,
            edit_notes=edit_notes,
        ),
    )
    return {
        "job_id": refreshed_job_id,
        "post_id": str(job.get("post_id")),
        "title": source_post.title,
        "desired_publish_at": desired_publish_at or "",
        "video_url": publish_media.media_url,
    }
