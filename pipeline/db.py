from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import logging
from typing import Any, Iterator

import psycopg
from psycopg.rows import dict_row

from pipeline.article_media import MediaAssetResult
from pipeline.content_gen import ContentGenerationResult
from pipeline.review_state import (
    review_patch_for_approval,
    review_patch_for_regeneration_start,
)
from pipeline.wj_ingest import SourcePostInput
from pipeline.video_gen import VideoAssetResult
from pipeline.voice_gen import VoiceAssetResult


LOGGER = logging.getLogger("wj_db")


@dataclass(frozen=True)
class UpsertSourcePostResult:
    post_id: str
    is_new: bool


@dataclass(frozen=True)
class PublishJob:
    id: str
    post_id: str
    persona_key: str
    platform: str
    status: str
    request_payload: dict[str, Any]
    retry_count: int
    max_retries: int


@dataclass(frozen=True)
class PublishAttempt:
    id: str
    job_id: str
    attempt_number: int


def _open_connection(db_url: str) -> psycopg.Connection:
    conn = psycopg.connect(
        db_url,
        row_factory=dict_row,
        prepare_threshold=None,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=3,
    )
    conn.execute("SET statement_timeout = '300s'")
    return conn


def _is_connection_alive(conn: psycopg.Connection) -> bool:
    try:
        conn.execute("SELECT 1")
        return True
    except psycopg.errors.InFailedSqlTransaction:
        try:
            conn.rollback()
            conn.execute("SELECT 1")
            LOGGER.info("DB connection recovered after rollback of failed transaction")
            return True
        except Exception:  # noqa: BLE001
            return False
    except (psycopg.OperationalError, psycopg.InterfaceError):
        return False


class ReconnectingConnection:
    """Proxy that transparently reconnects when PgBouncer drops an idle connection.

    Implements the subset of psycopg.Connection used by pipeline DB helpers
    (cursor, execute) so callers can treat it as a drop-in replacement.
    """

    def __init__(self, db_url: str) -> None:
        self._db_url = db_url
        self._conn = _open_connection(db_url)

    @property
    def conn(self) -> psycopg.Connection:
        return self._conn

    def ensure_alive(self) -> None:
        if _is_connection_alive(self._conn):
            return
        LOGGER.warning("DB connection lost — reconnecting")
        try:
            self._conn.close()
        except Exception:  # noqa: BLE001
            pass
        self._conn = _open_connection(self._db_url)

    def cursor(self, *args: Any, **kwargs: Any) -> Any:
        self.ensure_alive()
        return self._conn.cursor(*args, **kwargs)

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        self.ensure_alive()
        return self._conn.execute(*args, **kwargs)

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()


@contextmanager
def db_connection(db_url: str) -> Iterator[ReconnectingConnection]:
    # TCP keepalives + reconnection guard against PgBouncer dropping
    # idle connections during long-running pipeline steps (video render).
    rconn = ReconnectingConnection(db_url)
    try:
        yield rconn
        rconn.commit()
    except Exception:
        try:
            rconn.rollback()
        except (psycopg.OperationalError, psycopg.InterfaceError):
            LOGGER.warning("Rollback failed — connection already lost")
        raise
    finally:
        try:
            rconn.close()
        except (psycopg.OperationalError, psycopg.InterfaceError):
            pass


def create_pipeline_run(conn: psycopg.Connection) -> str:
    with conn.cursor() as cur:
        cur.execute("insert into pipeline_runs (status) values ('running') returning id")
        row = cur.fetchone()
        assert row is not None
        return str(row["id"])


def ensure_pipeline_schema(conn: psycopg.Connection) -> None:
    statements = [
        """
        alter table if exists media_assets
          add column if not exists media_quality_summary jsonb not null default '{}'::jsonb
        """,
        """
        create table if not exists persona_profiles (
          id uuid primary key default gen_random_uuid(),
          persona_key text not null unique,
          display_name text not null default '',
          metricool_user_id text,
          metricool_blog_id text,
          metricool_target_platforms text[] not null default '{}'::text[],
          is_active boolean not null default true,
          created_at timestamptz not null default now(),
          updated_at timestamptz not null default now()
        )
        """,
        """
        alter table if exists publish_jobs
          add column if not exists persona_key text not null default 'default'
        """,
        """
        create table if not exists optimization_recommendations (
          id uuid primary key default gen_random_uuid(),
          persona_key text not null references persona_profiles(persona_key) on update cascade,
          diagnosis text not null,
          confidence numeric(6,5) not null default 0,
          sample_size integer not null default 0,
          window_start timestamptz,
          window_end timestamptz,
          recommended_overrides jsonb not null default '{}'::jsonb,
          rationale text not null default '',
          status text not null default 'proposed' check (status in ('proposed', 'applied', 'skipped')),
          created_at timestamptz not null default now(),
          updated_at timestamptz not null default now()
        )
        """,
        """
        create table if not exists runtime_overrides (
          id uuid primary key default gen_random_uuid(),
          persona_key text not null references persona_profiles(persona_key) on update cascade,
          key text not null,
          value jsonb not null,
          value_type text not null,
          source_recommendation_id uuid references optimization_recommendations(id) on delete set null,
          is_active boolean not null default true,
          applied_at timestamptz not null default now(),
          expires_at timestamptz,
          created_at timestamptz not null default now(),
          updated_at timestamptz not null default now()
        )
        """,
        """
        create table if not exists video_performance_metrics (
          id uuid primary key default gen_random_uuid(),
          persona_key text not null references persona_profiles(persona_key) on update cascade,
          publish_job_id uuid references publish_jobs(id) on delete set null,
          platform text not null default 'metricool',
          external_post_id text not null,
          metric_timestamp timestamptz not null default now(),
          views integer,
          likes integer,
          comments integer,
          shares integer,
          saves integer,
          watch_time_seconds numeric(12,2),
          avg_watch_seconds numeric(10,2),
          avg_retention_ratio numeric(8,5),
          completion_rate numeric(8,5),
          engagement_rate numeric(8,5),
          metrics jsonb not null default '{}'::jsonb,
          source text not null default 'metricool_api',
          created_at timestamptz not null default now(),
          updated_at timestamptz not null default now()
        )
        """,
        """
        alter table if exists content_assets
          add column if not exists series_tag text,
          add column if not exists series_part integer
        """,
        """
        create table if not exists ab_experiments (
          id uuid primary key default gen_random_uuid(),
          persona_key text not null references persona_profiles(persona_key) on update cascade,
          experiment_name text not null,
          variable_name text not null,
          variants jsonb not null default '[]'::jsonb,
          status text not null default 'active' check (status in ('active', 'paused', 'completed')),
          hypothesis text not null default '',
          conclusion text,
          start_date timestamptz not null default now(),
          end_date timestamptz,
          created_at timestamptz not null default now(),
          updated_at timestamptz not null default now(),
          unique (persona_key, experiment_name)
        )
        """,
        """
        create table if not exists ab_assignments (
          id uuid primary key default gen_random_uuid(),
          experiment_id uuid not null references ab_experiments(id) on delete cascade,
          post_id uuid not null references source_posts(id) on delete cascade,
          variant_key text not null,
          assigned_at timestamptz not null default now(),
          unique (experiment_id, post_id)
        )
        """,
        """
        alter table if exists content_assets
          add column if not exists experiment_id uuid,
          add column if not exists variant_key text,
          add column if not exists content_signals jsonb not null default '{}'::jsonb
        """,
        """
        alter table if exists content_assets
          add column if not exists thumbnail_url text,
          add column if not exists thumbnail_source text
        """,
        """
        create table if not exists source_gate_failures (
          id uuid primary key default gen_random_uuid(),
          source text not null,
          source_guid text not null,
          gate text not null,
          failure_count integer not null default 0,
          last_issues jsonb not null default '[]'::jsonb,
          last_failed_at timestamptz not null default now(),
          suppressed_until timestamptz,
          created_at timestamptz not null default now(),
          updated_at timestamptz not null default now(),
          unique (source, source_guid, gate)
        )
        """,
        """
        create index if not exists idx_source_gate_failures_gate_suppressed_until
          on source_gate_failures (gate, suppressed_until)
        """,
        """
        create table if not exists post_quality_evaluations (
          id uuid primary key default gen_random_uuid(),
          post_id uuid not null references source_posts(id) on delete cascade,
          run_id uuid references pipeline_runs(id) on delete set null,
          persona_key text not null references persona_profiles(persona_key) on update cascade,
          evaluated_at timestamptz not null default now(),
          script_specificity_score numeric(8,5) not null default 0,
          narrative_flow_score numeric(8,5) not null default 0,
          visual_relevance_score numeric(8,5) not null default 0,
          visual_variety_score numeric(8,5) not null default 0,
          first_two_seconds_hook_score numeric(8,5) not null default 0,
          composite_score numeric(8,5) not null default 0,
          passed boolean not null default false,
          failing_dimensions jsonb not null default '[]'::jsonb,
          metadata jsonb not null default '{}'::jsonb,
          created_at timestamptz not null default now(),
          updated_at timestamptz not null default now()
        )
        """,
        """
        create index if not exists idx_post_quality_evaluations_persona_time
          on post_quality_evaluations (persona_key, evaluated_at desc)
        """,
        """
        create unique index if not exists idx_post_quality_evaluations_post_run
          on post_quality_evaluations (post_id, run_id)
          where run_id is not null
        """,
    ]
    skipped_statements = 0
    with conn.cursor() as cur:
        # Avoid a single blocked DDL lock aborting the publish run.
        cur.execute("set local lock_timeout = '5s'")
        for statement in statements:
            try:
                cur.execute("savepoint pipeline_schema_stmt")
                cur.execute(statement)
                cur.execute("release savepoint pipeline_schema_stmt")
            except psycopg.Error as exc:
                skipped_statements += 1
                cur.execute("rollback to savepoint pipeline_schema_stmt")
                cur.execute("release savepoint pipeline_schema_stmt")
                LOGGER.warning("Schema compatibility statement skipped due to DB lock/timeout: %s", exc)
    LOGGER.info(
        "Ensured pipeline schema compatibility checks (skipped_statements=%s)",
        skipped_statements,
    )


def finish_pipeline_run(
    conn: psycopg.Connection,
    *,
    run_id: str,
    status: str,
    posts_seen: int,
    posts_processed: int,
    errors: list[dict[str, Any]],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update pipeline_runs
            set
              status = %(status)s,
              posts_seen = %(posts_seen)s,
              posts_processed = %(posts_processed)s,
              finished_at = clock_timestamp(),
              errors = %(errors)s::jsonb
            where id = %(run_id)s::uuid
            """,
            {
                "status": status,
                "posts_seen": posts_seen,
                "posts_processed": posts_processed,
                "errors": json.dumps(errors),
                "run_id": run_id,
            },
        )


def _is_successfully_processed(cur: Any, post_id: str) -> bool:
    cur.execute(
        """
        select exists(
            select 1 from publish_jobs where post_id = %(pid)s::uuid and status = 'published'
        ) as has_publish
        """,
        {"pid": post_id},
    )
    row = cur.fetchone()
    return bool(row and row["has_publish"])


def link_exists_in_source_posts(conn: psycopg.Connection, link: str) -> bool:
    normalized = link.strip().rstrip("/").lower()
    if not normalized:
        return False
    with conn.cursor() as cur:
        cur.execute(
            """
            select 1
            from source_posts
            where lower(rtrim(link, '/')) = %(link)s
            limit 1
            """,
            {"link": normalized},
        )
        return cur.fetchone() is not None


def upsert_source_post(
    conn: psycopg.Connection, post: SourcePostInput, *, force_recycle: bool = False
) -> UpsertSourcePostResult:
    with conn.cursor() as cur:
        cur.execute(
            """
            select id
            from source_posts
            where source = %(source)s and source_guid = %(source_guid)s
            """,
            {"source": post.source, "source_guid": post.source_guid},
        )
        existing = cur.fetchone()
        is_new = existing is None

        if existing:
            already_published = _is_successfully_processed(cur, str(existing["id"]))
            if already_published:
                LOGGER.info(
                    "Skipping already-published post source=%s guid=%s post_id=%s",
                    post.source,
                    post.source_guid,
                    existing["id"],
                )
            elif force_recycle:
                LOGGER.info(
                    "Force-recycling failed post for safety-net reprocess source=%s guid=%s",
                    post.source,
                    post.source_guid,
                )
                cur.execute(
                    "delete from source_posts where id = %(id)s::uuid",
                    {"id": str(existing["id"])},
                )
                is_new = True
            else:
                LOGGER.info(
                    "Recycling previously failed post for retry source=%s guid=%s",
                    post.source,
                    post.source_guid,
                )
                cur.execute(
                    "delete from source_posts where id = %(id)s::uuid",
                    {"id": str(existing["id"])},
                )
                is_new = True

        cur.execute(
            """
            insert into source_posts
              (source, source_guid, title, description, link, published_at, raw_payload)
            values
              (%(source)s, %(source_guid)s, %(title)s, %(description)s, %(link)s, %(published_at)s, %(raw_payload)s::jsonb)
            on conflict (source, source_guid)
            do update set
              title = excluded.title,
              description = excluded.description,
              link = excluded.link,
              published_at = excluded.published_at,
              raw_payload = excluded.raw_payload
            returning id
            """,
            {
                "source": post.source,
                "source_guid": post.source_guid,
                "title": post.title,
                "description": post.description,
                "link": post.link,
                "published_at": post.published_at,
                "raw_payload": json.dumps(post.raw_payload, default=str),
            },
        )
        row = cur.fetchone()
        assert row is not None
        return UpsertSourcePostResult(post_id=str(row["id"]), is_new=is_new)


def is_source_gate_suppressed(
    conn: psycopg.Connection,
    *,
    source: str,
    source_guid: str,
    gate: str,
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            select suppressed_until
            from source_gate_failures
            where source = %(source)s
              and source_guid = %(source_guid)s
              and gate = %(gate)s
            """,
            {"source": source, "source_guid": source_guid, "gate": gate},
        )
        row = cur.fetchone()

    if not isinstance(row, dict):
        return False
    suppressed_until = row.get("suppressed_until")
    if not isinstance(suppressed_until, datetime):
        return False
    if suppressed_until.tzinfo is None:
        suppressed_until = suppressed_until.replace(tzinfo=timezone.utc)
    return suppressed_until > datetime.now(timezone.utc)


def record_source_gate_failure(
    conn: psycopg.Connection,
    *,
    source: str,
    source_guid: str,
    gate: str,
    issues: list[str],
    suppress_after: int,
    suppress_days: int,
) -> None:
    safe_suppress_after = max(1, int(suppress_after))
    safe_suppress_days = max(1, int(suppress_days))
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into source_gate_failures
              (source, source_guid, gate, failure_count, last_issues, last_failed_at, suppressed_until)
            values
              (%(source)s, %(source_guid)s, %(gate)s, 1, %(issues)s::jsonb, now(), null)
            on conflict (source, source_guid, gate)
            do update set
              failure_count = source_gate_failures.failure_count + 1,
              last_issues = excluded.last_issues,
              last_failed_at = now(),
              suppressed_until = case
                when source_gate_failures.failure_count + 1 >= %(suppress_after)s
                  then now() + make_interval(days => %(suppress_days)s)
                else null
              end
            """,
            {
                "source": source,
                "source_guid": source_guid,
                "gate": gate,
                "issues": json.dumps(issues),
                "suppress_after": safe_suppress_after,
                "suppress_days": safe_suppress_days,
            },
        )


def clear_source_gate_failure(
    conn: psycopg.Connection,
    *,
    source: str,
    source_guid: str,
    gate: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            delete from source_gate_failures
            where source = %(source)s
              and source_guid = %(source_guid)s
              and gate = %(gate)s
            """,
            {"source": source, "source_guid": source_guid, "gate": gate},
        )


def list_recent_source_post_titles(
    conn: psycopg.Connection, *, lookback_hours: int = 72
) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select title
            from source_posts
            where title is not null
              and title != ''
              and created_at > now() - make_interval(hours => %(hours)s)
            order by created_at desc
            """,
            {"hours": lookback_hours},
        )
        return [str(row["title"]) for row in cur.fetchall()]


def list_other_headlines_for_day(
    conn: psycopg.Connection,
    *,
    post_id: str,
    published_at: datetime | None,
    limit: int = 5,
) -> list[str]:
    safe_limit = max(0, int(limit))
    if safe_limit == 0:
        return []

    target_published_at = published_at or datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute(
            """
            select title
            from source_posts
            where id != %(post_id)s::uuid
              and title is not null
              and btrim(title) != ''
              and (published_at at time zone 'utc')::date = (%(published_at)s::timestamptz at time zone 'utc')::date
            order by published_at desc nulls last
            limit %(limit)s
            """,
            {
                "post_id": post_id,
                "published_at": target_published_at,
                "limit": safe_limit,
            },
        )
        rows = cur.fetchall()

    return [str(row["title"]).strip() for row in rows if str(row["title"]).strip()]


def upsert_content_asset(
    conn: psycopg.Connection,
    *,
    post_id: str,
    content: ContentGenerationResult,
    experiment_id: str | None = None,
    variant_key: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into content_assets
              (post_id, script_10s, caption_instagram, caption_tiktok, caption_youtube, caption_x, hashtags, tone, language, model_name, prompt_version, series_tag, series_part, experiment_id, variant_key, content_signals)
            values
              (%(post_id)s::uuid, %(script_10s)s, %(caption_instagram)s, %(caption_tiktok)s, %(caption_youtube)s, %(caption_x)s, %(hashtags)s, %(tone)s, %(language)s, %(model_name)s, %(prompt_version)s, %(series_tag)s, %(series_part)s, %(experiment_id)s::uuid, %(variant_key)s, %(content_signals)s::jsonb)
            on conflict (post_id)
            do update set
              script_10s = excluded.script_10s,
              caption_instagram = excluded.caption_instagram,
              caption_tiktok = excluded.caption_tiktok,
              caption_youtube = excluded.caption_youtube,
              caption_x = excluded.caption_x,
              hashtags = excluded.hashtags,
              tone = excluded.tone,
              language = excluded.language,
              model_name = excluded.model_name,
              prompt_version = excluded.prompt_version,
              series_tag = excluded.series_tag,
              series_part = excluded.series_part,
              experiment_id = excluded.experiment_id,
              variant_key = excluded.variant_key,
              content_signals = excluded.content_signals
            """,
            {
                "post_id": post_id,
                "script_10s": content.script_10s,
                "caption_instagram": content.caption_instagram,
                "caption_tiktok": content.caption_tiktok,
                "caption_youtube": content.caption_youtube,
                "caption_x": content.caption_x,
                "hashtags": content.hashtags,
                "tone": content.tone,
                "language": content.language,
                "model_name": content.model_name,
                "prompt_version": content.prompt_version,
                "series_tag": content.series_tag,
                "series_part": content.series_part,
                "experiment_id": experiment_id,
                "variant_key": variant_key,
                "content_signals": json.dumps(content.content_signals or {}),
            },
        )


def update_content_asset_thumbnail(
    conn: psycopg.Connection,
    *,
    post_id: str,
    thumbnail_url: str,
    thumbnail_source: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update content_assets
            set thumbnail_url = %(thumbnail_url)s,
                thumbnail_source = %(thumbnail_source)s
            where post_id = %(post_id)s::uuid
            """,
            {
                "post_id": post_id,
                "thumbnail_url": thumbnail_url,
                "thumbnail_source": thumbnail_source,
            },
        )


def list_recent_series_tags(
    conn: psycopg.Connection,
    *,
    persona_key: str,
    lookback_days: int = 30,
    limit: int = 10,
) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select distinct ca.series_tag
            from content_assets ca
            join source_posts sp on sp.id = ca.post_id
            join publish_jobs pj on pj.post_id = sp.id
            where pj.persona_key = %(persona_key)s
              and ca.series_tag is not null
              and ca.created_at > now() - make_interval(days => %(lookback_days)s)
            order by ca.series_tag
            limit %(limit)s
            """,
            {
                "persona_key": persona_key,
                "lookback_days": lookback_days,
                "limit": limit,
            },
        )
        return [row["series_tag"] for row in cur.fetchall()]


def upsert_voice_asset(
    conn: psycopg.Connection,
    *,
    post_id: str,
    voice_id: str,
    voice_result: VoiceAssetResult,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into voice_assets
              (post_id, elevenlabs_voice_id, audio_url, audio_duration_sec, status, error)
            values
              (%(post_id)s::uuid, %(voice_id)s, %(audio_url)s, %(audio_duration_sec)s, %(status)s, %(error)s)
            on conflict (post_id)
            do update set
              elevenlabs_voice_id = excluded.elevenlabs_voice_id,
              audio_url = excluded.audio_url,
              audio_duration_sec = excluded.audio_duration_sec,
              status = excluded.status,
              error = excluded.error
            """,
            {
                "post_id": post_id,
                "voice_id": voice_id,
                "audio_url": voice_result.audio_url,
                "audio_duration_sec": voice_result.audio_duration_sec,
                "status": voice_result.status,
                "error": voice_result.error,
            },
        )


def upsert_media_asset(
    conn: psycopg.Connection,
    *,
    post_id: str,
    source_page_url: str,
    media_result: MediaAssetResult,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into media_assets
              (post_id, media_type, media_url, source_page_url, selection_reason, media_quality_summary, status)
            values
              (%(post_id)s::uuid, %(media_type)s, %(media_url)s, %(source_page_url)s, %(selection_reason)s, %(media_quality_summary)s::jsonb, 'ready')
            on conflict (post_id)
            do update set
              media_type = excluded.media_type,
              media_url = excluded.media_url,
              source_page_url = excluded.source_page_url,
              selection_reason = excluded.selection_reason,
              media_quality_summary = excluded.media_quality_summary,
              status = excluded.status
            """,
            {
                "post_id": post_id,
                "media_type": media_result.media_type,
                "media_url": media_result.media_url,
                "source_page_url": source_page_url,
                "selection_reason": media_result.selection_reason,
                "media_quality_summary": json.dumps(media_result.quality_summary or {}),
            },
        )


def upsert_video_asset(
    conn: psycopg.Connection,
    *,
    post_id: str,
    template_name: str,
    video_result: VideoAssetResult,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into video_assets
              (post_id, template_name, video_url, subtitle_url, video_duration_sec, status, error)
            values
              (%(post_id)s::uuid, %(template_name)s, %(video_url)s, %(subtitle_url)s, %(video_duration_sec)s, %(status)s, %(error)s)
            on conflict (post_id)
            do update set
              template_name = excluded.template_name,
              video_url = excluded.video_url,
              subtitle_url = excluded.subtitle_url,
              video_duration_sec = excluded.video_duration_sec,
              status = excluded.status,
              error = excluded.error
            """,
            {
                "post_id": post_id,
                "template_name": template_name,
                "video_url": video_result.video_url,
                "subtitle_url": video_result.subtitle_url,
                "video_duration_sec": video_result.video_duration_sec,
                "status": video_result.status,
                "error": video_result.error,
            },
        )


def _hash_publish_payload(payload: dict[str, Any]) -> str:
    serialized = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _normalize_link_for_lookup(link: str) -> str:
    normalized = str(link or "").strip().lower()
    if not normalized:
        return ""
    if normalized.endswith("/") and len(normalized) > 1:
        normalized = normalized[:-1]
    return normalized


def _publish_job_from_row(row: dict[str, Any]) -> PublishJob:
    return PublishJob(
        id=str(row["id"]),
        post_id=str(row["post_id"]),
        persona_key=str(row["persona_key"]),
        platform=str(row["platform"]),
        status=str(row["status"]),
        request_payload=dict(row["request_payload"]),
        retry_count=int(row["retry_count"]),
        max_retries=int(row["max_retries"]),
    )


def upsert_publish_job(
    conn: psycopg.Connection,
    *,
    post_id: str,
    persona_key: str,
    platform: str,
    payload: dict[str, Any],
    max_retries: int,
    compliance_checks: list[dict[str, Any]],
) -> PublishJob:
    request_hash = _hash_publish_payload(payload)
    idempotency_key = f"{persona_key}:{post_id}:{platform}"

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into publish_jobs
              (post_id, persona_key, platform, status, idempotency_key, request_hash, request_payload, max_retries, compliance_checks)
            values
              (%(post_id)s::uuid, %(persona_key)s, %(platform)s, 'queued', %(idempotency_key)s, %(request_hash)s, %(request_payload)s::jsonb, %(max_retries)s, %(compliance_checks)s::jsonb)
            on conflict (post_id, platform)
            do update set
              persona_key = excluded.persona_key,
              request_hash = excluded.request_hash,
              request_payload = excluded.request_payload,
              max_retries = excluded.max_retries,
              compliance_checks = excluded.compliance_checks,
              status = case
                when publish_jobs.status = 'published' then publish_jobs.status
                else 'queued'
              end,
              retry_count = case
                when publish_jobs.status = 'published' then publish_jobs.retry_count
                else 0
              end,
              next_retry_at = null,
              last_error = null,
              error_category = null
            returning id, post_id, persona_key, platform, status, request_payload, retry_count, max_retries
            """,
            {
                "post_id": post_id,
                "persona_key": persona_key,
                "platform": platform,
                "idempotency_key": idempotency_key,
                "request_hash": request_hash,
                "request_payload": json.dumps(payload, default=str),
                "max_retries": max_retries,
                "compliance_checks": json.dumps(compliance_checks, default=str),
            },
        )
        row = cur.fetchone()
        assert row is not None
        return PublishJob(
            id=str(row["id"]),
            post_id=str(row["post_id"]),
            persona_key=str(row["persona_key"]),
            platform=str(row["platform"]),
            status=str(row["status"]),
            request_payload=dict(row["request_payload"]),
            retry_count=int(row["retry_count"]),
            max_retries=int(row["max_retries"]),
        )


def claim_publish_jobs_ready(
    conn: psycopg.Connection,
    *,
    persona_key: str,
    platforms: list[str],
    max_jobs: int,
    require_review_approval: bool,
    stale_in_progress_minutes: int,
) -> list[PublishJob]:
    if not platforms:
        return []

    normalized_platforms = [
        platform.strip().lower()
        for platform in platforms
        if platform and platform.strip()
    ]
    if not normalized_platforms:
        return []

    safe_max_jobs = max(1, int(max_jobs))
    stale_minutes = max(1, int(stale_in_progress_minutes))

    with conn.cursor() as cur:
        cur.execute(
            """
            with candidates as (
              select id
              from publish_jobs
              where platform = any(%(platforms)s::text[])
                and coalesce(persona_key, 'default') = %(persona_key)s
                and retry_count < max_retries
                and (
                  (
                    status in ('queued', 'failed')
                    and (next_retry_at is null or next_retry_at <= clock_timestamp())
                  )
                  or (
                    status = 'in_progress'
                    and updated_at <= (
                      clock_timestamp() - make_interval(mins => %(stale_minutes)s)
                    )
                  )
                )
                and (
                  not %(require_review_approval)s
                  or platform != 'metricool'
                  or coalesce(nullif(request_payload->>'approval_status', ''), 'pending') = 'approved'
                )
              order by created_at asc
              for update skip locked
              limit %(max_jobs)s
            ),
            claimed as (
              update publish_jobs pj
              set
                status = 'in_progress',
                next_retry_at = null,
                last_error = case
                  when pj.status = 'in_progress' then coalesce(pj.last_error, 'stale_in_progress_reclaimed')
                  else pj.last_error
                end,
                error_category = case
                  when pj.status = 'in_progress' then coalesce(pj.error_category, 'stale_in_progress_reclaimed')
                  else pj.error_category
                end,
                updated_at = clock_timestamp()
              from candidates c
              where pj.id = c.id
              returning
                pj.id,
                pj.post_id,
                coalesce(pj.persona_key, 'default') as persona_key,
                pj.platform,
                pj.status,
                pj.request_payload,
                pj.retry_count,
                pj.max_retries
            )
            select cl.*
            from claimed cl
            join publish_jobs pj on pj.id = cl.id
            order by pj.created_at asc
            """,
            {
                "persona_key": persona_key,
                "platforms": normalized_platforms,
                "max_jobs": safe_max_jobs,
                "stale_minutes": stale_minutes,
                "require_review_approval": bool(require_review_approval),
            },
        )
        rows = cur.fetchall()

    return [_publish_job_from_row(row) for row in rows]


def list_recent_published_jobs_for_platform(
    conn: psycopg.Connection,
    *,
    platform: str,
    persona_key: str,
    lookback_days: int,
    limit: int = 100,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              pj.id,
              pj.post_id,
              coalesce(pj.persona_key, 'default') as persona_key,
              pj.platform,
              pj.external_post_id,
              pj.published_at
            from publish_jobs pj
            where pj.platform = %(platform)s
              and coalesce(pj.persona_key, 'default') = %(persona_key)s
              and pj.status = 'published'
              and pj.external_post_id is not null
              and pj.published_at >= now() - make_interval(days => %(lookback_days)s)
            order by pj.published_at desc
            limit %(limit)s
            """,
            {
                "platform": platform,
                "persona_key": persona_key,
                "lookback_days": max(1, int(lookback_days)),
                "limit": max(1, int(limit)),
            },
        )
        return [dict(row) for row in cur.fetchall()]


def get_source_post_by_id(conn: psycopg.Connection, *, post_id: str) -> SourcePostInput | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select source, source_guid, title, description, link, published_at, raw_payload
            from source_posts
            where id = %(post_id)s::uuid
            """,
            {"post_id": post_id},
        )
        row = cur.fetchone()
    if row is None:
        return None
    raw_payload = row.get("raw_payload")
    safe_payload = raw_payload if isinstance(raw_payload, dict) else {}
    return SourcePostInput(
        source=str(row.get("source") or "world_journal"),
        source_guid=str(row.get("source_guid") or post_id),
        title=str(row.get("title") or "").strip(),
        description=str(row.get("description") or "").strip(),
        link=str(row.get("link") or "").strip(),
        published_at=row.get("published_at"),
        raw_payload=safe_payload,
    )


def get_publish_job_record(
    conn: psycopg.Connection,
    *,
    job_id: str,
    persona_key: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              id,
              post_id,
              coalesce(persona_key, 'default') as persona_key,
              platform,
              status,
              request_payload,
              retry_count,
              max_retries,
              created_at,
              updated_at
            from publish_jobs
            where id = %(job_id)s::uuid
              and coalesce(persona_key, 'default') = %(persona_key)s
            """,
            {"job_id": job_id, "persona_key": persona_key},
        )
        row = cur.fetchone()
    return dict(row) if row is not None else None


def list_metricool_review_jobs(
    conn: psycopg.Connection,
    *,
    persona_key: str,
    limit: int = 25,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              pj.id,
              pj.post_id,
              pj.status,
              pj.created_at,
              pj.request_payload,
              coalesce(nullif(pj.request_payload->>'approval_status', ''), 'pending') as approval_status,
              sp.title,
              sp.link
            from publish_jobs pj
            join source_posts sp on sp.id = pj.post_id
            where pj.platform = 'metricool'
              and coalesce(pj.persona_key, 'default') = %(persona_key)s
              and pj.status in ('queued', 'failed', 'in_progress')
            order by
              case coalesce(nullif(pj.request_payload->>'approval_status', ''), 'pending')
                when 'approved' then 0
                when 'pending' then 1
                when 'regenerating' then 2
                when 'rejected' then 3
                else 4
              end,
              pj.created_at asc
            limit %(limit)s
            """,
            {
                "persona_key": persona_key,
                "limit": max(1, int(limit)),
            },
        )
        return [dict(row) for row in cur.fetchall()]


def list_metricool_jobs_needing_review_post(
    conn: psycopg.Connection,
    *,
    persona_key: str,
    limit: int = 10,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              pj.id,
              pj.post_id,
              pj.request_payload,
              sp.title,
              sp.link
            from publish_jobs pj
            join source_posts sp on sp.id = pj.post_id
            where pj.platform = 'metricool'
              and coalesce(pj.persona_key, 'default') = %(persona_key)s
              and pj.status in ('queued', 'failed')
              and coalesce(nullif(pj.request_payload->>'approval_status', ''), 'pending') = 'pending'
              and coalesce(nullif(pj.request_payload->>'review_message_id', ''), '') = ''
            order by pj.created_at asc
            limit %(limit)s
            """,
            {
                "persona_key": persona_key,
                "limit": max(1, int(limit)),
            },
        )
        return [dict(row) for row in cur.fetchall()]


def _acquire_publish_job_review_lock(conn: psycopg.Connection, *, job_id: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "select pg_try_advisory_xact_lock(hashtext(%(lock_key)s)) as acquired",
            {"lock_key": f"publish_review:{job_id}"},
        )
        row = cur.fetchone()
    return bool(row and row.get("acquired"))


def update_publish_job_request_payload(
    conn: psycopg.Connection,
    *,
    job_id: str,
    persona_key: str,
    payload_patch: dict[str, Any],
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            update publish_jobs
            set request_payload = coalesce(request_payload, '{}'::jsonb) || %(payload_patch)s::jsonb
            where id = %(job_id)s::uuid
              and coalesce(persona_key, 'default') = %(persona_key)s
            returning id
            """,
            {
                "job_id": job_id,
                "persona_key": persona_key,
                "payload_patch": json.dumps(payload_patch, default=str),
            },
        )
        row = cur.fetchone()
    return row is not None


def set_metricool_job_review_message_refs(
    conn: psycopg.Connection,
    *,
    job_id: str,
    persona_key: str,
    review_channel_id: str,
    review_message_id: str,
    review_thread_id: str,
) -> bool:
    return update_publish_job_request_payload(
        conn,
        job_id=job_id,
        persona_key=persona_key,
        payload_patch={
            "review_channel_id": review_channel_id,
            "review_message_id": review_message_id,
            "review_thread_id": review_thread_id,
        },
    )


def set_metricool_job_review_approval(
    conn: psycopg.Connection,
    *,
    job_id: str,
    persona_key: str,
    actor: str,
    approved: bool,
) -> bool:
    if not _acquire_publish_job_review_lock(conn, job_id=job_id):
        return False
    with conn.cursor() as cur:
        cur.execute(
            """
            select status
            from publish_jobs
            where id = %(job_id)s::uuid
              and coalesce(persona_key, 'default') = %(persona_key)s
              and platform = 'metricool'
            for update
            """,
            {"job_id": job_id, "persona_key": persona_key},
        )
        row = cur.fetchone()
        if row is None:
            return False
        if str(row.get("status") or "") in {"published", "skipped", "dead_letter"}:
            return False
    return update_publish_job_request_payload(
        conn,
        job_id=job_id,
        persona_key=persona_key,
        payload_patch=review_patch_for_approval(actor=actor, approved=approved),
    )


def set_metricool_job_review_regenerating(
    conn: psycopg.Connection,
    *,
    job_id: str,
    persona_key: str,
    actor: str,
    edit_notes: str,
) -> bool:
    if not _acquire_publish_job_review_lock(conn, job_id=job_id):
        return False
    with conn.cursor() as cur:
        cur.execute(
            """
            select status
            from publish_jobs
            where id = %(job_id)s::uuid
              and coalesce(persona_key, 'default') = %(persona_key)s
              and platform = 'metricool'
            for update
            """,
            {"job_id": job_id, "persona_key": persona_key},
        )
        row = cur.fetchone()
        if row is None:
            return False
        if str(row.get("status") or "") in {"published", "skipped", "dead_letter"}:
            return False
    return update_publish_job_request_payload(
        conn,
        job_id=job_id,
        persona_key=persona_key,
        payload_patch=review_patch_for_regeneration_start(actor=actor, edit_notes=edit_notes),
    )


def list_recent_published_article_links(
    conn: psycopg.Connection,
    *,
    platform: str,
    persona_key: str,
    limit: int = 4,
) -> list[dict[str, str]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              pj.request_payload->>'article_url' as article_url,
              pj.request_payload->>'video_title_short' as video_title
            from publish_jobs pj
            where pj.platform = %(platform)s
              and coalesce(pj.persona_key, 'default') = %(persona_key)s
              and pj.status = 'published'
              and pj.external_post_id is not null
              and pj.request_payload->>'article_url' is not null
              and pj.request_payload->>'article_url' != ''
            order by pj.published_at desc
            limit %(limit)s
            """,
            {
                "platform": platform,
                "persona_key": persona_key,
                "limit": max(1, int(limit)),
            },
        )
        results: list[dict[str, str]] = []
        for row in cur.fetchall():
            url = str(row.get("article_url", "") or "").strip()
            title = str(row.get("video_title", "") or "").strip() or "News Update"
            if url:
                results.append({"url": url, "text": title})
        return results


def has_published_post_for_platforms(
    conn: psycopg.Connection,
    *,
    post_id: str,
    persona_key: str,
    platforms: list[str],
) -> bool:
    normalized_platforms = [
        platform.strip().lower()
        for platform in platforms
        if platform and platform.strip()
    ]
    if not normalized_platforms:
        return False

    with conn.cursor() as cur:
        cur.execute(
            """
            select exists(
              select 1
              from publish_jobs pj
              where pj.post_id = %(post_id)s::uuid
                and coalesce(pj.persona_key, 'default') = %(persona_key)s
                and pj.platform = any(%(platforms)s::text[])
                and pj.status = 'published'
                and pj.external_post_id is not null
                and btrim(pj.external_post_id) != ''
            ) as has_match
            """,
            {
                "post_id": post_id,
                "persona_key": persona_key,
                "platforms": normalized_platforms,
            },
        )
        row = cur.fetchone()
    return bool(row and row.get("has_match"))


def has_published_link_for_platforms(
    conn: psycopg.Connection,
    *,
    link: str,
    persona_key: str,
    platforms: list[str],
    exclude_post_id: str | None = None,
) -> bool:
    normalized_link = _normalize_link_for_lookup(link)
    if not normalized_link:
        return False

    normalized_platforms = [
        platform.strip().lower()
        for platform in platforms
        if platform and platform.strip()
    ]
    if not normalized_platforms:
        return False

    with conn.cursor() as cur:
        cur.execute(
            """
            select exists(
              select 1
              from source_posts sp
              join publish_jobs pj on pj.post_id = sp.id
              where coalesce(pj.persona_key, 'default') = %(persona_key)s
                and pj.platform = any(%(platforms)s::text[])
                and pj.status = 'published'
                and pj.external_post_id is not null
                and btrim(pj.external_post_id) != ''
                and sp.link is not null
                and btrim(sp.link) != ''
                and lower(regexp_replace(btrim(sp.link), '/+$', '')) = %(normalized_link)s
                and (
                  %(exclude_post_id)s::uuid is null
                  or sp.id <> %(exclude_post_id)s::uuid
                )
            ) as has_match
            """,
            {
                "persona_key": persona_key,
                "platforms": normalized_platforms,
                "normalized_link": normalized_link,
                "exclude_post_id": exclude_post_id,
            },
        )
        row = cur.fetchone()
    return bool(row and row.get("has_match"))


def list_published_links_for_platforms(
    conn: psycopg.Connection,
    *,
    persona_key: str,
    platforms: list[str],
    source_filter: str | None = None,
) -> list[dict[str, str]]:
    normalized_platforms = [
        platform.strip().lower()
        for platform in platforms
        if platform and platform.strip()
    ]
    if not normalized_platforms:
        return []

    with conn.cursor() as cur:
        cur.execute(
            """
            select
              sp.link,
              sp.source,
              pj.platform,
              pj.external_post_id
            from source_posts sp
            join publish_jobs pj on pj.post_id = sp.id
            where coalesce(pj.persona_key, 'default') = %(persona_key)s
              and pj.status = 'published'
              and pj.platform = any(%(platforms)s::text[])
              and pj.external_post_id is not null
              and btrim(pj.external_post_id) != ''
              and sp.link is not null
              and btrim(sp.link) != ''
              and (
                %(source_filter)s::text is null
                or sp.source = %(source_filter)s::text
              )
            """,
            {
                "persona_key": persona_key,
                "platforms": normalized_platforms,
                "source_filter": source_filter,
            },
        )
        return [
            {
                "link": str(row.get("link") or "").strip(),
                "source": str(row.get("source") or "").strip(),
                "platform": str(row.get("platform") or "").strip().lower(),
                "external_post_id": str(row.get("external_post_id") or "").strip(),
            }
            for row in cur.fetchall()
        ]


def list_wj_published_links_for_platforms(
    conn: psycopg.Connection,
    *,
    persona_key: str,
    platforms: list[str],
) -> list[dict[str, str]]:
    return list_published_links_for_platforms(
        conn,
        persona_key=persona_key,
        platforms=platforms,
        source_filter="world_journal",
    )


def list_wj_posts_with_youtube_status(
    conn: psycopg.Connection,
    *,
    persona_key: str = "default",
) -> list[dict[str, Any]]:
    """List world_journal posts with their YouTube publish job external_post_id (if any)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              sp.id,
              sp.link,
              sp.source_guid,
              sp.title,
              pj.external_post_id as youtube_external_post_id
            from source_posts sp
            left join publish_jobs pj
              on pj.post_id = sp.id
              and pj.platform = 'youtube'
              and pj.status = 'published'
              and coalesce(pj.persona_key, 'default') = %(persona_key)s
            where sp.source = 'world_journal'
            """,
            {"persona_key": persona_key},
        )
        return [dict(row) for row in cur.fetchall()]


def delete_source_post_by_id(conn: psycopg.Connection, post_id: str) -> None:
    """Delete a source_post by id (cascades to video_assets, publish_jobs, etc)."""
    with conn.cursor() as cur:
        cur.execute("delete from source_posts where id = %(id)s::uuid", {"id": post_id})


def upsert_persona_profile(
    conn: psycopg.Connection,
    *,
    persona_key: str,
    metricool_user_id: str,
    metricool_blog_id: str,
    metricool_target_platforms: list[str],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into persona_profiles
              (persona_key, display_name, metricool_user_id, metricool_blog_id, metricool_target_platforms, is_active)
            values
              (%(persona_key)s, %(display_name)s, %(metricool_user_id)s, %(metricool_blog_id)s, %(metricool_target_platforms)s::text[], true)
            on conflict (persona_key)
            do update set
              metricool_user_id = excluded.metricool_user_id,
              metricool_blog_id = excluded.metricool_blog_id,
              metricool_target_platforms = excluded.metricool_target_platforms,
              is_active = true
            """,
            {
                "persona_key": persona_key,
                "display_name": persona_key.replace("-", " ").title(),
                "metricool_user_id": metricool_user_id or None,
                "metricool_blog_id": metricool_blog_id or None,
                "metricool_target_platforms": metricool_target_platforms,
            },
        )


def create_optimization_recommendation(
    conn: psycopg.Connection,
    *,
    persona_key: str,
    diagnosis: str,
    confidence: float,
    sample_size: int,
    window_start: datetime | None,
    window_end: datetime | None,
    recommended_overrides: dict[str, Any],
    rationale: str,
    status: str = "proposed",
) -> str:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into optimization_recommendations
              (persona_key, diagnosis, confidence, sample_size, window_start, window_end, recommended_overrides, rationale, status)
            values
              (%(persona_key)s, %(diagnosis)s, %(confidence)s, %(sample_size)s, %(window_start)s, %(window_end)s, %(recommended_overrides)s::jsonb, %(rationale)s, %(status)s)
            returning id
            """,
            {
                "persona_key": persona_key,
                "diagnosis": diagnosis,
                "confidence": max(0.0, min(1.0, confidence)),
                "sample_size": max(0, int(sample_size)),
                "window_start": window_start,
                "window_end": window_end,
                "recommended_overrides": json.dumps(recommended_overrides, default=str),
                "rationale": rationale,
                "status": status,
            },
        )
        row = cur.fetchone()
        assert row is not None
        return str(row["id"])


def upsert_video_performance_metric(
    conn: psycopg.Connection,
    *,
    persona_key: str,
    publish_job_id: str | None,
    platform: str,
    external_post_id: str,
    metric_timestamp: datetime,
    normalized_metrics: dict[str, Any],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into video_performance_metrics
              (persona_key, publish_job_id, platform, external_post_id, metric_timestamp, views, likes, comments, shares, saves, watch_time_seconds, avg_watch_seconds, avg_retention_ratio, completion_rate, engagement_rate, metrics)
            values
              (%(persona_key)s, %(publish_job_id)s::uuid, %(platform)s, %(external_post_id)s, %(metric_timestamp)s, %(views)s, %(likes)s, %(comments)s, %(shares)s, %(saves)s, %(watch_time_seconds)s, %(avg_watch_seconds)s, %(avg_retention_ratio)s, %(completion_rate)s, %(engagement_rate)s, %(metrics)s::jsonb)
            on conflict (persona_key, platform, external_post_id, metric_timestamp)
            do update set
              views = excluded.views,
              likes = excluded.likes,
              comments = excluded.comments,
              shares = excluded.shares,
              saves = excluded.saves,
              watch_time_seconds = excluded.watch_time_seconds,
              avg_watch_seconds = excluded.avg_watch_seconds,
              avg_retention_ratio = excluded.avg_retention_ratio,
              completion_rate = excluded.completion_rate,
              engagement_rate = excluded.engagement_rate,
              metrics = excluded.metrics
            """,
            {
                "persona_key": persona_key,
                "publish_job_id": publish_job_id,
                "platform": platform,
                "external_post_id": external_post_id,
                "metric_timestamp": metric_timestamp,
                "views": normalized_metrics.get("views"),
                "likes": normalized_metrics.get("likes"),
                "comments": normalized_metrics.get("comments"),
                "shares": normalized_metrics.get("shares"),
                "saves": normalized_metrics.get("saves"),
                "watch_time_seconds": normalized_metrics.get("watch_time_seconds"),
                "avg_watch_seconds": normalized_metrics.get("avg_watch_seconds"),
                "avg_retention_ratio": normalized_metrics.get("avg_retention_ratio"),
                "completion_rate": normalized_metrics.get("completion_rate"),
                "engagement_rate": normalized_metrics.get("engagement_rate"),
                "metrics": json.dumps(normalized_metrics, default=str),
            },
        )


def upsert_post_quality_evaluation(
    conn: psycopg.Connection,
    *,
    post_id: str,
    run_id: str | None,
    persona_key: str,
    scores: dict[str, Any],
    passed: bool,
    failing_dimensions: list[str],
    metadata: dict[str, Any] | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into post_quality_evaluations
              (
                post_id,
                run_id,
                persona_key,
                script_specificity_score,
                narrative_flow_score,
                visual_relevance_score,
                visual_variety_score,
                first_two_seconds_hook_score,
                composite_score,
                passed,
                failing_dimensions,
                metadata
              )
            values
              (
                %(post_id)s::uuid,
                %(run_id)s::uuid,
                %(persona_key)s,
                %(script_specificity_score)s,
                %(narrative_flow_score)s,
                %(visual_relevance_score)s,
                %(visual_variety_score)s,
                %(first_two_seconds_hook_score)s,
                %(composite_score)s,
                %(passed)s,
                %(failing_dimensions)s::jsonb,
                %(metadata)s::jsonb
              )
            on conflict (post_id, run_id) where run_id is not null
            do update set
              script_specificity_score = excluded.script_specificity_score,
              narrative_flow_score = excluded.narrative_flow_score,
              visual_relevance_score = excluded.visual_relevance_score,
              visual_variety_score = excluded.visual_variety_score,
              first_two_seconds_hook_score = excluded.first_two_seconds_hook_score,
              composite_score = excluded.composite_score,
              passed = excluded.passed,
              failing_dimensions = excluded.failing_dimensions,
              metadata = excluded.metadata,
              updated_at = now()
            """,
            {
                "post_id": post_id,
                "run_id": run_id,
                "persona_key": persona_key,
                "script_specificity_score": scores.get("script_specificity_score", 0.0),
                "narrative_flow_score": scores.get("narrative_flow_score", 0.0),
                "visual_relevance_score": scores.get("visual_relevance_score", 0.0),
                "visual_variety_score": scores.get("visual_variety_score", 0.0),
                "first_two_seconds_hook_score": scores.get("first_two_seconds_hook_score", 0.0),
                "composite_score": scores.get("composite_score", 0.0),
                "passed": bool(passed),
                "failing_dimensions": json.dumps(failing_dimensions, default=str),
                "metadata": json.dumps(metadata or {}, default=str),
            },
        )


def get_quality_baseline_summary(
    conn: psycopg.Connection,
    *,
    persona_key: str,
    lookback_days: int,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            with quality as (
              select
                count(*) as evaluations,
                count(*) filter (where passed) as passed_evaluations,
                avg(composite_score) as avg_composite,
                avg(script_specificity_score) as avg_script_specificity,
                avg(narrative_flow_score) as avg_narrative_flow,
                avg(visual_relevance_score) as avg_visual_relevance,
                avg(visual_variety_score) as avg_visual_variety,
                avg(first_two_seconds_hook_score) as avg_first_two_seconds_hook
              from post_quality_evaluations
              where persona_key = %(persona_key)s
                and evaluated_at >= now() - make_interval(days => %(lookback_days)s)
            ),
            publishing as (
              select
                count(*) as jobs_total,
                count(*) filter (where status = 'published') as jobs_published,
                count(*) filter (where status = 'failed') as jobs_failed,
                count(*) filter (where status = 'skipped') as jobs_skipped
              from publish_jobs
              where coalesce(persona_key, 'default') = %(persona_key)s
                and created_at >= now() - make_interval(days => %(lookback_days)s)
            ),
            performance as (
              select
                avg(completion_rate) as avg_completion_rate,
                avg(engagement_rate) as avg_engagement_rate,
                avg(avg_watch_seconds) as avg_watch_seconds,
                count(*) as metric_rows
              from video_performance_metrics
              where persona_key = %(persona_key)s
                and metric_timestamp >= now() - make_interval(days => %(lookback_days)s)
            )
            select
              quality.*,
              publishing.jobs_total,
              publishing.jobs_published,
              publishing.jobs_failed,
              publishing.jobs_skipped,
              performance.avg_completion_rate,
              performance.avg_engagement_rate,
              performance.avg_watch_seconds,
              performance.metric_rows
            from quality, publishing, performance
            """,
            {
                "persona_key": persona_key,
                "lookback_days": max(1, int(lookback_days)),
            },
        )
        row = cur.fetchone()
    return dict(row) if row else {}


def list_recent_quality_performance_rows(
    conn: psycopg.Connection,
    *,
    persona_key: str,
    lookback_days: int,
    limit: int = 200,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            with latest_publish as (
              select
                pj.post_id,
                max(pj.published_at) as published_at
              from publish_jobs pj
              where coalesce(pj.persona_key, 'default') = %(persona_key)s
                and pj.status = 'published'
                and pj.published_at >= now() - make_interval(days => %(lookback_days)s)
              group by pj.post_id
            )
            select
              pqe.post_id,
              pqe.evaluated_at,
              pqe.script_specificity_score,
              pqe.narrative_flow_score,
              pqe.visual_relevance_score,
              pqe.visual_variety_score,
              pqe.first_two_seconds_hook_score,
              pqe.composite_score,
              pqe.passed,
              vpm.metric_timestamp,
              vpm.completion_rate,
              vpm.engagement_rate,
              vpm.avg_watch_seconds,
              vpm.views,
              vpm.likes,
              vpm.shares,
              vpm.saves
            from post_quality_evaluations pqe
            left join latest_publish lp on lp.post_id = pqe.post_id
            left join publish_jobs pj on pj.post_id = pqe.post_id and pj.published_at = lp.published_at
            left join video_performance_metrics vpm on vpm.publish_job_id = pj.id
            where pqe.persona_key = %(persona_key)s
              and pqe.evaluated_at >= now() - make_interval(days => %(lookback_days)s)
            order by pqe.evaluated_at desc
            limit %(limit)s
            """,
            {
                "persona_key": persona_key,
                "lookback_days": max(1, int(lookback_days)),
                "limit": max(1, int(limit)),
            },
        )
        return [dict(row) for row in cur.fetchall()]


def replace_active_runtime_overrides(
    conn: psycopg.Connection,
    *,
    persona_key: str,
    overrides: dict[str, Any],
    source_recommendation_id: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update runtime_overrides
            set is_active = false
            where persona_key = %(persona_key)s
              and is_active = true
            """,
            {"persona_key": persona_key},
        )
        for key, value in overrides.items():
            if isinstance(value, bool):
                value_type = "bool"
            elif isinstance(value, int):
                value_type = "int"
            elif isinstance(value, float):
                value_type = "float"
            else:
                value_type = "str"
            cur.execute(
                """
                insert into runtime_overrides
                  (persona_key, key, value, value_type, source_recommendation_id, is_active)
                values
                  (%(persona_key)s, %(key)s, %(value)s::jsonb, %(value_type)s, %(source_recommendation_id)s::uuid, true)
                """,
                {
                    "persona_key": persona_key,
                    "key": key,
                    "value": json.dumps(value, default=str),
                    "value_type": value_type,
                    "source_recommendation_id": source_recommendation_id,
                },
            )


def list_active_runtime_overrides(
    conn: psycopg.Connection,
    *,
    persona_key: str,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select key, value
            from runtime_overrides
            where persona_key = %(persona_key)s
              and is_active = true
            """,
            {"persona_key": persona_key},
        )
        rows = cur.fetchall()
    return {str(row["key"]): row["value"] for row in rows}


def list_recent_video_metrics(
    conn: psycopg.Connection,
    *,
    persona_key: str,
    lookback_days: int,
    limit: int = 200,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from video_performance_metrics
            where persona_key = %(persona_key)s
              and metric_timestamp >= now() - make_interval(days => %(lookback_days)s)
            order by metric_timestamp desc
            limit %(limit)s
            """,
            {
                "persona_key": persona_key,
                "lookback_days": max(1, int(lookback_days)),
                "limit": max(1, int(limit)),
            },
        )
        return [dict(row) for row in cur.fetchall()]


def list_signal_performance_rows(
    conn: psycopg.Connection,
    *,
    persona_key: str,
    lookback_days: int,
    limit: int = 300,
) -> list[dict[str, Any]]:
    """Join video_performance_metrics with content_assets.content_signals for signal-level analysis."""
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              vpm.platform,
              vpm.views,
              vpm.likes,
              vpm.comments,
              vpm.shares,
              vpm.saves,
              vpm.completion_rate,
              vpm.engagement_rate,
              ca.content_signals
            from video_performance_metrics vpm
            join publish_jobs pj on pj.id = vpm.publish_job_id
            join content_assets ca on ca.post_id = pj.post_id
            where vpm.persona_key = %(persona_key)s
              and vpm.metric_timestamp >= now() - make_interval(days => %(lookback_days)s)
              and vpm.views > 0
              and ca.content_signals is not null
              and ca.content_signals != '{}'::jsonb
            order by vpm.metric_timestamp desc
            limit %(limit)s
            """,
            {
                "persona_key": persona_key,
                "lookback_days": max(1, int(lookback_days)),
                "limit": max(1, int(limit)),
            },
        )
        return [dict(row) for row in cur.fetchall()]


def count_publish_jobs_for_platform(
    conn: psycopg.Connection,
    *,
    platform: str,
    persona_key: str | None = None,
) -> int:
    with conn.cursor() as cur:
        if persona_key:
            cur.execute(
                """
                select count(*) as total
                from publish_jobs
                where platform = %(platform)s
                  and coalesce(persona_key, 'default') = %(persona_key)s
                """,
                {"platform": platform, "persona_key": persona_key},
            )
        else:
            cur.execute(
                """
                select count(*) as total
                from publish_jobs
                where platform = %(platform)s
                """,
                {"platform": platform},
            )
        row = cur.fetchone()
    return int(row["total"]) if row is not None else 0


def create_publish_attempt(
    conn: psycopg.Connection,
    *,
    job_id: str,
    request_payload: dict[str, Any],
) -> PublishAttempt:
    with conn.cursor() as cur:
        cur.execute(
            """
            select coalesce(max(attempt_number), 0) as latest_attempt
            from publish_attempts
            where publish_job_id = %(job_id)s::uuid
            """,
            {"job_id": job_id},
        )
        row = cur.fetchone()
        assert row is not None
        attempt_number = int(row["latest_attempt"]) + 1

        cur.execute(
            """
            insert into publish_attempts
              (publish_job_id, attempt_number, status, started_at, request_payload)
            values
              (%(job_id)s::uuid, %(attempt_number)s, 'in_progress', clock_timestamp(), %(request_payload)s::jsonb)
            returning id
            """,
            {
                "job_id": job_id,
                "attempt_number": attempt_number,
                "request_payload": json.dumps(request_payload, default=str),
            },
        )
        created = cur.fetchone()
        assert created is not None

        cur.execute(
            """
            update publish_jobs
            set status = 'in_progress'
            where id = %(job_id)s::uuid
            """,
            {"job_id": job_id},
        )

    return PublishAttempt(
        id=str(created["id"]),
        job_id=job_id,
        attempt_number=attempt_number,
    )


def mark_publish_attempt_published(
    conn: psycopg.Connection,
    *,
    attempt_id: str,
    job_id: str,
    external_post_id: str,
    response_payload: dict[str, Any],
    http_status: int | None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update publish_attempts
            set
              status = 'published',
              finished_at = clock_timestamp(),
              response_payload = %(response_payload)s::jsonb,
              http_status = %(http_status)s
            where id = %(attempt_id)s::uuid
            """,
            {
                "attempt_id": attempt_id,
                "response_payload": json.dumps(response_payload, default=str),
                "http_status": http_status,
            },
        )
        cur.execute(
            """
            update publish_jobs
            set
              status = 'published',
              external_post_id = %(external_post_id)s,
              published_at = clock_timestamp(),
              next_retry_at = null,
              last_error = null,
              error_category = null
            where id = %(job_id)s::uuid
            """,
            {"job_id": job_id, "external_post_id": external_post_id},
        )


def mark_publish_attempt_skipped(
    conn: psycopg.Connection,
    *,
    attempt_id: str,
    job_id: str,
    reason: str,
    error_category: str,
    response_payload: dict[str, Any] | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update publish_attempts
            set
              status = 'skipped',
              finished_at = clock_timestamp(),
              response_payload = %(response_payload)s::jsonb,
              error_category = %(error_category)s,
              error_message = %(reason)s
            where id = %(attempt_id)s::uuid
            """,
            {
                "attempt_id": attempt_id,
                "response_payload": json.dumps(response_payload or {}, default=str),
                "error_category": error_category,
                "reason": reason,
            },
        )
        cur.execute(
            """
            update publish_jobs
            set
              status = 'skipped',
              next_retry_at = null,
              last_error = %(reason)s,
              error_category = %(error_category)s
            where id = %(job_id)s::uuid
            """,
            {"job_id": job_id, "reason": reason, "error_category": error_category},
        )


def mark_publish_attempt_failed(
    conn: psycopg.Connection,
    *,
    attempt_id: str,
    job_id: str,
    error_message: str,
    error_category: str,
    retryable: bool,
    response_payload: dict[str, Any] | None = None,
    http_status: int | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update publish_attempts
            set
              status = 'failed',
              finished_at = clock_timestamp(),
              response_payload = %(response_payload)s::jsonb,
              http_status = %(http_status)s,
              error_category = %(error_category)s,
              error_message = %(error_message)s
            where id = %(attempt_id)s::uuid
            """,
            {
                "attempt_id": attempt_id,
                "response_payload": json.dumps(response_payload or {}, default=str),
                "http_status": http_status,
                "error_category": error_category,
                "error_message": error_message,
            },
        )
        cur.execute(
            """
            select retry_count, max_retries
            from publish_jobs
            where id = %(job_id)s::uuid
            for update
            """,
            {"job_id": job_id},
        )
        row = cur.fetchone()
        assert row is not None
        retry_count = int(row["retry_count"]) + 1
        max_retries = int(row["max_retries"])
        retries_remaining = retry_count < max_retries
        next_retry_at = None
        status = "dead_letter"
        if retryable and retries_remaining:
            delay_seconds = 30 * retry_count
            if error_category == "youtube_upload_limit":
                # YouTube upload-limit errors usually require a cooldown window.
                delay_seconds = 6 * 60 * 60 * retry_count
            next_retry_at = datetime.now(timezone.utc).timestamp() + delay_seconds
            status = "failed"

        cur.execute(
            """
            update publish_jobs
            set
              status = %(status)s,
              retry_count = %(retry_count)s,
              next_retry_at = to_timestamp(%(next_retry_at)s),
              last_error = %(error_message)s,
              error_category = %(error_category)s
            where id = %(job_id)s::uuid
            """,
            {
                "job_id": job_id,
                "status": status,
                "retry_count": retry_count,
                "next_retry_at": next_retry_at,
                "error_message": error_message,
                "error_category": error_category,
            },
        )
