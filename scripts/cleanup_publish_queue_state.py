#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
from typing import Any

from pipeline.config import bootstrap_runtime_env, load_settings
from pipeline.db import db_connection
from pipeline.review_state import REVIEW_STATUS_APPROVED, review_defaults, utc_now_iso


LOGGER = logging.getLogger("cleanup_publish_queue_state")


def _count_legacy_metricool_jobs_missing_approval(*, conn: Any, persona_key: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            select count(*) as total
            from publish_jobs
            where platform = 'metricool'
              and coalesce(persona_key, 'default') = %(persona_key)s
              and status in ('queued', 'failed')
              and coalesce(nullif(request_payload->>'approval_status', ''), '') = ''
            """,
            {"persona_key": persona_key},
        )
        row = cur.fetchone()
    return int(row["total"]) if row is not None else 0


def _patch_legacy_metricool_jobs_missing_approval(*, conn: Any, persona_key: str) -> int:
    payload_patch = review_defaults()
    payload_patch["approval_status"] = REVIEW_STATUS_APPROVED
    payload_patch["approval_by"] = "system:cleanup"
    payload_patch["approval_at"] = utc_now_iso()

    with conn.cursor() as cur:
        cur.execute(
            """
            update publish_jobs
            set
              request_payload = coalesce(request_payload, '{}'::jsonb) || %(payload_patch)s::jsonb,
              updated_at = clock_timestamp()
            where platform = 'metricool'
              and coalesce(persona_key, 'default') = %(persona_key)s
              and status in ('queued', 'failed')
              and coalesce(nullif(request_payload->>'approval_status', ''), '') = ''
            """,
            {
                "persona_key": persona_key,
                "payload_patch": json.dumps(payload_patch, default=str),
            },
        )
        return int(cur.rowcount)


def _list_duplicate_published_attempts(*, conn: Any, persona_key: str, limit: int = 100) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              pj.id as publish_job_id,
              pj.post_id,
              sp.source,
              sp.link,
              count(*) as published_attempts,
              min(pa.started_at) as first_started_at,
              max(pa.started_at) as last_started_at
            from publish_attempts pa
            join publish_jobs pj on pj.id = pa.publish_job_id
            join source_posts sp on sp.id = pj.post_id
            where pa.status = 'published'
              and coalesce(pj.persona_key, 'default') = %(persona_key)s
            group by pj.id, pj.post_id, sp.source, sp.link
            having count(*) > 1
            order by count(*) desc, max(pa.started_at) desc
            limit %(limit)s
            """,
            {
                "persona_key": persona_key,
                "limit": max(1, int(limit)),
            },
        )
        return [dict(row) for row in cur.fetchall()]


def _count_stale_in_progress_jobs(*, conn: Any, persona_key: str, stale_minutes: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            select count(*) as total
            from publish_jobs
            where coalesce(persona_key, 'default') = %(persona_key)s
              and status = 'in_progress'
              and updated_at <= (clock_timestamp() - make_interval(mins => %(stale_minutes)s))
            """,
            {
                "persona_key": persona_key,
                "stale_minutes": max(1, int(stale_minutes)),
            },
        )
        row = cur.fetchone()
    return int(row["total"]) if row is not None else 0


def _normalize_stale_in_progress_jobs(*, conn: Any, persona_key: str, stale_minutes: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            with stale as (
              select id
              from publish_jobs
              where coalesce(persona_key, 'default') = %(persona_key)s
                and status = 'in_progress'
                and updated_at <= (clock_timestamp() - make_interval(mins => %(stale_minutes)s))
              for update skip locked
            )
            update publish_jobs pj
            set
              status = case
                when pj.retry_count < pj.max_retries then 'failed'
                else 'dead_letter'
              end,
              next_retry_at = case
                when pj.retry_count < pj.max_retries then clock_timestamp()
                else null
              end,
              last_error = coalesce(pj.last_error, 'cleanup: stale in_progress normalized'),
              error_category = coalesce(pj.error_category, 'stale_in_progress'),
              updated_at = clock_timestamp()
            from stale
            where pj.id = stale.id
            """,
            {
                "persona_key": persona_key,
                "stale_minutes": max(1, int(stale_minutes)),
            },
        )
        return int(cur.rowcount)


def main() -> int:
    parser = argparse.ArgumentParser(description="Clean legacy publish queue state and report duplicate published attempts")
    parser.add_argument("--persona-key", default=None, help="Persona key to clean (default: PERSONA_KEY env)")
    parser.add_argument(
        "--stale-minutes",
        type=int,
        default=None,
        help="Treat in_progress jobs older than this as stale (default: PUBLISH_CLAIM_STALE_IN_PROGRESS_MINUTES)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply mutations. Without this flag, the script runs in report-only mode.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    bootstrap_runtime_env()
    settings = load_settings()

    persona_key = str(args.persona_key or settings.persona_key or "default").strip() or "default"
    stale_minutes = max(1, int(args.stale_minutes or settings.publish_claim_stale_in_progress_minutes))

    LOGGER.info("Starting queue cleanup persona_key=%s stale_minutes=%s mode=%s", persona_key, stale_minutes, "apply" if args.apply else "report")

    with db_connection(settings.supabase_db_url) as conn:
        legacy_missing_approval = _count_legacy_metricool_jobs_missing_approval(conn=conn, persona_key=persona_key)
        stale_in_progress = _count_stale_in_progress_jobs(conn=conn, persona_key=persona_key, stale_minutes=stale_minutes)
        duplicate_incidents = _list_duplicate_published_attempts(conn=conn, persona_key=persona_key, limit=200)

        LOGGER.info("Legacy queued/failed metricool jobs missing approval_status=%s", legacy_missing_approval)
        LOGGER.info("Stale in_progress jobs older than %s minutes=%s", stale_minutes, stale_in_progress)
        LOGGER.info("Duplicate published-attempt incidents=%s", len(duplicate_incidents))

        if duplicate_incidents:
            for row in duplicate_incidents[:20]:
                LOGGER.warning(
                    "Duplicate published attempts job_id=%s post_id=%s source=%s attempts=%s link=%s first=%s last=%s",
                    row.get("publish_job_id"),
                    row.get("post_id"),
                    row.get("source"),
                    row.get("published_attempts"),
                    row.get("link"),
                    row.get("first_started_at"),
                    row.get("last_started_at"),
                )

        patched_legacy = 0
        normalized_stale = 0

        if args.apply:
            if not settings.metricool_review_required:
                patched_legacy = _patch_legacy_metricool_jobs_missing_approval(conn=conn, persona_key=persona_key)
            else:
                LOGGER.info("Skipping legacy approval patch because METRICOOL_REVIEW_REQUIRED=true")

            normalized_stale = _normalize_stale_in_progress_jobs(
                conn=conn,
                persona_key=persona_key,
                stale_minutes=stale_minutes,
            )

        LOGGER.info(
            "Cleanup summary mode=%s persona_key=%s patched_legacy=%s normalized_stale=%s duplicate_incidents=%s",
            "apply" if args.apply else "report",
            persona_key,
            patched_legacy,
            normalized_stale,
            len(duplicate_incidents),
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
