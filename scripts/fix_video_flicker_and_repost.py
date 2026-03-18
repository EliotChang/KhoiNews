#!/usr/bin/env python3
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Any, Iterator
from urllib.parse import unquote, urlparse

import requests

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.config import bootstrap_runtime_env, load_settings
from pipeline.db import db_connection, delete_source_post_by_id
from pipeline.main import run_pipeline
from pipeline.video_gen import _upload_bytes_to_supabase_storage


LOGGER = logging.getLogger("fix_video_flicker_and_repost")
TARGET_PIXEL_FORMAT = "yuv420p"
TARGET_COLOR_RANGE = "tv"


def _probe_video_stream(*, media_path: str) -> dict[str, str] | None:
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=codec_name,pix_fmt,color_range,avg_frame_rate,r_frame_rate,width,height",
                "-of",
                "json",
                media_path,
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (subprocess.SubprocessError, FileNotFoundError):
        return None
    try:
        payload = json.loads(result.stdout)
        stream = payload.get("streams", [{}])[0]
        return {
            "codec_name": str(stream.get("codec_name", "")),
            "pix_fmt": str(stream.get("pix_fmt", "")),
            "color_range": str(stream.get("color_range", "")),
            "avg_frame_rate": str(stream.get("avg_frame_rate", "")),
            "r_frame_rate": str(stream.get("r_frame_rate", "")),
            "width": str(stream.get("width", "")),
            "height": str(stream.get("height", "")),
        }
    except (json.JSONDecodeError, IndexError, TypeError, ValueError):
        return None


def _is_compliant_stream(stream_info: dict[str, str] | None) -> tuple[bool, str]:
    if not stream_info:
        return False, "stream_missing"
    pix_fmt = str(stream_info.get("pix_fmt") or "").strip().lower()
    color_range = str(stream_info.get("color_range") or "").strip().lower()
    reasons: list[str] = []
    if pix_fmt != TARGET_PIXEL_FORMAT:
        reasons.append(f"pix_fmt={pix_fmt or 'missing'}")
    if color_range != TARGET_COLOR_RANGE:
        reasons.append(f"color_range={color_range or 'missing'}")
    return len(reasons) == 0, ",".join(reasons) if reasons else "ok"


def _extract_public_object_path(*, public_url: str, bucket_name: str) -> str:
    parsed = urlparse(public_url)
    marker = f"/storage/v1/object/public/{bucket_name}/"
    idx = parsed.path.find(marker)
    if idx < 0:
        raise ValueError(f"Could not parse storage object path from URL: {public_url}")
    raw_path = parsed.path[idx + len(marker) :]
    normalized = unquote(raw_path).strip("/")
    if not normalized:
        raise ValueError(f"Parsed empty storage object path from URL: {public_url}")
    return normalized


def _transcode_to_limited_range(*, input_path: Path, output_path: Path, crf: int) -> None:
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-i",
            str(input_path),
            "-c:v",
            "libx264",
            "-crf",
            str(crf),
            "-vf",
            f"format={TARGET_PIXEL_FORMAT}",
            "-pix_fmt",
            TARGET_PIXEL_FORMAT,
            "-color_range",
            TARGET_COLOR_RANGE,
            "-movflags",
            "+faststart",
            "-c:a",
            "copy",
            str(output_path),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=180,
    )


@contextmanager
def _temporary_env(overrides: dict[str, str]) -> Iterator[None]:
    original: dict[str, str | None] = {key: os.environ.get(key) for key in overrides}
    try:
        for key, value in overrides.items():
            os.environ[key] = value
        yield
    finally:
        for key, prior in original.items():
            if prior is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = prior


def _fetch_post_state(conn: Any, *, post_id: str) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              sp.id as post_id,
              sp.link,
              sp.title,
              sp.source_guid,
              sp.source,
              sp.description,
              sp.published_at,
              sp.raw_payload,
              sp.created_at,
              v.status as video_status,
              v.video_url,
              v.updated_at as video_updated_at
            from source_posts sp
            left join video_assets v on v.post_id = sp.id
            where sp.id = %(post_id)s::uuid
            """,
            {"post_id": post_id},
        )
        row = cur.fetchone()
    return dict(row) if row is not None else None


def _find_new_post_for_link(
    conn: Any,
    *,
    link: str,
    excluded_post_id: str,
    min_created_at: datetime,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select id as post_id, link, title, created_at
            from source_posts
            where link = %(link)s
              and id <> %(excluded_post_id)s::uuid
              and created_at >= %(min_created_at)s
            order by created_at desc
            limit 1
            """,
            {
                "link": link,
                "excluded_post_id": excluded_post_id,
                "min_created_at": min_created_at,
            },
        )
        row = cur.fetchone()
    return dict(row) if row is not None else None


def _fetch_latest_metricool_publish(conn: Any, *, post_id: str) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              id,
              status,
              external_post_id,
              updated_at,
              published_at,
              persona_key,
              request_payload->>'media_url' as media_url,
              request_payload->>'selection_reason' as selection_reason,
              request_payload
            from publish_jobs
            where post_id = %(post_id)s::uuid
              and platform = 'metricool'
            order by updated_at desc
            limit 1
            """,
            {"post_id": post_id},
        )
        row = cur.fetchone()
    return dict(row) if row is not None else None


def _restore_original_post_state(
    conn: Any,
    *,
    post_state: dict[str, Any],
    metricool_job: dict[str, Any] | None,
) -> None:
    post_id = str(post_state.get("post_id"))
    link = str(post_state.get("link") or "").strip()
    source_guid = str(post_state.get("source_guid") or "").strip() or link or post_id
    source = str(post_state.get("source") or "world_journal").strip() or "world_journal"
    title = str(post_state.get("title") or "").strip()
    description = str(post_state.get("description") or "").strip()
    video_url = str(post_state.get("video_url") or "").strip() or None
    video_status = str(post_state.get("video_status") or "generated").strip() or "generated"
    published_at = post_state.get("published_at")
    raw_payload = post_state.get("raw_payload")
    if not isinstance(raw_payload, dict):
        raw_payload = {}

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into source_posts
              (id, source, source_guid, title, description, link, published_at, raw_payload)
            values
              (%(id)s::uuid, %(source)s, %(source_guid)s, %(title)s, %(description)s, %(link)s, %(published_at)s, %(raw_payload)s::jsonb)
            on conflict (id)
            do update set
              source = excluded.source,
              source_guid = excluded.source_guid,
              title = excluded.title,
              description = excluded.description,
              link = excluded.link,
              published_at = excluded.published_at,
              raw_payload = excluded.raw_payload
            """,
            {
                "id": post_id,
                "source": source,
                "source_guid": source_guid,
                "title": title,
                "description": description,
                "link": link,
                "published_at": published_at,
                "raw_payload": json.dumps(raw_payload, default=str),
            },
        )
        if video_url:
            cur.execute(
                """
                insert into video_assets (post_id, template_name, video_url, status, error)
                values (%(post_id)s::uuid, 'fish_lipsync', %(video_url)s, %(status)s, null)
                on conflict (post_id)
                do update set
                  template_name = excluded.template_name,
                  video_url = excluded.video_url,
                  status = excluded.status,
                  error = null
                """,
                {"post_id": post_id, "video_url": video_url, "status": video_status},
            )

        if metricool_job and str(metricool_job.get("status") or "").strip() == "published":
            payload = metricool_job.get("request_payload")
            if not isinstance(payload, dict):
                payload = {}
            request_hash = hashlib.sha256(
                json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
            ).hexdigest()
            persona_key = str(metricool_job.get("persona_key") or "default").strip() or "default"
            cur.execute(
                """
                insert into publish_jobs
                  (post_id, persona_key, platform, status, idempotency_key, request_hash, request_payload, max_retries, compliance_checks, external_post_id, published_at)
                values
                  (%(post_id)s::uuid, %(persona_key)s, 'metricool', 'published', %(idempotency_key)s, %(request_hash)s, %(request_payload)s::jsonb, 3, '[]'::jsonb, %(external_post_id)s, now())
                on conflict (post_id, platform)
                do update set
                  persona_key = excluded.persona_key,
                  status = 'published',
                  idempotency_key = excluded.idempotency_key,
                  request_hash = excluded.request_hash,
                  request_payload = excluded.request_payload,
                  max_retries = excluded.max_retries,
                  compliance_checks = excluded.compliance_checks,
                  external_post_id = excluded.external_post_id,
                  published_at = excluded.published_at,
                  next_retry_at = null,
                  last_error = null,
                  error_category = null
                """,
                {
                    "post_id": post_id,
                    "persona_key": persona_key,
                    "idempotency_key": f"{persona_key}:{post_id}:metricool",
                    "request_hash": request_hash,
                    "request_payload": json.dumps(payload, default=str),
                    "external_post_id": str(metricool_job.get("external_post_id") or ""),
                },
            )


def _rewrite_existing_video_for_post(*, settings: Any, post_id: str, video_url: str, dry_run: bool) -> None:
    before_stream = _probe_video_stream(media_path=video_url)
    LOGGER.info("Current stream post_id=%s stream=%s", post_id, before_stream)
    compliant_before, reason_before = _is_compliant_stream(before_stream)
    if compliant_before:
        LOGGER.info("Video already compliant for post_id=%s (%s)", post_id, reason_before)
        return
    if dry_run:
        LOGGER.info("Dry run: would transcode and overwrite storage object for post_id=%s (%s)", post_id, reason_before)
        return

    object_path = _extract_public_object_path(public_url=video_url, bucket_name=settings.supabase_video_bucket)
    with tempfile.TemporaryDirectory(prefix="flicker-fix-") as tmp_dir:
        input_path = Path(tmp_dir) / "input.mp4"
        output_path = Path(tmp_dir) / "output.mp4"
        response = requests.get(video_url, timeout=settings.request_timeout_seconds)
        response.raise_for_status()
        input_path.write_bytes(response.content)

        _transcode_to_limited_range(
            input_path=input_path,
            output_path=output_path,
            crf=settings.video_crf if settings.video_crf > 0 else 18,
        )
        if not output_path.exists() or output_path.stat().st_size < 1_000:
            raise RuntimeError(f"Transcoded output missing/too small for post_id={post_id}")

        after_stream = _probe_video_stream(media_path=str(output_path))
        compliant_after, reason_after = _is_compliant_stream(after_stream)
        LOGGER.info("Transcoded stream post_id=%s stream=%s", post_id, after_stream)
        if not compliant_after:
            raise RuntimeError(
                f"Transcoded output is non-compliant for post_id={post_id}: {reason_after}"
            )

        uploaded_url = _upload_bytes_to_supabase_storage(
            supabase_url=settings.supabase_url,
            supabase_service_role_key=settings.supabase_service_role_key,
            bucket_name=settings.supabase_video_bucket,
            object_path=object_path,
            payload_bytes=output_path.read_bytes(),
            content_type="video/mp4",
            timeout_seconds=settings.request_timeout_seconds,
        )
        LOGGER.info(
            "Overwrote storage object for post_id=%s object_path=%s uploaded_url=%s",
            post_id,
            object_path,
            uploaded_url,
        )
        verify_stream = _probe_video_stream(media_path=video_url)
        verify_ok, verify_reason = _is_compliant_stream(verify_stream)
        LOGGER.info("Post-upload stream probe post_id=%s stream=%s", post_id, verify_stream)
        if not verify_ok:
            raise RuntimeError(f"Uploaded object remains non-compliant for post_id={post_id}: {verify_reason}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Rewrite a generated video to yuv420p/tv and repost via recycle + pipeline rerun.",
    )
    parser.add_argument("--post-id", required=True, help="source_posts.id UUID to repair and repost")
    parser.add_argument("--dry-run", action="store_true", help="Show actions without mutating DB/storage")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    bootstrap_runtime_env()
    settings = load_settings()
    started_at = datetime.now(timezone.utc)

    with db_connection(settings.supabase_db_url) as conn:
        post_state = _fetch_post_state(conn, post_id=args.post_id)
        original_metricool_job = _fetch_latest_metricool_publish(conn, post_id=args.post_id)
    if post_state is None:
        raise RuntimeError(f"Post not found: {args.post_id}")
    link = str(post_state.get("link") or "").strip()
    title = str(post_state.get("title") or "").strip()
    if not link:
        raise RuntimeError(f"Post has no link: {args.post_id}")
    video_url = str(post_state.get("video_url") or "").strip()
    if not video_url:
        raise RuntimeError(f"Post has no video URL to rewrite: {args.post_id}")

    LOGGER.info(
        "Target post post_id=%s title=%s link=%s video_status=%s",
        args.post_id,
        title,
        link,
        post_state.get("video_status"),
    )
    try:
        _rewrite_existing_video_for_post(
            settings=settings,
            post_id=args.post_id,
            video_url=video_url,
            dry_run=args.dry_run,
        )

        if args.dry_run:
            LOGGER.info("Dry run: would delete source post %s and rerun pipeline once with clone-only settings", args.post_id)
            return 0

        with db_connection(settings.supabase_db_url) as conn:
            delete_source_post_by_id(conn, post_id=args.post_id)
        LOGGER.info("Deleted source post %s for recycle", args.post_id)

        with _temporary_env(
            {
                "FALLBACK_FEEDS_ENABLED": "false",
                "TOP_HEADLINES_PER_RUN": "1",
                "MAX_POSTS_PER_RUN": "1",
            }
        ):
            run_pipeline()
        LOGGER.info("Pipeline rerun completed")

        with db_connection(settings.supabase_db_url) as conn:
            new_post = _find_new_post_for_link(
                conn,
                link=link,
                excluded_post_id=args.post_id,
                min_created_at=started_at,
            )
            if not new_post:
                raise RuntimeError(f"No new source post found for link after rerun: {link}")
            new_post_id = str(new_post["post_id"])

            metricool_job = _fetch_latest_metricool_publish(conn, post_id=new_post_id)
            if not metricool_job:
                raise RuntimeError(f"No metricool publish job found for new post: {new_post_id}")
            if str(metricool_job.get("status") or "") != "published":
                raise RuntimeError(
                    f"Metricool publish job not published for new post={new_post_id} status={metricool_job.get('status')}"
                )
            published_media_url = str(metricool_job.get("media_url") or "").strip()
            if not published_media_url:
                raise RuntimeError(f"Published metricool job missing media_url for new post: {new_post_id}")

        new_stream = _probe_video_stream(media_path=published_media_url)
        stream_ok, stream_reason = _is_compliant_stream(new_stream)
        if not stream_ok:
            raise RuntimeError(
                f"Reposted media stream is non-compliant for new_post_id={new_post_id}: {stream_reason}"
            )

        LOGGER.info(
            "Success old_post_id=%s new_post_id=%s metricool_external_post_id=%s media_url=%s stream=%s",
            args.post_id,
            new_post_id,
            metricool_job.get("external_post_id"),
            published_media_url,
            new_stream,
        )
        return 0
    except Exception:
        if not args.dry_run:
            try:
                with db_connection(settings.supabase_db_url) as conn:
                    _restore_original_post_state(
                        conn,
                        post_state=post_state,
                        metricool_job=original_metricool_job,
                    )
                LOGGER.warning("Restored original DB state for post_id=%s after failure", args.post_id)
            except Exception as restore_error:  # noqa: BLE001
                LOGGER.error("Failed to restore original DB state for post_id=%s error=%s", args.post_id, restore_error)
        raise


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        LOGGER.error("Repair/repost failed: %s", exc)
        raise SystemExit(1)
