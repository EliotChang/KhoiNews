from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import re
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import psycopg
import requests

from pipeline.config import Settings, bootstrap_runtime_env, load_settings
from pipeline.content_gen import ContentGenerationResult
from pipeline.db import (
    PublishJob,
    claim_publish_jobs_ready,
    create_publish_attempt,
    db_connection,
    has_published_link_for_platforms,
    mark_publish_attempt_failed,
    mark_publish_attempt_published,
    mark_publish_attempt_skipped,
    upsert_publish_job,
)
from pipeline.review_state import REVIEW_STATUS_APPROVED, review_defaults, utc_now_iso
from pipeline.wj_ingest import SourcePostInput
from pipeline.text_sanitize import contains_url_text
from pipeline.voice_gen import VoiceAssetResult

_CJK_CHAR_RE = re.compile(r"[\u4e00-\u9fff\u3400-\u4dbf\uf900-\ufaff]")


def _cjk_word_count(text: str) -> int:
    cjk_chars = len(_CJK_CHAR_RE.findall(text))
    non_cjk = _CJK_CHAR_RE.sub("", text).strip()
    latin_words = len(non_cjk.split()) if non_cjk else 0
    return cjk_chars + latin_words


LOGGER = logging.getLogger("wj_publish")
SUPPORTED_PLATFORMS = {"metricool"}
MIN_SCRIPT_WORDS = 40
MIN_AUDIO_SECONDS = 12.0
MAX_AUDIO_SECONDS = 55.0
MIN_VIDEO_SECONDS = 14.0
MAX_VIDEO_SECONDS = 60.0
MAX_AUDIO_VIDEO_DELTA_SECONDS = 7.0
SENSITIVE_QUERY_KEYS = {
    "access_token",
    "client_secret",
    "fb_exchange_token",
    "refresh_token",
    "api_key",
    "token",
}


@dataclass(frozen=True)
class MediaPublishPayload:
    media_type: str
    media_url: str
    selection_reason: str


@dataclass(frozen=True)
class PublishResult:
    status: str
    external_post_id: str | None
    retryable: bool
    error_category: str | None
    error_message: str | None
    http_status: int | None
    response_payload: dict[str, Any]


def _safe_response_json(response: requests.Response | None) -> dict[str, Any]:
    if response is None:
        return {}
    try:
        payload = response.json()
    except ValueError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _sanitize_error_message(raw_message: str) -> str:
    if not raw_message:
        return ""

    sanitized = re.sub(r"(Bearer\s+)[A-Za-z0-9._-]+", r"\1[REDACTED]", raw_message, flags=re.IGNORECASE)

    def _sanitize_url(match: re.Match[str]) -> str:
        url = match.group(0)
        parsed = urlsplit(url)
        if not parsed.query:
            return url
        query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
        redacted_pairs = []
        for key, value in query_pairs:
            if key.lower() in SENSITIVE_QUERY_KEYS:
                redacted_pairs.append((key, "[REDACTED]"))
            else:
                redacted_pairs.append((key, value))
        return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(redacted_pairs), parsed.fragment))

    return re.sub(r"https?://\S+", _sanitize_url, sanitized)


def _is_probable_mp4_url(url: str) -> bool:
    if not url:
        return False
    path = urlsplit(url).path.lower()
    return path.endswith(".mp4")


def _normalize_platforms(platforms: list[str]) -> list[str]:
    deduped: list[str] = []
    for platform in platforms:
        normalized = platform.strip().lower()
        if normalized == "metricool" and normalized not in deduped:
            deduped.append(normalized)
    return deduped


def _resolve_publish_platforms(*, settings: Settings, platforms_override: list[str] | None = None) -> list[str]:
    configured = _normalize_platforms(platforms_override or settings.publish_platforms)
    if not configured:
        return ["metricool"]
    return configured


def _metricool_config_error(*, settings: Settings) -> str | None:
    if not settings.metricool_publish_enabled:
        return "Metricool publish is disabled"
    if not settings.metricool_user_token:
        return "Metricool configuration missing METRICOOL_USER_TOKEN"
    if not settings.metricool_user_id:
        return "Metricool configuration missing METRICOOL_USER_ID"
    if not settings.metricool_blog_id:
        return "Metricool configuration missing METRICOOL_BLOG_ID"
    if not settings.metricool_target_platforms:
        return "Metricool configuration missing METRICOOL_TARGET_PLATFORMS"
    return None


def _metricool_headers(*, settings: Settings) -> dict[str, str]:
    return {
        "X-Mc-Auth": settings.metricool_user_token,
        "Content-Type": "application/json",
    }


def _metricool_auth_query(*, settings: Settings) -> dict[str, str]:
    return {
        "userId": settings.metricool_user_id,
        "blogId": settings.metricool_blog_id,
    }


def _metricool_provider(network: str) -> str:
    normalized = network.strip().lower()
    if normalized in {"x", "twitter"}:
        return "twitter"
    return normalized


def _metricool_external_post_id(response_payload: dict[str, Any]) -> str:
    candidates = [
        str(response_payload.get("id", "")).strip(),
        str(response_payload.get("postId", "")).strip(),
        str(response_payload.get("publicationId", "")).strip(),
    ]
    data_node = response_payload.get("data", {})
    if isinstance(data_node, dict):
        candidates.extend(
            [
                str(data_node.get("id", "")).strip(),
                str(data_node.get("postId", "")).strip(),
                str(data_node.get("publicationId", "")).strip(),
            ]
        )
    for candidate in candidates:
        if candidate:
            return candidate
    return ""


def _metricool_publication_datetime(value: str) -> str:
    raw = value.strip()
    if not raw:
        return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%S")
    try:
        normalized = raw.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        return parsed.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%S")
    except ValueError:
        if len(raw) >= 19:
            return raw[:19]
        return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%S")


def _metricool_normalize_media_url(*, settings: Settings, media_url: str) -> str:
    if not media_url:
        return media_url
    endpoint = f"{settings.metricool_api_url.rstrip('/')}/actions/normalize/image/url"
    response = requests.get(
        endpoint,
        params={**_metricool_auth_query(settings=settings), "url": media_url},
        headers=_metricool_headers(settings=settings),
        timeout=settings.request_timeout_seconds,
    )
    response.raise_for_status()
    response_payload = _safe_response_json(response)
    candidates = [
        str(response_payload.get("url", "")).strip(),
        str(response_payload.get("normalizedUrl", "")).strip(),
    ]
    data_node = response_payload.get("data")
    if isinstance(data_node, dict):
        candidates.extend(
            [
                str(data_node.get("url", "")).strip(),
                str(data_node.get("normalizedUrl", "")).strip(),
            ]
        )
    for candidate in candidates:
        if candidate:
            return candidate
    return media_url


def _metricool_combined_caption(payload: dict[str, Any]) -> str:
    candidates = [
        str(payload.get("caption_instagram", "")).strip(),
        str(payload.get("caption_tiktok", "")).strip(),
        str(payload.get("caption_youtube", "")).strip(),
        str(payload.get("caption_x", "")).strip(),
    ]
    for candidate in candidates:
        if candidate:
            return candidate[:2200]
    return ""


def _metricool_failure_from_exception(exc: Exception) -> PublishResult:
    if isinstance(exc, requests.HTTPError):
        status_code = exc.response.status_code if exc.response is not None else None
        return PublishResult(
            status="failed",
            external_post_id=None,
            retryable=bool(status_code and status_code >= 500),
            error_category="metricool_http",
            error_message=_sanitize_error_message(str(exc)),
            http_status=status_code,
            response_payload=_safe_response_json(exc.response),
        )
    if isinstance(exc, requests.RequestException):
        return PublishResult(
            status="failed",
            external_post_id=None,
            retryable=True,
            error_category="metricool_network",
            error_message=_sanitize_error_message(str(exc)),
            http_status=None,
            response_payload={},
        )
    return PublishResult(
        status="failed",
        external_post_id=None,
        retryable=False,
        error_category="metricool_response",
        error_message=_sanitize_error_message(str(exc)),
        http_status=None,
        response_payload={},
    )


def _publish_metricool(*, settings: Settings, payload: dict[str, Any]) -> PublishResult:
    config_error = _metricool_config_error(settings=settings)
    if config_error:
        return PublishResult(
            status="skipped" if config_error == "Metricool publish is disabled" else "failed",
            external_post_id=None,
            retryable=False,
            error_category="metricool_disabled" if config_error == "Metricool publish is disabled" else "metricool_config",
            error_message=config_error,
            http_status=None,
            response_payload={},
        )

    endpoint = f"{settings.metricool_api_url.rstrip('/')}/v2/scheduler/posts"
    normalized_media_url = str(payload.get("media_url", "")).strip()
    try:
        normalized_media_url = _metricool_normalize_media_url(settings=settings, media_url=normalized_media_url)
    except requests.RequestException as exc:
        LOGGER.warning("Metricool media URL normalization failed detail=%s", str(exc))

    desired_publish_raw = payload.get("desired_publish_at")
    desired_publish_at = str(desired_publish_raw).strip() if desired_publish_raw is not None else ""
    if desired_publish_at.lower() == "none":
        desired_publish_at = ""

    provider_names: list[str] = []
    for platform in settings.metricool_target_platforms:
        provider_name = _metricool_provider(platform)
        if provider_name in {"twitter", "x"}:
            continue
        if provider_name not in provider_names:
            provider_names.append(provider_name)

    if not provider_names:
        return PublishResult(
            status="failed",
            external_post_id=None,
            retryable=False,
            error_category="metricool_config",
            error_message="No eligible Metricool providers after filtering unsupported networks",
            http_status=None,
            response_payload={},
        )

    video_title = str(payload.get("video_title_short", "")).strip()[:100]
    if not video_title:
        video_title = str(payload.get("title", "")).strip()[:100] or "News Update"

    request_payload: dict[str, Any] = {
        "text": _metricool_combined_caption(payload),
        "firstCommentText": str(payload.get("article_url", "")).strip(),
        "providers": [{"network": provider_name} for provider_name in provider_names],
        "autoPublish": True,
        "saveExternalMediaFiles": False,
        "shortener": False,
        "draft": False,
        "media": [normalized_media_url] if normalized_media_url else [],
        "publicationDate": {
            "dateTime": _metricool_publication_datetime(desired_publish_at),
            "timezone": "UTC",
        },
    }
    if "instagram" in provider_names:
        request_payload["instagramData"] = {
            "autoPublish": True,
            "type": "REEL",
            "showReelOnFeed": True,
        }
    if "youtube" in provider_names:
        request_payload["youtubeData"] = {
            "title": video_title,
            "type": "short",
            "privacy": "public",
            "madeForKids": False,
        }
    if "tiktok" in provider_names:
        request_payload["tiktokData"] = {
            "privacyOption": "PUBLIC_TO_EVERYONE",
            "disableComment": False,
            "disableDuet": False,
            "disableStitch": False,
        }

    try:
        response = requests.post(
            endpoint,
            params=_metricool_auth_query(settings=settings),
            json=request_payload,
            headers=_metricool_headers(settings=settings),
            timeout=settings.request_timeout_seconds,
        )
        response.raise_for_status()
        response_payload = _safe_response_json(response)
        external_post_id = _metricool_external_post_id(response_payload)
        if not external_post_id:
            raise ValueError("Metricool response missing post identifier")
        return PublishResult(
            status="published",
            external_post_id=external_post_id,
            retryable=False,
            error_category=None,
            error_message=None,
            http_status=response.status_code,
            response_payload={"providers": provider_names, "response": response_payload},
        )
    except Exception as exc:  # noqa: BLE001
        return _metricool_failure_from_exception(exc)


def _coerce_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _compliance_checks(*, platform: str, payload: dict[str, Any], enforce_compliance: bool) -> tuple[list[dict[str, Any]], str | None]:
    checks: list[dict[str, Any]] = []
    blockers: list[str] = []

    if platform != "metricool":
        checks.append({"name": "platform_supported", "passed": False})
        blockers.append(f"unsupported platform: {platform}")
        return checks, "; ".join(blockers)

    media_type = str(payload.get("media_type", "")).strip().lower()
    media_url = str(payload.get("media_url", "")).strip()
    title = str(payload.get("title", "")).strip()

    checks.append({"name": "title_present", "passed": bool(title)})
    if not title:
        blockers.append("title is required")

    checks.append({"name": "media_url_present", "passed": bool(media_url)})
    if not media_url:
        blockers.append("media_url is required")

    checks.append({"name": "media_url_https", "passed": media_url.startswith("https://")})
    if media_url and not media_url.startswith("https://"):
        blockers.append("media_url must be https")

    checks.append({"name": "video_required", "passed": media_type == "video"})
    if media_type != "video":
        blockers.append("metricool requires video media")

    checks.append({"name": "video_mp4_required", "passed": _is_probable_mp4_url(media_url)})
    if media_url and not _is_probable_mp4_url(media_url):
        blockers.append("metricool media_url must be an mp4 URL")

    caption = _metricool_combined_caption(payload)
    checks.append({"name": "caption_present", "passed": bool(caption)})
    if not caption:
        blockers.append("caption is required")
    caption_contains_url = contains_url_text(caption)
    checks.append({"name": "caption_url_check", "passed": not caption_contains_url})
    if caption_contains_url:
        blockers.append("caption contains URL-like text")

    script_text = str(payload.get("script_10s", "")).strip()
    script_contains_url = contains_url_text(script_text)
    checks.append({"name": "script_url_check", "passed": not script_contains_url})
    if script_contains_url:
        blockers.append("script contains URL-like text")

    script_words = _cjk_word_count(script_text)
    checks.append({"name": "script_words_min", "passed": script_words >= MIN_SCRIPT_WORDS, "words": script_words})
    if script_words < MIN_SCRIPT_WORDS:
        blockers.append(f"script must contain at least {MIN_SCRIPT_WORDS} words")

    audio_duration = _coerce_float(payload.get("audio_duration_sec"))
    video_duration = _coerce_float(payload.get("video_duration_sec"))

    checks.append({"name": "audio_duration_present", "passed": audio_duration is not None})
    if audio_duration is None:
        blockers.append("audio duration is required")

    checks.append({"name": "video_duration_present", "passed": video_duration is not None})
    if video_duration is None:
        blockers.append("video duration is required")

    if audio_duration is not None:
        audio_in_bounds = MIN_AUDIO_SECONDS <= audio_duration <= MAX_AUDIO_SECONDS
        checks.append({"name": "audio_duration_bounds", "passed": audio_in_bounds, "value": audio_duration})
        if not audio_in_bounds:
            blockers.append(f"audio duration must be between {MIN_AUDIO_SECONDS:.0f}s and {MAX_AUDIO_SECONDS:.0f}s")

    if video_duration is not None:
        video_in_bounds = MIN_VIDEO_SECONDS <= video_duration <= MAX_VIDEO_SECONDS
        checks.append({"name": "video_duration_bounds", "passed": video_in_bounds, "value": video_duration})
        if not video_in_bounds:
            blockers.append(f"video duration must be between {MIN_VIDEO_SECONDS:.0f}s and {MAX_VIDEO_SECONDS:.0f}s")

    if audio_duration is not None and video_duration is not None:
        delta = abs(video_duration - audio_duration)
        sync_ok = delta <= MAX_AUDIO_VIDEO_DELTA_SECONDS and video_duration + 0.75 >= audio_duration
        checks.append({"name": "audio_video_sync", "passed": sync_ok, "delta": delta})
        if not sync_ok:
            blockers.append("audio/video duration mismatch suggests truncation")

    banned_fragments = ["you won't believe", "stay tuned", "drop your thoughts below"]
    lowered_caption = caption.lower()
    banned_matches = [phrase for phrase in banned_fragments if phrase in lowered_caption]
    checks.append({"name": "banned_phrase_check", "passed": len(banned_matches) == 0, "matches": banned_matches})
    if banned_matches:
        blockers.append("caption contains disallowed engagement-bait phrases")

    if not enforce_compliance:
        return checks, None
    return checks, "; ".join(blockers) if blockers else None


def _dispatch_job(*, settings: Settings, job: PublishJob) -> PublishResult:
    if job.platform == "metricool":
        return _publish_metricool(settings=settings, payload=job.request_payload)
    return PublishResult(
        status="skipped",
        external_post_id=None,
        retryable=False,
        error_category="unsupported_platform",
        error_message=f"Unsupported platform {job.platform}",
        http_status=None,
        response_payload={},
    )


def enqueue_publish_jobs_for_post(
    conn: psycopg.Connection,
    *,
    settings: Settings,
    post: SourcePostInput,
    post_id: str,
    content: ContentGenerationResult,
    media: MediaPublishPayload | None,
    voice: VoiceAssetResult | None,
    desired_publish_at: str | None = None,
    desired_publish_at_by_platform: dict[str, str] | None = None,
    platform_job_limits: dict[str, int] | None = None,
    thumbnail_url: str | None = None,
    video_duration_sec: float | None = None,
) -> list[PublishJob]:
    del desired_publish_at_by_platform, platform_job_limits
    platforms = _resolve_publish_platforms(settings=settings)
    if not settings.publish_enabled or not platforms:
        return []
    if media is None:
        LOGGER.info("Skipping publish enqueue for post_id=%s because no media was found", post_id)
        return []

    if media.media_type != "video" or not _is_probable_mp4_url(media.media_url):
        LOGGER.warning(
            "Skipping publish enqueue for post_id=%s because media is not publishable MP4 media_type=%s media_url=%s",
            post_id,
            media.media_type,
            media.media_url,
        )
        return []

    allow_duplicate_link_repost = bool(getattr(settings, "allow_duplicate_link_repost", False))
    if not allow_duplicate_link_repost:
        duplicate_link_exists = has_published_link_for_platforms(
            conn,
            link=post.link,
            persona_key=settings.persona_key,
            platforms=platforms,
            exclude_post_id=post_id,
        )
        if duplicate_link_exists:
            LOGGER.info(
                "Skipping publish enqueue for post_id=%s because link is already published article_url=%s",
                post_id,
                post.link,
            )
            return []

    payload = {
        "post_id": post_id,
        "persona_key": settings.persona_key,
        "title": post.title,
        "description": post.description,
        "article_url": post.link,
        "video_title_short": content.video_title_short,
        "caption_instagram": content.caption_instagram,
        "caption_tiktok": content.caption_tiktok,
        "caption_youtube": content.caption_youtube,
        "caption_x": content.caption_x,
        "hashtags": content.hashtags,
        "script_10s": content.script_10s,
        "media_type": media.media_type,
        "media_url": media.media_url,
        "selection_reason": media.selection_reason,
        "audio_url": voice.audio_url if voice else None,
        "audio_duration_sec": voice.audio_duration_sec if voice else None,
        "video_duration_sec": video_duration_sec,
        "thumbnail_url": thumbnail_url or "",
        "desired_publish_at": desired_publish_at,
    }

    payload.update(review_defaults())
    if not bool(getattr(settings, "metricool_review_required", False)):
        payload["approval_status"] = REVIEW_STATUS_APPROVED
        payload["approval_by"] = "system:auto"
        payload["approval_at"] = utc_now_iso()

    checks, blocker = _compliance_checks(
        platform="metricool",
        payload=payload,
        enforce_compliance=settings.publish_enforce_compliance,
    )
    if blocker and settings.publish_enforce_compliance:
        LOGGER.warning("Compliance blocked enqueue for post_id=%s platform=metricool: %s", post_id, blocker)
        return []

    job = upsert_publish_job(
        conn,
        post_id=post_id,
        persona_key=settings.persona_key,
        platform="metricool",
        payload=payload,
        max_retries=settings.publish_max_retries,
        compliance_checks=checks,
    )
    return [job]


def dispatch_ready_publish_jobs(
    conn: psycopg.Connection,
    *,
    settings: Settings,
    max_jobs: int | None = None,
    platforms_override: list[str] | None = None,
) -> dict[str, int]:
    platforms = _resolve_publish_platforms(settings=settings, platforms_override=platforms_override)
    if not settings.publish_enabled or not platforms:
        return {"queued": 0, "published": 0, "failed": 0, "skipped": 0}

    ready_jobs = claim_publish_jobs_ready(
        conn,
        persona_key=settings.persona_key,
        platforms=platforms,
        max_jobs=max_jobs or settings.publish_max_jobs_per_run,
        require_review_approval=bool(getattr(settings, "metricool_review_required", False)),
        stale_in_progress_minutes=max(1, int(getattr(settings, "publish_claim_stale_in_progress_minutes", 45))),
    )
    counters = {"queued": len(ready_jobs), "published": 0, "failed": 0, "skipped": 0}

    for job in ready_jobs:
        checks, blocker = _compliance_checks(
            platform=job.platform,
            payload=job.request_payload,
            enforce_compliance=settings.publish_enforce_compliance,
        )
        attempt = create_publish_attempt(conn, job_id=job.id, request_payload=job.request_payload)
        if blocker:
            mark_publish_attempt_skipped(
                conn,
                attempt_id=attempt.id,
                job_id=job.id,
                reason=blocker,
                error_category="compliance",
                response_payload={"checks": checks},
            )
            counters["skipped"] += 1
            conn.commit()
            continue

        result = _dispatch_job(settings=settings, job=job)
        if result.status == "published" and result.external_post_id:
            mark_publish_attempt_published(
                conn,
                attempt_id=attempt.id,
                job_id=job.id,
                external_post_id=result.external_post_id,
                response_payload=result.response_payload,
                http_status=result.http_status,
            )
            counters["published"] += 1
            conn.commit()
            continue

        if result.status == "skipped":
            mark_publish_attempt_skipped(
                conn,
                attempt_id=attempt.id,
                job_id=job.id,
                reason=result.error_message or "Skipped by adapter",
                error_category=result.error_category or "publish_skipped",
                response_payload=result.response_payload,
            )
            counters["skipped"] += 1
            conn.commit()
            continue

        mark_publish_attempt_failed(
            conn,
            attempt_id=attempt.id,
            job_id=job.id,
            error_message=result.error_message or "Unknown publish error",
            error_category=result.error_category or "publish_failed",
            retryable=result.retryable,
            response_payload=result.response_payload,
            http_status=result.http_status,
        )
        counters["failed"] += 1
        conn.commit()

    return counters


def _metricool_get_bio_buttons(*, settings: Settings) -> list[dict[str, Any]]:
    endpoint = f"{settings.metricool_api_url.rstrip('/')}/linkinbio/instagram/getbioButtons"
    response = requests.get(
        endpoint,
        params=_metricool_auth_query(settings=settings),
        headers=_metricool_headers(settings=settings),
        timeout=settings.request_timeout_seconds,
    )
    response.raise_for_status()
    data = response.json()
    if isinstance(data, list):
        return data
    return []


def _metricool_add_bio_button(*, settings: Settings, text: str, link: str) -> dict[str, Any]:
    endpoint = f"{settings.metricool_api_url.rstrip('/')}/linkinbio/instagram/addcatalogButton"
    response = requests.get(
        endpoint,
        params={**_metricool_auth_query(settings=settings), "textButton": text, "link": link},
        headers=_metricool_headers(settings=settings),
        timeout=settings.request_timeout_seconds,
    )
    response.raise_for_status()
    return _safe_response_json(response)


def _metricool_edit_bio_button(*, settings: Settings, item_id: int, text: str, link: str) -> dict[str, Any]:
    endpoint = f"{settings.metricool_api_url.rstrip('/')}/linkinbio/instagram/editcatalogbutton"
    response = requests.get(
        endpoint,
        params={**_metricool_auth_query(settings=settings), "itemid": item_id, "link": link, "text": text},
        headers=_metricool_headers(settings=settings),
        timeout=settings.request_timeout_seconds,
    )
    response.raise_for_status()
    return _safe_response_json(response)


def _metricool_delete_bio_button(*, settings: Settings, item_id: int) -> None:
    endpoint = f"{settings.metricool_api_url.rstrip('/')}/linkinbio/instagram/deletecatalogitem"
    response = requests.delete(
        endpoint,
        params={**_metricool_auth_query(settings=settings), "itemid": item_id},
        headers=_metricool_headers(settings=settings),
        timeout=settings.request_timeout_seconds,
    )
    response.raise_for_status()


def update_metricool_link_in_bio(*, settings: Settings, recent_links: list[dict[str, str]]) -> None:
    if not settings.metricool_link_in_bio_enabled:
        return
    if not settings.metricool_user_token or not settings.metricool_user_id or not settings.metricool_blog_id:
        LOGGER.warning("Skipping link-in-bio update: Metricool credentials missing")
        return
    if not recent_links:
        return

    max_links = settings.metricool_link_in_bio_max_links
    links_to_set = recent_links[:max_links]

    try:
        existing_buttons = _metricool_get_bio_buttons(settings=settings)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Metricool link-in-bio: failed to fetch existing buttons error=%s", exc)
        existing_buttons = []

    for idx, link_entry in enumerate(links_to_set):
        text = link_entry.get("text", "Source")[:120]
        url = link_entry.get("url", "")
        if not url:
            continue
        try:
            if idx < len(existing_buttons):
                button = existing_buttons[idx]
                button_id = button.get("id")
                if button_id is not None:
                    _metricool_edit_bio_button(settings=settings, item_id=int(button_id), text=text, link=url)
                    continue
            _metricool_add_bio_button(settings=settings, text=text, link=url)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Metricool link-in-bio: failed to set button idx=%d error=%s", idx, exc)

    if len(existing_buttons) > max_links:
        for button in existing_buttons[max_links:]:
            button_id = button.get("id")
            if button_id is None:
                continue
            try:
                _metricool_delete_bio_button(settings=settings, item_id=int(button_id))
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Metricool link-in-bio: failed to delete button %s error=%s", button_id, exc)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Metricool publishing CLI")
    parser.add_argument("--platform", choices=sorted(SUPPORTED_PLATFORMS), help="Publish only one platform")
    parser.add_argument("--max-jobs", type=int, default=24, help="Maximum queued jobs to dispatch")
    parser.add_argument(
        "--all-ready",
        action="store_true",
        help="Dispatch all ready jobs up to --max-jobs",
    )
    return parser


def cli_dispatch_ready_jobs() -> None:
    bootstrap_runtime_env()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    parser = _build_parser()
    args = parser.parse_args()
    settings = load_settings()

    platforms_override = [args.platform] if args.platform else None

    with db_connection(settings.supabase_db_url) as conn:
        counters = dispatch_ready_publish_jobs(
            conn,
            settings=settings,
            max_jobs=args.max_jobs if args.all_ready else 1,
            platforms_override=platforms_override,
        )
    LOGGER.info("Publish dispatch finished: %s", counters)


if __name__ == "__main__":
    cli_dispatch_ready_jobs()
