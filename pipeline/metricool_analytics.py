from __future__ import annotations

import difflib
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg
import requests

from pipeline.config import Settings
from pipeline.db import upsert_video_performance_metric

LOGGER = logging.getLogger("wj_metricool_analytics")

CAPTION_MATCH_THRESHOLD = 0.7
TIMESTAMP_MATCH_WINDOW_HOURS = 2


@dataclass(frozen=True)
class MetricoolReel:
    reel_id: str
    published_at: datetime | None
    content: str
    url: str
    image_url: str
    likes: int
    comments: int
    interactions: int
    engagement: float
    views: int
    reach: int
    saved: int
    shares: int
    impressions: int
    average_watch_time: float
    video_view_total_time: float
    duration_seconds: int
    reels_skip_rate: float
    reposts: int
    video_views: int


@dataclass(frozen=True)
class MetricoolTikTokVideo:
    video_id: str
    published_at: datetime | None
    content: str
    url: str
    likes: int
    comments: int
    shares: int
    views: int
    reach: int
    average_watch_time: float
    video_view_total_time: float
    duration_seconds: int
    engagement: float
    impressions: int
    saves: int
    reposts: int


@dataclass
class DBPublishedPost:
    post_id: str
    publish_job_id: str
    external_post_id: str
    published_at: datetime | None
    title: str
    link: str
    script_10s: str
    caption_instagram: str
    caption_tiktok: str
    content_signals: dict[str, Any] = field(default_factory=dict)


@dataclass
class MatchedReel:
    reel: MetricoolReel
    db_post: DBPublishedPost
    match_method: str
    match_score: float


@dataclass
class MatchedTikTokVideo:
    video: MetricoolTikTokVideo
    db_post: DBPublishedPost
    match_method: str
    match_score: float


def _parse_metricool_datetime(dt_info: dict[str, Any] | None) -> datetime | None:
    if not dt_info:
        return None
    epoch_ms = dt_info.get("epochSecond")
    if epoch_ms is not None:
        try:
            return datetime.fromtimestamp(int(epoch_ms), tz=timezone.utc)
        except (ValueError, OSError):
            pass
    raw = dt_info.get("dateTime") or dt_info.get("date")
    if not raw:
        return None
    raw = str(raw).strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S"):
        try:
            parsed = datetime.strptime(raw, fmt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        except ValueError:
            continue
    try:
        normalized = raw.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _safe_int(val: Any, default: int = 0) -> int:
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def _safe_float(val: Any, default: float = 0.0) -> float:
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _parse_reel(raw: dict[str, Any]) -> MetricoolReel:
    return MetricoolReel(
        reel_id=str(raw.get("reelId") or raw.get("id") or "").strip(),
        published_at=_parse_metricool_datetime(raw.get("publishedAt")),
        content=str(raw.get("content") or "").strip(),
        url=str(raw.get("url") or "").strip(),
        image_url=str(raw.get("imageUrl") or "").strip(),
        likes=_safe_int(raw.get("likes")),
        comments=_safe_int(raw.get("comments")),
        interactions=_safe_int(raw.get("interactions")),
        engagement=_safe_float(raw.get("engagement")),
        views=_safe_int(raw.get("views")),
        reach=_safe_int(raw.get("reach")),
        saved=_safe_int(raw.get("saved")),
        shares=_safe_int(raw.get("shares")),
        impressions=_safe_int(raw.get("impressions") or raw.get("impressionsTotal")),
        average_watch_time=_safe_float(raw.get("averageWatchTime")),
        video_view_total_time=_safe_float(raw.get("videoViewTotalTime")),
        duration_seconds=_safe_int(raw.get("durationSeconds")),
        reels_skip_rate=_safe_float(raw.get("reelsSkipRate")),
        reposts=_safe_int(raw.get("reposts")),
        video_views=_safe_int(raw.get("videoViews")),
    )


def fetch_instagram_reels(
    *,
    settings: Settings,
    lookback_days: int | None = None,
) -> list[MetricoolReel]:
    if not settings.metricool_user_token:
        LOGGER.warning("Metricool analytics: missing METRICOOL_USER_TOKEN, skipping fetch")
        return []
    if not settings.metricool_user_id or not settings.metricool_blog_id:
        LOGGER.warning("Metricool analytics: missing METRICOOL_USER_ID or METRICOOL_BLOG_ID, skipping fetch")
        return []

    days = lookback_days or settings.metricool_analytics_lookback_days
    now = datetime.now(timezone.utc)
    from_dt = (now - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S")
    to_dt = now.strftime("%Y-%m-%dT%H:%M:%S")

    endpoint = f"{settings.metricool_api_url.rstrip('/')}/v2/analytics/reels/instagram"
    params = {
        "userId": settings.metricool_user_id,
        "blogId": settings.metricool_blog_id,
        "from": from_dt,
        "to": to_dt,
        "timezone": "UTC",
    }
    headers = {
        "X-Mc-Auth": settings.metricool_user_token,
        "Content-Type": "application/json",
    }

    LOGGER.info("Fetching Instagram reels from Metricool from=%s to=%s", from_dt, to_dt)
    try:
        response = requests.get(
            endpoint,
            params=params,
            headers=headers,
            timeout=settings.request_timeout_seconds,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        LOGGER.error("Metricool analytics fetch failed: %s", exc)
        return []

    payload = response.json()
    raw_reels: list[dict[str, Any]] = []
    if isinstance(payload, list):
        raw_reels = payload
    elif isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            raw_reels = data
        else:
            raw_reels = [payload]

    reels = [_parse_reel(r) for r in raw_reels if isinstance(r, dict)]
    LOGGER.info("Fetched %d Instagram reels from Metricool", len(reels))
    return reels


def _parse_tiktok_video(raw: dict[str, Any]) -> MetricoolTikTokVideo:
    return MetricoolTikTokVideo(
        video_id=str(raw.get("videoId") or raw.get("id") or "").strip(),
        published_at=_parse_metricool_datetime(raw.get("publishedAt")),
        content=str(raw.get("content") or raw.get("description") or "").strip(),
        url=str(raw.get("url") or "").strip(),
        likes=_safe_int(raw.get("likes")),
        comments=_safe_int(raw.get("comments")),
        shares=_safe_int(raw.get("shares")),
        views=_safe_int(raw.get("views") or raw.get("videoViews")),
        reach=_safe_int(raw.get("reach")),
        average_watch_time=_safe_float(raw.get("averageWatchTime")),
        video_view_total_time=_safe_float(raw.get("videoViewTotalTime")),
        duration_seconds=_safe_int(raw.get("durationSeconds")),
        engagement=_safe_float(raw.get("engagement")),
        impressions=_safe_int(raw.get("impressions") or raw.get("impressionsTotal")),
        saves=_safe_int(raw.get("saved") or raw.get("saves")),
        reposts=_safe_int(raw.get("reposts")),
    )


def fetch_tiktok_videos(
    *,
    settings: Settings,
    lookback_days: int | None = None,
) -> list[MetricoolTikTokVideo]:
    if not settings.metricool_user_token:
        LOGGER.warning("Metricool TikTok analytics: missing METRICOOL_USER_TOKEN, skipping fetch")
        return []
    if not settings.metricool_user_id or not settings.metricool_blog_id:
        LOGGER.warning("Metricool TikTok analytics: missing METRICOOL_USER_ID or METRICOOL_BLOG_ID, skipping fetch")
        return []

    days = lookback_days or settings.metricool_analytics_lookback_days
    now = datetime.now(timezone.utc)
    from_dt = (now - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S")
    to_dt = now.strftime("%Y-%m-%dT%H:%M:%S")

    endpoint = f"{settings.metricool_api_url.rstrip('/')}/v2/analytics/posts/tiktok"
    params = {
        "userId": settings.metricool_user_id,
        "blogId": settings.metricool_blog_id,
        "from": from_dt,
        "to": to_dt,
        "timezone": "UTC",
    }
    headers = {
        "X-Mc-Auth": settings.metricool_user_token,
        "Content-Type": "application/json",
    }

    LOGGER.info("Fetching TikTok videos from Metricool from=%s to=%s", from_dt, to_dt)
    try:
        response = requests.get(
            endpoint,
            params=params,
            headers=headers,
            timeout=settings.request_timeout_seconds,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        LOGGER.error("Metricool TikTok analytics fetch failed: %s", exc)
        return []

    payload = response.json()
    raw_videos: list[dict[str, Any]] = []
    if isinstance(payload, list):
        raw_videos = payload
    elif isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            raw_videos = data
        else:
            raw_videos = [payload]

    videos = [_parse_tiktok_video(v) for v in raw_videos if isinstance(v, dict)]
    LOGGER.info("Fetched %d TikTok videos from Metricool", len(videos))
    return videos


def fetch_db_published_posts(
    conn: psycopg.Connection,
    *,
    persona_key: str,
    lookback_days: int,
) -> list[DBPublishedPost]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                sp.id AS post_id,
                pj.id AS publish_job_id,
                pj.external_post_id,
                pj.published_at,
                sp.title,
                sp.link,
                ca.script_10s,
                ca.caption_instagram,
                ca.caption_tiktok,
                ca.content_signals
            FROM source_posts sp
            JOIN content_assets ca ON ca.post_id = sp.id
            JOIN publish_jobs pj ON pj.post_id = sp.id
            WHERE pj.platform = 'metricool'
              AND pj.status = 'published'
              AND coalesce(pj.persona_key, 'default') = %(persona_key)s
              AND pj.published_at >= now() - make_interval(days => %(lookback_days)s)
            ORDER BY pj.published_at DESC
            """,
            {"persona_key": persona_key, "lookback_days": max(1, int(lookback_days))},
        )
        rows = cur.fetchall()

    posts: list[DBPublishedPost] = []
    for row in rows:
        signals = row.get("content_signals")
        if not isinstance(signals, dict):
            signals = {}
        pub_at = row.get("published_at")
        if isinstance(pub_at, datetime) and pub_at.tzinfo is None:
            pub_at = pub_at.replace(tzinfo=timezone.utc)
        posts.append(
            DBPublishedPost(
                post_id=str(row["post_id"]),
                publish_job_id=str(row["publish_job_id"]),
                external_post_id=str(row.get("external_post_id") or ""),
                published_at=pub_at,
                title=str(row.get("title") or "").strip(),
                link=str(row.get("link") or "").strip(),
                script_10s=str(row.get("script_10s") or "").strip(),
                caption_instagram=str(row.get("caption_instagram") or "").strip(),
                caption_tiktok=str(row.get("caption_tiktok") or "").strip(),
                content_signals=signals,
            )
        )
    LOGGER.info("Fetched %d published posts from DB (lookback=%d days)", len(posts), lookback_days)
    return posts


def _normalize_caption(text: str) -> str:
    return " ".join(text.lower().split())


def _caption_similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, _normalize_caption(a), _normalize_caption(b)).ratio()


def _timestamp_close(dt_a: datetime | None, dt_b: datetime | None, window_hours: int) -> bool:
    if dt_a is None or dt_b is None:
        return False
    delta = abs((dt_a - dt_b).total_seconds())
    return delta <= window_hours * 3600


def match_reels_to_posts(
    reels: list[MetricoolReel],
    posts: list[DBPublishedPost],
) -> list[MatchedReel]:
    matched: list[MatchedReel] = []
    used_post_ids: set[str] = set()

    for reel in reels:
        best_post: DBPublishedPost | None = None
        best_score = 0.0
        best_method = ""

        for post in posts:
            if post.post_id in used_post_ids:
                continue
            sim = _caption_similarity(reel.content, post.caption_instagram)
            if sim >= CAPTION_MATCH_THRESHOLD and sim > best_score:
                best_post = post
                best_score = sim
                best_method = "caption"

        if not best_post:
            for post in posts:
                if post.post_id in used_post_ids:
                    continue
                if _timestamp_close(reel.published_at, post.published_at, TIMESTAMP_MATCH_WINDOW_HOURS):
                    best_post = post
                    best_score = 0.5
                    best_method = "timestamp"
                    break

        if best_post:
            used_post_ids.add(best_post.post_id)
            matched.append(MatchedReel(
                reel=reel,
                db_post=best_post,
                match_method=best_method,
                match_score=best_score,
            ))

    LOGGER.info(
        "Matched %d/%d reels to DB posts (caption=%d, timestamp=%d)",
        len(matched),
        len(reels),
        sum(1 for m in matched if m.match_method == "caption"),
        sum(1 for m in matched if m.match_method == "timestamp"),
    )
    return matched


def store_reel_metrics(
    conn: psycopg.Connection,
    matched: list[MatchedReel],
    *,
    persona_key: str,
) -> int:
    stored = 0
    for m in matched:
        reel = m.reel
        total_interactions = reel.likes + reel.comments + reel.shares + reel.saved
        engagement_rate = (total_interactions / reel.views) if reel.views > 0 else 0.0
        completion_rate = None
        if reel.duration_seconds > 0 and reel.average_watch_time > 0:
            completion_rate = min(1.0, reel.average_watch_time / reel.duration_seconds)

        normalized: dict[str, Any] = {
            "views": reel.views,
            "likes": reel.likes,
            "comments": reel.comments,
            "shares": reel.shares,
            "saves": reel.saved,
            "watch_time_seconds": reel.video_view_total_time,
            "avg_watch_seconds": reel.average_watch_time,
            "avg_retention_ratio": completion_rate,
            "completion_rate": completion_rate,
            "engagement_rate": engagement_rate,
            "reach": reel.reach,
            "impressions": reel.impressions,
            "reposts": reel.reposts,
            "skip_rate": reel.reels_skip_rate,
            "duration_seconds": reel.duration_seconds,
            "reel_id": reel.reel_id,
            "reel_url": reel.url,
            "match_method": m.match_method,
            "match_score": m.match_score,
        }

        upsert_video_performance_metric(
            conn,
            persona_key=persona_key,
            publish_job_id=m.db_post.publish_job_id,
            platform="instagram",
            external_post_id=reel.reel_id or m.db_post.external_post_id,
            metric_timestamp=reel.published_at or datetime.now(timezone.utc),
            normalized_metrics=normalized,
        )
        stored += 1

    LOGGER.info("Stored %d reel metrics in video_performance_metrics", stored)
    return stored


def match_tiktok_videos_to_posts(
    videos: list[MetricoolTikTokVideo],
    posts: list[DBPublishedPost],
) -> list[MatchedTikTokVideo]:
    matched: list[MatchedTikTokVideo] = []
    used_post_ids: set[str] = set()

    for video in videos:
        best_post: DBPublishedPost | None = None
        best_score = 0.0
        best_method = ""

        for post in posts:
            if post.post_id in used_post_ids:
                continue
            tiktok_sim = _caption_similarity(video.content, post.caption_tiktok)
            ig_sim = _caption_similarity(video.content, post.caption_instagram)
            sim = max(tiktok_sim, ig_sim)
            if sim >= CAPTION_MATCH_THRESHOLD and sim > best_score:
                best_post = post
                best_score = sim
                best_method = "caption"

        if not best_post:
            for post in posts:
                if post.post_id in used_post_ids:
                    continue
                if _timestamp_close(video.published_at, post.published_at, TIMESTAMP_MATCH_WINDOW_HOURS):
                    best_post = post
                    best_score = 0.5
                    best_method = "timestamp"
                    break

        if best_post:
            used_post_ids.add(best_post.post_id)
            matched.append(MatchedTikTokVideo(
                video=video,
                db_post=best_post,
                match_method=best_method,
                match_score=best_score,
            ))

    LOGGER.info(
        "Matched %d/%d TikTok videos to DB posts (caption=%d, timestamp=%d)",
        len(matched),
        len(videos),
        sum(1 for m in matched if m.match_method == "caption"),
        sum(1 for m in matched if m.match_method == "timestamp"),
    )
    return matched


def store_tiktok_metrics(
    conn: psycopg.Connection,
    matched: list[MatchedTikTokVideo],
    *,
    persona_key: str,
) -> int:
    stored = 0
    for m in matched:
        video = m.video
        total_interactions = video.likes + video.comments + video.shares + video.saves
        engagement_rate = (total_interactions / video.views) if video.views > 0 else 0.0
        completion_rate = None
        if video.duration_seconds > 0 and video.average_watch_time > 0:
            completion_rate = min(1.0, video.average_watch_time / video.duration_seconds)

        normalized: dict[str, Any] = {
            "views": video.views,
            "likes": video.likes,
            "comments": video.comments,
            "shares": video.shares,
            "saves": video.saves,
            "watch_time_seconds": video.video_view_total_time,
            "avg_watch_seconds": video.average_watch_time,
            "avg_retention_ratio": completion_rate,
            "completion_rate": completion_rate,
            "engagement_rate": engagement_rate,
            "reach": video.reach,
            "impressions": video.impressions,
            "reposts": video.reposts,
            "duration_seconds": video.duration_seconds,
            "video_id": video.video_id,
            "video_url": video.url,
            "match_method": m.match_method,
            "match_score": m.match_score,
        }

        upsert_video_performance_metric(
            conn,
            persona_key=persona_key,
            publish_job_id=m.db_post.publish_job_id,
            platform="tiktok",
            external_post_id=video.video_id or m.db_post.external_post_id,
            metric_timestamp=video.published_at or datetime.now(timezone.utc),
            normalized_metrics=normalized,
        )
        stored += 1

    LOGGER.info("Stored %d TikTok video metrics in video_performance_metrics", stored)
    return stored


def fetch_and_store_metricool_analytics(
    conn: psycopg.Connection,
    *,
    settings: Settings,
    lookback_days: int | None = None,
) -> int:
    if not settings.metricool_analytics_enabled:
        LOGGER.debug("Metricool analytics disabled, skipping")
        return 0

    days = lookback_days or settings.metricool_analytics_lookback_days
    total_stored = 0

    posts = fetch_db_published_posts(
        conn,
        persona_key=settings.persona_key,
        lookback_days=days,
    )
    if not posts:
        LOGGER.info("No published posts found in DB for lookback=%d days", days)
        return 0

    reels = fetch_instagram_reels(settings=settings, lookback_days=days)
    if reels:
        matched_reels = match_reels_to_posts(reels, posts)
        if matched_reels:
            total_stored += store_reel_metrics(conn, matched_reels, persona_key=settings.persona_key)
    else:
        LOGGER.info("No Instagram reels fetched from Metricool")

    tiktok_videos = fetch_tiktok_videos(settings=settings, lookback_days=days)
    if tiktok_videos:
        matched_tiktok = match_tiktok_videos_to_posts(tiktok_videos, posts)
        if matched_tiktok:
            total_stored += store_tiktok_metrics(conn, matched_tiktok, persona_key=settings.persona_key)
    else:
        LOGGER.info("No TikTok videos fetched from Metricool")

    return total_stored
