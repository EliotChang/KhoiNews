from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import logging
import re
import time
from typing import Any, TypeVar
from urllib.parse import urlparse

from pipeline.article_media import MediaAssetResult, MediaCandidate, extract_article_context, extract_best_media_from_article
from pipeline.config import bootstrap_runtime_env, load_settings
from pipeline.content_gen import generate_content_pack, validate_script_for_profile, validate_source_context
from pipeline.db import (
    clear_source_gate_failure,
    create_pipeline_run,
    db_connection,
    ensure_pipeline_schema,
    finish_pipeline_run,
    get_quality_baseline_summary,
    is_source_gate_suppressed,
    link_exists_in_source_posts,
    list_published_links_for_platforms,
    list_recent_published_article_links,
    list_recent_series_tags,
    record_source_gate_failure,
    update_content_asset_thumbnail,
    upsert_persona_profile,
    upsert_content_asset,
    upsert_post_quality_evaluation,
    upsert_media_asset,
    upsert_source_post,
    upsert_video_asset,
    upsert_voice_asset,
)
from pipeline.thumbnail_gen import generate_thumbnail, upload_thumbnail_to_supabase
from pipeline.media_quality import MediaQualityGateConfig, enforce_image_quality_gate
from pipeline.publish import (
    MAX_AUDIO_SECONDS,
    MAX_AUDIO_VIDEO_DELTA_SECONDS,
    MAX_VIDEO_SECONDS,
    MIN_AUDIO_SECONDS,
    MIN_VIDEO_SECONDS,
    MediaPublishPayload,
    dispatch_ready_publish_jobs,
    enqueue_publish_jobs_for_post,
    update_metricool_link_in_bio,
)
from pipeline.wj_ingest import (
    IngestResult,
    WJFeedUnavailableError,
    SourcePostInput,
    fetch_wj_posts,
)
from pipeline.metricool_analytics import fetch_and_store_metricool_analytics
from pipeline.publish_quality import evaluate_publish_quality
from pipeline.quality_feedback import SignalBoost, analyze_quality_performance_feedback, analyze_signal_performance
from pipeline.video_gen import generate_fish_lipsync_video
from pipeline.voice_gen import generate_elevenlabs_voice


T = TypeVar("T")
LOGGER = logging.getLogger("wj_pipeline")
PRE_VOICE_GATE_NAME = "pre_voice"
ENGAGEMENT_SCORING_VERSION = "v1"
_ENGAGEMENT_IMPACT_KEYWORDS = (
    "policy", "congress", "senate", "house", "supreme court", "scotus",
    "federal", "government", "sanction", "war", "ceasefire", "military",
    "inflation", "interest rate", "fed", "unemployment", "recession", "gdp",
    "tariff", "regulation", "lawsuit", "investigation", "safety", "health",
    "earthquake", "hurricane", "outbreak", "security", "pentagon",
    "ai", "artificial intelligence", "ceo", "startup", "acquisition",
    "antitrust", "data breach", "privacy", "layoff", "ipo",
    "openai", "google", "apple", "microsoft", "meta", "amazon",
    "cybersecurity", "hack", "exploit", "outage",
)
_ENGAGEMENT_HOOK_PATTERNS = (
    re.compile(r"\?"),
    re.compile(r"\b\d+(?:\.\d+)?%?\b"),
    re.compile(r"\b(?:accused|charged|banned|fired|leaked|explodes|crash|crashes)\b"),
    re.compile(r"\b(?:just in|breaking|urgent|alert)\b"),
    re.compile(r"\b(?:musk|trump|biden|zuckerberg|swift|bezos)\b"),
    re.compile(r"\b(?:ceo|founder|steps? down|resign|launch|unveil|acquire)\b"),
    re.compile(r"\b(?:you|your|you're)\b"),
    re.compile(r"^\s*\$?\d[\d,.]*[%$]?"),
    re.compile(r"\b(?:today|tonight|right now|this week|starting)\b"),
)
_ENGAGEMENT_HARD_NEWS_KEYWORDS = (
    "policy", "congress", "senate", "house", "supreme court", "scotus",
    "federal", "war", "ceasefire", "military", "inflation", "interest rate",
    "economy", "market", "tariff", "regulation", "lawsuit", "investigation",
    "public health", "security", "energy",
)
_ENGAGEMENT_GOVERNMENT_GEO_KEYWORDS = (
    "policy", "congress", "senate", "house", "supreme court", "scotus",
    "federal", "government", "doj", "justice department", "fbi", "irs",
    "state department", "pentagon", "governor", "attorney general",
    "war", "ceasefire", "military", "sanction", "nato", "treaty",
    "iran", "china", "russia", "ukraine", "nuclear", "missile",
    "diplomacy", "embassy", "cartel", "passport", "dhs", "tsa",
    "election", "primary", "ballot",
)
_ENGAGEMENT_CULTURE_KEYWORDS = (
    "celebrity", "music", "movie", "film", "fashion", "sports", "nfl", "nba",
    "fifa", "olympics", "tiktok", "instagram", "viral", "influencer",
)
_ENGAGEMENT_CREDIBLE_HOST_HINTS = (
    "reuters.com", "apnews.com", "npr.org", "bbc.", "wsj.com", "ft.com",
    "bloomberg.com", "nytimes.com", "washingtonpost.com", "theguardian.com",
    "economist.com", "axios.com", "politico.com",
)
_ENGAGEMENT_PENALTY_PATTERNS: tuple[tuple[str, re.Pattern[str], float], ...] = (
    ("listicle_or_review", re.compile(r"\b(?:review|reviews|hands[- ]on|unboxing|first look|comparison|compared|vs\.?)\b"), 0.2),
    ("commerce_or_affiliate", re.compile(r"\b(?:deal|deals|coupon|buy now|shop now|affiliate|discount)\b"), 0.18),
    ("opinion_or_editorial", re.compile(r"\b(?:opinion|editorial|op-ed|columnist)\b"), 0.16),
    ("abstract_literary", re.compile(r"\b(?:memoir|essay|literary|diary|chronicles?|meditation|reflection)\b"), 0.15),
)
_ENGAGEMENT_STOPWORDS = {
    "the", "and", "for", "that", "with", "from", "this", "have", "your",
    "into", "after", "over", "under", "about", "their", "they", "will",
    "would", "there", "were", "where", "when", "what", "which", "while",
    "than", "then", "just", "more", "most", "very",
}


def _log_wj_config(*, wj_base_url: str) -> None:
    LOGGER.info("World Journal base URL: %s", wj_base_url)


class BatchProcessingResult:
    def __init__(
        self,
        *,
        posts_processed: int,
        new_posts_seen: int,
        existing_posts_seen: int,
        jobs_enqueued: int,
        buffer_jobs_enqueued: int,
        run_errors: list[dict[str, str]],
        skip_reason_counts: dict[str, int],
    ) -> None:
        self.posts_processed = posts_processed
        self.new_posts_seen = new_posts_seen
        self.existing_posts_seen = existing_posts_seen
        self.jobs_enqueued = jobs_enqueued
        self.buffer_jobs_enqueued = buffer_jobs_enqueued
        self.run_errors = run_errors
        self.skip_reason_counts = skip_reason_counts


def run_with_retry(fn: Callable[[], T], *, retries: int = 3, base_sleep_seconds: float = 1.2) -> T:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt == retries:
                break
            sleep_seconds = base_sleep_seconds * attempt
            LOGGER.warning("Attempt %s/%s failed. Retrying in %.1fs. Error: %s", attempt, retries, sleep_seconds, exc)
            time.sleep(sleep_seconds)

    assert last_error is not None
    raise last_error


def _published_sort_value(post: SourcePostInput) -> float:
    if not post.published_at:
        return 0.0
    return post.published_at.timestamp()


def _fallback_top_posts(posts: list[SourcePostInput], limit: int = 3) -> list[SourcePostInput]:
    ranked = sorted(posts, key=lambda post: (-_published_sort_value(post), post.source_guid))
    return ranked[:limit]


def _is_publishable_mp4_url(url: str) -> bool:
    path = urlparse(url).path.lower()
    return path.endswith(".mp4")


def _normalize_link_for_coverage(link: str) -> str:
    normalized = str(link or "").strip().lower()
    if not normalized:
        return ""
    if normalized.endswith("/") and len(normalized) > 1:
        normalized = normalized[:-1]
    return normalized


def _link_coverage_variants(link: str) -> set[str]:
    normalized = _normalize_link_for_coverage(link)
    if not normalized:
        return set()
    variants = {normalized}
    if not normalized.endswith("/"):
        variants.add(normalized + "/")
    return variants


def _split_external_ids(external_post_id: str) -> set[str]:
    tokens = re.split(r"[\s,]+", str(external_post_id or "").strip())
    return {token for token in tokens if token}


def _covered_links_from_live_inventory(
    *,
    published_link_rows: list[dict[str, str]],
    live_external_ids_by_platform: dict[str, set[str]],
) -> set[str]:
    covered_links: set[str] = set()
    for row in published_link_rows:
        platform = str(row.get("platform") or "").strip().lower()
        if not platform:
            continue
        live_ids = live_external_ids_by_platform.get(platform, set())
        if not live_ids:
            continue

        external_ids = _split_external_ids(str(row.get("external_post_id") or ""))
        if not external_ids or not (external_ids & live_ids):
            continue

        covered_links.update(_link_coverage_variants(str(row.get("link") or "")))

    return covered_links


def _live_external_ids_by_platform(settings: Any) -> dict[str, set[str]]:
    del settings
    return {}


def _collect_live_covered_links(*, conn: Any, settings: Any) -> set[str]:
    if bool(getattr(settings, "allow_duplicate_link_repost", False)):
        LOGGER.info("Link dedupe disabled for this run via ALLOW_DUPLICATE_LINK_REPOST")
        return set()

    persona_key = str(getattr(settings, "persona_key", "default") or "default").strip() or "default"
    publish_platforms = [
        str(platform).strip().lower()
        for platform in getattr(settings, "publish_platforms", [])
        if str(platform).strip()
    ]
    if not publish_platforms:
        return set()

    try:
        published_rows = list_published_links_for_platforms(
            conn,
            persona_key=persona_key,
            platforms=publish_platforms,
        )
    except Exception as inventory_error:  # noqa: BLE001
        LOGGER.warning("Failed loading published link inventory error=%s", inventory_error)
        return set()

    covered_links: set[str] = set()
    for row in published_rows:
        covered_links.update(_link_coverage_variants(str(row.get("link") or "")))
    return covered_links


def _buffer_jobs_target_for_run(*, total_buffer_jobs: int, initial_queue_size: int) -> int:
    safe_initial_queue_size = max(1, initial_queue_size)
    if total_buffer_jobs < safe_initial_queue_size:
        return safe_initial_queue_size - total_buffer_jobs
    return 1


def _next_utc_schedule_anchor(*, spacing_hours: int, now_utc: datetime | None = None) -> datetime:
    spacing = max(1, spacing_hours)
    current = (now_utc or datetime.now(timezone.utc)).astimezone(timezone.utc).replace(microsecond=0)
    floored_hour = (current.hour // spacing) * spacing
    anchor = current.replace(hour=floored_hour, minute=0, second=0, microsecond=0)
    if anchor <= current:
        anchor += timedelta(hours=spacing)
    return anchor


def _scheduled_slot_iso(*, anchor_utc: datetime, spacing_hours: int, slot_index: int) -> str:
    slot_dt = anchor_utc + timedelta(hours=max(1, spacing_hours) * max(0, slot_index))
    return slot_dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _word_count(value: str) -> int:
    return len(re.sub(r"\s+", " ", str(value or "").strip()).split())


def _content_topic_bucket(*, title: str, description: str, extracted_context: str) -> str:
    combined = f"{title} {description} {extracted_context}".lower()
    gov_geo_hits = sum(1 for term in _ENGAGEMENT_GOVERNMENT_GEO_KEYWORDS if term in combined)
    hard_hits = sum(1 for term in _ENGAGEMENT_HARD_NEWS_KEYWORDS if term in combined)
    culture_hits = sum(1 for term in _ENGAGEMENT_CULTURE_KEYWORDS if term in combined)
    if gov_geo_hits >= 2 and gov_geo_hits >= culture_hits:
        return "government_geo"
    if hard_hits > culture_hits:
        return "hard_news_other"
    if culture_hits > hard_hits:
        return "culture"
    return "general"


def _timeliness_score(*, published_at: datetime | None, now_utc: datetime) -> tuple[float, bool]:
    if published_at is None:
        return 0.35, False
    age_hours = max(0.0, (now_utc - published_at.astimezone(timezone.utc)).total_seconds() / 3600.0)
    if age_hours <= 6:
        return 1.0, False
    if age_hours <= 12:
        return 0.92, False
    if age_hours <= 24:
        return 0.8, False
    if age_hours <= 36:
        return 0.62, False
    if age_hours <= 48:
        return 0.45, False
    if age_hours <= 72:
        return 0.28, False
    return 0.1, True


def _impact_signal_score(*, title: str, description: str, extracted_context: str) -> float:
    combined = f"{title} {description} {extracted_context}".lower()
    hits = sum(1 for keyword in _ENGAGEMENT_IMPACT_KEYWORDS if keyword in combined)
    return min(1.0, hits / 4.0)


def _hook_strength_score(*, title: str, description: str) -> float:
    combined = f"{title} {description}".lower()
    hits = sum(1 for pattern in _ENGAGEMENT_HOOK_PATTERNS if pattern.search(combined))
    return min(1.0, hits / 5.0)


def _specificity_score(*, description: str, extracted_context: str) -> float:
    combined = f"{description} {extracted_context}".lower()
    tokens = [
        token for token in re.findall(r"[a-z0-9][a-z0-9'-]{2,}", combined)
        if token not in _ENGAGEMENT_STOPWORDS
    ]
    if not tokens:
        return 0.0
    unique_ratio = len(set(tokens)) / len(tokens)
    return min(1.0, unique_ratio / 0.6)


def _context_richness_score(
    *,
    description_words: int,
    extracted_context_words: int,
    specificity: float,
    min_context_words: int,
) -> float:
    context_words = max(description_words, extracted_context_words)
    richness = min(1.0, context_words / max(float(min_context_words * 2), 80.0))
    return min(1.0, (richness * 0.72) + (specificity * 0.28))


def _credibility_score(*, post: SourcePostInput, article_host: str) -> float:
    if any(hint in article_host for hint in _ENGAGEMENT_CREDIBLE_HOST_HINTS):
        return 1.0
    if post.source == "world_journal":
        return 0.85
    if post.source.startswith("fallback_reuters") or post.source.startswith("fallback_feeds_reuters"):
        return 0.95
    if post.source.startswith("fallback_apnews") or post.source.startswith("fallback_feeds_apnews"):
        return 0.92
    return 0.62


_TIKTOK_CONTROVERSY_TERMS = (
    "accused", "charged", "banned", "fired", "leaked", "scandal",
    "outrage", "backlash", "lawsuit", "fraud", "exposed", "caught",
    "slammed", "ripped", "blasted", "destroys", "claps back",
)
_TIKTOK_EMOTION_TERMS = (
    "shocking", "insane", "wild", "terrifying", "heartbreaking",
    "furious", "stunned", "unbelievable", "disgusting", "disturbing",
)
_TIKTOK_NAME_RECOGNITION = (
    "trump", "biden", "musk", "zuckerberg", "swift", "bezos",
    "kardashian", "rihanna", "drake", "obama", "putin", "zelensky",
    "gates", "oprah", "beyonce", "lebron", "ronaldo", "messi",
)


def _platform_fit_score(*, title: str, description: str, hook_strength: float) -> float:
    title_words = _word_count(title)
    if 7 <= title_words <= 15:
        length_score = 1.0
    elif 5 <= title_words <= 18:
        length_score = 0.78
    else:
        length_score = 0.55
    punctuation_bonus = 0.1 if "?" in title or ":" in title else 0.0
    ig_fit = min(1.0, (length_score * 0.75) + (hook_strength * 0.2) + punctuation_bonus)

    combined = f"{title} {description}".lower()
    controversy_hits = sum(1 for term in _TIKTOK_CONTROVERSY_TERMS if term in combined)
    controversy_score = min(1.0, controversy_hits / 2.0)

    emotion_hits = sum(1 for term in _TIKTOK_EMOTION_TERMS if term in combined)
    emotion_score = min(1.0, emotion_hits / 2.0)

    name_hits = sum(1 for name in _TIKTOK_NAME_RECOGNITION if name in combined)
    name_score = min(1.0, name_hits / 1.0)

    tiktok_fit = min(1.0, (hook_strength * 0.3) + (controversy_score * 0.25) + (name_score * 0.25) + (emotion_score * 0.2))

    return min(1.0, (ig_fit * 0.5) + (tiktok_fit * 0.5))


_CIVIC_SCALE_KEYWORDS = (
    "billion", "million", "trillion", "percent", "nationwide", "worldwide",
    "every american", "all users", "all workers", "all students",
    "thousands", "hundreds of thousands", "tens of millions",
)
_CIVIC_ACTION_KEYWORDS = (
    "signed", "enacted", "ruled", "ordered", "indicted", "charged",
    "banned", "approved", "vetoed", "overturned", "struck down",
    "sentenced", "convicted", "impeached", "sanctioned", "subpoenaed",
    "recalled", "suspended", "revoked", "blocked",
)
_CIVIC_PRECEDENT_KEYWORDS = (
    "first", "unprecedented", "new law", "new rule", "landmark",
    "historic", "never before", "first time", "record high", "record low",
    "all-time", "highest ever", "lowest ever",
)
_CIVIC_POPULATION_KEYWORDS = (
    "jobs", "healthcare", "housing", "wages", "prices", "taxes",
    "benefits", "schools", "pensions", "insurance", "rent", "tuition",
    "social security", "medicare", "medicaid", "veterans",
    "drinking water", "food safety", "drug prices",
)


def _civic_importance_score(*, title: str, description: str, extracted_context: str) -> float:
    combined = f"{title} {description} {extracted_context}".lower()
    scale_hits = sum(1 for kw in _CIVIC_SCALE_KEYWORDS if kw in combined)
    action_hits = sum(1 for kw in _CIVIC_ACTION_KEYWORDS if kw in combined)
    precedent_hits = sum(1 for kw in _CIVIC_PRECEDENT_KEYWORDS if kw in combined)
    population_hits = sum(1 for kw in _CIVIC_POPULATION_KEYWORDS if kw in combined)
    total = scale_hits + action_hits + precedent_hits + population_hits
    return min(1.0, total / 5.0)


def _mix_adjustment(*, topic_bucket: str, hook_strength: float, content_mix_profile: str) -> float:
    profile = str(content_mix_profile or "hard_news_culture").strip().lower()
    if profile == "balanced_geo":
        if topic_bucket == "government_geo":
            return 0.06
        if topic_bucket == "hard_news_other":
            return 0.02
        if topic_bucket == "culture":
            return 0.01
        return 0.0
    if profile == "hard_news_only":
        if topic_bucket in ("government_geo", "hard_news_other"):
            return 0.08
        if topic_bucket == "culture":
            return -0.06
        return -0.01
    if profile == "culture_forward":
        if topic_bucket == "culture":
            return 0.08 if hook_strength >= 0.35 else 0.04
        if topic_bucket in ("government_geo", "hard_news_other"):
            return -0.01
        return 0.0
    if profile == "tiktok_optimized":
        if topic_bucket == "culture":
            return 0.08 if hook_strength >= 0.30 else 0.05
        if topic_bucket == "government_geo":
            return 0.03 if hook_strength >= 0.45 else 0.0
        if topic_bucket == "hard_news_other":
            return 0.04 if hook_strength >= 0.40 else 0.01
        return 0.0
    if profile == "engagement_adaptive":
        if hook_strength >= 0.5:
            return 0.06
        if topic_bucket == "culture" and hook_strength >= 0.30:
            return 0.05
        if topic_bucket in ("government_geo", "hard_news_other"):
            return 0.04
        return 0.01
    if topic_bucket in ("government_geo", "hard_news_other"):
        return 0.06
    if topic_bucket == "culture":
        return 0.04 if hook_strength >= 0.35 else 0.01
    return 0.0


def _penalty_breakdown(
    *,
    post: SourcePostInput,
    title: str,
    description: str,
    article_host: str,
    topic_bucket: str,
    context_words: int,
    min_context_words: int,
    stale_story: bool,
    domain_count: int,
    topic_count: int,
) -> tuple[float, list[str]]:
    del article_host
    combined = f"{title} {description} {post.link}".lower()
    total_penalty = 0.0
    reasons: list[str] = []
    for label, pattern, penalty_value in _ENGAGEMENT_PENALTY_PATTERNS:
        if pattern.search(combined):
            total_penalty += penalty_value
            reasons.append(label)
    if stale_story:
        total_penalty += 0.14
        reasons.append("stale_story")
    if min_context_words <= context_words < (min_context_words + 15):
        total_penalty += 0.07
        reasons.append("weak_context_margin")
    if domain_count > 1:
        repetitive_domain_penalty = min(0.12, 0.035 * (domain_count - 1))
        total_penalty += repetitive_domain_penalty
        reasons.append("repetitive_domain")
    if topic_count > 1 and topic_bucket != "general":
        repetitive_topic_penalty = min(0.1, 0.03 * (topic_count - 1))
        total_penalty += repetitive_topic_penalty
        reasons.append("repetitive_topic")
    return min(0.45, total_penalty), reasons


def _upsert_post_payload(
    *,
    post: SourcePostInput,
    extracted_context: str,
    extracted_context_words: int,
    engagement_payload: dict[str, Any],
) -> SourcePostInput:
    raw_payload = dict(post.raw_payload) if isinstance(post.raw_payload, dict) else {}
    if extracted_context:
        raw_payload["pre_extracted_context"] = extracted_context
    raw_payload["pre_extracted_context_words"] = extracted_context_words
    raw_payload["engagement"] = engagement_payload
    return replace(post, raw_payload=raw_payload)


def _mark_floor_backfill_selected(post: SourcePostInput) -> SourcePostInput:
    raw_payload = dict(post.raw_payload) if isinstance(post.raw_payload, dict) else {}
    engagement_payload = dict(raw_payload.get("engagement") or {})
    engagement_payload["status"] = "selected_floor_backfill"
    raw_payload["engagement"] = engagement_payload
    return replace(post, raw_payload=raw_payload)


def _compute_performance_boost(
    *,
    topic_bucket: str,
    title: str,
    description: str,
    signal_boosts: list[SignalBoost],
) -> float:
    if not signal_boosts:
        return 0.0
    total = 0.0
    for boost in signal_boosts:
        if boost.dimension == "topic_category":
            topic_map = {
                "government_geo": "government",
                "hard_news_other": "tech",
            }
            mapped_bucket = topic_map.get(topic_bucket, topic_bucket)
            if boost.value == mapped_bucket or boost.value == topic_bucket:
                total += boost.boost
        elif boost.dimension == "hook_type":
            combined = f"{title} {description}".lower()
            hook_match = False
            if boost.value == "question" and "?" in combined:
                hook_match = True
            elif boost.value == "name-drop" and any(name in combined for name in ("trump", "musk", "biden", "zuckerberg", "swift", "bezos")):
                hook_match = True
            elif boost.value == "number-lead" and re.search(r"\b\d+[%$]?\b", title):
                hook_match = True
            elif boost.value == "controversy" and any(word in combined for word in ("accused", "charged", "banned", "fired", "leaked", "crash")):
                hook_match = True
            elif boost.value == "breaking-event" and any(word in combined for word in ("just", "breaking", "now", "urgent")):
                hook_match = True
            if hook_match:
                total += boost.boost
    return max(-0.08, min(0.08, total))


def select_top_headlines_with_engagement(
    *,
    posts: list[SourcePostInput],
    settings: Any,
    candidate_origin: str,
    top_n: int,
    min_score: float,
    signal_boosts: list[SignalBoost] | None = None,
) -> tuple[list[SourcePostInput], dict[str, int]]:
    if not posts:
        return [], {"total_candidates": 0, "below_threshold_count": 0}

    scoring_enabled = bool(getattr(settings, "engagement_scoring_enabled", True))
    now_utc = datetime.now(timezone.utc)
    evaluated_rows: list[dict[str, Any]] = []
    topic_counter: Counter[str] = Counter()
    domain_counter: Counter[str] = Counter()

    for post in posts:
        extracted_context = ""
        if bool(getattr(settings, "pre_voice_metadata_enrichment_enabled", True)):
            preloaded = ""
            if isinstance(post.raw_payload, dict):
                preloaded = str(post.raw_payload.get("pre_extracted_context", "")).strip()
            if preloaded:
                extracted_context = preloaded
            else:
                try:
                    extracted_context = run_with_retry(
                        lambda: extract_article_context(
                            page_url=post.link,
                            timeout_seconds=getattr(settings, "request_timeout_seconds", 20),
                            max_words=getattr(settings, "article_context_max_words", 220),
                        ),
                        retries=2,
                        base_sleep_seconds=0.6,
                    )
                except Exception as context_error:  # noqa: BLE001
                    LOGGER.warning(
                        "Engagement context extraction failed source=%s guid=%s error=%s",
                        post.source,
                        post.source_guid,
                        context_error,
                    )
        extracted_context_words = _word_count(extracted_context)
        description_words = _word_count(post.description)
        article_host = _normalized_host(urlparse(post.link).netloc)
        topic_bucket = _content_topic_bucket(
            title=post.title,
            description=post.description,
            extracted_context=extracted_context,
        )
        topic_counter.update([topic_bucket])
        if article_host:
            domain_counter.update([article_host])
        evaluated_rows.append(
            {
                "post": post,
                "extracted_context": extracted_context,
                "extracted_context_words": extracted_context_words,
                "description_words": description_words,
                "article_host": article_host,
                "topic_bucket": topic_bucket,
            }
        )

    selected_posts: list[SourcePostInput] = []
    below_threshold_count = 0

    for row in evaluated_rows:
        post = row["post"]
        extracted_context = row["extracted_context"]
        extracted_context_words = int(row["extracted_context_words"])
        description_words = int(row["description_words"])
        article_host = str(row["article_host"])
        topic_bucket = str(row["topic_bucket"])
        published_sort_value = _published_sort_value(post)
        components: dict[str, float] = {}
        penalties: list[str] = []
        penalty_total = 0.0

        if scoring_enabled:
            timeliness, stale_story = _timeliness_score(published_at=post.published_at, now_utc=now_utc)
            impact = _impact_signal_score(
                title=post.title,
                description=post.description,
                extracted_context=extracted_context,
            )
            hook_strength = _hook_strength_score(title=post.title, description=post.description)
            specificity = _specificity_score(
                description=post.description,
                extracted_context=extracted_context,
            )
            context_richness = _context_richness_score(
                description_words=description_words,
                extracted_context_words=extracted_context_words,
                specificity=specificity,
                min_context_words=max(1, int(getattr(settings, "article_context_min_words", 40))),
            )
            credibility = _credibility_score(post=post, article_host=article_host)
            ig_fit = _platform_fit_score(title=post.title, description=post.description, hook_strength=hook_strength)
            civic_importance = _civic_importance_score(
                title=post.title,
                description=post.description,
                extracted_context=extracted_context,
            )
            mix_adjustment = _mix_adjustment(
                topic_bucket=topic_bucket,
                hook_strength=hook_strength,
                content_mix_profile=str(getattr(settings, "content_mix_profile", "hard_news_culture")),
            )
            penalty_total, penalties = _penalty_breakdown(
                post=post,
                title=post.title,
                description=post.description,
                article_host=article_host,
                topic_bucket=topic_bucket,
                context_words=max(description_words, extracted_context_words),
                min_context_words=max(1, int(getattr(settings, "article_context_min_words", 40))),
                stale_story=stale_story,
                domain_count=domain_counter.get(article_host, 0),
                topic_count=topic_counter.get(topic_bucket, 0),
            )
            components = {
                "timeliness": timeliness,
                "impact": impact,
                "civic_importance": civic_importance,
                "hook_strength": hook_strength,
                "context_richness": context_richness,
                "credibility": credibility,
                "ig_fit": ig_fit,
                "mix_adjustment": mix_adjustment,
            }
            performance_boost = _compute_performance_boost(
                topic_bucket=topic_bucket,
                title=post.title,
                description=post.description,
                signal_boosts=signal_boosts or [],
            )
            if performance_boost != 0.0:
                components["performance_boost"] = performance_boost
            raw_score = (
                (timeliness * 0.20)
                + (impact * 0.22)
                + (civic_importance * 0.12)
                + (hook_strength * 0.14)
                + (context_richness * 0.16)
                + (credibility * 0.12)
                + (ig_fit * 0.04)
                + mix_adjustment
                + performance_boost
            )
            score = max(0.0, min(1.0, raw_score - penalty_total))
        else:
            components = {"recency_only": 1.0}
            score = 1.0

        row["score"] = score
        row["components"] = components
        row["penalties"] = penalties
        row["penalty_total"] = penalty_total
        row["published_sort_value"] = published_sort_value
        row["civic_importance"] = float(components.get("civic_importance", 0.0))

    sorted_rows = sorted(
        evaluated_rows,
        key=lambda row: (
            -float(row["score"]),
            -float(row["published_sort_value"]),
            str(row["post"].source_guid),
        ),
    )

    for row in sorted_rows:
        post = row["post"]
        score = float(row["score"])
        penalties = list(row["penalties"])
        status = "selected"
        if scoring_enabled and score < min_score:
            below_threshold_count += 1
            status = "rejected_penalty" if penalties else "rejected_low_score"

        engagement_payload = {
            "scoring_version": ENGAGEMENT_SCORING_VERSION,
            "scoring_enabled": scoring_enabled,
            "candidate_origin": candidate_origin,
            "score": round(score, 4),
            "threshold": round(min_score, 4),
            "components": {name: round(float(value), 4) for name, value in dict(row["components"]).items()},
            "penalties": penalties,
            "penalty_total": round(float(row["penalty_total"]), 4),
            "topic_bucket": row["topic_bucket"],
            "status": status,
        }
        scored_post = _upsert_post_payload(
            post=post,
            extracted_context=row["extracted_context"],
            extracted_context_words=int(row["extracted_context_words"]),
            engagement_payload=engagement_payload,
        )
        LOGGER.info(
            "Engagement candidate source=%s guid=%s origin=%s status=%s score=%.3f threshold=%.3f components=%s penalties=%s",
            post.source,
            post.source_guid,
            candidate_origin,
            status,
            score,
            min_score,
            engagement_payload["components"],
            penalties or "none",
        )
        if status == "selected" and len(selected_posts) < max(1, top_n):
            selected_posts.append(scored_post)

    if scoring_enabled and len(selected_posts) >= 2:
        selected_guids = {p.source_guid for p in selected_posts}
        importance_ranked = sorted(
            [r for r in sorted_rows if float(r["score"]) >= min_score],
            key=lambda r: -float(r["civic_importance"]),
        )
        best_importance_row = next(
            (r for r in importance_ranked if r["post"].source_guid not in selected_guids),
            None,
        )
        if best_importance_row and float(best_importance_row["civic_importance"]) >= 0.4:
            worst_idx = min(range(len(selected_posts)), key=lambda i: _get_civic_importance(selected_posts[i]))
            worst_importance = _get_civic_importance(selected_posts[worst_idx])
            if float(best_importance_row["civic_importance"]) > worst_importance:
                swap_post = best_importance_row["post"]
                swap_payload = {
                    "scoring_version": ENGAGEMENT_SCORING_VERSION,
                    "scoring_enabled": scoring_enabled,
                    "candidate_origin": candidate_origin,
                    "score": round(float(best_importance_row["score"]), 4),
                    "threshold": round(min_score, 4),
                    "components": {k: round(float(v), 4) for k, v in dict(best_importance_row["components"]).items()},
                    "penalties": list(best_importance_row["penalties"]),
                    "penalty_total": round(float(best_importance_row["penalty_total"]), 4),
                    "topic_bucket": best_importance_row["topic_bucket"],
                    "status": "selected_importance_slot",
                }
                swap_scored = _upsert_post_payload(
                    post=swap_post,
                    extracted_context=best_importance_row["extracted_context"],
                    extracted_context_words=int(best_importance_row["extracted_context_words"]),
                    engagement_payload=swap_payload,
                )
                LOGGER.info(
                    "Importance slot: swapping in guid=%s (civic_importance=%.2f) for guid=%s (civic_importance=%.2f)",
                    swap_post.source_guid,
                    float(best_importance_row["civic_importance"]),
                    selected_posts[worst_idx].source_guid,
                    worst_importance,
                )
                selected_posts[worst_idx] = swap_scored

    return selected_posts, {
        "total_candidates": len(posts),
        "below_threshold_count": below_threshold_count,
    }


def _get_civic_importance(post: SourcePostInput) -> float:
    if isinstance(post.raw_payload, dict):
        engagement = post.raw_payload.get("engagement", {})
        if isinstance(engagement, dict):
            components = engagement.get("components", {})
            if isinstance(components, dict):
                return float(components.get("civic_importance", 0.0))
    return 0.0


def select_top_headlines(
    *,
    posts: list[SourcePostInput],
    top_n: int = 3,
) -> list[SourcePostInput]:
    ranked = sorted(posts, key=lambda post: (-_published_sort_value(post), post.source_guid))
    return ranked[:top_n]


def _ordered_fallback_feed_urls(*, feed_urls: list[str], world_first: bool) -> list[str]:
    if not world_first:
        return list(feed_urls)

    def _priority(url: str) -> int:
        host = urlparse(url).netloc.lower()
        if "reuters" in host:
            return 0
        if "apnews" in host or host.endswith("ap.org"):
            return 1
        if "npr" in host:
            return 2
        return 3

    return sorted(feed_urls, key=lambda url: (_priority(url), url))


def _normalized_host(value: str) -> str:
    host = value.strip().lower()
    if host.startswith("www."):
        return host[4:]
    return host


def _matches_blocked_domain(*, host: str, blocked_domain: str) -> bool:
    if not blocked_domain:
        return False
    normalized_host = _normalized_host(host)
    normalized_blocked = _normalized_host(blocked_domain)
    return normalized_host == normalized_blocked or normalized_host.endswith(f".{normalized_blocked}")


def _blocked_reason_for_post(
    *,
    post: SourcePostInput,
    topic_blocklist_enabled: bool,
    topic_block_terms: list[str],
    source_domain_blocklist: list[str],
) -> str | None:
    if post.source == "world_journal":
        return None

    if not topic_blocklist_enabled:
        return None

    link_host = _normalized_host(urlparse(post.link).netloc)
    if link_host:
        for blocked_domain in source_domain_blocklist:
            if _matches_blocked_domain(host=link_host, blocked_domain=blocked_domain):
                return f"domain_blocked:{blocked_domain}"

    if isinstance(post.raw_payload, dict):
        feed_url = str(post.raw_payload.get("feed_url", "")).strip()
        feed_host = _normalized_host(urlparse(feed_url).netloc)
        if feed_host:
            for blocked_domain in source_domain_blocklist:
                if _matches_blocked_domain(host=feed_host, blocked_domain=blocked_domain):
                    return f"domain_blocked:{blocked_domain}"

    searchable_text = " ".join(
        [
            re.sub(r"\s+", " ", post.title or "").lower(),
            re.sub(r"\s+", " ", post.description or "").lower(),
            re.sub(r"\s+", " ", post.link or "").lower(),
            re.sub(r"\s+", " ", post.source or "").lower(),
        ]
    )
    for term in topic_block_terms:
        normalized_term = term.strip().lower()
        if normalized_term and normalized_term in searchable_text:
            return f"topic_blocked:{normalized_term}"
    return None


def _filter_blocked_posts(
    *,
    posts: list[SourcePostInput],
    topic_blocklist_enabled: bool,
    topic_block_terms: list[str],
    source_domain_blocklist: list[str],
) -> tuple[list[SourcePostInput], int]:
    if not posts:
        return posts, 0

    allowed_posts: list[SourcePostInput] = []
    blocked_count = 0
    for post in posts:
        blocked_reason = _blocked_reason_for_post(
            post=post,
            topic_blocklist_enabled=topic_blocklist_enabled,
            topic_block_terms=topic_block_terms,
            source_domain_blocklist=source_domain_blocklist,
        )
        if not blocked_reason:
            allowed_posts.append(post)
            continue
        blocked_count += 1
        LOGGER.info(
            "Skipping blocked post source=%s guid=%s reason=%s",
            post.source,
            post.source_guid,
            blocked_reason,
        )
    return allowed_posts, blocked_count


def _fallback_media_from_web_thumbnail(
    *,
    settings: Any,
    post_id: str,
    title: str,
    script_text: str,
    article_image_url: str | None = None,
) -> MediaAssetResult | None:
    try:
        # Reuse existing thumbnail sourcing for restricted/paywalled pages.
        fallback_thumbnail = None
        strategies = ["web-sourced", "gemini-generated"]
        if article_image_url:
            strategies.insert(0, "article-image")
        for strategy in strategies:
            fallback_thumbnail = generate_thumbnail(
                settings=settings,
                strategy=strategy,
                title=title,
                script=script_text,
                article_image_url=article_image_url,
            )
            if fallback_thumbnail:
                break
        if not fallback_thumbnail:
            return None
        fallback_media_url = upload_thumbnail_to_supabase(
            settings=settings,
            post_id=f"{post_id}-media-fallback",
            thumbnail=fallback_thumbnail,
        )
        LOGGER.info(
            "Applied web-thumbnail media fallback post_id=%s source=%s",
            post_id,
            fallback_thumbnail.source,
        )
        return MediaAssetResult(
            media_type="image",
            media_url=fallback_media_url,
            selection_reason=f"fallback:{fallback_thumbnail.source}",
            media_candidates=[
                MediaCandidate(
                    media_type="image",
                    media_url=fallback_media_url,
                    selection_reason=f"fallback:{fallback_thumbnail.source}",
                    priority=-10,
                )
            ],
            quality_summary={
                "fallback_source": fallback_thumbnail.source,
                "fallback_description": fallback_thumbnail.description,
                "fallback_url": fallback_thumbnail.url,
            },
        )
    except Exception as fallback_error:  # noqa: BLE001
        LOGGER.warning(
            "Media fallback generation failed post_id=%s error=%s",
            post_id,
            fallback_error,
        )
        return None


def _media_candidates_as_dicts(media: MediaAssetResult | None) -> list[dict[str, Any]]:
    if not media:
        return []
    return [
        {
            "media_type": candidate.media_type,
            "media_url": candidate.media_url,
            "selection_reason": candidate.selection_reason,
            "priority": candidate.priority,
        }
        for candidate in media.media_candidates
    ]


def _augment_media_with_supplemental_images(
    *,
    settings: Any,
    post_id: str,
    title: str,
    script_text: str,
    base_media: MediaAssetResult | None,
    desired_image_count: int,
) -> MediaAssetResult | None:
    if base_media is None:
        return None
    candidates = list(base_media.media_candidates)
    existing_urls = {candidate.media_url for candidate in candidates}
    image_count = sum(1 for candidate in candidates if candidate.media_type == "image")
    if image_count >= desired_image_count:
        return base_media

    strategies = ["web-sourced", "gemini-generated"]
    if base_media.media_type == "image" and base_media.media_url:
        strategies.insert(0, "article-image")
    for strategy in strategies:
        if image_count >= desired_image_count:
            break
        try:
            thumbnail_result = generate_thumbnail(
                settings=settings,
                strategy=strategy,
                title=title,
                script=script_text,
                article_image_url=base_media.media_url if base_media.media_type == "image" else None,
            )
        except Exception as strategy_error:  # noqa: BLE001
            LOGGER.warning(
                "Supplemental image strategy failed post_id=%s strategy=%s error=%s",
                post_id,
                strategy,
                strategy_error,
            )
            continue
        if not thumbnail_result:
            continue
        upload_url = upload_thumbnail_to_supabase(
            settings=settings,
            post_id=f"{post_id}-supp-{image_count + 1}",
            thumbnail=thumbnail_result,
        )
        if upload_url in existing_urls:
            continue
        existing_urls.add(upload_url)
        image_count += 1
        candidates.append(
            MediaCandidate(
                media_type="image",
                media_url=upload_url,
                selection_reason=f"supplemental:{thumbnail_result.source}",
                priority=-20 - image_count,
            )
        )

    if not candidates:
        return base_media
    preferred = next((candidate for candidate in candidates if candidate.media_type == "image"), candidates[0])
    return MediaAssetResult(
        media_type=preferred.media_type,
        media_url=preferred.media_url,
        selection_reason=f"{base_media.selection_reason};supplemental_images",
        media_candidates=candidates,
        quality_summary=base_media.quality_summary,
    )


def _log_quality_gate_skip(*, source: str, guid: str, summary: dict[str, Any] | None) -> None:
    """Log image quality gate failure with scores and reject reasons for easier debugging."""
    if not summary:
        LOGGER.info(
            "Skipping post source=%s guid=%s reason=image_quality_gate_failed",
            source, guid,
        )
        return
    top_scores: list[str] = []
    reject_reasons: list[str] = []
    for assessment in summary.get("assessments", []):
        label = assessment.get("media_url", "?")[:60]
        composite = assessment.get("composite_score")
        decision = assessment.get("decision")
        top_scores.append(f"{label}(composite={composite}, decision={decision})")
        reject_reasons.extend(assessment.get("reject_reasons", []))
    LOGGER.info(
        "Skipping post source=%s guid=%s reason=image_quality_gate_failed candidates=%s reject_reasons=%s",
        source,
        guid,
        top_scores[:3] or "none",
        list(dict.fromkeys(reject_reasons))[:6] or "none",
    )


def _process_ranked_posts_batch(
    *,
    conn: Any,
    settings: Any,
    media_quality_config: MediaQualityGateConfig,
    run_id: str,
    ranked_posts: list[SourcePostInput],
    target_processed_posts: int,
    posts_processed_start: int,
    utc_schedule_anchor: datetime,
    live_covered_links: set[str],
    recent_series_tags: list[str] | None = None,
    force_recycle: bool = False,
) -> BatchProcessingResult:
    posts_processed = posts_processed_start
    new_posts_seen = 0
    existing_posts_seen = 0
    jobs_enqueued = 0
    buffer_jobs_enqueued = 0
    run_errors: list[dict[str, str]] = []
    skip_reason_counts: Counter[str] = Counter()

    def _record_skip(reason: str) -> None:
        skip_reason_counts[reason] += 1

    for post in ranked_posts:
        if posts_processed >= target_processed_posts:
            break
        try:
            conn.execute("SAVEPOINT post_processing")
            coverage_variants = _link_coverage_variants(post.link or post.source_guid)
            if coverage_variants and coverage_variants & live_covered_links:
                existing_posts_seen += 1
                _record_skip("already_live")
                LOGGER.info(
                    "Skipping already-live post source=%s guid=%s",
                    post.source,
                    post.source_guid,
                )
                continue

            if post.source == "world_journal":
                try:
                    is_suppressed = is_source_gate_suppressed(
                        conn,
                        source=post.source,
                        source_guid=post.source_guid,
                        gate=PRE_VOICE_GATE_NAME,
                    )
                    should_bypass_suppression_for_recheck = (
                        is_suppressed
                        and not bool(str(post.description or "").strip())
                    )
                    if is_suppressed and not should_bypass_suppression_for_recheck:
                        LOGGER.info(
                            "Skipping post source=%s guid=%s reason=pre_voice_gate_suppressed",
                            post.source,
                            post.source_guid,
                        )
                        continue
                    if should_bypass_suppression_for_recheck:
                        LOGGER.info(
                            "Bypassing pre-voice suppression for recheck source=%s guid=%s reason=missing_rss_description",
                            post.source,
                            post.source_guid,
                        )
                except Exception as suppression_lookup_error:  # noqa: BLE001
                    LOGGER.warning(
                        "Pre-voice suppression lookup failed source=%s guid=%s error=%s",
                        post.source,
                        post.source_guid,
                        suppression_lookup_error,
                    )

            if post.link and not force_recycle and link_exists_in_source_posts(conn, post.link):
                existing_posts_seen += 1
                LOGGER.info(
                    "Skipping duplicate link source=%s guid=%s link=%s",
                    post.source,
                    post.source_guid,
                    post.link,
                )
                continue

            upserted = upsert_source_post(conn, post, force_recycle=force_recycle)
            if not upserted.is_new:
                existing_posts_seen += 1
                _record_skip("existing_post")
                LOGGER.info("Skipping existing post source=%s guid=%s", post.source, post.source_guid)
                continue
            new_posts_seen += 1

            rss_description = str(post.description or "").strip()
            effective_description = rss_description

            rss_description_words = _word_count(rss_description)
            extracted_context = ""
            pre_extracted_context = (
                str(post.raw_payload.get("pre_extracted_context", "")).strip()
                if isinstance(post.raw_payload, dict)
                else ""
            )
            if pre_extracted_context:
                extracted_context = pre_extracted_context
            elif settings.pre_voice_metadata_enrichment_enabled:
                try:
                    extracted_context = run_with_retry(
                        lambda: extract_article_context(
                            page_url=post.link,
                            timeout_seconds=settings.request_timeout_seconds,
                            max_words=settings.article_context_max_words,
                        ),
                        retries=2,
                        base_sleep_seconds=0.8,
                    )
                except Exception as enrichment_error:  # noqa: BLE001
                    LOGGER.warning(
                        "Pre-voice context extraction failed source=%s guid=%s error=%s",
                        post.source,
                        post.source_guid,
                        enrichment_error,
                    )

            extracted_context_words = _word_count(extracted_context)
            if extracted_context and extracted_context_words > rss_description_words:
                effective_description = extracted_context
                LOGGER.info(
                    "Using extracted article context source=%s guid=%s rss_words=%s extracted_words=%s",
                    post.source,
                    post.source_guid,
                    rss_description_words,
                    extracted_context_words,
                )

            best_context_words = max(rss_description_words, extracted_context_words)
            low_context_failure = best_context_words < settings.article_context_min_words
            if low_context_failure:
                pre_voice_issues = [
                    (
                        "article context below minimum threshold: "
                        f"{best_context_words} < {settings.article_context_min_words}"
                    )
                ]
            else:
                pre_voice_issues = validate_source_context(
                    title=post.title,
                    description=effective_description,
                    article_url=post.link,
                    min_description_words=settings.pre_voice_description_min_words,
                )

            should_use_title_only_fallback = (
                post.source == "world_journal"
                and settings.pre_voice_allow_title_only_fallback
                and not low_context_failure
                and "description lacks enough context" in pre_voice_issues
            )
            if should_use_title_only_fallback:
                title_words = len(re.sub(r"\s+", " ", post.title or "").strip().split())
                title_only_issues = validate_source_context(
                    title=post.title,
                    description="",
                    article_url=post.link,
                    min_description_words=0,
                )
                if not title_only_issues and title_words >= settings.pre_voice_title_only_min_words:
                    effective_description = post.title
                    pre_voice_issues = []
                    LOGGER.info(
                        "Pre-voice accepted via title-only fallback source=%s guid=%s title_words=%s",
                        post.source,
                        post.source_guid,
                        title_words,
                    )

            if pre_voice_issues:
                _record_skip("pre_voice_gate_failed")
                if post.source == "world_journal":
                    try:
                        record_source_gate_failure(
                            conn,
                            source=post.source,
                            source_guid=post.source_guid,
                            gate=PRE_VOICE_GATE_NAME,
                            issues=pre_voice_issues,
                            suppress_after=settings.pre_voice_fail_suppress_after,
                            suppress_days=settings.pre_voice_fail_suppress_days,
                        )
                    except Exception as record_failure_error:  # noqa: BLE001
                        LOGGER.warning(
                            "Failed to record pre-voice gate failure source=%s guid=%s error=%s",
                            post.source,
                            post.source_guid,
                            record_failure_error,
                        )
                LOGGER.info(
                    "Skipping post source=%s guid=%s reason=pre_voice_gate_failed issues=%s",
                    post.source,
                    post.source_guid,
                    pre_voice_issues,
                )
                continue
            if post.source == "world_journal":
                try:
                    clear_source_gate_failure(
                        conn,
                        source=post.source,
                        source_guid=post.source_guid,
                        gate=PRE_VOICE_GATE_NAME,
                    )
                except Exception as clear_failure_error:  # noqa: BLE001
                    LOGGER.warning(
                        "Failed clearing pre-voice gate failure state source=%s guid=%s error=%s",
                        post.source,
                        post.source_guid,
                        clear_failure_error,
                    )

            post_civic_importance = _get_civic_importance(post)
            effective_target_words = settings.content_script_target_words
            if post_civic_importance >= 0.6:
                effective_target_words = settings.content_script_target_words + 8
                LOGGER.info(
                    "Flex duration: civic_importance=%.2f, bumping target_words %d -> %d for guid=%s",
                    post_civic_importance,
                    settings.content_script_target_words,
                    effective_target_words,
                    post.source_guid,
                )

            content = generate_content_pack(
                api_key=settings.anthropic_api_key,
                model_name=settings.anthropic_model,
                title=post.title,
                description=effective_description,
                article_url=post.link,
                script_target_seconds=settings.content_script_target_seconds,
                script_target_words=effective_target_words,
                script_max_words_buffer=settings.content_script_max_words_buffer,
                script_min_words=settings.content_script_min_words,
                script_min_facts=settings.content_script_min_facts,
                script_min_sentences=settings.content_script_min_sentences,
                script_max_sentences=settings.content_script_max_sentences,
                recent_series_tags=recent_series_tags,
            )
            post_script_issues = validate_script_for_profile(
                script_text=content.script_10s,
                title=post.title,
                description=effective_description,
                article_url=post.link,
                script_target_seconds=settings.content_script_target_seconds,
                script_target_words=effective_target_words,
                script_max_words_buffer=settings.content_script_max_words_buffer,
                script_min_words=settings.content_script_min_words,
                script_min_facts=settings.content_script_min_facts,
                script_min_sentences=settings.content_script_min_sentences,
                script_max_sentences=settings.content_script_max_sentences,
            )
            if post_script_issues:
                _record_skip("post_script_gate_failed")
                LOGGER.info(
                    "Skipping post source=%s guid=%s reason=post_script_gate_failed issues=%s",
                    post.source,
                    post.source_guid,
                    post_script_issues,
                )
                continue
            upsert_content_asset(
                conn,
                post_id=upserted.post_id,
                content=content,
            )

            media = None
            quality_gate_failure_summary: dict[str, Any] | None = None
            try:
                rss_entry_payload = (
                    post.raw_payload.get("entry")
                    if isinstance(post.raw_payload, dict) and isinstance(post.raw_payload.get("entry"), dict)
                    else None
                )
                media = run_with_retry(
                    lambda: extract_best_media_from_article(
                        page_url=post.link,
                        timeout_seconds=settings.request_timeout_seconds,
                        rss_entry_payload=rss_entry_payload,
                    )
                )
            except Exception as media_error:  # noqa: BLE001
                LOGGER.warning("Media extraction failed for source=%s guid=%s error=%s", post.source, post.source_guid, media_error)
            if media is None:
                media = _fallback_media_from_web_thumbnail(
                    settings=settings,
                    post_id=upserted.post_id,
                    title=post.title,
                    script_text=content.script_10s,
                    article_image_url=None,
                )

            def _run_media_quality_gate(candidate_media: MediaAssetResult) -> Any:
                return run_with_retry(
                    lambda: enforce_image_quality_gate(
                        media_result=candidate_media,
                        title=post.title,
                        description=effective_description,
                        article_url=post.link,
                        api_key=settings.anthropic_api_key,
                        config=media_quality_config,
                    )
                )

            if media and media_quality_config.enabled:
                quality_gate = _run_media_quality_gate(media)
                if quality_gate.media_result:
                    media = quality_gate.media_result
                else:
                    quality_gate_failure_summary = quality_gate.quality_summary

            needs_visual_recovery = bool(
                media
                and settings.video_require_image_media
                and not any(candidate.media_type == "image" for candidate in media.media_candidates)
            )
            if quality_gate_failure_summary or needs_visual_recovery:
                fallback_media = _fallback_media_from_web_thumbnail(
                    settings=settings,
                    post_id=upserted.post_id,
                    title=post.title,
                    script_text=content.script_10s,
                    article_image_url=media.media_url if media and media.media_type == "image" else None,
                )
                if fallback_media:
                    if media_quality_config.enabled:
                        fallback_gate = _run_media_quality_gate(fallback_media)
                        if fallback_gate.media_result:
                            media = fallback_gate.media_result
                            quality_gate_failure_summary = None
                            LOGGER.info(
                                "Recovered media via thumbnail fallback source=%s guid=%s",
                                post.source,
                                post.source_guid,
                            )
                        else:
                            quality_gate_failure_summary = fallback_gate.quality_summary
                    else:
                        media = fallback_media
                        quality_gate_failure_summary = None
                elif quality_gate_failure_summary:
                    LOGGER.info(
                        "Thumbnail fallback unavailable after media quality failure source=%s guid=%s",
                        post.source,
                        post.source_guid,
                    )

            quality_gate_failed = quality_gate_failure_summary is not None
            if quality_gate_failed and media:
                media = MediaAssetResult(
                    media_type=media.media_type,
                    media_url=media.media_url,
                    selection_reason=f"{media.selection_reason};quality_gate_failed",
                    media_candidates=media.media_candidates,
                    quality_summary=quality_gate_failure_summary,
                )
            if media:
                upsert_media_asset(conn, post_id=upserted.post_id, source_page_url=post.link, media_result=media)
            if quality_gate_failed:
                _record_skip("image_quality_gate_failed")
                _log_quality_gate_skip(
                    source=post.source,
                    guid=post.source_guid,
                    summary=quality_gate_failure_summary,
                )
                continue
            media = _augment_media_with_supplemental_images(
                settings=settings,
                post_id=upserted.post_id,
                title=post.title,
                script_text=content.script_10s,
                base_media=media,
                desired_image_count=max(2, settings.video_media_max_images),
            )
            if media:
                upsert_media_asset(conn, post_id=upserted.post_id, source_page_url=post.link, media_result=media)
            has_image_candidate = bool(
                media
                and any(candidate.media_type == "image" for candidate in media.media_candidates)
            )
            has_video_candidate = bool(
                media
                and any(candidate.media_type == "video" for candidate in media.media_candidates)
            )
            if settings.video_require_image_media and not has_image_candidate and not has_video_candidate:
                _record_skip("no_image_or_video_media")
                LOGGER.info(
                    "Skipping post source=%s guid=%s reason=no_image_or_video_media",
                    post.source,
                    post.source_guid,
                )
                continue

            quality_scores = evaluate_publish_quality(
                title=post.title,
                description=effective_description,
                script_text=content.script_10s,
                media_candidates=_media_candidates_as_dicts(media),
                score_threshold=settings.quality_rubric_min_composite_score,
                per_dimension_min=settings.quality_rubric_min_dimension_score,
            )
            rubric_attempt = 0
            while (
                settings.quality_rubric_enabled
                and quality_scores.failing_dimensions
                and rubric_attempt < settings.quality_rubric_max_regen_attempts
            ):
                rubric_attempt += 1
                failing_dimensions = set(quality_scores.failing_dimensions)
                if {"ScriptSpecificityScore", "NarrativeFlowScore", "First2sHookScore"} & failing_dimensions:
                    content = generate_content_pack(
                        api_key=settings.anthropic_api_key,
                        model_name=settings.anthropic_model,
                        title=post.title,
                        description=effective_description,
                        article_url=post.link,
                        script_target_seconds=settings.content_script_target_seconds,
                        script_target_words=effective_target_words,
                        script_max_words_buffer=settings.content_script_max_words_buffer,
                        script_min_words=settings.content_script_min_words,
                        script_min_facts=settings.content_script_min_facts,
                        script_min_sentences=settings.content_script_min_sentences,
                        script_max_sentences=settings.content_script_max_sentences,
                        recent_series_tags=recent_series_tags,
                        experiment_prompt_modifier=(
                            "Rewrite the script for maximum retention using the Hook / Easy Explanation / Twist structure: "
                            "open with a scroll-stopping hook (~15字) containing the strongest specific fact, "
                            "explain the story clearly in the middle (3-5句, ~85字) so the viewer fully understands, "
                            "end with a twist or reveal (~15字) that leaves the viewer stunned. "
                            "Keep the total script under 130字. Every sentence must create or resolve tension — no filler."
                        ),
                    )
                    upsert_content_asset(
                        conn,
                        post_id=upserted.post_id,
                        content=content,
                    )
                if {"VisualRelevanceScore", "VisualVarietyScore", "First2sHookScore"} & failing_dimensions:
                    media = _augment_media_with_supplemental_images(
                        settings=settings,
                        post_id=upserted.post_id,
                        title=post.title,
                        script_text=content.script_10s,
                        base_media=media,
                        desired_image_count=max(3, settings.video_media_max_images),
                    )
                    if media:
                        upsert_media_asset(conn, post_id=upserted.post_id, source_page_url=post.link, media_result=media)
                quality_scores = evaluate_publish_quality(
                    title=post.title,
                    description=effective_description,
                    script_text=content.script_10s,
                    media_candidates=_media_candidates_as_dicts(media),
                    score_threshold=settings.quality_rubric_min_composite_score,
                    per_dimension_min=settings.quality_rubric_min_dimension_score,
                )

            upsert_post_quality_evaluation(
                conn,
                post_id=upserted.post_id,
                run_id=run_id,
                persona_key=settings.persona_key,
                scores={
                    "script_specificity_score": quality_scores.script_specificity,
                    "narrative_flow_score": quality_scores.narrative_flow,
                    "visual_relevance_score": quality_scores.visual_relevance,
                    "visual_variety_score": quality_scores.visual_variety,
                    "first_two_seconds_hook_score": quality_scores.first_two_seconds_hook,
                    "composite_score": quality_scores.composite,
                },
                passed=not quality_scores.failing_dimensions,
                failing_dimensions=quality_scores.failing_dimensions,
                metadata={
                    "source": post.source,
                    "source_guid": post.source_guid,
                    "rubric_attempts": rubric_attempt,
                },
            )
            if settings.quality_rubric_enabled and quality_scores.failing_dimensions:
                LOGGER.info(
                    "Skipping post source=%s guid=%s reason=quality_rubric_failed failing=%s composite=%.3f",
                    post.source,
                    post.source_guid,
                    quality_scores.failing_dimensions,
                    quality_scores.composite,
                )
                continue

            voice = run_with_retry(
                lambda: generate_elevenlabs_voice(
                    api_key=settings.elevenlabs_api_key,
                    voice_id=settings.elevenlabs_voice_id,
                    text=content.script_10s,
                    post_id=upserted.post_id,
                    supabase_url=settings.supabase_url,
                    supabase_service_role_key=settings.supabase_service_role_key,
                    supabase_voice_bucket=settings.supabase_voice_bucket,
                    timeout_seconds=settings.request_timeout_seconds,
                    model_id=settings.elevenlabs_tts_model_id,
                    voice_stability=settings.elevenlabs_voice_stability,
                    voice_similarity_boost=settings.elevenlabs_voice_similarity_boost,
                    apply_text_normalization=settings.elevenlabs_apply_text_normalization,
                )
            )
            if voice.status != "generated":
                LOGGER.warning(
                    "Voice generation failed for post_id=%s status=%s error=%s",
                    upserted.post_id,
                    voice.status,
                    voice.error,
                )
            if hasattr(conn, "ensure_alive"):
                conn.ensure_alive()
            upsert_voice_asset(
                conn,
                post_id=upserted.post_id,
                voice_id=settings.elevenlabs_voice_id,
                voice_result=voice,
            )

            if not voice.audio_url:
                _record_skip("voice_audio_missing")
                LOGGER.warning(
                    "Skipping video generation for post_id=%s — no audio URL from voice gen (status=%s)",
                    upserted.post_id,
                    voice.status,
                )
                continue

            post_image_url = media.media_url if media and media.media_type == "image" else None
            date_label = (
                post.published_at.astimezone().strftime("%B %d, %Y")
                if post.published_at
                else datetime.now().astimezone().strftime("%B %d, %Y")
            )
            video = generate_fish_lipsync_video(
                settings=settings,
                post_id=upserted.post_id,
                audio_url=voice.audio_url,
                post_image_url=post_image_url,
                audio_duration_sec=voice.audio_duration_sec,
                post_title=content.video_title_short,
                date_label=date_label,
                script_text=content.script_10s,
                media_candidates=media.media_candidates if media else [],
                voice_alignment=voice.alignment,
                script_10s_en=content.script_10s_en,
            )
            if video.status != "generated":
                LOGGER.warning(
                    "Fish video generation not successful for source=%s guid=%s status=%s error=%s",
                    post.source,
                    post.source_guid,
                    video.status,
                    video.error,
                )
            if hasattr(conn, "ensure_alive"):
                conn.ensure_alive()
            upsert_video_asset(
                conn,
                post_id=upserted.post_id,
                template_name="fish_lipsync",
                video_result=video,
            )

            thumbnail_url: str | None = None
            if settings.thumbnail_generation_enabled:
                article_image_for_thumbnail = media.media_url if media and media.media_type == "image" else None
                try:
                    thumbnail_result = generate_thumbnail(
                        settings=settings,
                        strategy="article-image",
                        title=post.title,
                        script=content.script_10s,
                        article_image_url=article_image_for_thumbnail,
                    )
                    if thumbnail_result:
                        thumbnail_url = upload_thumbnail_to_supabase(
                            settings=settings,
                            post_id=upserted.post_id,
                            thumbnail=thumbnail_result,
                        )
                        update_content_asset_thumbnail(
                            conn,
                            post_id=upserted.post_id,
                            thumbnail_url=thumbnail_url,
                            thumbnail_source=thumbnail_result.source,
                        )
                except Exception as thumb_error:  # noqa: BLE001
                    LOGGER.warning(
                        "Thumbnail generation/upload failed post_id=%s strategy=article-image error=%s",
                        upserted.post_id, thumb_error,
                    )

            publish_media: MediaPublishPayload | None = None
            if video.status == "generated" and video.video_url:
                publish_media = MediaPublishPayload(
                    media_type="video",
                    media_url=video.video_url,
                    selection_reason="fish_lipsync_render",
                )
            elif media and media.media_type == "video":
                publish_media = MediaPublishPayload(
                    media_type=media.media_type,
                    media_url=media.media_url,
                    selection_reason=f"{media.selection_reason};fallback_source_video",
                )
            elif media:
                publish_media = MediaPublishPayload(
                    media_type=media.media_type,
                    media_url=media.media_url,
                    selection_reason=f"{media.selection_reason};fallback_source_media",
                )

            if settings.publish_enabled and (publish_media is None or publish_media.media_type != "video"):
                _record_skip("no_video_publish_media")
                LOGGER.info(
                    "Skipping post source=%s guid=%s reason=no_video_publish_media",
                    post.source,
                    post.source_guid,
                )
                continue
            if settings.publish_enabled and publish_media is not None and publish_media.media_type == "video" and not _is_publishable_mp4_url(publish_media.media_url):
                _record_skip("non_mp4_publish_media")
                LOGGER.info(
                    "Skipping post source=%s guid=%s reason=non_mp4_publish_media media_url=%s",
                    post.source,
                    post.source_guid,
                    publish_media.media_url,
                )
                continue

            script_word_count = len(content.script_10s.split())
            voice_duration = float(voice.audio_duration_sec) if voice.audio_duration_sec is not None else None
            video_duration = float(video.video_duration_sec) if video.video_duration_sec is not None else None
            post_voice_issues: list[str] = []
            if voice_duration is None:
                post_voice_issues.append("audio duration missing")
            else:
                if voice_duration < MIN_AUDIO_SECONDS or voice_duration > MAX_AUDIO_SECONDS:
                    post_voice_issues.append(
                        f"audio duration out of bounds: {voice_duration:.2f}s "
                        f"(expected {MIN_AUDIO_SECONDS:.0f}-{MAX_AUDIO_SECONDS:.0f}s)"
                    )
                expected_audio_floor = script_word_count / 3.8
                if voice_duration < expected_audio_floor:
                    post_voice_issues.append(
                        f"audio too short for script length: {voice_duration:.2f}s for {script_word_count} words"
                    )

            if post_voice_issues and voice_duration is not None and voice_duration > MAX_AUDIO_SECONDS:
                trimmed_script = _trim_script_sentences(content.script_10s, max_words=script_word_count - 8)
                if trimmed_script and len(trimmed_script.split()) >= 40:
                    LOGGER.info(
                        "Audio too long (%.1fs); retrying with trimmed script (%s -> %s words) post_id=%s",
                        voice_duration,
                        script_word_count,
                        len(trimmed_script.split()),
                        upserted.post_id,
                    )
                    content = replace(content, script_10s=trimmed_script)
                    upsert_content_asset(
                        conn,
                        post_id=upserted.post_id,
                        content=content,
                    )
                    retry_voice = run_with_retry(
                        lambda: generate_elevenlabs_voice(
                            api_key=settings.elevenlabs_api_key,
                            voice_id=settings.elevenlabs_voice_id,
                            text=content.script_10s,
                            post_id=upserted.post_id,
                            supabase_url=settings.supabase_url,
                            supabase_service_role_key=settings.supabase_service_role_key,
                            supabase_voice_bucket=settings.supabase_voice_bucket,
                            timeout_seconds=settings.request_timeout_seconds,
                            model_id=settings.elevenlabs_tts_model_id,
                            voice_stability=settings.elevenlabs_voice_stability,
                            voice_similarity_boost=settings.elevenlabs_voice_similarity_boost,
                            apply_text_normalization=settings.elevenlabs_apply_text_normalization,
                        )
                    )
                    if retry_voice.audio_url and retry_voice.audio_duration_sec is not None:
                        retry_duration = float(retry_voice.audio_duration_sec)
                        if MIN_AUDIO_SECONDS <= retry_duration <= MAX_AUDIO_SECONDS:
                            voice = retry_voice
                            upsert_voice_asset(conn, post_id=upserted.post_id, voice_id=settings.elevenlabs_voice_id, voice_result=voice)
                            voice_duration = retry_duration
                            post_image_url_retry = media.media_url if media and media.media_type == "image" else None
                            date_label_retry = (
                                post.published_at.astimezone().strftime("%B %d, %Y")
                                if post.published_at
                                else datetime.now().astimezone().strftime("%B %d, %Y")
                            )
                            video = generate_fish_lipsync_video(
                                settings=settings,
                                post_id=upserted.post_id,
                                audio_url=voice.audio_url,
                                post_image_url=post_image_url_retry,
                                audio_duration_sec=voice.audio_duration_sec,
                                post_title=content.video_title_short,
                                date_label=date_label_retry,
                                script_text=content.script_10s,
                                media_candidates=media.media_candidates if media else [],
                                voice_alignment=voice.alignment,
                                script_10s_en=content.script_10s_en,
                            )
                            if hasattr(conn, "ensure_alive"):
                                conn.ensure_alive()
                            upsert_video_asset(conn, post_id=upserted.post_id, template_name="fish_lipsync", video_result=video)
                            video_duration = float(video.video_duration_sec) if video.video_duration_sec is not None else None
                            script_word_count = len(content.script_10s.split())
                            post_voice_issues = []
                            LOGGER.info(
                                "Audio retry succeeded post_id=%s duration=%.1fs words=%s",
                                upserted.post_id,
                                retry_duration,
                                script_word_count,
                            )
                        else:
                            LOGGER.info(
                                "Audio retry still out of bounds (%.1fs) post_id=%s",
                                retry_duration,
                                upserted.post_id,
                            )

            if post_voice_issues:
                _record_skip("post_voice_gate_failed")
                LOGGER.info(
                    "Skipping post source=%s guid=%s reason=post_voice_gate_failed issues=%s",
                    post.source,
                    post.source_guid,
                    post_voice_issues,
                )
                continue

            pre_publish_issues: list[str] = []
            if video_duration is None:
                pre_publish_issues.append("video duration missing")
            else:
                if video_duration < MIN_VIDEO_SECONDS or video_duration > MAX_VIDEO_SECONDS:
                    pre_publish_issues.append(
                        f"video duration out of bounds: {video_duration:.2f}s "
                        f"(expected {MIN_VIDEO_SECONDS:.0f}-{MAX_VIDEO_SECONDS:.0f}s)"
                    )
                if voice_duration is not None:
                    if video_duration + 0.75 < voice_duration:
                        pre_publish_issues.append(
                            f"video shorter than audio by more than tolerance ({video_duration:.2f}s vs {voice_duration:.2f}s)"
                        )
                    if abs(video_duration - voice_duration) > MAX_AUDIO_VIDEO_DELTA_SECONDS:
                        pre_publish_issues.append(
                            f"audio/video mismatch indicates truncation ({video_duration:.2f}s vs {voice_duration:.2f}s)"
                        )
            if pre_publish_issues:
                _record_skip("pre_publish_gate_failed")
                LOGGER.info(
                    "Skipping post source=%s guid=%s reason=pre_publish_gate_failed issues=%s",
                    post.source,
                    post.source_guid,
                    pre_publish_issues,
                )
                continue

            enqueued_jobs = enqueue_publish_jobs_for_post(
                conn,
                settings=settings,
                post=post,
                post_id=upserted.post_id,
                content=content,
                media=publish_media,
                voice=voice,
                thumbnail_url=thumbnail_url,
                video_duration_sec=video_duration,
                desired_publish_at=_scheduled_slot_iso(
                    anchor_utc=utc_schedule_anchor,
                    spacing_hours=settings.buffer_schedule_spacing_hours,
                    slot_index=posts_processed,
                ),
            )
            if enqueued_jobs:
                jobs_enqueued += len(enqueued_jobs)
                posts_processed += 1
            else:
                _record_skip("publish_enqueue_blocked")
                LOGGER.info(
                    "Skipping post source=%s guid=%s reason=publish_enqueue_blocked",
                    post.source,
                    post.source_guid,
                )
            conn.execute("RELEASE SAVEPOINT post_processing")
        except Exception as post_error:  # noqa: BLE001
            try:
                conn.execute("ROLLBACK TO SAVEPOINT post_processing")
            except Exception:  # noqa: BLE001
                pass
            LOGGER.exception("Failed processing post source=%s guid=%s", post.source, post.source_guid)
            run_errors.append({"source_guid": post.source_guid, "error": str(post_error)})

    return BatchProcessingResult(
        posts_processed=posts_processed,
        new_posts_seen=new_posts_seen,
        existing_posts_seen=existing_posts_seen,
        jobs_enqueued=jobs_enqueued,
        buffer_jobs_enqueued=buffer_jobs_enqueued,
        run_errors=run_errors,
        skip_reason_counts=dict(skip_reason_counts),
    )


def _ingest_with_primary_failover(settings: Any, *, conn: Any = None) -> IngestResult:
    try:
        primary = run_with_retry(
            lambda: fetch_wj_posts(
                base_url=settings.wj_base_url,
                category_paths=settings.wj_category_paths,
                timeout_seconds=settings.request_timeout_seconds,
                max_posts=settings.max_posts_per_run,
            )
        )
        LOGGER.info("Ingest source=wj_scraper posts=%s", len(primary.posts))
        return primary
    except WJFeedUnavailableError as primary_error:
        raise RuntimeError(f"World Journal ingest failed: {primary_error}") from primary_error


def _select_ranked_candidates_with_floor(
    *,
    posts: list[SourcePostInput],
    ingest_source: str,
    settings: Any,
    signal_boosts: list[SignalBoost] | None = None,
) -> tuple[list[SourcePostInput], dict[str, int]]:
    target_processed_posts = max(1, int(getattr(settings, "top_headlines_per_run", 1)))
    cadence_floor = max(1, int(getattr(settings, "cadence_min_posts_per_run", 1)))
    floor_required_posts = min(target_processed_posts, cadence_floor)
    engagement_min_score = max(0.0, min(1.0, float(getattr(settings, "engagement_min_score", 0.62))))
    engagement_floor_score = max(0.0, min(1.0, float(getattr(settings, "engagement_floor_score", 0.5))))

    ranked_posts: list[SourcePostInput] = []
    primary_candidates = 0
    fallback_candidates = 0
    selected_primary = 0
    selected_fallback = 0
    below_threshold_count = 0

    if ingest_source == "primary_rss":
        primary_candidates = len(posts)
        primary_selected_posts, primary_scoring_stats = select_top_headlines_with_engagement(
            posts=posts,
            settings=settings,
            candidate_origin="primary",
            top_n=len(posts),
            min_score=engagement_min_score,
            signal_boosts=signal_boosts,
        )
        selected_primary = len(primary_selected_posts)
        below_threshold_count += primary_scoring_stats["below_threshold_count"]
        ranked_posts.extend(primary_selected_posts)

        if selected_primary < floor_required_posts:
            LOGGER.info(
                "Primary candidates below floor (selected=%s floor=%s); no fallback feeds configured for WJ pipeline",
                selected_primary,
                floor_required_posts,
            )
    else:
        fallback_candidates = len(posts)
        fallback_selected_posts, fallback_scoring_stats = select_top_headlines_with_engagement(
            posts=posts,
            settings=settings,
            candidate_origin="fallback",
            top_n=len(posts),
            min_score=engagement_floor_score,
            signal_boosts=signal_boosts,
        )
        ranked_posts.extend(fallback_selected_posts)
        selected_fallback = len(fallback_selected_posts)
        below_threshold_count += fallback_scoring_stats["below_threshold_count"]

    return ranked_posts, {
        "target_processed_posts": target_processed_posts,
        "floor_required_posts": floor_required_posts,
        "primary_candidates": primary_candidates,
        "fallback_candidates": fallback_candidates,
        "selected_primary": selected_primary,
        "selected_fallback": selected_fallback,
        "below_threshold_count": below_threshold_count,
    }


def _relaxed_media_quality_config(base: MediaQualityGateConfig) -> MediaQualityGateConfig:
    """Return a copy of *base* with thresholds lowered for safety-net retries.

    Heuristic-only pass (LLM not required), lower composite/size minimums.
    """
    return replace(
        base,
        require_llm_pass=False,
        min_composite_score=0.35,
        min_image_width=480,
        min_image_height=480,
    )


def _trim_script_sentences(script: str, max_words: int) -> str | None:
    """Drop trailing sentences until the script fits within *max_words*.

    Returns ``None`` if trimming would leave fewer than 3 sentences.
    """
    sentences = re.split(r"(?<=[.!?])\s+", script.strip())
    while len(sentences) > 3 and len(" ".join(sentences).split()) > max_words:
        sentences.pop()
    trimmed = " ".join(sentences)
    if len(trimmed.split()) > max_words or len(sentences) < 3:
        return None
    return trimmed


def run_pipeline() -> None:
    bootstrap_runtime_env()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    settings = load_settings()
    _log_wj_config(wj_base_url=settings.wj_base_url)
    media_quality_weight_total = max(0.01, settings.media_quality_heuristic_weight + settings.media_quality_llm_weight)
    media_quality_config = MediaQualityGateConfig(
        enabled=settings.media_quality_gate_enabled,
        max_candidates=settings.media_quality_max_candidates,
        timeout_seconds=settings.request_timeout_seconds,
        min_image_width=settings.media_quality_min_image_width,
        min_image_height=settings.media_quality_min_image_height,
        min_image_bytes=settings.media_quality_min_image_bytes,
        min_aspect_ratio=settings.media_quality_min_aspect_ratio,
        max_aspect_ratio=settings.media_quality_max_aspect_ratio,
        min_entropy=settings.media_quality_min_entropy,
        min_sharpness=settings.media_quality_min_sharpness,
        require_llm_pass=settings.media_quality_require_llm_pass,
        llm_model_name=settings.media_quality_llm_model,
        llm_min_quality_score=settings.media_quality_llm_min_quality_score,
        llm_min_relevance_score=settings.media_quality_llm_min_relevance_score,
        min_composite_score=settings.media_quality_min_composite_score,
        heuristic_weight=settings.media_quality_heuristic_weight / media_quality_weight_total,
        llm_weight=settings.media_quality_llm_weight / media_quality_weight_total,
        aspect_ratio_penalty=settings.media_quality_aspect_ratio_penalty,
        llm_assessment_retries=settings.media_quality_llm_assessment_retries,
        allow_llm_failure_fallback=settings.media_quality_allow_llm_failure_fallback,
        llm_failure_heuristic_min_score=settings.media_quality_llm_failure_heuristic_min_score,
    )

    with db_connection(settings.supabase_db_url) as conn:
        ensure_pipeline_schema(conn)
        upsert_persona_profile(
            conn,
            persona_key=settings.persona_key,
            metricool_user_id=settings.metricool_user_id,
            metricool_blog_id=settings.metricool_blog_id,
            metricool_target_platforms=settings.metricool_target_platforms,
        )
        run_id = create_pipeline_run(conn)
        posts_seen = 0
        posts_processed = 0
        new_posts_seen = 0
        existing_posts_seen = 0
        jobs_enqueued = 0
        jobs_dispatched = 0
        primary_candidates = 0
        fallback_candidates = 0
        selected_primary = 0
        selected_fallback = 0
        below_threshold_count = 0
        skip_reason_counts: Counter[str] = Counter()
        run_errors: list[dict[str, str]] = []

        try:
            baseline_summary = get_quality_baseline_summary(
                conn,
                persona_key=settings.persona_key,
                lookback_days=settings.quality_baseline_lookback_days,
            )
            if baseline_summary:
                LOGGER.info(
                    "Quality baseline %s-day: evals=%s pass=%s avg_composite=%.3f published=%s failed=%s skipped=%s completion=%.3f engagement=%.3f",
                    settings.quality_baseline_lookback_days,
                    baseline_summary.get("evaluations", 0),
                    baseline_summary.get("passed_evaluations", 0),
                    float(baseline_summary.get("avg_composite") or 0.0),
                    baseline_summary.get("jobs_published", 0),
                    baseline_summary.get("jobs_failed", 0),
                    baseline_summary.get("jobs_skipped", 0),
                    float(baseline_summary.get("avg_completion_rate") or 0.0),
                    float(baseline_summary.get("avg_engagement_rate") or 0.0),
                )
            if settings.metricool_analytics_enabled:
                try:
                    analytics_stored = fetch_and_store_metricool_analytics(conn, settings=settings)
                    LOGGER.info("Metricool analytics: stored %d metrics (IG + TikTok)", analytics_stored)
                except Exception as analytics_error:  # noqa: BLE001
                    LOGGER.warning("Metricool analytics fetch failed (non-blocking): %s", analytics_error)
            try:
                recommendation_id = analyze_quality_performance_feedback(conn, settings=settings)
                if recommendation_id:
                    LOGGER.info("Quality-performance recommendation created id=%s", recommendation_id)
            except Exception as feedback_error:  # noqa: BLE001
                LOGGER.warning("Quality feedback analysis failed (non-blocking): %s", feedback_error)

            cached_signal_boosts: list[SignalBoost] = []
            try:
                cached_signal_boosts = analyze_signal_performance(conn, settings=settings)
                if cached_signal_boosts:
                    LOGGER.info(
                        "Signal performance boosts loaded: %s",
                        [(b.dimension, b.value, b.boost) for b in cached_signal_boosts],
                    )
            except Exception as signal_error:  # noqa: BLE001
                LOGGER.warning("Signal performance analysis failed (non-blocking): %s", signal_error)

            ingest = _ingest_with_primary_failover(settings, conn=conn)
            posts = ingest.posts
            posts, blocked_posts = _filter_blocked_posts(
                posts=posts,
                topic_blocklist_enabled=settings.topic_blocklist_enabled,
                topic_block_terms=settings.topic_block_terms,
                source_domain_blocklist=settings.source_domain_blocklist,
            )
            if blocked_posts:
                LOGGER.info("Blocked %s posts before ranking source=%s", blocked_posts, ingest.source)
            posts_seen = len(posts)
            ranked_posts, selection_stats = _select_ranked_candidates_with_floor(
                posts=posts,
                ingest_source=ingest.source,
                settings=settings,
                signal_boosts=cached_signal_boosts,
            )
            target_processed_posts = selection_stats["target_processed_posts"]
            floor_required_posts = selection_stats["floor_required_posts"]
            primary_candidates = selection_stats["primary_candidates"]
            fallback_candidates = selection_stats["fallback_candidates"]
            selected_primary = selection_stats["selected_primary"]
            selected_fallback = selection_stats["selected_fallback"]
            below_threshold_count = selection_stats["below_threshold_count"]

            override_raw = str(settings.publish_schedule_at_override or "").strip()
            if override_raw:
                utc_schedule_anchor = datetime.fromisoformat(
                    override_raw.replace("Z", "+00:00")
                ).astimezone(timezone.utc).replace(microsecond=0)
                LOGGER.info("Using PUBLISH_SCHEDULE_AT override anchor=%s", utc_schedule_anchor.isoformat())
            else:
                utc_schedule_anchor = _next_utc_schedule_anchor(spacing_hours=settings.buffer_schedule_spacing_hours)
            LOGGER.info(
                "Ranked %s headlines; targeting %s posts floor_required=%s schedule_anchor=%s source=%s",
                len(ranked_posts),
                target_processed_posts,
                floor_required_posts,
                utc_schedule_anchor.isoformat().replace("+00:00", "Z"),
                ingest.source,
            )
            LOGGER.info(
                "Selection summary source=%s primary_candidates=%s fallback_candidates=%s selected_primary=%s selected_fallback=%s below_threshold_count=%s",
                ingest.source,
                primary_candidates,
                fallback_candidates,
                selected_primary,
                selected_fallback,
                below_threshold_count,
            )

            try:
                recent_series = list_recent_series_tags(conn, persona_key=settings.persona_key)
            except Exception:  # noqa: BLE001
                recent_series = []

            batch = _process_ranked_posts_batch(
                conn=conn,
                settings=settings,
                media_quality_config=media_quality_config,
                run_id=run_id,
                ranked_posts=ranked_posts,
                target_processed_posts=target_processed_posts,
                posts_processed_start=posts_processed,
                utc_schedule_anchor=utc_schedule_anchor,
                live_covered_links=_collect_live_covered_links(conn=conn, settings=settings),
                recent_series_tags=recent_series,
            )
            posts_processed = batch.posts_processed
            new_posts_seen += batch.new_posts_seen
            existing_posts_seen += batch.existing_posts_seen
            jobs_enqueued += batch.jobs_enqueued
            skip_reason_counts.update(batch.skip_reason_counts)
            run_errors.extend(batch.run_errors)

            if posts_processed == 0 and ranked_posts:
                relaxed_mqc = _relaxed_media_quality_config(media_quality_config)
                LOGGER.info(
                    "Safety net: 0 posts processed; attempting force-reprocess with relaxed quality gate"
                )
                safety_batch = _process_ranked_posts_batch(
                    conn=conn,
                    settings=settings,
                    media_quality_config=relaxed_mqc,
                    run_id=run_id,
                    ranked_posts=ranked_posts,
                    target_processed_posts=1,
                    posts_processed_start=0,
                    utc_schedule_anchor=utc_schedule_anchor,
                    live_covered_links=_collect_live_covered_links(conn=conn, settings=settings),
                    recent_series_tags=recent_series,
                    force_recycle=True,
                )
                posts_processed += safety_batch.posts_processed
                new_posts_seen += safety_batch.new_posts_seen
                existing_posts_seen += safety_batch.existing_posts_seen
                jobs_enqueued += safety_batch.jobs_enqueued
                run_errors.extend(safety_batch.run_errors)

            if posts_processed == 0:
                LOGGER.info("Safety net: still 0 posts after force-reprocess; no fallback feeds in WJ pipeline")

            if posts_processed == 0 and ranked_posts:
                disabled_mqc = replace(media_quality_config, enabled=False)
                LOGGER.info(
                    "Safety net (final): still 0 posts; bypassing image quality gate for best candidate"
                )
                bypass_batch = _process_ranked_posts_batch(
                    conn=conn,
                    settings=settings,
                    media_quality_config=disabled_mqc,
                    run_id=run_id,
                    ranked_posts=ranked_posts[:1],
                    target_processed_posts=1,
                    posts_processed_start=0,
                    utc_schedule_anchor=utc_schedule_anchor,
                    live_covered_links=_collect_live_covered_links(conn=conn, settings=settings),
                    recent_series_tags=recent_series,
                    force_recycle=True,
                )
                posts_processed += bypass_batch.posts_processed
                new_posts_seen += bypass_batch.new_posts_seen
                existing_posts_seen += bypass_batch.existing_posts_seen
                jobs_enqueued += bypass_batch.jobs_enqueued
                run_errors.extend(bypass_batch.run_errors)

            if posts_processed < target_processed_posts:
                LOGGER.warning(
                    "Processed %s/%s posts; candidate quality gates reduced output",
                    posts_processed,
                    target_processed_posts,
                )

            if hasattr(conn, "ensure_alive"):
                conn.ensure_alive()
            publish_counts = dispatch_ready_publish_jobs(
                conn,
                settings=settings,
                max_jobs=settings.publish_max_jobs_per_run,
            )
            jobs_dispatched = publish_counts["published"] + publish_counts["failed"] + publish_counts["skipped"]
            LOGGER.info(
                "Publish dispatch summary queued=%s published=%s failed=%s skipped=%s",
                publish_counts["queued"],
                publish_counts["published"],
                publish_counts["failed"],
                publish_counts["skipped"],
            )
            LOGGER.info(
                "Run visibility summary source=%s posts_seen=%s new_posts_seen=%s existing_posts_seen=%s jobs_enqueued=%s jobs_dispatched=%s primary_candidates=%s fallback_candidates=%s selected_primary=%s selected_fallback=%s below_threshold_count=%s",
                ingest.source,
                posts_seen,
                new_posts_seen,
                existing_posts_seen,
                jobs_enqueued,
                jobs_dispatched,
                primary_candidates,
                fallback_candidates,
                selected_primary,
                selected_fallback,
                below_threshold_count,
            )
            if skip_reason_counts:
                LOGGER.info(
                    "Skip reason summary source=%s reasons=%s",
                    ingest.source,
                    dict(sorted(skip_reason_counts.items())),
                )
            if publish_counts["published"] == 0:
                LOGGER.warning(
                    "No posts were published this run source=%s queued=%s jobs_enqueued=%s skip_reasons=%s",
                    ingest.source,
                    publish_counts["queued"],
                    jobs_enqueued,
                    dict(sorted(skip_reason_counts.items())),
                )

            if publish_counts["published"] > 0 and settings.metricool_link_in_bio_enabled:
                try:
                    recent_links = list_recent_published_article_links(
                        conn,
                        platform="metricool",
                        persona_key=settings.persona_key,
                        limit=settings.metricool_link_in_bio_max_links,
                    )
                    update_metricool_link_in_bio(settings=settings, recent_links=recent_links)
                except Exception as bio_error:  # noqa: BLE001
                    LOGGER.warning("Link-in-bio update failed error=%s", bio_error)

            finish_pipeline_run(
                conn,
                run_id=run_id,
                status="success" if not run_errors else "partial_success",
                posts_seen=posts_seen,
                posts_processed=posts_processed,
                errors=run_errors,
            )
            LOGGER.info(
                "Pipeline completed status=%s source=%s posts_seen=%s posts_processed=%s jobs_enqueued=%s jobs_dispatched=%s primary_candidates=%s fallback_candidates=%s selected_primary=%s selected_fallback=%s below_threshold_count=%s errors=%s",
                "success" if not run_errors else "partial_success",
                ingest.source,
                posts_seen,
                posts_processed,
                jobs_enqueued,
                jobs_dispatched,
                primary_candidates,
                fallback_candidates,
                selected_primary,
                selected_fallback,
                below_threshold_count,
                len(run_errors),
            )
        except Exception as run_error:  # noqa: BLE001
            LOGGER.exception("Pipeline failed")
            run_errors.append({"error": str(run_error)})
            try:
                if hasattr(conn, "ensure_alive"):
                    conn.ensure_alive()
                finish_pipeline_run(
                    conn,
                    run_id=run_id,
                    status="failed",
                    posts_seen=posts_seen,
                    posts_processed=posts_processed,
                    errors=run_errors,
                )
            except Exception as cleanup_error:  # noqa: BLE001
                LOGGER.warning("Failed to record pipeline failure in DB: %s", cleanup_error)
            raise


if __name__ == "__main__":
    run_pipeline()
