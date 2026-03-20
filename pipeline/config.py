from __future__ import annotations

from dataclasses import dataclass
import os
from urllib.parse import urlparse

from dotenv import load_dotenv

DEFAULT_FALLBACK_FEED_URLS: list[str] = [
    "https://feeds.reuters.com/reuters/worldNews",
    "https://feeds.reuters.com/reuters/businessNews",
    "https://feeds.apnews.com/apf-topnews",
    "https://feeds.apnews.com/apf-politics",
    "https://feeds.npr.org/1003/rss.xml",
    "https://feeds.npr.org/1001/rss.xml",
    "https://feeds.bbci.co.uk/news/world/rss.xml",
]

DEFAULT_WJ_CATEGORY_PATHS = [
    "/wj/cate/breaking",
    "/wj/cate/breaking/121006",
    "/wj/cate/breaking/121103",
    "/wj/cate/breaking/121099",
    "/wj/cate/breaking/121102",
    "/wj/cate/breaking/121010",
    "/wj/cate/breaking/121098",
]

DEFAULT_TOPIC_BLOCK_TERMS = [
    "廣告",
    "贊助",
    "工商",
    "訂閱",
    "促銷",
    "review",
    "reviews",
    "hands-on",
    "hands on",
    "unboxing",
    "first look",
    "benchmark",
    "benchmarks",
    "camera test",
    "vs.",
    "comparison",
    "compared",
    "memoir",
    "essay",
    "literary",
    "diary",
    "thought exercise",
    "new york review",
    "paris review",
    "breakfast",
]

DEFAULT_SOURCE_DOMAIN_BLOCKLIST: list[str] = [
    "theverge.com",
    "techradar.com",
    "gsmarena.com",
    "androidauthority.com",
    "9to5google.com",
    "9to5mac.com",
    "engadget.com",
    "tomsguide.com",
    "cnet.com",
    "mashable.com",
    "pocket-lint.com",
]

def bootstrap_runtime_env() -> None:
    load_dotenv()


def _require_env(key: str) -> str:
    value = os.getenv(key)
    if value:
        return value
    raise ValueError(f"Missing required environment variable: {key}")


def _optional_int_env(key: str, default: int) -> int:
    raw = os.getenv(key)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"Environment variable {key} must be an integer") from exc


def _optional_str_env(key: str, default: str) -> str:
    raw = os.getenv(key)
    if raw is None:
        return default
    value = raw.strip()
    if not value:
        return default
    return value


def _optional_float_env(key: str, default: float) -> float:
    raw = os.getenv(key)
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"Environment variable {key} must be a float") from exc


def _optional_bool_env(key: str, default: bool) -> bool:
    raw = os.getenv(key)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if not normalized:
        return default
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"Environment variable {key} must be a boolean-like value")


def _optional_csv_env(key: str, default: list[str]) -> list[str]:
    raw = os.getenv(key)
    if raw is None:
        return list(default)
    normalized = [item.strip() for item in raw.split(",")]
    return [item for item in normalized if item]


def _derive_supabase_url_from_db_url(db_url: str) -> str:
    parsed = urlparse(db_url)
    username = parsed.username or ""
    if username.startswith("postgres."):
        project_ref = username.removeprefix("postgres.")
        if project_ref:
            return f"https://{project_ref}.supabase.co"
    raise ValueError("SUPABASE_URL is required when it cannot be derived from SUPABASE_DB_URL")


@dataclass(frozen=True)
class Settings:
    persona_key: str
    supabase_db_url: str
    supabase_url: str
    supabase_service_role_key: str
    supabase_voice_bucket: str
    elevenlabs_api_key: str
    elevenlabs_voice_id: str
    elevenlabs_tts_model_id: str
    elevenlabs_voice_stability: float
    elevenlabs_voice_similarity_boost: float
    elevenlabs_apply_text_normalization: bool
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_region: str
    wj_base_url: str
    wj_category_paths: list[str]
    content_language: str
    fallback_feeds_enabled: bool
    fallback_feeds_world_first: bool
    fallback_feeds_max_posts: int
    fallback_feed_urls: list[str]
    fallback_min_jobs_per_run: int
    fallback_min_novel_posts: int
    topic_blocklist_enabled: bool
    topic_block_terms: list[str]
    source_domain_blocklist: list[str]
    headline_dedup_enabled: bool
    headline_dedup_similarity_threshold: float
    headline_dedup_lookback_hours: int
    anthropic_model: str
    content_script_target_seconds: int
    content_script_target_words: int
    content_script_max_words_buffer: int
    content_script_min_words: int
    content_script_min_facts: int
    content_script_min_sentences: int
    content_script_max_sentences: int
    pre_voice_description_min_words: int
    pre_voice_metadata_enrichment_enabled: bool
    pre_voice_allow_title_only_fallback: bool
    pre_voice_title_only_min_words: int
    pre_voice_fail_suppress_after: int
    pre_voice_fail_suppress_days: int
    article_context_min_words: int
    article_context_max_words: int
    max_posts_per_run: int
    top_headlines_per_run: int
    engagement_scoring_enabled: bool
    engagement_min_score: float
    engagement_floor_score: float
    cadence_min_posts_per_run: int
    content_mix_profile: str
    request_timeout_seconds: int
    publish_enabled: bool
    publish_platforms: list[str]
    publish_max_retries: int
    publish_max_jobs_per_run: int
    publish_claim_stale_in_progress_minutes: int
    publish_enforce_compliance: bool
    publish_min_audio_seconds: float
    publish_min_video_seconds: float
    publish_audio_video_max_delta_seconds: float
    allow_duplicate_link_repost: bool
    youtube_client_id: str
    youtube_client_secret: str
    youtube_refresh_token: str
    youtube_channel_id: str
    youtube_schedule_spacing_hours: int
    instagram_user_id: str
    instagram_access_token: str
    instagram_api_version: str
    instagram_app_id: str
    instagram_app_secret: str
    instagram_auto_refresh_enabled: bool
    instagram_persist_refreshed_token: bool
    instagram_upload_strategy: str
    instagram_container_wait_timeout_seconds: int
    instagram_container_poll_interval_seconds: int
    buffer_publish_enabled: bool
    buffer_key: str
    buffer_organization_id: str
    buffer_api_url: str
    buffer_schedule_spacing_hours: int
    publish_schedule_at_override: str
    buffer_initial_queue_size: int
    buffer_primary_platforms_only: bool
    buffer_required_services: list[str]
    metricool_publish_enabled: bool
    metricool_user_token: str
    metricool_api_url: str
    metricool_user_id: str
    metricool_blog_id: str
    metricool_target_platforms: list[str]
    metricool_review_required: bool
    metricool_require_instagram_or_youtube: bool
    metricool_link_in_bio_enabled: bool
    metricool_link_in_bio_max_links: int
    metricool_analytics_enabled: bool
    metricool_analytics_lookback_days: int
    metricool_analytics_fetch_limit: int
    discord_bot_token: str
    discord_application_id: str
    discord_guild_id: str
    discord_review_channel_id: str
    discord_poll_seconds: int
    discord_allow_all_members: bool
    enable_video_render: bool
    remotion_project_dir: str
    fish_mouth_frames_dir: str
    fish_background_image_path: str
    supabase_video_bucket: str
    video_sensitivity: float
    video_freq_start: int
    video_freq_end: int
    video_fish_x: float
    video_fish_y: float
    video_fish_scale: float
    video_bg_x: float
    video_bg_y: float
    video_bg_scale: float
    video_post_image_y: float
    video_post_image_scale: float
    video_show_debug: bool
    video_intro_duration_seconds: float
    video_intro_music_path: str
    video_intro_music_volume: float
    video_outro_audio_path: str
    video_outro_volume: float
    video_title_ticker_speed: float
    video_browser_executable: str
    video_npm_executable: str
    video_npx_executable: str
    video_render_concurrency: int
    video_codec: str
    video_crf: int
    video_pixel_format: str
    video_bitrate: str
    video_audio_bitrate: str
    video_caption_y: float
    video_captions_enabled: bool
    video_caption_words_per_line: int
    video_caption_offset_seconds: float
    video_caption_alignment_enabled: bool
    video_caption_alignment_provider: str
    video_caption_pause_gap_seconds: float
    video_caption_max_cue_duration_seconds: float
    video_caption_min_cue_duration_seconds: float
    video_caption_max_words_per_cue: int
    video_caption_max_en_words_per_cue: int
    video_caption_min_alignment_coverage: float
    video_media_max_images: int
    video_media_display_seconds: float
    video_require_image_media: bool
    media_quality_gate_enabled: bool
    media_quality_max_candidates: int
    media_quality_min_image_width: int
    media_quality_min_image_height: int
    media_quality_min_image_bytes: int
    media_quality_min_aspect_ratio: float
    media_quality_max_aspect_ratio: float
    media_quality_min_entropy: float
    media_quality_min_sharpness: float
    media_quality_require_llm_pass: bool
    media_quality_llm_model: str
    media_quality_llm_min_quality_score: float
    media_quality_llm_min_relevance_score: float
    media_quality_min_composite_score: float
    media_quality_heuristic_weight: float
    media_quality_llm_weight: float
    media_quality_aspect_ratio_penalty: float
    media_quality_llm_assessment_retries: int
    media_quality_allow_llm_failure_fallback: bool
    media_quality_llm_failure_heuristic_min_score: float
    quality_rubric_enabled: bool
    quality_rubric_min_composite_score: float
    quality_rubric_min_dimension_score: float
    quality_rubric_max_regen_attempts: int
    quality_baseline_lookback_days: int
    thumbnail_generation_enabled: bool
    thumbnail_fetch_retries: int
    thumbnail_provider_cooldown_seconds: int
    gemini_api_key: str
    gemini_image_model: str
    gemini_image_fallback_model: str
    google_custom_search_api_key: str
    google_custom_search_cx: str


def load_settings() -> Settings:
    supabase_db_url = _require_env("SUPABASE_DB_URL")
    supabase_url = _optional_str_env("SUPABASE_URL", "")
    if not supabase_url:
        supabase_url = _derive_supabase_url_from_db_url(supabase_db_url)

    publish_platforms_raw = _optional_str_env("PUBLISH_PLATFORMS", "metricool")
    publish_platform_candidates = [item.strip().lower() for item in publish_platforms_raw.split(",") if item.strip()]
    if publish_platform_candidates and publish_platform_candidates != ["metricool"]:
        raise ValueError(
            "PUBLISH_PLATFORMS only supports 'metricool' in the consolidated single-workflow pipeline"
        )
    publish_platforms = ["metricool"]
    fallback_feed_urls = _optional_csv_env("FALLBACK_FEED_URLS", DEFAULT_FALLBACK_FEED_URLS)
    if not fallback_feed_urls:
        fallback_feed_urls = list(DEFAULT_FALLBACK_FEED_URLS)
    topic_block_terms = [item.strip().lower() for item in _optional_csv_env("TOPIC_BLOCK_TERMS", DEFAULT_TOPIC_BLOCK_TERMS) if item.strip()]
    source_domain_blocklist = [
        item.strip().lower()
        for item in _optional_csv_env("SOURCE_DOMAIN_BLOCKLIST", DEFAULT_SOURCE_DOMAIN_BLOCKLIST)
        if item.strip()
    ]
    elevenlabs_voice_stability = max(0.0, min(1.0, _optional_float_env("ELEVENLABS_VOICE_STABILITY", 0.4)))
    elevenlabs_voice_similarity_boost = max(
        0.0,
        min(1.0, _optional_float_env("ELEVENLABS_VOICE_SIMILARITY_BOOST", 0.8)),
    )
    publish_min_audio_seconds = max(0.0, _optional_float_env("PUBLISH_MIN_AUDIO_SECONDS", 12.0))
    publish_min_video_seconds = max(0.0, _optional_float_env("PUBLISH_MIN_VIDEO_SECONDS", 14.0))
    publish_audio_video_max_delta_seconds = max(
        0.0,
        _optional_float_env("PUBLISH_AUDIO_VIDEO_MAX_DELTA_SECONDS", 6.0),
    )
    video_intro_music_volume = max(0.0, min(1.0, _optional_float_env("VIDEO_INTRO_MUSIC_VOLUME", 0.5)))
    video_outro_volume = max(0.0, min(2.0, _optional_float_env("VIDEO_OUTRO_VOLUME", 1.0)))
    gemini_api_key = _optional_str_env("GEMINI_API_KEY", "")
    gemini_image_model = _optional_str_env("GEMINI_IMAGE_MODEL", "gemini-nano-banana-pro")
    gemini_image_fallback_model = _optional_str_env("GEMINI_IMAGE_FALLBACK_MODEL", "gemini-2.5-flash-image-preview")
    google_custom_search_api_key = _optional_str_env("GOOGLE_CUSTOM_SEARCH_API_KEY", "") or gemini_api_key

    return Settings(
        persona_key=_optional_str_env("PERSONA_KEY", "default"),
        supabase_db_url=supabase_db_url,
        supabase_url=supabase_url,
        supabase_service_role_key=_require_env("SUPABASE_SERVICE_ROLE_KEY"),
        supabase_voice_bucket=_optional_str_env("SUPABASE_VOICE_BUCKET", "voice-assets"),
        elevenlabs_api_key=_require_env("ELEVENLABS_API_KEY"),
        elevenlabs_voice_id=_require_env("ELEVENLABS_VOICE_ID"),
        elevenlabs_tts_model_id=_optional_str_env("ELEVENLABS_TTS_MODEL_ID", "eleven_multilingual_v2"),
        elevenlabs_voice_stability=elevenlabs_voice_stability,
        elevenlabs_voice_similarity_boost=elevenlabs_voice_similarity_boost,
        elevenlabs_apply_text_normalization=_optional_bool_env("ELEVENLABS_APPLY_TEXT_NORMALIZATION", True),
        aws_access_key_id=_require_env("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=_require_env("AWS_SECRET_ACCESS_KEY"),
        aws_region=_optional_str_env("AWS_REGION", "us-east-1"),
        wj_base_url=_optional_str_env("WJ_BASE_URL", "https://www.worldjournal.com"),
        wj_category_paths=_optional_csv_env("WJ_CATEGORY_PATHS", DEFAULT_WJ_CATEGORY_PATHS),
        content_language=_optional_str_env("CONTENT_LANGUAGE", "zh-TW"),
        fallback_feeds_enabled=_optional_bool_env("FALLBACK_FEEDS_ENABLED", True),
        fallback_feeds_world_first=_optional_bool_env("FALLBACK_FEEDS_WORLD_FIRST", True),
        fallback_feeds_max_posts=max(1, _optional_int_env("FALLBACK_FEEDS_MAX_POSTS", 25)),
        fallback_feed_urls=fallback_feed_urls,
        fallback_min_jobs_per_run=max(1, _optional_int_env("FALLBACK_MIN_JOBS_PER_RUN", 1)),
        fallback_min_novel_posts=max(0, _optional_int_env("FALLBACK_MIN_NOVEL_POSTS", 3)),
        topic_blocklist_enabled=_optional_bool_env("TOPIC_BLOCKLIST_ENABLED", True),
        topic_block_terms=topic_block_terms,
        source_domain_blocklist=source_domain_blocklist,
        headline_dedup_enabled=_optional_bool_env("HEADLINE_DEDUP_ENABLED", True),
        headline_dedup_similarity_threshold=float(_optional_str_env("HEADLINE_DEDUP_SIMILARITY_THRESHOLD", "0.45")),
        headline_dedup_lookback_hours=max(1, _optional_int_env("HEADLINE_DEDUP_LOOKBACK_HOURS", 72)),
        anthropic_model=os.getenv("ANTHROPIC_MODEL", "anthropic.claude-opus-4-6-v1"),
        content_script_target_seconds=max(8, _optional_int_env("CONTENT_SCRIPT_TARGET_SECONDS", 35)),
        content_script_target_words=max(20, _optional_int_env("CONTENT_SCRIPT_TARGET_WORDS", 140)),
        content_script_max_words_buffer=max(0, _optional_int_env("CONTENT_SCRIPT_MAX_WORDS_BUFFER", 15)),
        content_script_min_words=max(20, _optional_int_env("CONTENT_SCRIPT_MIN_WORDS", 100)),
        content_script_min_facts=max(1, _optional_int_env("CONTENT_SCRIPT_MIN_FACTS", 3)),
        content_script_min_sentences=max(1, _optional_int_env("CONTENT_SCRIPT_MIN_SENTENCES", 5)),
        content_script_max_sentences=max(1, _optional_int_env("CONTENT_SCRIPT_MAX_SENTENCES", 8)),
        pre_voice_description_min_words=max(0, _optional_int_env("PRE_VOICE_DESCRIPTION_MIN_WORDS", 8)),
        pre_voice_metadata_enrichment_enabled=_optional_bool_env("PRE_VOICE_METADATA_ENRICHMENT_ENABLED", True),
        pre_voice_allow_title_only_fallback=_optional_bool_env("PRE_VOICE_ALLOW_TITLE_ONLY_FALLBACK", True),
        pre_voice_title_only_min_words=max(4, _optional_int_env("PRE_VOICE_TITLE_ONLY_MIN_WORDS", 10)),
        pre_voice_fail_suppress_after=max(1, _optional_int_env("PRE_VOICE_FAIL_SUPPRESS_AFTER", 3)),
        pre_voice_fail_suppress_days=max(1, _optional_int_env("PRE_VOICE_FAIL_SUPPRESS_DAYS", 7)),
        article_context_min_words=max(1, _optional_int_env("ARTICLE_CONTEXT_MIN_WORDS", 40)),
        article_context_max_words=max(40, _optional_int_env("ARTICLE_CONTEXT_MAX_WORDS", 220)),
        max_posts_per_run=_optional_int_env("MAX_POSTS_PER_RUN", 25),
        top_headlines_per_run=max(1, _optional_int_env("TOP_HEADLINES_PER_RUN", 3)),
        engagement_scoring_enabled=_optional_bool_env("ENGAGEMENT_SCORING_ENABLED", True),
        engagement_min_score=max(0.0, min(1.0, _optional_float_env("ENGAGEMENT_MIN_SCORE", 0.55))),
        engagement_floor_score=max(0.0, min(1.0, _optional_float_env("ENGAGEMENT_FLOOR_SCORE", 0.50))),
        cadence_min_posts_per_run=max(1, _optional_int_env("CADENCE_MIN_POSTS_PER_RUN", 2)),
        content_mix_profile=_optional_str_env("CONTENT_MIX_PROFILE", "hard_news_culture"),
        request_timeout_seconds=_optional_int_env("REQUEST_TIMEOUT_SECONDS", 20),
        publish_enabled=_optional_bool_env("PUBLISH_ENABLED", False),
        publish_platforms=publish_platforms,
        publish_max_retries=_optional_int_env("PUBLISH_MAX_RETRIES", 3),
        publish_max_jobs_per_run=max(1, _optional_int_env("PUBLISH_MAX_JOBS_PER_RUN", 3)),
        publish_claim_stale_in_progress_minutes=max(1, _optional_int_env("PUBLISH_CLAIM_STALE_IN_PROGRESS_MINUTES", 45)),
        publish_enforce_compliance=_optional_bool_env("PUBLISH_ENFORCE_COMPLIANCE", True),
        publish_min_audio_seconds=publish_min_audio_seconds,
        publish_min_video_seconds=publish_min_video_seconds,
        publish_audio_video_max_delta_seconds=publish_audio_video_max_delta_seconds,
        allow_duplicate_link_repost=_optional_bool_env("ALLOW_DUPLICATE_LINK_REPOST", False),
        youtube_client_id="",
        youtube_client_secret="",
        youtube_refresh_token="",
        youtube_channel_id="",
        youtube_schedule_spacing_hours=8,
        instagram_user_id="",
        instagram_access_token="",
        instagram_api_version="v25.0",
        instagram_app_id="",
        instagram_app_secret="",
        instagram_auto_refresh_enabled=False,
        instagram_persist_refreshed_token=False,
        instagram_upload_strategy="auto",
        instagram_container_wait_timeout_seconds=300,
        instagram_container_poll_interval_seconds=5,
        buffer_publish_enabled=False,
        buffer_key="",
        buffer_organization_id="",
        buffer_api_url="https://api.buffer.com",
        buffer_schedule_spacing_hours=max(1, _optional_int_env("PUBLISH_SCHEDULE_SPACING_HOURS", 6)),
        publish_schedule_at_override=_optional_str_env("PUBLISH_SCHEDULE_AT", ""),
        buffer_initial_queue_size=1,
        buffer_primary_platforms_only=True,
        buffer_required_services=["twitter", "youtube", "instagram"],
        metricool_publish_enabled=_optional_bool_env("METRICOOL_PUBLISH_ENABLED", False),
        metricool_user_token=_optional_str_env("METRICOOL_USER_TOKEN", ""),
        metricool_api_url=_optional_str_env("METRICOOL_API_URL", "https://app.metricool.com/api"),
        metricool_user_id=_optional_str_env("METRICOOL_USER_ID", ""),
        metricool_blog_id=_optional_str_env("METRICOOL_BLOG_ID", ""),
        metricool_target_platforms=[
            item.strip().lower()
            for item in _optional_csv_env("METRICOOL_TARGET_PLATFORMS", ["tiktok", "instagram", "youtube"])
            if item.strip()
        ],
        metricool_review_required=_optional_bool_env("METRICOOL_REVIEW_REQUIRED", False),
        metricool_require_instagram_or_youtube=False,
        metricool_link_in_bio_enabled=_optional_bool_env("METRICOOL_LINK_IN_BIO_ENABLED", True),
        metricool_link_in_bio_max_links=max(1, _optional_int_env("METRICOOL_LINK_IN_BIO_MAX_LINKS", 4)),
        metricool_analytics_enabled=_optional_bool_env("METRICOOL_ANALYTICS_ENABLED", False),
        metricool_analytics_lookback_days=max(1, _optional_int_env("METRICOOL_ANALYTICS_LOOKBACK_DAYS", 7)),
        metricool_analytics_fetch_limit=max(1, _optional_int_env("METRICOOL_ANALYTICS_FETCH_LIMIT", 100)),
        discord_bot_token=_optional_str_env("DISCORD_BOT_TOKEN", ""),
        discord_application_id=_optional_str_env("DISCORD_APPLICATION_ID", ""),
        discord_guild_id=_optional_str_env("DISCORD_GUILD_ID", ""),
        discord_review_channel_id=_optional_str_env("DISCORD_REVIEW_CHANNEL_ID", ""),
        discord_poll_seconds=max(15, _optional_int_env("DISCORD_POLL_SECONDS", 60)),
        discord_allow_all_members=_optional_bool_env("DISCORD_ALLOW_ALL_MEMBERS", True),
        enable_video_render=_optional_bool_env("ENABLE_VIDEO_RENDER", True),
        remotion_project_dir=_optional_str_env("REMOTION_PROJECT_DIR", "pipeline/video_templates/fish_lipsync"),
        fish_mouth_frames_dir=_optional_str_env(
            "FISH_MOUTH_FRAMES_DIR",
            "pipeline/video_templates/fish_lipsync/public/mouth",
        ),
        fish_background_image_path=_optional_str_env(
            "FISH_BACKGROUND_IMAGE_PATH",
            "pipeline/video_templates/fish_lipsync/public/background.png",
        ),
        supabase_video_bucket=_optional_str_env("SUPABASE_VIDEO_BUCKET", "video-assets"),
        video_sensitivity=_optional_float_env("VIDEO_SENSITIVITY", 0.04),
        video_freq_start=_optional_int_env("VIDEO_FREQ_START", 1),
        video_freq_end=_optional_int_env("VIDEO_FREQ_END", 40),
        video_fish_x=_optional_float_env("VIDEO_FISH_X", 0.0),
        video_fish_y=_optional_float_env("VIDEO_FISH_Y", 14.3),
        video_fish_scale=_optional_float_env("VIDEO_FISH_SCALE", 70.0),
        video_bg_x=_optional_float_env("VIDEO_BG_X", 0.0),
        video_bg_y=_optional_float_env("VIDEO_BG_Y", 0.0),
        video_bg_scale=_optional_float_env("VIDEO_BG_SCALE", 100.0),
        video_post_image_y=_optional_float_env("VIDEO_POST_IMAGE_Y", -24.0),
        video_post_image_scale=_optional_float_env("VIDEO_POST_IMAGE_SCALE", 62.0),
        video_show_debug=_optional_bool_env("VIDEO_SHOW_DEBUG", False),
        video_intro_duration_seconds=_optional_float_env("VIDEO_INTRO_DURATION_SECONDS", 2.0),
        video_intro_music_path=_optional_str_env(
            "VIDEO_INTRO_MUSIC_PATH",
            "",
        ),
        video_intro_music_volume=video_intro_music_volume,
        video_outro_audio_path=_optional_str_env(
            "VIDEO_OUTRO_AUDIO_PATH",
            "pipeline/video_templates/fish_lipsync/public/audio/breaking_news_outro.wav",
        ),
        video_outro_volume=video_outro_volume,
        video_title_ticker_speed=_optional_float_env("VIDEO_TITLE_TICKER_SPEED", 120.0),
        video_browser_executable=_optional_str_env("VIDEO_BROWSER_EXECUTABLE", ""),
        video_npm_executable=_optional_str_env("VIDEO_NPM_EXECUTABLE", ""),
        video_npx_executable=_optional_str_env("VIDEO_NPX_EXECUTABLE", ""),
        video_render_concurrency=max(0, _optional_int_env("VIDEO_RENDER_CONCURRENCY", 0)),
        video_codec=_optional_str_env("VIDEO_CODEC", "h264"),
        video_crf=max(0, _optional_int_env("VIDEO_CRF", 18)),
        video_pixel_format=_optional_str_env("VIDEO_PIXEL_FORMAT", "yuv420p"),
        video_bitrate=_optional_str_env("VIDEO_BITRATE", "8M"),
        video_audio_bitrate=_optional_str_env("VIDEO_AUDIO_BITRATE", "192k"),
        video_caption_y=_optional_float_env("VIDEO_CAPTION_Y", -4.0),
        video_captions_enabled=_optional_bool_env("VIDEO_CAPTIONS_ENABLED", True),
        video_caption_words_per_line=_optional_int_env("VIDEO_CAPTION_WORDS_PER_LINE", 4),
        video_caption_offset_seconds=_optional_float_env("VIDEO_CAPTION_OFFSET_SECONDS", 0.0),
        video_caption_alignment_enabled=_optional_bool_env("VIDEO_CAPTION_ALIGNMENT_ENABLED", True),
        video_caption_alignment_provider=_optional_str_env("VIDEO_CAPTION_ALIGNMENT_PROVIDER", "elevenlabs"),
        video_caption_pause_gap_seconds=_optional_float_env("VIDEO_CAPTION_PAUSE_GAP_SECONDS", 0.42),
        video_caption_max_cue_duration_seconds=_optional_float_env("VIDEO_CAPTION_MAX_CUE_DURATION_SECONDS", 2.8),
        video_caption_min_cue_duration_seconds=_optional_float_env("VIDEO_CAPTION_MIN_CUE_DURATION_SECONDS", 0.35),
        video_caption_max_words_per_cue=max(1, _optional_int_env("VIDEO_CAPTION_MAX_WORDS_PER_CUE", 6)),
        video_caption_max_en_words_per_cue=max(1, _optional_int_env("VIDEO_CAPTION_MAX_EN_WORDS_PER_CUE", 8)),
        video_caption_min_alignment_coverage=_optional_float_env("VIDEO_CAPTION_MIN_ALIGNMENT_COVERAGE", 0.6),
        video_media_max_images=max(1, _optional_int_env("VIDEO_MEDIA_MAX_IMAGES", 1)),
        video_media_display_seconds=_optional_float_env("VIDEO_MEDIA_DISPLAY_SECONDS", 2.5),
        video_require_image_media=_optional_bool_env("VIDEO_REQUIRE_IMAGE_MEDIA", True),
        media_quality_gate_enabled=_optional_bool_env("MEDIA_QUALITY_GATE_ENABLED", True),
        media_quality_max_candidates=max(1, _optional_int_env("MEDIA_QUALITY_MAX_CANDIDATES", 8)),
        media_quality_min_image_width=max(1, _optional_int_env("MEDIA_QUALITY_MIN_IMAGE_WIDTH", 640)),
        media_quality_min_image_height=max(1, _optional_int_env("MEDIA_QUALITY_MIN_IMAGE_HEIGHT", 640)),
        media_quality_min_image_bytes=max(1, _optional_int_env("MEDIA_QUALITY_MIN_IMAGE_BYTES", 20000)),
        media_quality_min_aspect_ratio=_optional_float_env("MEDIA_QUALITY_MIN_ASPECT_RATIO", 0.5),
        media_quality_max_aspect_ratio=_optional_float_env("MEDIA_QUALITY_MAX_ASPECT_RATIO", 2.2),
        media_quality_min_entropy=_optional_float_env("MEDIA_QUALITY_MIN_ENTROPY", 4.0),
        media_quality_min_sharpness=_optional_float_env("MEDIA_QUALITY_MIN_SHARPNESS", 25.0),
        media_quality_require_llm_pass=_optional_bool_env("MEDIA_QUALITY_REQUIRE_LLM_PASS", True),
        media_quality_llm_model=_optional_str_env("MEDIA_QUALITY_LLM_MODEL", os.getenv("ANTHROPIC_MODEL", "anthropic.claude-opus-4-6-v1")),
        media_quality_llm_min_quality_score=_optional_float_env("MEDIA_QUALITY_LLM_MIN_QUALITY_SCORE", 0.45),
        media_quality_llm_min_relevance_score=_optional_float_env("MEDIA_QUALITY_LLM_MIN_RELEVANCE_SCORE", 0.5),
        media_quality_min_composite_score=_optional_float_env("MEDIA_QUALITY_MIN_COMPOSITE_SCORE", 0.50),
        media_quality_heuristic_weight=_optional_float_env("MEDIA_QUALITY_HEURISTIC_WEIGHT", 0.45),
        media_quality_llm_weight=_optional_float_env("MEDIA_QUALITY_LLM_WEIGHT", 0.55),
        media_quality_aspect_ratio_penalty=_optional_float_env("MEDIA_QUALITY_ASPECT_RATIO_PENALTY", 0.32),
        media_quality_llm_assessment_retries=max(1, _optional_int_env("MEDIA_QUALITY_LLM_ASSESSMENT_RETRIES", 2)),
        media_quality_allow_llm_failure_fallback=_optional_bool_env("MEDIA_QUALITY_ALLOW_LLM_FAILURE_FALLBACK", True),
        media_quality_llm_failure_heuristic_min_score=_optional_float_env(
            "MEDIA_QUALITY_LLM_FAILURE_HEURISTIC_MIN_SCORE",
            0.72,
        ),
        quality_rubric_enabled=_optional_bool_env("QUALITY_RUBRIC_ENABLED", True),
        quality_rubric_min_composite_score=_optional_float_env("QUALITY_RUBRIC_MIN_COMPOSITE_SCORE", 0.52),
        quality_rubric_min_dimension_score=_optional_float_env("QUALITY_RUBRIC_MIN_DIMENSION_SCORE", 0.35),
        quality_rubric_max_regen_attempts=max(0, _optional_int_env("QUALITY_RUBRIC_MAX_REGEN_ATTEMPTS", 2)),
        quality_baseline_lookback_days=max(1, _optional_int_env("QUALITY_BASELINE_LOOKBACK_DAYS", 7)),
        thumbnail_generation_enabled=_optional_bool_env("THUMBNAIL_GENERATION_ENABLED", True),
        thumbnail_fetch_retries=max(0, _optional_int_env("THUMBNAIL_FETCH_RETRIES", 2)),
        thumbnail_provider_cooldown_seconds=max(
            30,
            _optional_int_env("THUMBNAIL_PROVIDER_COOLDOWN_SECONDS", 300),
        ),
        gemini_api_key=gemini_api_key,
        gemini_image_model=gemini_image_model,
        gemini_image_fallback_model=gemini_image_fallback_model,
        google_custom_search_api_key=google_custom_search_api_key,
        google_custom_search_cx=_optional_str_env("GOOGLE_CUSTOM_SEARCH_CX", ""),
    )
