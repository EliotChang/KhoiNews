# News Fish Now -- Production Standards

## Pipeline Flow

```
Ingest -> Rank -> Content -> Media -> Voice -> Video -> Publish
```

### 1. Ingest
- Fetch RSS from clone.fyi (via RSS.app or direct homepage scrape fallback)
- Fetch from fallback feeds (Reuters, AP, NPR, BBC) when clone.fyi yields insufficient jobs
- **Content-exhaustion supplement**: After primary ingest, the pipeline checks how many posts are novel (not already in DB). If novel count < `FALLBACK_MIN_NOVEL_POSTS` (default 3), fallback feed posts are automatically merged into the candidate pool even though the primary feed succeeded.
- **Safety net fallback**: If 0 posts survive all quality gates after processing (including force-reprocess), the pipeline fetches and processes fallback feeds as a last resort.
- Filter by topic blocklist and source domain blocklist
- Max posts per run: 25

### 2. Rank
- Claude ranks all ingested headlines for short-form potential
- Select top N headlines (`TOP_HEADLINES_PER_RUN`, default: 1) with media-quality backfill
- Scoring uses a weighted composite of timeliness (0.20), impact (0.22), civic importance (0.12), hook strength (0.14), context richness (0.18), credibility (0.12), IG fit (0.02), and topic mix adjustment
- **Importance slot**: After normal ranking, the pipeline guarantees at least 1 high-civic-importance story per run by swapping in the top-importance candidate if it wasn't already selected

### 3. Content Generation
- **Model**: Claude (`claude-opus-4-6`)
- **Temperature**: 0.45
- **Max tokens**: 4096
- **Prompt version**: v5
- **Repair loop**: If initial script fails substance validation, a repair prompt is sent at temperature 0.3. If repair also fails, falls back to a deterministic template.
- **Substance checks**: min word count (70% of target), max word count (target 72 + buffer 8 = 80, with 20% sentence-completion overshoot), max sentences (5), min source fact hits, must contain implication cue, must contain stakes markers (a number, named affected group, or deadline)
- **Soft validation**: Specificity anchor (first 2 sentences) and consequence anchor (final sentence) checks are always soft warnings. These are requested in repair prompts but will not block publishing regardless of whether core metrics pass.
- **Flexible duration**: Stories with civic_importance >= 0.6 get target_words bumped by 8 (72 -> 80, ~27s) to allow room for context on complex topics
- **Script trimming**: If the LLM exceeds `max_words`, the trimmer extends forward to the next sentence ending (up to ~120% of max_words) so the narrative always completes. Falls back to hard word-count cutoff only when no sentence ending is found within the cap.
- **Output**: `ContentGenerationResult` with script, video title, 4 platform captions (Instagram, TikTok, YouTube, X), hashtags, tone, language, series metadata
- **Hashtag enforcement**: After LLM generation, hashtags from the `hashtags` array are automatically appended to Instagram, TikTok, and X captions if missing
- **X caption**: Dedicated `caption_x` field (max 280 chars) with smart word-boundary truncation

### 4. Media Extraction
- Extract images and video from article pages (RSS media, OpenGraph, meta tags, page scraping)
- Run hybrid quality gate on image candidates
- Posts without passing media are skipped when `VIDEO_REQUIRE_IMAGE_MEDIA` is true

### 5. Voice Generation
- **Provider**: ElevenLabs TTS
- **Output**: MP3 uploaded to Supabase Storage (`voice-assets` bucket)
- **Alignment**: ElevenLabs returns per-word timestamps used for caption timing

### 6. Video Render
- **Engine**: Remotion 4 with headless Chrome
- **Template**: `FishLipSync` composition
- **Output**: MP4 + SRT sidecar uploaded to Supabase Storage (`video-assets` bucket)
- **Duration source**: ffprobe on downloaded audio → upstream `audio_duration_sec` → 17s fallback
- **Script-length guard**: Warns when audio duration is <60% of expected minimum (word count / 3.5 wps)
- **Pixel format normalization**: After render, if ffprobe detects `yuvj420p` (full-range), the video is re-encoded to `yuv420p` (TV-range) via ffmpeg to prevent color shifts on social platforms

### 7. Publish
- Enqueue jobs for configured platforms
- Compliance gate blocks posts failing required checks when `PUBLISH_ENFORCE_COMPLIANCE` is true
- Duration compliance bounds: audio must be between 12s and 38s, video must be between 14s and 42s (hardcoded in `publish.py`; the ceiling accounts for outro audio added on top of voice)
- Dispatch via Buffer, Metricool, or direct platform APIs

## Video Render Specs

| Property         | Value              |
|------------------|--------------------|
| Resolution       | 1080 x 1920       |
| Aspect ratio     | 9:16 (vertical)   |
| Frame rate       | 30 fps             |
| Frame image format | png (lossless)   |
| Codec            | h264               |
| CRF              | 18                 |
| Pixel format     | yuv420p (normalized post-render) |
| Video bitrate    | 8M                 |
| Audio bitrate    | 192k               |
| Duration range   | ~20--25 seconds    |
| Render concurrency | 0 (Remotion auto) |
| Node requirement | Node 20+           |

## Static Assets

All visual assets for the Remotion template live in `pipeline/video_templates/fish_lipsync/public/`:

| Asset | Path | Source |
|-------|------|--------|
| Mouth frames | `public/mouth/mouth_0.png` -- `mouth_5.png` | Hand-drawn, committed |
| Backgrounds | `public/backgrounds/bg_original.png`, `bg_00.png` -- `bg_05.png` | Generated via `generate_backgrounds.py` (Gemini), committed |
| Background references | `public/background references/` | Reference swatches for Gemini generation |
| Desk overlay | `public/desk-only.png` | Hand-drawn, committed |
| Fallback background | `public/background.png` | Legacy; copied by `_ensure_static_assets()` but not used by the Remotion component |

The pipeline copies mouth frames and `background.png` from `FISH_MOUTH_FRAMES_DIR` / `FISH_BACKGROUND_IMAGE_PATH` into `public/` at render time. Both default to the in-repo locations so no external assets are required.

## Audio Specs

| Property         | Value              |
|------------------|--------------------|
| Provider         | ElevenLabs         |
| Format           | MP3                |
| Alignment        | ElevenLabs forced alignment timestamps |
| Storage bucket   | `voice-assets`     |

## Caption System

Captions are burned into the video frame and also exported as SRT sidecar files.

| Parameter                    | Default | Env var                                |
|------------------------------|---------|----------------------------------------|
| Captions enabled             | true    | `VIDEO_CAPTIONS_ENABLED`               |
| Words per cue                | 6       | `VIDEO_CAPTION_WORDS_PER_LINE`         |
| Max words per cue            | 6       | `VIDEO_CAPTION_MAX_WORDS_PER_CUE`     |
| Offset (seconds)             | 0.0     | `VIDEO_CAPTION_OFFSET_SECONDS`         |
| Alignment enabled            | true    | `VIDEO_CAPTION_ALIGNMENT_ENABLED`      |
| Alignment provider           | elevenlabs | `VIDEO_CAPTION_ALIGNMENT_PROVIDER`  |
| Pause gap (seconds)          | 0.42    | `VIDEO_CAPTION_PAUSE_GAP_SECONDS`      |
| Max cue duration (seconds)   | 2.8     | `VIDEO_CAPTION_MAX_CUE_DURATION_SECONDS` |
| Min cue duration (seconds)   | 0.35    | `VIDEO_CAPTION_MIN_CUE_DURATION_SECONDS` |
| Min alignment coverage       | 0.60    | `VIDEO_CAPTION_MIN_ALIGNMENT_COVERAGE` |

When alignment coverage drops below the threshold, the system falls back to heuristic timing.

## Media Quality Gate

A hybrid scoring system combining heuristic analysis with LLM vision assessment.

### Thresholds

| Parameter                     | Default | Env var                                     |
|-------------------------------|---------|---------------------------------------------|
| Gate enabled                  | true    | `MEDIA_QUALITY_GATE_ENABLED`                |
| Max candidates per post       | 8       | `MEDIA_QUALITY_MAX_CANDIDATES`              |
| Min image width               | 640px   | `MEDIA_QUALITY_MIN_IMAGE_WIDTH`             |
| Min image height              | 640px   | `MEDIA_QUALITY_MIN_IMAGE_HEIGHT`            |
| Min image bytes               | 20 KB   | `MEDIA_QUALITY_MIN_IMAGE_BYTES`             |
| Min aspect ratio              | 0.5     | `MEDIA_QUALITY_MIN_ASPECT_RATIO`            |
| Max aspect ratio              | 2.2     | `MEDIA_QUALITY_MAX_ASPECT_RATIO`            |
| Min entropy                   | 4.0     | `MEDIA_QUALITY_MIN_ENTROPY`                 |
| Min sharpness                 | 25.0    | `MEDIA_QUALITY_MIN_SHARPNESS`               |

### Composite Scoring

| Parameter                     | Default | Env var                                     |
|-------------------------------|---------|---------------------------------------------|
| Min composite score           | 0.50    | `MEDIA_QUALITY_MIN_COMPOSITE_SCORE`         |
| Heuristic weight              | 0.45    | `MEDIA_QUALITY_HEURISTIC_WEIGHT`            |
| LLM weight                    | 0.55    | `MEDIA_QUALITY_LLM_WEIGHT`                  |
| Aspect ratio penalty          | 0.32    | `MEDIA_QUALITY_ASPECT_RATIO_PENALTY`        |

### LLM Assessment

| Parameter                     | Default | Env var                                     |
|-------------------------------|---------|---------------------------------------------|
| Require LLM pass              | true    | `MEDIA_QUALITY_REQUIRE_LLM_PASS`            |
| LLM model                     | claude-opus-4-6   | `MEDIA_QUALITY_LLM_MODEL`        |
| Min quality score             | 0.45    | `MEDIA_QUALITY_LLM_MIN_QUALITY_SCORE`       |
| Min relevance score           | 0.50    | `MEDIA_QUALITY_LLM_MIN_RELEVANCE_SCORE`     |
| Assessment retries            | 2       | `MEDIA_QUALITY_LLM_ASSESSMENT_RETRIES`      |
| Allow LLM failure fallback    | true    | `MEDIA_QUALITY_ALLOW_LLM_FAILURE_FALLBACK`  |
| Fallback heuristic min score  | 0.72    | `MEDIA_QUALITY_LLM_FAILURE_HEURISTIC_MIN_SCORE` |

### Tuning Ladder

- **Strict**: Raise composite threshold to 0.62+, keep `REQUIRE_LLM_PASS=true`, disable LLM fallback
- **Balanced** (default): Use shipped defaults (composite 0.50, LLM quality 0.45)
- **High-recall**: Lower composite threshold further, lower LLM minimums, increase max candidates

## Publish Quality Rubric

A second-level scoring gate after script validation. Scores 5 dimensions and blocks on composite or hard-fail dimension thresholds.

### Thresholds

| Parameter                     | Default | Env var                                     |
|-------------------------------|---------|---------------------------------------------|
| Rubric enabled                | true    | `QUALITY_RUBRIC_ENABLED`                    |
| Min composite score           | 0.52    | `QUALITY_RUBRIC_MIN_COMPOSITE_SCORE`        |
| Min dimension score           | 0.35    | `QUALITY_RUBRIC_MIN_DIMENSION_SCORE`        |
| Max regen attempts            | 2       | `QUALITY_RUBRIC_MAX_REGEN_ATTEMPTS`         |
| Baseline lookback days        | 7       | `QUALITY_BASELINE_LOOKBACK_DAYS`            |

### Dimensions

| Dimension              | Weight | Hard-fail |
|------------------------|--------|-----------|
| ScriptSpecificityScore | 0.24   | No (soft-warn) |
| NarrativeFlowScore     | 0.24   | Yes       |
| VisualRelevanceScore   | 0.20   | No (soft-warn) |
| VisualVarietyScore     | 0.18   | No (soft-warn) |
| First2sHookScore       | 0.14   | No (soft-warn) |

Only `NarrativeFlowScore` and `CompositeScore` can independently block publishing. All other dimensions contribute to the composite but are soft-warn only.

## Publish Cadence

| Parameter                 | Default   | Env var                         |
|---------------------------|-----------|----------------------------------|
| Publish enabled           | false     | `PUBLISH_ENABLED`                |
| Platforms                 | buffer    | `PUBLISH_PLATFORMS`              |
| Max retries               | 3         | `PUBLISH_MAX_RETRIES`            |
| Max jobs per run          | 1         | `PUBLISH_MAX_JOBS_PER_RUN`       |
| Enforce compliance        | true      | `PUBLISH_ENFORCE_COMPLIANCE`     |

### Scheduling

| Platform  | Spacing         | Env var                           |
|-----------|-----------------|-----------------------------------|
| YouTube   | 8 hours         | `YOUTUBE_SCHEDULE_SPACING_HOURS`  |
| Buffer    | 6 hours         | `BUFFER_SCHEDULE_SPACING_HOURS`   |

GitHub Actions cron runs every 6 hours (`0 */6 * * *`), targeting 4x/day output.

### Fallback Feed Behavior

| Parameter                 | Default   | Env var                           |
|---------------------------|-----------|-----------------------------------|
| Fallback enabled          | true      | `FALLBACK_FEEDS_ENABLED`          |
| Min novel posts           | 3         | `FALLBACK_MIN_NOVEL_POSTS`        |
| Fallback max posts        | 25        | `FALLBACK_FEEDS_MAX_POSTS`        |
| Engagement min score      | 0.55      | `ENGAGEMENT_MIN_SCORE`            |
| Engagement floor score    | 0.50      | `ENGAGEMENT_FLOOR_SCORE`          |

When the primary RSS feed returns fewer than `FALLBACK_MIN_NOVEL_POSTS` posts that are not already in the DB, fallback feeds (Reuters, AP, NPR, BBC) are automatically fetched and merged into the candidate pool. A second safety net fetches fallback feeds if 0 posts survive all quality gates after processing.

### CI System Dependencies

| Dependency | Purpose |
|------------|---------|
| ffmpeg/ffprobe | Audio duration measurement for video render |
| Chrome (stable) | Headless browser for Remotion render |
| Node 20 | Remotion runtime |
| Python 3.12 | Pipeline runtime |

### Platform-Specific

**Buffer**: Primary routing when enabled. Required services: twitter, youtube, instagram. Initial queue size: 1.

**Metricool**: Publishing and Instagram analytics. Targets: tiktok, instagram, youtube. Twitter/X is blocked at the code level and will be skipped even if added to the config. The source article URL is posted as the first comment (`firstCommentText`) on each target platform. After publish dispatch, the Metricool Linkin Bio page is updated with the 4 most recent article source URLs (`METRICOOL_LINK_IN_BIO_ENABLED=true`, `METRICOOL_LINK_IN_BIO_MAX_LINKS=4`). Each button shows the video title and links to the original article. Instagram Reels analytics can be fetched via `METRICOOL_ANALYTICS_ENABLED=true` (see Metricool Instagram Analytics section below).

**YouTube**: Uses OAuth refresh token. Future-timestamped uploads are sent as `private` with `publishAt`.

**Instagram**: Graph API with automatic token refresh on auth errors. Container polling with 300s timeout.

**TikTok**: Direct API upload via open.tiktokapis.com.

**X (Twitter)**: OAuth 1.0a with API key/secret + access token/secret.

## Metricool Instagram Analytics

Metricool analytics fetch Instagram Reels engagement data via the `/v2/analytics/reels/instagram` endpoint. The pipeline matches reels to DB records using caption similarity (primary) and timestamp proximity (fallback), then stores metrics in `video_performance_metrics` with `platform='instagram'` and `source='metricool_api'`.

| Parameter                     | Default | Env var                              |
|-------------------------------|---------|--------------------------------------|
| Enabled                       | false   | `METRICOOL_ANALYTICS_ENABLED`        |
| Lookback days                 | 7       | `METRICOOL_ANALYTICS_LOOKBACK_DAYS`  |
| Fetch limit                   | 100     | `METRICOOL_ANALYTICS_FETCH_LIMIT`    |

When enabled, the fetch runs at the start of each pipeline run (before content ingest) as a non-blocking step. Failures are logged but do not abort the pipeline. The standalone analysis script (`scripts/instagram_analytics.py`) can also be run independently for ad-hoc reporting.

## YouTube Analytics (Primary Analytics Source)

YouTube Data API v3 is the primary YouTube analytics source. Metricool provides the Instagram analytics channel.

| Parameter                     | Default | Env var                              |
|-------------------------------|---------|--------------------------------------|
| Enabled                       | true    | `YOUTUBE_ANALYTICS_ENABLED`          |
| Lookback days                 | 7       | `YOUTUBE_ANALYTICS_LOOKBACK_DAYS`    |
| Fetch limit                   | 50      | `YOUTUBE_ANALYTICS_FETCH_LIMIT`      |

### Available metrics (Data API v3)

- **views, likes, comments**: Direct from `videos.list` statistics
- **engagement_rate**: `(likes + comments) / views`
- **estimated watch time**: `views * duration * assumed_avg_view_pct` (40% for Shorts, 30% for long-form)
- **retention / completion rate**: Not available from Data API (requires Analytics API with verified OAuth consent screen); autotune handles this gracefully

Metrics are fetched every pipeline run and stored in `video_performance_metrics`. Autotune uses views + engagement when retention data is unavailable.

## Thumbnail Generation

Custom thumbnails are generated for each video using one of three A/B-tested strategies.

| Parameter                     | Default | Env var                              |
|-------------------------------|---------|--------------------------------------|
| Enabled                       | true    | `THUMBNAIL_GENERATION_ENABLED`       |
| Gemini API key                | --      | `GEMINI_API_KEY`                     |
| Google Custom Search API key  | --      | `GOOGLE_CUSTOM_SEARCH_API_KEY`       |
| Google Custom Search CX       | --      | `GOOGLE_CUSTOM_SEARCH_CX`           |

Thumbnails are uploaded to YouTube via `thumbnails.set` API after video upload.

## Autotune

Performance optimization feedback loop using YouTube Data API v3 metrics.

| Parameter                     | Default | Env var                                   |
|-------------------------------|---------|-------------------------------------------|
| Autotune enabled              | true    | `AUTOTUNE_ENABLED`                        |
| Apply overrides               | true    | `AUTOTUNE_APPLY_OVERRIDES`                |
| Min sample size               | 10      | `AUTOTUNE_MIN_SAMPLE_SIZE`                |
| Max daily adjustment ratio    | 0.10    | `AUTOTUNE_MAX_DAILY_ADJUSTMENT_RATIO`     |
| Min views for reliable data   | 100     | `AUTOTUNE_MIN_VIEWS_FOR_RELIABLE_RETENTION` |

### Override Bounds

| Override key                  | Min | Max |
|-------------------------------|-----|-----|
| CONTENT_SCRIPT_TARGET_WORDS   | 20  | 250 |
| CONTENT_SCRIPT_TARGET_SECONDS | 8   | 90  |
| TOP_HEADLINES_PER_RUN         | 1   | 10  |
| YOUTUBE_SCHEDULE_SPACING_HOURS| 1   | 24  |
| BUFFER_SCHEDULE_SPACING_HOURS | 1   | 24  |

## File and Storage Conventions

### Supabase Storage Buckets

| Bucket          | Contents            |
|-----------------|---------------------|
| `voice-assets`  | MP3 audio files     |
| `video-assets`  | MP4 video + SRT subtitles |

### Database Tables

| Table                         | Purpose                                  |
|-------------------------------|------------------------------------------|
| `source_posts`                | Ingested articles                        |
| `content_assets`              | Scripts, captions, metadata              |
| `media_assets`                | Images/video URLs + quality gate summary |
| `voice_assets`                | MP3 URLs, audio status                   |
| `video_assets`                | MP4 + SRT URLs, video status             |
| `publish_jobs`                | Publish queue entries                    |
| `publish_attempts`            | Publish attempt audit trail              |
| `pipeline_runs`               | Run logs with status and error tracking  |
| `persona_profiles`            | Persona configuration                   |
| `runtime_overrides`           | Autotune override storage                |
| `optimization_recommendations`| Autotune recommendations                 |
| `video_performance_metrics`   | YouTube Data API v3 analytics data       |

### Idempotency

Pipeline is idempotent with unique key `(source, source_guid)`. In addition, link-level dedup checks `source_posts` for any existing row with the same normalized URL before inserting, preventing duplicate coverage when the same article arrives from different RSS feeds. Rerunning on the same content does not create duplicates.

## A/B Testing

Content experiments that test one variable at a time across the daily video output.

### Pipeline Integration

The pipeline auto-rotates experiments at the start of each run via `maybe_rotate_experiment()`. If the active experiment has sufficient data, it is evaluated, concluded, and a new experiment is started from the queue. Each post is assigned to a variant via round-robin. For prompt-based variables, the variant's prompt modifier is injected into content generation. For behavior-based variables (e.g., `thumbnail_source`), the variant controls pipeline behavior directly.

### Database Tables

| Table                | Purpose                                           |
|----------------------|---------------------------------------------------|
| `ab_experiments`     | Experiment definitions (name, variable, variants) |
| `ab_assignments`     | Per-post variant assignments                      |

### Content Asset Extensions

| Column            | Type    | Purpose                                        |
|-------------------|---------|------------------------------------------------|
| `experiment_id`   | uuid    | FK to active experiment (nullable)             |
| `variant_key`     | text    | Assigned variant for this content (nullable)   |
| `content_signals`  | jsonb   | Auto-classified topic, hook, length, title formula |

### Content Signals

Every generated script is automatically classified on four dimensions:

| Signal           | Values                                                                |
|------------------|-----------------------------------------------------------------------|
| `topic_category` | government, geopolitical, tech, culture, finance, science, celebrity |
| `hook_type`      | breaking-event, question, number-lead, name-drop, controversy        |
| `length_bucket`  | short (20-25s), medium (26-30s), long (31-40s)                      |
| `title_formula`  | verb-subject-suspense, subject-did-thing, question-hook, name-drop-event, danger-statement |

### Testable Variables

| Variable           | Modifier scope              | Type      |
|--------------------|-----------------------------|-----------|
| `hook_type`        | First sentence style        | Prompt    |
| `script_length`    | Word/duration target        | Prompt    |
| `title_formula`    | Title construction pattern  | Prompt    |
| `caption_style`    | Platform caption tone       | Prompt    |
| `thumbnail_source` | Thumbnail generation strategy | Behavior |

### Experiment Lifecycle (auto-rotating)

1. `maybe_rotate_experiment()` checks if active experiment has >= 14 samples per variant
2. If yes, evaluate with composite scoring
3. If >15% score difference: adopt winner, conclude, start next experiment from queue
4. If <5%: inconclusive; if 21+ days old: force-conclude and move on
5. Queue rotates: hook_type -> thumbnail_source -> script_length -> title_formula -> caption_style

## Analytics

Performance analysis framework documented in `.channel/analytics.md`. Key components:

- **Composite scoring**: views (40%) + engagement rate (25%) + like ratio (15%) + completion rate (20%)
- **Performance tiers**: viral (1000+), strong (500-999), average (100-499), underperforming (<100 views within 48h)
- **Analytics report**: `python -m pipeline.analytics_report` parses pasted YouTube Studio data and generates markdown performance reports with signal correlation analysis

## Model Choices

| Task                   | Model                     | Provider    |
|------------------------|---------------------------|-------------|
| Script generation      | claude-opus-4-6           | Anthropic   |
| Script repair          | claude-opus-4-6           | Anthropic   |
| Media quality LLM      | claude-opus-4-6           | Anthropic   |
| Headline ranking       | claude-opus-4-6           | Anthropic   |
| Thumbnail search query | claude-opus-4-6           | Anthropic   |
| Text-to-speech         | ElevenLabs TTS            | ElevenLabs  |
| Video render           | Remotion 4 + Chrome       | Local       |
| Background gen         | gemini-3-pro-image-preview| Google      |
| Thumbnail gen          | gemini-3-pro-image-preview| Google      |
| YouTube Analytics      | YouTube Data API v3       | Google      |
