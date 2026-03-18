# CloneFYI News Pipeline

Deterministic single-workflow pipeline for CloneFYI shorts.

## Architecture
- Single production entrypoint: `.github/workflows/clonefyi-pipeline.yml`
- Single publish path: Metricool only (`platform=metricool`)
- Single video template: fish lipsync renderer
- No standalone publish-dispatch/force-publish/watchdog workflows
- No autotune, runtime overrides, or A/B rotation in execution path

## End-to-End Run Flow
1. Scheduled GitHub Action triggers `python -m pipeline.main`.
2. Primary ingest fetches CloneFYI RSS (`primary_rss`).
3. If primary ingest fails (unavailable/non-200/invalid feed), trusted fallback feeds are used (`trusted_fallback_feed`) when enabled.
4. Blocklist filtering removes disallowed domains/topics.
5. Headlines are ranked deterministically by recency (no LLM ranking variance).
6. For each candidate:
   - Pre-voice gate validates source context quality (title/description/url).
   - Article context extraction uses structured body parsing (`article`/`main`/paragraph aggregation) with metadata fallback.
   - Low-context links are skipped before script generation.
   - Repeated pre-voice failures are temporarily suppressed to reduce retry churn.
   - Script is generated in one fixed editorial profile (neutral, 20-35s, 70-90 words, 4-5 sentences).
   - Post-script gate validates substantive/factual bounds.
   - Media extraction and quality gate run.
   - Voice is generated.
   - Post-voice gate blocks short/truncated audio.
   - Video render runs with fish lipsync template.
   - Pre-publish gate blocks short/truncated video or script/audio/video mismatch.
   - Publish job enqueues only when all gates pass.
7. In-run publish dispatch sends ready jobs through Metricool.
8. Optional Metricool link-in-bio update runs for newly published posts.

## Required Environment Variables
- Database/storage:
  - `SUPABASE_DB_URL`
  - `SUPABASE_URL`
  - `SUPABASE_SERVICE_ROLE_KEY`
  - `SUPABASE_VOICE_BUCKET`
  - `SUPABASE_VIDEO_BUCKET`
- Generation:
  - `ANTHROPIC_API_KEY`
  - `ANTHROPIC_MODEL`
  - `ELEVENLABS_API_KEY`
  - `ELEVENLABS_VOICE_ID`
- Publishing:
  - `PUBLISH_ENABLED`
  - `PUBLISH_MAX_RETRIES`
  - `PUBLISH_MAX_JOBS_PER_RUN`
  - `PUBLISH_ENFORCE_COMPLIANCE`
  - `METRICOOL_PUBLISH_ENABLED`
  - `METRICOOL_USER_TOKEN`
  - `METRICOOL_API_URL`
  - `METRICOOL_USER_ID`
  - `METRICOOL_BLOG_ID`
  - `METRICOOL_TARGET_PLATFORMS`
  - `METRICOOL_REVIEW_REQUIRED`
  - `METRICOOL_LINK_IN_BIO_ENABLED`
  - `METRICOOL_LINK_IN_BIO_MAX_LINKS`
- Ingest:
  - `CLONEFYI_RSS_APP_URL`
- Pipeline profile/gates:
  - `CONTENT_SCRIPT_TARGET_SECONDS`
  - `CONTENT_SCRIPT_TARGET_WORDS`
  - `CONTENT_SCRIPT_MAX_WORDS_BUFFER`
  - `CONTENT_SCRIPT_MIN_FACTS`
  - `CONTENT_SCRIPT_MAX_SENTENCES`
  - `PRE_VOICE_DESCRIPTION_MIN_WORDS`
  - `PRE_VOICE_METADATA_ENRICHMENT_ENABLED`
  - `PRE_VOICE_ALLOW_TITLE_ONLY_FALLBACK`
  - `PRE_VOICE_TITLE_ONLY_MIN_WORDS`
  - `PRE_VOICE_FAIL_SUPPRESS_AFTER`
  - `PRE_VOICE_FAIL_SUPPRESS_DAYS`
  - `ARTICLE_CONTEXT_MIN_WORDS`
  - `ARTICLE_CONTEXT_MAX_WORDS`
  - `MAX_POSTS_PER_RUN`
  - `TOP_HEADLINES_PER_RUN`
  - `REQUEST_TIMEOUT_SECONDS`
  - `TOPIC_BLOCKLIST_ENABLED`
  - `TOPIC_BLOCK_TERMS`
  - `SOURCE_DOMAIN_BLOCKLIST`
- Render/thumbnail:
  - `ENABLE_VIDEO_RENDER`
  - `REMOTION_PROJECT_DIR`
  - `FISH_MOUTH_FRAMES_DIR`
  - `FISH_BACKGROUND_IMAGE_PATH`
  - `VIDEO_INTRO_MUSIC_PATH`
  - `VIDEO_BREAKING_NEWS_AUDIO_PATH`
  - `VIDEO_INTRO_MUSIC_VOLUME`
  - `VIDEO_BREAKING_NEWS_VOLUME`
  - `VIDEO_INTRO_DUCK_TO_VOLUME`
  - `VIDEO_INTRO_DUCK_SECONDS`
  - `VIDEO_REQUIRE_INTRO_AND_BREAKING_AUDIO`
  - `VIDEO_BROWSER_EXECUTABLE`
  - `THUMBNAIL_GENERATION_ENABLED`
  - `GEMINI_API_KEY`
  - `GOOGLE_CUSTOM_SEARCH_API_KEY`
  - `GOOGLE_CUSTOM_SEARCH_CX`

## Notes
- `PUBLISH_PLATFORMS` accepts only `metricool`.
- Primary ingest source tags are written to logs and source payloads.
- Ops: set `CLONEFYI_RSS_APP_URL` (including GitHub secret) to `https://clone.fyi/rss.xml` and avoid `rss.app` URLs.
- Runtime canonicalizes legacy `rss.app` primary URLs to `https://clone.fyi/rss.xml` as a safety net.
- Use canonical `VIDEO_*` keys for intro/breaking audio (`VIDEO_INTRO_MUSIC_PATH`, `VIDEO_BREAKING_NEWS_AUDIO_PATH`); legacy alias keys are not supported.
- Historical DB tables are preserved for compatibility; removed optimization/adapters are not used by runtime.
