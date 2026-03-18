-- 010: YouTube Analytics integration and thumbnail A/B testing
--
-- Adds thumbnail columns to content_assets for tracking which thumbnail
-- strategy was used per video. The video_performance_metrics table already
-- supports multiple platforms via the platform column; YouTube Analytics
-- data is stored with platform='youtube' and source='youtube_analytics_api'.

alter table if exists content_assets
  add column if not exists thumbnail_url text,
  add column if not exists thumbnail_source text;

-- Index for querying experiments by variable + status (auto-rotation queries)
create index if not exists idx_ab_experiments_variable_status
  on ab_experiments (persona_key, variable_name, status, end_date desc);
