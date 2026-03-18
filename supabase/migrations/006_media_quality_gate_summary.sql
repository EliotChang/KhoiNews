alter table if exists media_assets
  add column if not exists media_quality_summary jsonb not null default '{}'::jsonb;
