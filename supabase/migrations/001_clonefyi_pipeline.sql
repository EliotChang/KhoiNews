-- Clone.fyi content pipeline schema
create extension if not exists pgcrypto;

create table if not exists source_posts (
  id uuid primary key default gen_random_uuid(),
  source text not null default 'clone_fyi',
  source_guid text not null,
  title text not null,
  description text,
  link text not null,
  published_at timestamptz,
  raw_payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (source, source_guid)
);

create index if not exists idx_source_posts_published_at
  on source_posts (published_at desc);

create table if not exists content_assets (
  id uuid primary key default gen_random_uuid(),
  post_id uuid not null references source_posts(id) on delete cascade,
  script_10s text not null,
  caption_instagram text not null,
  caption_tiktok text not null,
  caption_youtube text not null,
  hashtags text[] not null default '{}',
  tone text not null default 'informative',
  language text not null default 'en',
  model_name text,
  prompt_version text not null default 'v1',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (post_id)
);

create table if not exists voice_assets (
  id uuid primary key default gen_random_uuid(),
  post_id uuid not null references source_posts(id) on delete cascade,
  elevenlabs_voice_id text not null,
  audio_url text,
  audio_duration_sec numeric(6,2),
  status text not null default 'pending',
  error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (post_id)
);

create table if not exists media_assets (
  id uuid primary key default gen_random_uuid(),
  post_id uuid not null references source_posts(id) on delete cascade,
  media_type text not null check (media_type in ('image', 'video')),
  media_url text not null,
  source_page_url text not null,
  selection_reason text not null,
  status text not null default 'ready',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (post_id)
);

create table if not exists pipeline_runs (
  id uuid primary key default gen_random_uuid(),
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  status text not null default 'running',
  posts_seen integer not null default 0,
  posts_processed integer not null default 0,
  errors jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create or replace function set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists trg_source_posts_updated_at on source_posts;
create trigger trg_source_posts_updated_at
before update on source_posts
for each row execute function set_updated_at();

drop trigger if exists trg_content_assets_updated_at on content_assets;
create trigger trg_content_assets_updated_at
before update on content_assets
for each row execute function set_updated_at();

drop trigger if exists trg_voice_assets_updated_at on voice_assets;
create trigger trg_voice_assets_updated_at
before update on voice_assets
for each row execute function set_updated_at();

drop trigger if exists trg_media_assets_updated_at on media_assets;
create trigger trg_media_assets_updated_at
before update on media_assets
for each row execute function set_updated_at();

drop trigger if exists trg_pipeline_runs_updated_at on pipeline_runs;
create trigger trg_pipeline_runs_updated_at
before update on pipeline_runs
for each row execute function set_updated_at();
