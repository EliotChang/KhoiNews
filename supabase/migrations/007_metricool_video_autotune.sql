create table if not exists persona_profiles (
  id uuid primary key default gen_random_uuid(),
  persona_key text not null unique,
  display_name text not null default '',
  metricool_user_id text,
  metricool_blog_id text,
  metricool_target_platforms text[] not null default '{}'::text[],
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

drop trigger if exists trg_persona_profiles_updated_at on persona_profiles;
create trigger trg_persona_profiles_updated_at
before update on persona_profiles
for each row execute function set_updated_at();

insert into persona_profiles (persona_key, display_name)
values ('default', 'Default Persona')
on conflict (persona_key) do nothing;

alter table publish_jobs
  add column if not exists persona_key text not null default 'default';

alter table publish_jobs
  drop constraint if exists publish_jobs_platform_check;

alter table publish_jobs
  add constraint publish_jobs_platform_check
  check (platform in ('youtube', 'instagram', 'buffer', 'tiktok', 'x', 'metricool'));

create index if not exists idx_publish_jobs_persona_key
  on publish_jobs (persona_key, created_at desc);

create table if not exists video_performance_metrics (
  id uuid primary key default gen_random_uuid(),
  persona_key text not null references persona_profiles(persona_key) on update cascade,
  publish_job_id uuid references publish_jobs(id) on delete set null,
  platform text not null default 'metricool',
  external_post_id text not null,
  metric_timestamp timestamptz not null default now(),
  views integer,
  likes integer,
  comments integer,
  shares integer,
  saves integer,
  watch_time_seconds numeric(12,2),
  avg_watch_seconds numeric(10,2),
  avg_retention_ratio numeric(8,5),
  completion_rate numeric(8,5),
  engagement_rate numeric(8,5),
  metrics jsonb not null default '{}'::jsonb,
  source text not null default 'metricool_api',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

drop trigger if exists trg_video_performance_metrics_updated_at on video_performance_metrics;
create trigger trg_video_performance_metrics_updated_at
before update on video_performance_metrics
for each row execute function set_updated_at();

create unique index if not exists idx_video_metrics_unique_snapshot
  on video_performance_metrics (persona_key, platform, external_post_id, metric_timestamp);

create index if not exists idx_video_metrics_persona_window
  on video_performance_metrics (persona_key, metric_timestamp desc);

create table if not exists optimization_recommendations (
  id uuid primary key default gen_random_uuid(),
  persona_key text not null references persona_profiles(persona_key) on update cascade,
  diagnosis text not null,
  confidence numeric(6,5) not null default 0,
  sample_size integer not null default 0,
  window_start timestamptz,
  window_end timestamptz,
  recommended_overrides jsonb not null default '{}'::jsonb,
  rationale text not null default '',
  status text not null default 'proposed' check (status in ('proposed', 'applied', 'skipped')),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

drop trigger if exists trg_optimization_recommendations_updated_at on optimization_recommendations;
create trigger trg_optimization_recommendations_updated_at
before update on optimization_recommendations
for each row execute function set_updated_at();

create index if not exists idx_optimization_recommendations_persona_created
  on optimization_recommendations (persona_key, created_at desc);

create table if not exists runtime_overrides (
  id uuid primary key default gen_random_uuid(),
  persona_key text not null references persona_profiles(persona_key) on update cascade,
  key text not null,
  value jsonb not null,
  value_type text not null,
  source_recommendation_id uuid references optimization_recommendations(id) on delete set null,
  is_active boolean not null default true,
  applied_at timestamptz not null default now(),
  expires_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

drop trigger if exists trg_runtime_overrides_updated_at on runtime_overrides;
create trigger trg_runtime_overrides_updated_at
before update on runtime_overrides
for each row execute function set_updated_at();

create unique index if not exists idx_runtime_overrides_active_unique
  on runtime_overrides (persona_key, key)
  where is_active;

create index if not exists idx_runtime_overrides_active_lookup
  on runtime_overrides (persona_key, is_active, key);
