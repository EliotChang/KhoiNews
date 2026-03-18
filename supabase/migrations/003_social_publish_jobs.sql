create table if not exists publish_jobs (
  id uuid primary key default gen_random_uuid(),
  post_id uuid not null references source_posts(id) on delete cascade,
  platform text not null check (platform in ('youtube', 'instagram', 'tiktok', 'x')),
  status text not null default 'queued' check (status in ('queued', 'in_progress', 'published', 'failed', 'skipped', 'dead_letter')),
  idempotency_key text not null,
  request_hash text not null,
  request_payload jsonb not null default '{}'::jsonb,
  external_post_id text,
  retry_count integer not null default 0,
  max_retries integer not null default 3,
  next_retry_at timestamptz,
  last_error text,
  error_category text,
  compliance_checks jsonb not null default '[]'::jsonb,
  published_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (post_id, platform),
  unique (idempotency_key)
);

create index if not exists idx_publish_jobs_status_next_retry
  on publish_jobs (status, next_retry_at, created_at);

create table if not exists publish_attempts (
  id uuid primary key default gen_random_uuid(),
  publish_job_id uuid not null references publish_jobs(id) on delete cascade,
  attempt_number integer not null,
  status text not null check (status in ('in_progress', 'published', 'failed', 'skipped')),
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  request_payload jsonb not null default '{}'::jsonb,
  response_payload jsonb not null default '{}'::jsonb,
  http_status integer,
  error_category text,
  error_message text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (publish_job_id, attempt_number)
);

drop trigger if exists trg_publish_jobs_updated_at on publish_jobs;
create trigger trg_publish_jobs_updated_at
before update on publish_jobs
for each row execute function set_updated_at();

drop trigger if exists trg_publish_attempts_updated_at on publish_attempts;
create trigger trg_publish_attempts_updated_at
before update on publish_attempts
for each row execute function set_updated_at();
