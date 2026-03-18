-- A/B testing experiments and assignments

create table if not exists ab_experiments (
  id uuid primary key default gen_random_uuid(),
  persona_key text not null references persona_profiles(persona_key) on update cascade,
  experiment_name text not null,
  variable_name text not null,
  variants jsonb not null default '[]'::jsonb,
  status text not null default 'active' check (status in ('active', 'paused', 'completed')),
  hypothesis text not null default '',
  conclusion text,
  start_date timestamptz not null default now(),
  end_date timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (persona_key, experiment_name)
);

drop trigger if exists trg_ab_experiments_updated_at on ab_experiments;
create trigger trg_ab_experiments_updated_at
before update on ab_experiments
for each row execute function set_updated_at();

create index if not exists idx_ab_experiments_active
  on ab_experiments (persona_key, status)
  where status = 'active';

create table if not exists ab_assignments (
  id uuid primary key default gen_random_uuid(),
  experiment_id uuid not null references ab_experiments(id) on delete cascade,
  post_id uuid not null references source_posts(id) on delete cascade,
  variant_key text not null,
  assigned_at timestamptz not null default now(),
  unique (experiment_id, post_id)
);

create index if not exists idx_ab_assignments_experiment
  on ab_assignments (experiment_id, variant_key);

create index if not exists idx_ab_assignments_post
  on ab_assignments (post_id);

alter table content_assets
  add column if not exists experiment_id uuid references ab_experiments(id) on delete set null,
  add column if not exists variant_key text,
  add column if not exists content_signals jsonb not null default '{}'::jsonb;

create index if not exists idx_content_assets_experiment
  on content_assets (experiment_id)
  where experiment_id is not null;
