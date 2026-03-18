create table if not exists video_assets (
  id uuid primary key default gen_random_uuid(),
  post_id uuid not null references source_posts(id) on delete cascade,
  template_name text not null default 'fish_lipsync',
  video_url text,
  video_duration_sec numeric(6,2),
  status text not null default 'pending',
  error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (post_id)
);

drop trigger if exists trg_video_assets_updated_at on video_assets;
create trigger trg_video_assets_updated_at
before update on video_assets
for each row execute function set_updated_at();

insert into storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
values (
  'video-assets',
  'video-assets',
  true,
  104857600,
  array['video/mp4']
)
on conflict (id)
do update set
  public = excluded.public,
  file_size_limit = excluded.file_size_limit,
  allowed_mime_types = excluded.allowed_mime_types;
