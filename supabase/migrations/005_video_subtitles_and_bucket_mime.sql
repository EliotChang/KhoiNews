alter table if exists video_assets
add column if not exists subtitle_url text;

update storage.buckets
set allowed_mime_types = array['video/mp4', 'application/x-subrip', 'text/plain']
where id = 'video-assets';
