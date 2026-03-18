alter table publish_jobs
  drop constraint if exists publish_jobs_platform_check;

alter table publish_jobs
  add constraint publish_jobs_platform_check
  check (platform in ('youtube', 'instagram', 'buffer', 'tiktok', 'x'));
