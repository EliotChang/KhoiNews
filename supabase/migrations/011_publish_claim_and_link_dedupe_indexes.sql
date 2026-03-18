-- 011: indexes for atomic publish claiming and link-level dedupe checks

create index if not exists idx_publish_jobs_claim_ready
  on publish_jobs (coalesce(persona_key, 'default'), platform, status, next_retry_at, created_at);

create index if not exists idx_publish_jobs_claim_stale_in_progress
  on publish_jobs (coalesce(persona_key, 'default'), platform, updated_at)
  where status = 'in_progress';

create index if not exists idx_publish_jobs_metricool_approval_status
  on publish_jobs (
    coalesce(persona_key, 'default'),
    platform,
    coalesce(nullif(request_payload->>'approval_status', ''), 'pending')
  )
  where platform = 'metricool';

create index if not exists idx_source_posts_link_normalized
  on source_posts (lower(regexp_replace(btrim(link), '/+$', '')))
  where link is not null
    and btrim(link) != '';
