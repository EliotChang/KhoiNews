-- Add dedicated X/Twitter caption column to content_assets
alter table content_assets
  add column if not exists caption_x text;
