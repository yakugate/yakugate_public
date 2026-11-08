-- vendors.transport gains 'soap' (config/0050, PO 2026-07-28) — every hot
-- table carrying a transport column must accept it too. Hot MDR tables are
-- per-account (records_<user>_<date> / mtlog_<user>_<date>), not per-vendor,
-- so an account's existing daily table could receive 'soap' traffic
-- TODAY if a SOAP vendor is onboarded and routed to before the next daily
-- rollover — unlike the queue side, waiting for the next day's fresh clone
-- of the template is not good enough here. Templates AND every table live
-- right now are altered explicitly (the set is small and known; a dynamic
-- information_schema loop was not worth it for this).
ALTER TABLE records_template
  MODIFY COLUMN transport ENUM('smpp','http','soap','blackhole') NOT NULL;

ALTER TABLE mtlog_template
  MODIFY COLUMN transport ENUM('smpp','http','soap','blackhole') NOT NULL;

ALTER TABLE mtlog_aracct_27_07_2026
  MODIFY COLUMN transport ENUM('smpp','http','soap','blackhole') NOT NULL;

ALTER TABLE mtlog_demo_26_07_2026
  MODIFY COLUMN transport ENUM('smpp','http','soap','blackhole') NOT NULL;

ALTER TABLE mtlog_demo_27_07_2026
  MODIFY COLUMN transport ENUM('smpp','http','soap','blackhole') NOT NULL;

ALTER TABLE mtlog_sidacct_26_07_2026
  MODIFY COLUMN transport ENUM('smpp','http','soap','blackhole') NOT NULL;

ALTER TABLE mtlog_sidacct_27_07_2026
  MODIFY COLUMN transport ENUM('smpp','http','soap','blackhole') NOT NULL;
