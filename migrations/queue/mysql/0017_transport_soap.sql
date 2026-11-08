-- vendors.transport gains 'soap' (config/0050, PO 2026-07-28) — the queue
-- row's own transport column (egress: which vendor this message is headed
-- to) must accept the same value. Only queue_template and existing
-- queue_<cid> tables need this now; any NEW vendor's queue table is cloned
-- from queue_template at onboarding time and inherits it automatically.
ALTER TABLE queue_template
  MODIFY COLUMN transport ENUM('smpp','http','soap','blackhole') NOT NULL;

ALTER TABLE queue_testvendor
  MODIFY COLUMN transport ENUM('smpp','http','soap','blackhole') NOT NULL;
