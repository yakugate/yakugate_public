-- Loss detection moves to the ACCOUNT (PO 2026-09-17): the switch that
-- refuses a send whose chosen route would sell below its buy cost
-- (403 LOSS_PREVENTED, S-03) is set per account on the Accounts screen.
-- companies.loss_detection_enabled (config/0079) stays in the schema —
-- migrations are additive — but the engine no longer reads it.
ALTER TABLE sms_service_config
  ADD COLUMN loss_detection_enabled TINYINT(1) NOT NULL DEFAULT 0
    COMMENT 'refuse 403 LOSS_PREVENTED when the chosen route would sell below its buy cost (per account, PO 2026-09-17)';
