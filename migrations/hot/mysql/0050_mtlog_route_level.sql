-- ============================================================================
-- 0050_mtlog_route_level.sql — which level of the rule sent it (2026-09-30)
--
-- route_level: 0 = Main, 1 = Backup 1, … ; send_round: how many rounds had
-- failed before this one (config/0131, queue/0035). Shown on the message
-- detail ("Sent by Backup 1, round 2"). NULL on rows written before.
-- One column per statement.
-- ============================================================================
ALTER TABLE mtlog_template ADD COLUMN route_level INT NULL;
ALTER TABLE mtlog_template ADD COLUMN send_round INT NULL;
