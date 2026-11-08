-- 0058_vendors_p3_tuning_columns.sql — restore the P3 per-vendor tuning columns
-- (found 2026-08-10 during the P3 test campaign). 0002 added these to vendors;
-- 0006_schema_v2 DROPped and recreated vendors WITHOUT them; the 2026-07-29
-- P3 scope pass then made loadVendors() SELECT them unconditionally — so on any
-- v2-provisioned config store P3 fails with Error 1054 and binds ZERO vendors.
-- Re-added per the standing PO decision (typed columns are the source of truth
-- for operational tuning — process-3-vendor-status-codes-design.md §7).
--
-- Defaults are 0 (= "use the engine's built-in fallback": window 10, backoff
-- base 1s, enquire_link 20s, response timeout 60s, throttle pause
-- YAKU_SMPP_THROTTLE_PAUSE_SEC, batch 500), NOT 0002's opinionated values —
-- this preserves the behavior every vendor has today; tuning is opt-in per
-- vendor. sessions defaults to 1 (a 0-session vendor would bind nothing).
-- poll_interval_ms / max_attempts are deliberately NOT restored: nothing in Go
-- reads them (max_attempts is P7's domain — see process-3-backlog.md).
ALTER TABLE vendors
  ADD COLUMN sessions             INT NOT NULL DEFAULT 1,   -- concurrent SMPP binds
  ADD COLUMN window_size          INT NOT NULL DEFAULT 0,   -- outstanding submit_sm (0 = engine default 10; queue_config.window overrides when set)
  ADD COLUMN retry_interval_sec   INT NOT NULL DEFAULT 0,   -- reconnect backoff base (0 = 1s, doubling to 30s cap)
  ADD COLUMN enquire_link_sec     INT NOT NULL DEFAULT 0,   -- keepalive period (0 = 20s)
  ADD COLUMN response_timeout_sec INT NOT NULL DEFAULT 0,   -- submit_sm_resp wait (0 = 60s)
  ADD COLUMN throttle_pause_sec   INT NOT NULL DEFAULT 0,   -- session pause after ESME_RTHROTTLED (0 = YAKU_SMPP_THROTTLE_PAUSE_SEC)
  ADD COLUMN batch_size           INT NOT NULL DEFAULT 0,   -- terminal-write flush size (0 = 500)
  ADD COLUMN msgid_transform      VARCHAR(32) NULL;         -- DLR id normalization: none|hex2dec|dec2hex|trim:<n>
