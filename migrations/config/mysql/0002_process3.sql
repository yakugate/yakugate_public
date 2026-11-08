-- ============================================================================
-- 0002  Process 3 + anti-abuse — CONFIG store additions (MySQL)
-- New: vendor_status_codes (per-vendor non-standard command_status mapping).
-- Plus the Process 3 session/timing columns on vendors.
-- ============================================================================

-- Per-vendor non-standard command_status table. Standard SMPP 3.4 codes use the
-- engine defaults; a row here overrides how THIS vendor's code is treated. Any
-- code not standard and not listed here => FAIL. scope: submit_sm_resp or DLR.
CREATE TABLE vendor_status_codes (
  id          BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  vendor_id   BIGINT UNSIGNED NOT NULL,
  scope       ENUM('submit','dlr') NOT NULL DEFAULT 'submit',
  code        INT UNSIGNED NOT NULL,                    -- command_status / DLR err code
  action      ENUM('success','retry','permanent') NOT NULL,
  description VARCHAR(190),
  UNIQUE KEY uq_vendor_code (vendor_id, scope, code),
  CONSTRAINT fk_vsc_vendor FOREIGN KEY (vendor_id) REFERENCES vendors(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Process 3 per-vendor session & timing config (existing: submit_throughput=TPS, bind=bind_mode).
ALTER TABLE vendors
  ADD COLUMN sessions             INT NOT NULL DEFAULT 1,     -- concurrent SMPP sessions (binds)
  ADD COLUMN window_size          INT NOT NULL DEFAULT 10,    -- outstanding submit_sm, per vendor
  ADD COLUMN retry_interval_sec   INT NOT NULL DEFAULT 10,    -- fixed reconnect interval
  ADD COLUMN enquire_link_sec     INT NOT NULL DEFAULT 30,    -- keepalive period
  ADD COLUMN response_timeout_sec INT NOT NULL DEFAULT 30,    -- bind/submit response wait
  ADD COLUMN poll_interval_ms     INT NOT NULL DEFAULT 1000,  -- claim poll period
  ADD COLUMN batch_size           INT NOT NULL DEFAULT 50,    -- rows per claim
  ADD COLUMN max_attempts         INT NOT NULL DEFAULT 5,     -- retryable attempts before dead-letter
  ADD COLUMN throttle_pause_sec   INT NOT NULL DEFAULT 10,    -- session pause after ESME_RTHROTTLED
  ADD COLUMN msgid_transform      VARCHAR(32) NULL;           -- DLR id normalization: none|hex2dec|dec2hex|trim:<n>

-- Note: rate limit is a single per-account TPS = accounts.throughput (INT, 0 =
-- unlimited); throughput_limit/campaign_tps_limit were removed. Sending quotas
-- moved to country_rules (per destination country). Prepaid balance moved to
-- accounts.balance; account_balance table removed. accounts.timezone drives the
-- quiet-hours window (quiet_start/quiet_end are minutes-of-day in that tz).
