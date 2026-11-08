-- 0074_webhook_delivery_alerts.sql — tell a company its own webhook is failing
-- (P5-B02/P6-B1, PO ruling 2026-08-31: "log it, and email the company").
--
-- WHY A TABLE AND NOT JUST AN EMAIL
--
-- The engine has no mailer, and the Admin API — which does — is forbidden from
-- reading the Queue store where client_dlr and mo_queue live (the P12 invariant
-- enforced by db/hlrSource.js: hlr_* tables and NOTHING else). So the engine
-- cannot send the mail and the mailer cannot see the failures.
--
-- This table is the handoff, and it lives in the CONFIG store precisely because
-- both sides may read it. The engine UPSERTS one row per failing endpoint as it
-- gives up on pushes; the Admin API poller reads it, emails the company, and
-- stamps notified_at. One row per endpoint, not per message: a customer whose
-- endpoint is down loses thousands of receipts and must receive ONE mail about
-- it, not thousands.
CREATE TABLE IF NOT EXISTS webhook_delivery_alerts (
  id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  endpoint_id     BIGINT UNSIGNED NOT NULL,           -- http_endpoints.id
  account_id      BIGINT UNSIGNED NOT NULL,
  kind            ENUM('dlr','mo') NOT NULL,
  url             VARCHAR(512) NOT NULL,
  first_failed_at DATETIME NOT NULL,
  last_failed_at  DATETIME NOT NULL,
  given_up_count  BIGINT UNSIGNED NOT NULL DEFAULT 0, -- messages discarded since first_failed_at
  last_error      VARCHAR(255) DEFAULT NULL,
  notified_at     DATETIME DEFAULT NULL,              -- when the company was last told
  created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_endpoint (endpoint_id),
  KEY idx_unnotified (notified_at, last_failed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
