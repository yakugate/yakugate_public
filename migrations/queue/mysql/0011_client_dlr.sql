-- ============================================================================
-- 0011_client_dlr.sql — client-facing DLR outbound queue (smsc leg) (2026-07-19)
-- After the DLR service correlates a vendor receipt (esme leg), it enqueues a
-- client_dlr job here; the process that holds the client's bind (smpp-server) or
-- an HTTP dlr_url poster claims it and delivers the receipt to the client
-- (deliver_sm on the bind, or POST dlr_url). PO decision D-022.
-- ============================================================================

CREATE TABLE IF NOT EXISTS client_dlr (
  id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  account_id      BIGINT UNSIGNED NOT NULL,
  client_msg_id   CHAR(26)        NOT NULL,   -- the id we returned to the client at submit
  status          VARCHAR(16)     NOT NULL,   -- delivered | failed | expired | ...
  receipt         VARCHAR(512)    NULL,       -- raw receipt text (audit)
  attempts        INT             NOT NULL DEFAULT 0,
  next_attempt_at DATETIME        NULL,
  created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_acct (account_id, next_attempt_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
