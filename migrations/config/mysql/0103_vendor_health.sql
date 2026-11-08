-- 0103_vendor_health.sql — failover follows real outages (PO 2026-09-14,
-- routing rules review).
--
-- Until now a vendor was "down" for routing only when an operator set it to
-- paused or disabled. A dropped SMPP bind, an open error breaker or an HTTP
-- vendor that kept failing changed nothing: new messages still went to it and
-- piled up in its queue.
--
-- down_after_sec: how long a vendor must have had no working connection
-- before it counts as down — routing then skips it and the messages already
-- waiting in its queue move to the next route. A short reconnect moves
-- nothing. recheck_after_sec: while down, how often it is tested again (SMPP
-- tries to bind; HTTP lets one real message through).
--
-- vendor_health: what each running vendor-role instance says about the
-- vendors it serves. One row per (vendor, instance), rewritten every few
-- seconds. failing_since is NULL while the vendor works. A vendor is down only
-- when every instance that reported recently says it is failing, and has for
-- at least down_after_sec. Rows from an instance that stopped reporting are
-- ignored once stale. Times are written and compared by the engine (UTC),
-- never with NOW(), so a database clock can't disagree with them.
ALTER TABLE vendors
  ADD COLUMN down_after_sec INT UNSIGNED NOT NULL DEFAULT 30
    COMMENT 'seconds without a working connection before the vendor counts as down',
  ADD COLUMN recheck_after_sec INT UNSIGNED NOT NULL DEFAULT 60
    COMMENT 'while down, seconds between tests (SMPP bind / one HTTP message)';

CREATE TABLE IF NOT EXISTS vendor_health (
  vendor_id     BIGINT UNSIGNED NOT NULL,
  instance      VARCHAR(128)    NOT NULL COMMENT 'host-pid of the vendor-role process',
  role          VARCHAR(32)     NOT NULL COMMENT 'smpp-vendor | http-vendor',
  failing_since DATETIME(3)     NULL     COMMENT 'UTC; NULL = working',
  reported_at   DATETIME(3)     NOT NULL COMMENT 'UTC; rows older than a few report intervals are ignored',
  detail        VARCHAR(190)    NULL     COMMENT 'why it is failing, for operators',
  PRIMARY KEY (vendor_id, instance),
  KEY idx_reported (reported_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
