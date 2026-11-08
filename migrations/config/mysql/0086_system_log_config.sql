-- Process 13 — the SYSTEM log level, settable from the portal
-- (PO 2026-09-04: "system level logs setting selection").
--
-- Accounts and vendors have carried their own log_config since 0081/0082.
-- The system's own level did not: it came only from YAKU_LOG_LEVEL, read once
-- at start-up, so changing it meant editing a file and restarting every role.
-- An operator cannot do that from the portal, and turning on debug during an
-- incident is exactly when a restart is least welcome.
--
-- ONE ROW, enforced. `id` is a CHECKed constant rather than an auto-increment:
-- a settings table that can grow a second row invites the question of which
-- row wins, and every reader would have to answer it the same way or drift.
--
-- Same JSON shape as accounts and vendors so one piece of Go reads all three.
-- The row's level is the DEFAULT every scope falls back to, so setting it to
-- `none` silences everything that has no level of its own — which is why the
-- portal says so next to the control.
CREATE TABLE IF NOT EXISTS system_log_config (
  id         TINYINT UNSIGNED NOT NULL DEFAULT 1 PRIMARY KEY,
  log_config JSON NOT NULL
             DEFAULT (JSON_OBJECT('level', 'info', 'max_size_mb', 100, 'retention_days', 30)),
  updated_by VARCHAR(64) DEFAULT NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT chk_system_log_single_row CHECK (id = 1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- The row must exist before anything reads it: a missing row would read as
-- "no system config", and every scope would silently fall back to the
-- compiled-in default instead of what the operator set.
INSERT IGNORE INTO system_log_config (id, log_config, updated_by)
VALUES (1, JSON_OBJECT('level', 'info', 'max_size_mb', 100, 'retention_days', 30), 'migration');
