-- ============================================================================
-- 0054_monitor_events_days.sql — what happened, kept after the detail goes
-- (Saleem 2026-10-05, D-126)
--
-- monitor_samples keeps every role's 5-second report for
-- YAKU_MONITOR_RETENTION_DAYS (default 3), then drops whole days. Two tables
-- keep what matters for longer — YAKU_MONITOR_SUMMARY_DAYS (default 10):
--
--   monitor_events  one row per thing that happened on a deployment: a role
--                   stopped or restarted, a vendor connection went down, a
--                   queue got stuck, latency or refusals went over their
--                   limit, the host ran hot, traffic stopped or spiked. A
--                   condition writes a `start` row and an `end` row; a one-off
--                   (a restart) writes `once`. `context` holds the whole
--                   deployment's reports at the interval before, at and after
--                   the event (gzip'd JSON), so the moment can still be opened
--                   once its detail rows are gone.
--   monitor_days    one row per deployment per UTC day: totals, peaks with
--                   their times, event counts; the rest (per-role uptime, an
--                   hourly series, the day's events) gzip'd in `body`.
--
-- Written by the engine's own monitoring upkeep (internal/monitor), one
-- process at a time; the Admin API only reads them.
-- ============================================================================
CREATE TABLE IF NOT EXISTS monitor_events (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  deployment_id VARCHAR(64)  NOT NULL,
  at            DATETIME(3)  NOT NULL COMMENT 'UTC: the sample that showed it',
  kind          VARCHAR(32)  NOT NULL COMMENT 'role_down, role_restarted, vendor_down, queue_stuck, …',
  phase         ENUM('start','end','once') NOT NULL,
  severity      ENUM('critical','warning','info') NOT NULL,
  subject       VARCHAR(128) NOT NULL DEFAULT '' COMMENT 'the role/instance, host or deployment it is about',
  detail        VARCHAR(500) NOT NULL DEFAULT '',
  interval_sec  SMALLINT UNSIGNED NOT NULL DEFAULT 5,
  context       MEDIUMBLOB NULL COMMENT 'gzip JSON: the deployment at the interval before, at and after',
  created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_monitor_event (deployment_id, kind, phase, subject, at),
  KEY idx_monitor_event_dep_at (deployment_id, at),
  KEY idx_monitor_event_at (at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS monitor_days (
  deployment_id   VARCHAR(64)  NOT NULL,
  day             DATE         NOT NULL COMMENT 'UTC day',
  interval_sec    SMALLINT UNSIGNED NOT NULL DEFAULT 5,
  samples         INT UNSIGNED NOT NULL DEFAULT 0,
  accepted        BIGINT UNSIGNED NOT NULL DEFAULT 0,
  rejected        BIGINT UNSIGNED NOT NULL DEFAULT 0,
  sent            BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'about: summed from the 60-second rates',
  receipts        BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'about',
  mo              BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'about',
  peak_tps_in     DECIMAL(10,2) NOT NULL DEFAULT 0,
  peak_tps_in_at  DATETIME NULL,
  peak_tps_out    DECIMAL(10,2) NOT NULL DEFAULT 0,
  peak_tps_out_at DATETIME NULL,
  peak_queue_depth    BIGINT UNSIGNED NOT NULL DEFAULT 0,
  peak_queue_depth_at DATETIME NULL,
  peak_queue_age_sec  BIGINT UNSIGNED NOT NULL DEFAULT 0,
  peak_queue_age_at   DATETIME NULL,
  peak_latency_ms     DECIMAL(10,2) NOT NULL DEFAULT 0,
  peak_latency_at     DATETIME NULL,
  events_critical INT UNSIGNED NOT NULL DEFAULT 0,
  events_warning  INT UNSIGNED NOT NULL DEFAULT 0,
  events_info     INT UNSIGNED NOT NULL DEFAULT 0,
  body            MEDIUMBLOB NULL COMMENT 'gzip JSON: role uptime, hourly series, the day''s events',
  created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (deployment_id, day),
  KEY idx_monitor_days_day (day)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Small state the upkeep keeps between passes (the event scan's watermark).
CREATE TABLE IF NOT EXISTS monitor_state (
  name       VARCHAR(64)  NOT NULL PRIMARY KEY,
  value      VARCHAR(255) NOT NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
