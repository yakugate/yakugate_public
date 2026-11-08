-- Process 13 Type 3 — time-boxed MT/MO/DLR flow capture (PO rulings
-- 2026-09-04: full message text, operator-only, files deleted after 24h).
--
-- A capture is a DIAGNOSTIC MODE, not a log stream. An operator turns it on
-- for ONE account or ONE vendor, for a bounded window, and every process that
-- touches that entity's traffic writes the full flow — the request as
-- received, each pipeline stage's decision, the wire request and response to
-- the vendor, the raw receipt — including the message text itself.
--
-- THE MESSAGE-TEXT CARVE-OUT, stated rather than implied. Every other log in
-- this engine avoids message bodies. A capture writes them, because the
-- questions it exists to answer (why did no template match, why was this
-- encoded as UCS-2, why did the spam gate fire, why did this segment
-- differently) cannot be answered without the text. What keeps that
-- acceptable is all four of these together, and removing any one of them
-- reopens the question:
--
--   1. an operator starts it deliberately — never on by default, and the
--      customer cannot start one (PO ruling 2026-09-04)
--   2. it is time-boxed to 1, 5 or 15 minutes and cannot be extended
--   3. it is scoped to one account or one vendor, not the whole engine
--   4. the file is deleted 24 hours after the window closes
--
-- window_minutes is CHECKed rather than left free-form so a capture cannot be
-- started for a day by a typo or a bad API caller.
--
-- No file_path column, unlike the July spec's sketch: one capture is written
-- by several ROLES (P1 sees the submit, P3 the vendor send, P5 the receipt),
-- so there is no single file. Each role writes its own
-- <log dir>/<role>/captures/capture_<id>.log and the portal stitches them by
-- id — a derived path cannot go stale the way a stored one can.
CREATE TABLE IF NOT EXISTS log_captures (
  id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  type           VARCHAR(16)  NOT NULL DEFAULT 'flow'
                 COMMENT 'flow (Type 3). traffic (Type 4, pcap) is not built and needs its own PO rulings',
  scope_type     ENUM('account','vendor') NOT NULL,
  scope_id       BIGINT UNSIGNED NOT NULL,
  -- The username or vendor cid, denormalised on purpose: the capture check
  -- runs on the message hot path and must be an in-memory map lookup by the
  -- name the process already has, never a join.
  scope_name     VARCHAR(190) NOT NULL,
  window_minutes INT UNSIGNED NOT NULL,
  requested_by   VARCHAR(64)  NOT NULL COMMENT 'operator user id — captures are operator-only',
  started_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  expires_at     DATETIME     NOT NULL COMMENT 'capture stops writing at this moment',
  purge_at       DATETIME     NOT NULL COMMENT 'the files are deleted at this moment (expires_at + 24h)',
  -- running  -> the window is open and processes are writing
  -- expired  -> the window closed; the files are still on disk to be read
  -- purged   -> the window closed AND the files have been deleted (purge_at)
  -- failed   -> the capture could not be started
  status         ENUM('running','expired','purged','failed') NOT NULL DEFAULT 'running',
  created_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT chk_log_captures_window CHECK (window_minutes IN (1, 5, 15)),
  KEY idx_log_captures_active (status, expires_at),
  KEY idx_log_captures_scope (scope_type, scope_name),
  KEY idx_log_captures_purge (purge_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
