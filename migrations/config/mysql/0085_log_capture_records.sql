-- Process 13 Type 3 — bringing a capture's records to the PORTAL
-- (PO 2026-09-04: "i want capture to act like bringing the captured logs of
-- gateway to the portal and show on portal").
--
-- Until now a capture only produced files on the gateway hosts, which the
-- portal cannot reach: the Admin API runs elsewhere, and there may be many
-- gateways. Records now also land here, in a table both sides already talk
-- to, and the portal tails it. No new network path, no host list, and a
-- gateway that is down simply contributes nothing rather than breaking the
-- view.
--
-- THE GATEWAY FILE IS STILL THE COMPLETE ONE (PO ruling). This table holds a
-- capped, most-recent window; the file on disk holds everything. Both are
-- deleted 24h after the capture's window closes.
--
-- WHY A CAP AT ALL. Measured today: one message produces 21 records (18
-- pipeline stages plus submit, accepted and the vendor exchange). A capture
-- of a busy account is therefore enormous:
--
--     10 msg/s x  5 min =      63,000 records
--    100 msg/s x  5 min =     630,000 records
--   4000 msg/s x 15 min =  75,600,000 records
--
-- and every one carries the message text. Unbounded, a single capture would
-- outweigh the rest of this database and could not be displayed anyway. The
-- ring keeps the NEWEST N (PO ruling: "keep the newest, drop the oldest"), so
-- an operator watching a problem happen sees it, and the file on the gateway
-- still has the beginning.
CREATE TABLE IF NOT EXISTS log_capture_records (
  id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  capture_id BIGINT UNSIGNED NOT NULL,
  -- Which gateway role wrote it: one capture spans several (P1 sees the
  -- submit, P3 the vendor send, P5 the receipt), and the portal stitches them
  -- back into one flow by ordering on id.
  role       VARCHAR(32)  NOT NULL,
  at         DATETIME(3)  NOT NULL COMMENT 'when the step happened, UTC (the engine stamps it, not the DB)',
  step       VARCHAR(32)  NOT NULL COMMENT 'submit | stage | accepted | vendor_request | vendor_response | receipt',
  -- Denormalised so the portal can filter one message out of a busy capture
  -- without opening every row's JSON.
  message_id CHAR(26)     DEFAULT NULL,
  line       JSON         NOT NULL COMMENT 'the record exactly as written to the gateway file',
  KEY idx_capture_records (capture_id, id),
  KEY idx_capture_message (capture_id, message_id),
  CONSTRAINT fk_capture_records_capture
    FOREIGN KEY (capture_id) REFERENCES log_captures (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
