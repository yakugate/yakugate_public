-- 0028_client_receipt_addresses.sql — fuller receipts to SMPP clients, and a
-- receipt for every part id (PO 2026-09-11).
--
-- client_dlr.src / dst: the original message's sender and destination. The
-- smpp-server puts them on the receipt deliver_sm (source = the handset,
-- destination = the sender), which is what SMPP clients match receipts
-- against. Until now both were sent as "0". NULL on rows written before this
-- migration; the push falls back to "0" for those.
--
-- client_part_ids: when an SMPP client sends a long message as separate parts,
-- each part gets its own message_id, but the parts are joined into ONE message
-- (D-096) carrying part 1's id. This table maps that id to every part's id, so
-- the receipt path can send each part its own receipt. Written by smpp-server
-- when a group completes; read when a receipt is queued for the client;
-- deleted once a final receipt has gone out; rows older than a week are swept
-- (a message that never got a final receipt must not keep its row for ever).
ALTER TABLE client_dlr
  ADD COLUMN src VARCHAR(32) NULL COMMENT 'original sender (becomes the receipt destination_addr)',
  ADD COLUMN dst VARCHAR(32) NULL COMMENT 'original destination (becomes the receipt source_addr)';

CREATE TABLE IF NOT EXISTS client_part_ids (
  msg_id     CHAR(26)     NOT NULL PRIMARY KEY COMMENT 'the joined message id (= part 1 id)',
  part_ids   TEXT         NOT NULL COMMENT 'comma-separated part ids, part order, part 1 first',
  created_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
