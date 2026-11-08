-- Process 13 Type 4 — time-boxed packet capture (PO rulings 2026-09-17).
--
-- A packet capture is one more kind of log_captures row (type = 'traffic'),
-- beside the Type 3 flow capture (type = 'flow', config/0084):
--
--   * operators only, and it takes the WHOLE listener port — there is no
--     per-tenant filter, so scope_type is the new value 'listener',
--     scope_name is the listener's name ("smpp-server") and scope_id its port;
--   * the window is the same 1, 5 or 15 minutes (the CHECK already holds);
--   * a REASON is required, and is kept on the row and in audit_log;
--   * the .pcap is stored encrypted with the gateway key on the gateway host
--     and deleted 24 hours after the window closes (purge_at, as for flow).
--
-- size_bytes is the encrypted file's size once the window has closed.
-- error says, in plain words, why a capture failed (no tcpdump, no
-- permission, no gateway key). truncated = the capture hit the size limit
-- (YAKU_PCAP_MAX_MB) before its window ended.
ALTER TABLE log_captures
  MODIFY COLUMN type VARCHAR(16) NOT NULL DEFAULT 'flow'
         COMMENT 'flow (Type 3) or traffic (Type 4 packet capture)',
  MODIFY COLUMN scope_type ENUM('account','vendor','system','listener') NOT NULL,
  ADD COLUMN reason     VARCHAR(500)    DEFAULT NULL COMMENT 'why the operator asked for it (required for traffic)',
  ADD COLUMN size_bytes BIGINT UNSIGNED DEFAULT NULL COMMENT 'traffic: encrypted file size on the gateway host',
  ADD COLUMN truncated  TINYINT(1)      NOT NULL DEFAULT 0 COMMENT 'traffic: stopped early at the size limit',
  ADD COLUMN error      VARCHAR(500)    DEFAULT NULL COMMENT 'traffic: why the capture failed',
  ADD KEY idx_log_captures_type (type, id);
