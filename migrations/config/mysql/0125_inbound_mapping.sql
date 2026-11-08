-- ============================================================================
-- 0125_inbound_mapping.sql — each inbound door reads the vendor's callback by
-- the vendor's own field names, and answers it the way the vendor expects
-- (2026-09-24, PO: "I can't see any request and response mapping of bodies
-- anywhere")
--
-- Until now a receipt could be read only through the vendor's HTTP-endpoint
-- "webhook" row, an incoming message only by guessing from a fixed list of
-- names in a flat body, and both answered an empty 200 — so a vendor that
-- expects "OK", {"status":"success"} or an XML acknowledgement saw every
-- callback as failed and sent it again.
--
--   request_map   {list, fields{ours: their path}, status_map, text_format}
--                 read by internal/callbackmap; DLR fields message_id,
--                 status, error_code; MO fields from, to, text, message_id,
--                 operator, coding
--   response_map  {ok{status, content_type, body}, fail{…}}; a body may use
--                 {{message_id}}, {{vendor_message_id}}, {{count}}
--
-- NULL = no mapping: the door behaves exactly as before, so this is safe to
-- apply to a live gateway. One column per statement (the bootstrap treats an
-- existing column as satisfied and skips the WHOLE statement).
-- ============================================================================

ALTER TABLE inbound_endpoints ADD COLUMN request_map JSON NULL
  COMMENT 'how to read the vendor callback: list, fields, status_map, text_format (internal/callbackmap)';

ALTER TABLE inbound_endpoints ADD COLUMN response_map JSON NULL
  COMMENT 'what to answer an authenticated callback: ok / fail {status, content_type, body}';
