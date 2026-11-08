-- registered_delivery is honoured (PO 2026-09-03, full SMPP 3.4 semantics,
-- landing immediately for every account).
--
-- Until now the byte was decoded and thrown away: P5 pushed a client-facing
-- receipt for every terminal message regardless of what the client actually
-- asked for at submit time. Two of the three push points (PushSubmitDLR,
-- PushTerminalDLR) run right where the full model.Message is still in scope,
-- so they gate on it directly with no query. The third — a genuine vendor
-- receipt arriving later, correlated via record_index by (vendor_id,
-- vendor_msg_id) — has no Message in scope, only whatever this locator row
-- carries. These two columns are that carry.
--
--   no_receipt            registered_delivery bits 0-1 == 00: the client
--                          asked for nothing. Suppress the push entirely.
--   failure_receipt_only  registered_delivery bits 0-1 == 10: the client
--                          only wants to hear about a failure. Suppress a
--                          DELIVRD push, keep FAILED/EXPIRED.
--
-- Both default 0 (send everything), so an HTTP-submitted message — which has
-- no registered_delivery concept at all in the native V4 contract — and any
-- row written before this migration are unaffected. Vendor egress still
-- requests a receipt from the VENDOR unconditionally either way (P3 hardcodes
-- registered_delivery=1 outbound) — we still need delivery truth for our own
-- retry logic and dlrlog reporting; these columns only gate what we push back
-- to the client who submitted the message, and (P9, same ruling: "no charge,
-- no receipt, no fee") whether that receipt is billed.
ALTER TABLE record_index
  ADD COLUMN no_receipt TINYINT(1) NOT NULL DEFAULT 0
    COMMENT 'client declined any delivery receipt at submit time (SMPP registered_delivery bits 0-1 = 00)',
  ADD COLUMN failure_receipt_only TINYINT(1) NOT NULL DEFAULT 0
    COMMENT 'client wants a receipt only on failure (SMPP registered_delivery bits 0-1 = 10)';
