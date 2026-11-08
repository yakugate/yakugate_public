-- The locator's half of "a receipt goes back to the bind that sent the
-- message" (PO 2026-09-05). Companion to queue/0025, which carries the same
-- slot on the queue row and on the receipt job.
--
-- Three of the four push points still hold the full model.Message when they
-- decide to notify the client (P3/P4's PushSubmitDLR + PushTerminalDLR, and
-- blackhole), so they read the slot straight off it. The fourth — a real
-- vendor receipt arriving minutes later and correlated by (vendor_id,
-- vendor_msg_id) — has no Message left, only this locator row. This column is
-- that carry, exactly as no_receipt/failure_receipt_only were in hot/0026.
--
-- 0 = the message did not come from an SMPP bind (HTTP submit), or predates
-- this migration: the receipt then goes to any live receive-capable bind for
-- that account, taking turns.
ALTER TABLE record_index
  ADD COLUMN bind_slot SMALLINT UNSIGNED NOT NULL DEFAULT 0
    COMMENT 'per-account SMPP bind slot the submit arrived on; 0 = none (HTTP submit or unknown)';
