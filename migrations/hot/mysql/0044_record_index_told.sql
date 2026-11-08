-- ============================================================================
-- 0044_record_index_told.sql — what the client was TOLD (2026-09-22)
--
-- The account receipt policy (config/0119) can have the gateway send a client
-- a receipt the vendor never sent, or a different status than the one the
-- vendor sent. The record must still hold the truth, so the invented answer
-- is kept HERE, beside the real status, never in place of it:
--
--   status       what really happened (vendor's word, unchanged)
--   told_status  what the client was told, when that is not the same thing
--
-- Everything operational keeps reading `status`: vendor quality, the delivery
-- ratio, retry, monitoring, the vendor summaries. What follows the client's
-- version instead is what the client can see or is charged for — the status
-- pull (GET /messages/{id}) and billing (PO 2026-09-22: bill as told).
--
-- told_status is also the "already told" flag: it is set ONCE, with
-- `WHERE told_status IS NULL`, so two roles racing over the same message can
-- only send one final receipt, and the vendor's later real receipt is
-- recorded without being pushed.
--
-- told_by names the rule that decided (on_accepted, on_refused, submit_code,
-- receipt, no_receipt), so support can see why a client was told what it was.
--
-- The index serves the dlr role's no-receipt sweep: messages of one account
-- still waiting for a receipt, nobody told yet, older than N minutes.
-- ============================================================================

ALTER TABLE record_index
  ADD COLUMN told_status VARCHAR(12) NULL
    COMMENT 'the final status the CLIENT was told, when the receipt policy invented or replaced it; NULL = the client was told what status says',
  ADD COLUMN told_err VARCHAR(16) NULL
    COMMENT 'the error code sent with that receipt',
  ADD COLUMN told_by VARCHAR(24) NULL
    COMMENT 'which receipt-policy rule decided it: on_accepted|on_refused|submit_code|receipt|no_receipt',
  ADD COLUMN told_at DATETIME NULL
    COMMENT 'when the client was told (the receipt may have been delayed past this)',
  ADD KEY idx_ri_awaiting (account_id, status, updated_at);
