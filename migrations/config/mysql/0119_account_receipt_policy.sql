-- ============================================================================
-- 0119_account_receipt_policy.sql — per-account receipt policy (2026-09-22)
--
-- Some vendors never send a delivery receipt: most HTTP vendors answer the
-- submit call and say nothing afterwards. The customer's system still waits
-- for a final status, so an operator has to be able to say, per account:
--
--   * the vendor took it        -> tell the client DELIVRD (or a split)
--   * the vendor refused it     -> tell the client UNDELIV, per refusal code
--   * a receipt arrived         -> tell the client a different status than
--                                  the one the vendor used (per stat/err)
--   * no receipt after N minutes-> tell the client something and stop waiting
--
-- Rulings taken with the policy (PO 2026-09-22):
--
--   * it lives on the ACCOUNT, not on routing rules;
--   * the RECORD keeps the vendor's real outcome. What the client was told is
--     kept beside it on record_index (told_status, hot/0044), never instead
--     of it, so reports, vendor quality and delivery ratios stay true;
--   * BILLING follows what the client was told (an account charged on
--     delivery is charged when the client was told delivered);
--   * once the client has been told a final status, the vendor's real receipt
--     is still recorded but NOT pushed — one final receipt per message.
--
-- The column holds one JSON object; internal/receiptpolicy is the only parser
-- and the Admin API validates against the same rules. NULL (the default, and
-- every account today) = nothing invented, nothing replaced.
-- ============================================================================

ALTER TABLE sms_service_config
  ADD COLUMN receipt_policy JSON DEFAULT NULL
    COMMENT 'invented/replaced client receipts (internal/receiptpolicy). NULL = off: the client hears only what the vendor said';
