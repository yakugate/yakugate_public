-- The locator's half of per-account body masking (PO 2026-09-08). Companion to
-- queue/0026.
--
-- A vendor's delivery receipt may echo the first characters of the message
-- back to us, and we store that raw text (dlrlog / raw_dlr) and forward it in
-- the dlr_url payload. By the time a receipt arrives the message is long gone
-- and the DLR process has no reference cache, so the only surviving record of
-- what the account asked for is this locator row — exactly the reason
-- no_receipt and client_ref live here too.
--
-- Only the text AFTER `text:` in a receipt is ever masked: masking digit runs
-- across the whole receipt would destroy the vendor's message id, the
-- counters and the timestamps, which is what correlation depends on.
ALTER TABLE record_index
  ADD COLUMN mask_body TINYINT(1) NOT NULL DEFAULT 0
    COMMENT 'the account asked for pii_masked: mask the text portion of any vendor receipt before storing or forwarding it';
