-- ============================================================================
-- 0013_account_dlr_url.sql — client DLR webhook URL (smsc leg, HTTP) (2026-07-19)
-- When a client isn't bound over SMPP, the DLR service's client_dlr queue is
-- drained by the dlr process, which POSTs each receipt to the account's dlr_url
-- (PO decision D-022: deliver_sm on the bind if bound, else dlr_url).
-- ============================================================================

ALTER TABLE accounts
  ADD COLUMN dlr_url VARCHAR(512) NULL AFTER jwt;
