-- GW-18 (PO ruled 2026-08-27): blocklist gains the COMPANY tier, and
-- sender_mask is renamed to `sender` — it always matched EXACTLY, and the old
-- name invited operators to write patterns (ACME*) that silently never fired,
-- the worst failure mode for a DND control. Matching stays exact by ruling.
-- company_id NULL = not company-scoped (global when account_id is also NULL,
-- account-scoped otherwise). One row now expresses a customer-wide block.
ALTER TABLE blocklist
  ADD COLUMN company_id BIGINT UNSIGNED NULL AFTER account_id,
  ADD KEY idx_blocklist_company (company_id),
  RENAME COLUMN sender_mask TO sender;
