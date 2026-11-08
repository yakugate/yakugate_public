-- GW-23 step 1 (PO approved 2026-08-27): index the ledger's idempotency guard.
-- internal/billing.Charge runs `SELECT 1 FROM balance_history WHERE reference=?
-- AND type='commit'` before EVERY charge; until now the table's only keys were
-- (account_id, created_at) and (company_id, created_at), so each charge paid a
-- full table scan that grew with the ledger. The larger lifecycle discussion
-- (partitioning, post-invoice roll-up) remains open in the GW-23 register entry.
ALTER TABLE balance_history ADD KEY idx_reference (reference, type);
