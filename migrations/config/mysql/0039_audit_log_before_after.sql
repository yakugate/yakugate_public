-- Hadal reconciliation, ledger/audit group (PO 2026-07-27, Salman): "our
-- purpose for audit logs are what we changed via portal... if someone add or
-- update or delete some records, we must log who did it, when, what was
-- before, what is new."
--
-- Confirms audit_log's existing action/entity/entity_id model is the right
-- shape (portal CRUD change-tracking) and is NOT meant to become an
-- HTTP-request access log — Hadal's audit_logs (method/route/params/query/
-- response_code/user_agent/content_type/feature/module) is a different tool
-- for a different job and is deliberately NOT adopted here. balance_history
-- needed no changes at all (already equal-or-ahead — see the schema doc).
--
-- Adds the one genuine gap: structured before/after snapshots. `detail`
-- (existing) stays as a free-form catch-all; before_data/after_data are the
-- new explicit "what changed" pair — NULL before_data on a create, NULL
-- after_data on a delete. company_id added for the same scoping consistency
-- balance_history already has (account_id + company_id together).
ALTER TABLE audit_log
  ADD COLUMN company_id BIGINT UNSIGNED DEFAULT NULL AFTER account_id,
  ADD COLUMN before_data JSON DEFAULT NULL AFTER detail,
  ADD COLUMN after_data JSON DEFAULT NULL AFTER before_data,
  ADD KEY idx_audit_log_company (company_id);
