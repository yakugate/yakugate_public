-- ============================================================================
-- 0051_spam_keywords_company_scope.sql — spam_keywords account_id -> company_id
-- (PO 2026-07-29): keyword lists scope to the COMPANY (the tenant, migration
-- config/0017's three-level hierarchy), not the individual account, so one
-- company-level exception covers every account under it. account_id=NULL
-- already meant "universal/global" and keeps that meaning as company_id=NULL.
--
-- Precedence (enforced in code, internal/pipeline/stage_spam.go): a
-- company's OWN 'allowed' row for a word wins over BOTH the universal
-- 'blocked' list and that company's own 'blocked' list — a company can
-- explicitly un-block a globally-blocked word for itself.
-- ============================================================================
-- NOTE (2026-07-30, signature-pass session): this migration originally
-- referenced `accounts`, which was dropped by config/0042 (D-075) before this
-- file was ever applied. References corrected to sms_service_config (the
-- accounts successor, same id space) — an edit to an UNAPPLIED migration,
-- allowed under the immutability rule.

-- Rows tied to an unlinked account (company_id=0 — never assigned to a real
-- company, per migration config/0017) have no company to scope to under the
-- new model. Keeping them would either silently promote a single account's
-- PRIVATE exception into a GLOBAL rule (dangerous) or leave an orphaned row —
-- neither is acceptable, so they're dropped. Pre-alpha: no production
-- traffic depends on these rows.
DELETE sk FROM spam_keywords sk
  JOIN sms_service_config a ON a.id = sk.account_id
 WHERE sk.account_id IS NOT NULL AND COALESCE(a.company_id, 0) = 0;

-- Orphaned rows (the account was since deleted) — same reasoning as above.
DELETE FROM spam_keywords
 WHERE account_id IS NOT NULL
   AND account_id NOT IN (SELECT id FROM sms_service_config);

ALTER TABLE spam_keywords
  ADD COLUMN company_id BIGINT UNSIGNED NULL AFTER account_id;

UPDATE spam_keywords sk
  JOIN sms_service_config a ON a.id = sk.account_id
   SET sk.company_id = a.company_id
 WHERE sk.account_id IS NOT NULL;

ALTER TABLE spam_keywords
  DROP KEY uq_list,
  DROP COLUMN account_id,
  ADD UNIQUE KEY uq_list (company_id, destination_country_code, list_type, category),
  ADD KEY idx_spam_company (company_id);
