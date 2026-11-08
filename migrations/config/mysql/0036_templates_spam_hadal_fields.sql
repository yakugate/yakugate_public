-- ============================================================================
-- Hadal reconciliation, templates/spam group (PO 2026-07-27, Salman):
-- "priority is message priority, if user is sending priority, then we will
-- consider that, otherwise template priority comes into effect" — confirms
-- Hadal's templates.priority is the SAME concept as our existing
-- templates.default_priority (migration config/0026, S-11) — no new priority
-- column needed here, just the approval-workflow and status fields below.
-- ============================================================================

ALTER TABLE templates
  ADD COLUMN company_id BIGINT UNSIGNED DEFAULT NULL AFTER account_id,
  ADD COLUMN is_disabled TINYINT(1) NOT NULL DEFAULT 0 AFTER status,
  ADD COLUMN approved_by_user_id VARCHAR(64) DEFAULT NULL AFTER lastaction_at,
  ADD COLUMN approved_at DATETIME DEFAULT NULL AFTER approved_by_user_id,
  ADD COLUMN rejected_by_user_id VARCHAR(64) DEFAULT NULL AFTER approved_at,
  ADD COLUMN rejected_at DATETIME DEFAULT NULL AFTER rejected_by_user_id,
  -- current_approval_level/required_approval_level complete the
  -- sms_service_config.template_approval_level feature (migration 0031) —
  -- that column had no matching per-template workflow state until now.
  ADD COLUMN current_approval_level VARCHAR(32) NOT NULL DEFAULT 'NONE' AFTER rejected_at,
  ADD COLUMN required_approval_level VARCHAR(32) NOT NULL DEFAULT 'NONE' AFTER current_approval_level,
  ADD COLUMN users_involved JSON DEFAULT NULL AFTER required_approval_level,
  ADD KEY idx_templates_company (company_id);

-- spam_keywords.keywords shape change: flat string array -> array of
-- {word, enabled} objects, giving per-keyword enable/disable (Hadal's real
-- gap) while keeping the batched-JSON-read structure (matches our own stated
-- "JSON for config read as a whole" principle — the whole list is matched
-- against every message at accept time, unlike Hadal's one-row-per-keyword).
-- Existing rows are converted in place; every keyword defaults enabled=true
-- (today's implicit behavior — every listed keyword was active).
UPDATE spam_keywords
   SET keywords = (
     SELECT JSON_ARRAYAGG(JSON_OBJECT('word', jt.word, 'enabled', TRUE))
     FROM JSON_TABLE(keywords, '$[*]' COLUMNS (word VARCHAR(500) PATH '$')) AS jt
   )
 WHERE JSON_LENGTH(keywords) > 0;
