-- ============================================================================
-- 0008_record_concat_ingress.sql — carry concat grouping + ingress onto the MDR
-- record (2026-07-20)
--
-- TC-MP-20 / D-038: udh_ref, udh_total, udh_seq and ingress existed ONLY on
-- queue_<vendor>, which is transient — the row is deleted when the message
-- finalizes. So MDR history could not answer "was this one 3-part message or
-- three separate messages?", nor "did this arrive over HTTP or SMPP?". That
-- blocks any concatenation-aware reporting (and would bake the blind spot into
-- whatever gets built on top of records).
--
-- Semantics mirror the queue: udh_total = 0 means "not concatenated" (a
-- single-part message), otherwise the part's group ref / total / sequence.
-- ingress is how the CLIENT submitted it, distinct from `transport` (the vendor
-- egress leg); NULL on rows written before this migration.
-- ============================================================================

ALTER TABLE records_template
  ADD COLUMN udh_ref   INT NOT NULL DEFAULT 0 AFTER segments,
  ADD COLUMN udh_total INT NOT NULL DEFAULT 0 AFTER udh_ref,
  ADD COLUMN udh_seq   INT NOT NULL DEFAULT 0 AFTER udh_total,
  ADD COLUMN ingress   ENUM('http','smpp') NULL AFTER protocol,
  ADD KEY idx_concat (account_id, udh_ref, udh_total);
