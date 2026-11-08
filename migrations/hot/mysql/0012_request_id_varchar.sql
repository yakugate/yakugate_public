-- ============================================================================
-- 0012_request_id_varchar.sql — request_id widened for client-supplied ids
-- (2026-07-26). See queue/0016 for the rationale; same change on the hot
-- record shape. Existing records_<tenant>_<YYYYMM> clones are reconciled at
-- startup by bootstrap's clone-column sync.
-- ============================================================================

ALTER TABLE records_template MODIFY request_id VARCHAR(64) NULL;
