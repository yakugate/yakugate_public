-- ============================================================================
-- 0010_rejected.sql — persist rejected submits (2026-07-22)
--
-- Until now a rejection produced an HTTP response and a log line and nothing
-- else: no row, no history, nothing for support or an audit to look at. The
-- reference gateway logged every rejection with full context (27.8k rows in a
-- single month for one tenant) and it is one of the most-used portal screens —
-- see saleem-hadal-db-review.md §7.3.1 and scope Draft 4.0 proposal S-01.
--
-- Rejected messages reuse records_* (status='rejected' already exists in the
-- enum) rather than getting their own table: same tenant-month partitioning,
-- same retention, one place to look for "what happened to my message".
--
-- request_info holds SANITIZED request context only — an allow-list of headers
-- (internal/pipeline/sanitize.go). Credential headers are never stored; the
-- reference's habit of persisting live tokens is the defect in review §6.0.
-- ============================================================================

ALTER TABLE records_template
  ADD COLUMN status_code  INT  NULL AFTER reject_reason,
  ADD COLUMN request_info JSON NULL AFTER status_code,
  ADD KEY idx_rejected (status, accepted_at);
