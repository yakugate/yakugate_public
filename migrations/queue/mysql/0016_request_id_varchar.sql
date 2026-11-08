-- ============================================================================
-- 0016_request_id_varchar.sql — request_id widened for client-supplied ids
-- (2026-07-26)
--
-- PO decision: when the client supplies a messageId, it BECOMES the request's
-- requestId ("your id names your request; our ids name the messages") — so
-- request_id must hold a client-chosen value up to the messageId cap (64
-- chars), not just a 26-char server ULID. Widening only (CHAR(26) →
-- VARCHAR(64)) — lossless for every existing row.
--
-- Existing queue_<cid> clones are reconciled automatically at startup by
-- bootstrap's clone-column sync (which learned widening-MODIFY alongside its
-- ADD-only behavior in the same change).
-- ============================================================================

ALTER TABLE queue_template MODIFY request_id VARCHAR(64) NOT NULL;
