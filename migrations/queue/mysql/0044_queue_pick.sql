-- ============================================================================
-- 0044_queue_pick.sql — a vendor's queue rows by short code and pick time
-- (issue 22 of the design audit, with issue 25's rulings; Saleem and the PO,
-- 23–26 Oct 2026).
--
-- A queue row is taken (read and deleted in one transaction), never leased:
-- it is ready once pick_at has come, and a retry is the row inserted again
-- with its next pick time and one more attempt. A vendor's rate is split
-- between its short codes (shortcode; '' = the vendor's common queue), each
-- read in priority order: the key below serves both the count and the take.
-- The short code compares by its bytes (utf8mb4_bin), as Redis keys and the
-- engine's match do: "Bank" and "BANK" are two queues in both stores.
-- status, next_attempt_at and the lease are no longer read.
--
-- Rows queued before this have no pick_at: the first start of this build
-- gives each one its next_attempt_at (a lease's end, a scheduled time) or
-- now (internal/store/mysql MTUpgrade). Existing queue_<cid> tables get the
-- columns at start (bootstrap syncCloneColumns) and the key from the named
-- list (syncCloneKeys). One column per ALTER (the bootstrap rule).
-- ============================================================================
ALTER TABLE queue_template ADD COLUMN shortcode VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT '';
ALTER TABLE queue_template ADD COLUMN pick_at DATETIME(3) NULL;
ALTER TABLE queue_template ADD KEY idx_pick (queue_owner, shortcode, priority, pick_at, id);
