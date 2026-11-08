-- ============================================================================
-- 0012_queue_username.sql — carry the client username on the queue row (2026-07-19)
-- P8/D-030: hot MDR tables are daily-per-username, but the vendor sender rebuilds
-- the message from the queue row (which only had account_id) — so username was
-- lost and every record collapsed into records_acct_<date>. The queue row is
-- self-describing (cf. ingress/transport/udh), so it now also carries username;
-- Claim restores it and Write files the record under records_<username>_<date>.
--
-- NOTE: this ALTERs the template only (fresh DBs clone queue_<cid> LIKE it).
-- Existing queue_<cid> tables are backfilled with the same column at deploy.
-- ============================================================================

ALTER TABLE queue_template
  ADD COLUMN username VARCHAR(64) NULL AFTER account_id;
