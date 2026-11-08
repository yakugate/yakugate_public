-- ============================================================================
-- 0037_queue_owner.sql — each deployment keeps its own queues
--
-- Saleem 2026-10-02 (D-121): "each deployment should have separate queues, not
-- to be shared by other gateways." Until now one vendor had ONE queue, and
-- every deployment that vendor was placed on drained it; queue/0033's
-- deployment_id only recorded who accepted a message. Now every queued
-- message — outgoing (queue_<vendor>, and the re-route inbox queue__reroute),
-- incoming (mo_queue) and client receipts (client_dlr) — names the
-- deployment that will deliver it, and a gateway takes only its own:
--
--   queue_owner = me     my message
--   queue_owner IS NULL  shared: queued before this, or by an install that
--                        never set YAKU_DEPLOYMENT_ID — any gateway takes it,
--                        exactly as before, so the backlog of a live upgrade
--                        drains with no step by hand
--
-- A deployment that is down keeps its messages: they wait for it (or expire
-- under their own validity) and no other gateway sends them.
--
-- queue_owner, not deployment_id: deployment_id falls back to the hostname, and
-- a container that comes back with a new one would strand everything its old
-- name queued. queue_owner is set only from YAKU_DEPLOYMENT_ID.
--
-- One column (or key) per ALTER statement: the bootstrap treats "already
-- exists" as satisfied and skips the whole statement. Existing queue_<cid>
-- clones take the column (syncCloneColumns) and the key (syncCloneKeys) from
-- the template at the next role start.
-- ============================================================================
ALTER TABLE queue_template
  ADD COLUMN queue_owner VARCHAR(64) NULL
    COMMENT 'YAKU_DEPLOYMENT_ID of the deployment that sends this message; NULL = shared (D-121)';

-- The claim reads only its own rows (and the shared ones) through this key,
-- so one deployment's claim never locks rows another deployment is about to
-- take (FOR UPDATE SKIP LOCKED locks every row its scan touches).
ALTER TABLE queue_template
  ADD KEY idx_owner_claim (queue_owner, status, next_attempt_at, id);

ALTER TABLE mo_queue
  ADD COLUMN queue_owner VARCHAR(64) NULL
    COMMENT 'YAKU_DEPLOYMENT_ID of the deployment that delivers this MO to the client; NULL = shared (D-121)';

ALTER TABLE mo_queue
  ADD KEY idx_mo_owner (queue_owner, status, account_id);

ALTER TABLE client_dlr
  ADD COLUMN queue_owner VARCHAR(64) NULL
    COMMENT 'YAKU_DEPLOYMENT_ID of the deployment that pushes this receipt to the client; NULL = shared (D-121)';

ALTER TABLE client_dlr
  ADD KEY idx_cdlr_owner (queue_owner, account_id);
