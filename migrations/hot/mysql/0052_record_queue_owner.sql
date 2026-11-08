-- ============================================================================
-- 0052_record_queue_owner.sql — whose queue a message came from
--
-- Saleem 2026-10-02 (D-121), with queue/0037: each deployment keeps its own
-- queues. A vendor's receipt can arrive on any gateway the vendor is placed
-- on (an SMSC answers on whichever bind it likes; an HTTP vendor posts to
-- one address), but the client is connected to the deployment that accepted
-- the message, so the receipt must be queued for THAT deployment to push.
-- The record is all the receipt path has, so the message's queue owner is
-- kept on it, written by the vendor role from the queue row it claimed.
--
-- NULL = shared (sent from a shared queue, or before this): the receipt goes
-- to the shared queue and any gateway delivers it, exactly as before.
-- Not deployment_id: that falls back to the hostname, which no queue is
-- named after.
-- ============================================================================
ALTER TABLE record_index
  ADD COLUMN queue_owner VARCHAR(64) NULL
    COMMENT 'YAKU_DEPLOYMENT_ID whose queue the message was sent from; NULL = shared (D-121)';
