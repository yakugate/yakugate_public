-- ============================================================================
-- 0039_ap_wide.sql — ap1/ap2/apg1/apg2 hold 255 characters (QA templates
-- API-013, 2026-10-14)
--
-- The client's own limit (stage_validate, since chunk-4 ruling 4.6) and a
-- template's (config/0130, the Admin API) is 255 characters, but the queue
-- row kept them in VARCHAR(50) (0015): a value of 51 or more failed the
-- enqueue and the client was told 503 QUEUE_UNAVAILABLE — and a template
-- carrying such a value failed every message it matched.
--
-- ALTERs the template only. Existing queue_<cid> clones are widened at start
-- by bootstrap.widenCloneColumns (a provable widening), and a clone that is
-- still 50 wide when a long value arrives is widened by the store itself on
-- that write (internal/store/mysql apwide.go). One column per statement.
-- ============================================================================
ALTER TABLE queue_template MODIFY ap1 VARCHAR(255) NULL;
ALTER TABLE queue_template MODIFY ap2 VARCHAR(255) NULL;
ALTER TABLE queue_template MODIFY apg1 VARCHAR(255) NULL;
ALTER TABLE queue_template MODIFY apg2 VARCHAR(255) NULL;
