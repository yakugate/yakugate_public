-- ============================================================================
-- 0167_billing_events.sql — the billing queue in MySQL, for an install with
-- no Redis (plan of 29 Oct 2026, step 3: "MySQL also does what Redis does").
--
-- With Redis the money events wait on the deployment's sell and buy queues
-- in the queue Redis (queues:<dep>:billing:<side>:db:<lane>:ready|delayed|
-- doing). With none they wait here, in the CONFIG store, beside the balance
-- of record: a prepaid balance moves (companies.balance) and its events are
-- written in ONE transaction, both or neither (internal/dal/mysql
-- billing_events.go). The db-worker books them into usage_buckets with the
-- same code it books the Redis queues with (internal/billingq).
--
--   scope     the deployment (billing.QueueScope: YAKU_DEPLOYMENT_ID, else
--             'shared')
--   side      sell | buy ('' = the one mixed queue of 26 Oct, never written)
--   lane      the company's lane (billing.LaneOf), 0..63
--   state     0 ready, 1 delayed (MySQL refused it once, due at score),
--             2 doing (claimed from delayed), 3 doing (claimed from ready)
--   score     the event's moment in ms (ready), or when it is due (delayed)
--   batch_id  the claimed batch's id (doing only): the guard row's id in
--             usage_batches, so a batch is booked once whatever stops where
--   mhash     MD5 of the member: an event put twice is there once, as a
--             sorted-set member is
--   member    the event (billing.Event JSON)
-- ============================================================================
CREATE TABLE IF NOT EXISTS billing_events (
  id        BIGINT UNSIGNED   NOT NULL AUTO_INCREMENT,
  scope     VARCHAR(64)       NOT NULL,
  side      VARCHAR(8)        NOT NULL,
  lane      SMALLINT UNSIGNED NOT NULL,
  state     TINYINT UNSIGNED  NOT NULL DEFAULT 0,
  score     BIGINT            NOT NULL,
  batch_id  VARCHAR(64)       NULL,
  mhash     BINARY(16)        NOT NULL,
  member    TEXT              NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_billing_event (scope, side, mhash),
  KEY idx_billing_lane (scope, side, lane, state, score)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
