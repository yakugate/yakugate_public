-- ============================================================================
-- 0169_handoff_lists.sql — receipts and incoming messages parked for another
-- gateway, for an install with no Redis (issue 5 of the design audit, plan
-- of 29 Oct 2026, step 3).
--
-- With Redis a receipt or MO taken in by one gateway for a client another
-- gateway serves is parked in the MAIN Redis (queues:<owner>:handoff:<kind>)
-- when the owner reads another queue store, and the owner moves it into its
-- own queue (internal/handoff). With none the lists are here, in the CONFIG
-- store, which every gateway of the install sees as they all see the main
-- Redis. Each queue store's own id is in that queue store
-- (queue/0045_queue_store_id, table qstore_id): gateways on one queue database read one id
-- and park nothing.
--
--   handoff_items   owner, kind (dlr | mo), the item as parked:
--                   {"p":<ms, the database's clock>,"x":<item>}
--   handoff_stores  deployment -> the id of the queue store it reads
-- ============================================================================
CREATE TABLE IF NOT EXISTS handoff_items (
  id     BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  owner  VARCHAR(64)     NOT NULL,
  kind   VARCHAR(8)      NOT NULL,
  item   MEDIUMTEXT      NOT NULL,
  PRIMARY KEY (id),
  KEY idx_handoff_head (owner, kind, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS handoff_stores (
  deployment  VARCHAR(64) NOT NULL,
  store_id    VARCHAR(64) NOT NULL,
  updated_at  DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (deployment)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
