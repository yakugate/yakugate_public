-- ============================================================================
-- 0045_queue_store_id.sql — this queue store's own id (issue 5 of the design
-- audit, plan of 29 Oct 2026, step 3).
--
-- Each gateway names the queue store it reads, so a receipt or MO taken in
-- for a client on a gateway that reads ANOTHER queue store is parked for it
-- (config/0169_handoff_lists) and not filed where it would never be read.
-- With Redis the id is qstore:id in the queue Redis. With none it is the one
-- row here: the first gateway to start writes a random id (INSERT IGNORE),
-- every gateway on this database reads the same one.
--
-- The table is NOT named queue_…: every queue_% table of this store is a
-- vendor queue cloned from queue_template (the bootstrap keeps them in step).
-- ============================================================================
CREATE TABLE IF NOT EXISTS qstore_id (
  k         TINYINT UNSIGNED NOT NULL,
  store_id  VARCHAR(64)      NOT NULL,
  PRIMARY KEY (k)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
