-- ============================================================================
-- 0168_queue_turns.sql — whose turn it is, for an install with no Redis
-- (plan of 29 Oct 2026, step 3).
--
-- With Redis a lane of the billing queue, and a deployment's hand-off list,
-- is worked by one process at a time through a key it SETs with a time to
-- live (…:lock). With none the turn is a row here: taken when free or run
-- out, renewed while it is the token's, let go by its token. Times are the
-- database's own clock (UTC), so the hosts' clocks do not matter.
--
--   name      what the turn is for: billing:<scope>:<side>:<lane>,
--             handoff:<owner>:<kind>, handoff:<owner>:<kind>:strays
--   token     the holder (a ULID or random id of the process)
--   until_at  when it runs out unless renewed (UTC)
-- ============================================================================
CREATE TABLE IF NOT EXISTS queue_turns (
  name      VARCHAR(191) NOT NULL,
  token     VARCHAR(64)  NOT NULL,
  until_at  DATETIME(6)  NOT NULL,
  PRIMARY KEY (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
