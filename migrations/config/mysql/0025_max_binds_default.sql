-- DEC-2 (D-065 direction, PO approved 2026-07-26): max_binds semantics change.
-- Absent in smpp_config JSON  -> default 10 (was: unenforced, doc default 1)
-- Explicit 0                  -> unlimited
-- Enforced with ESME_RALYBND against live binds across ALL modes.
--
-- Backfill: rows carrying the old doc-default of 1 predate the finding that a
-- bind is serialized against itself (a cap of 1 would throttle a client to
-- ~200-1,000 msg/s and read as a gateway fault). Operators who genuinely want
-- 1 must re-assert it after this migration.
UPDATE accounts
   SET smpp_config = JSON_SET(smpp_config, '$.max_binds', 10)
 WHERE smpp_config IS NOT NULL
   AND JSON_EXTRACT(smpp_config, '$.max_binds') = 1;
