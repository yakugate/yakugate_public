-- The login ladders and the guessing guard for a gateway with no Redis (DAL
-- plan of 29 Oct 2026, step 3). With Redis set these tables are not used.
-- Times are unix MILLISECONDS; 0 = not set.
--
-- blocks: one row per id a ladder keys ("u:alice", "ip:203.0.113.7", "t:" and
--   a token, a bare address for the guessing guard). The row is found by the
--   SHA-256 of the id (a token is long); `id` keeps the id itself for the
--   list of who is locked. What Redis keeps in four keys is four columns:
--     fails, fails_until  the failures of the current minute (lkf:)
--     locked_until        when the lock ends (lku:, its TTL)
--     level, level_until  the rung reached, and when it falls back (lkl:)
--     climb_until         a trip before this climbs a rung (lke:)
--   `answer` is what the last failure answered (read back in the same
--   statement); `ends_at` is when everything of the row has ended, for the
--   sweeper. Who is locked now is read through idx_blocks_locked (Redis's
--   lkidx).
--
-- block_addresses + block_identities: the identities that failed from one
--   address (Redis's spr:<address> set). The address row holds how many
--   different ones there are and when the set ends (each failure keeps it
--   for the window again); the identities are kept by their SHA-256.
CREATE TABLE IF NOT EXISTS blocks (
  id_hash      BINARY(32) NOT NULL PRIMARY KEY,
  id           TEXT NOT NULL,
  fails        INT NOT NULL DEFAULT 0,
  fails_until  BIGINT NOT NULL DEFAULT 0,
  locked_until BIGINT NOT NULL DEFAULT 0,
  level        TINYINT NOT NULL DEFAULT 0,
  level_until  BIGINT NOT NULL DEFAULT 0,
  climb_until  BIGINT NOT NULL DEFAULT 0,
  answer       BIGINT NOT NULL DEFAULT 0,
  ends_at      BIGINT NOT NULL DEFAULT 0,
  KEY idx_blocks_locked (locked_until),
  KEY idx_blocks_ends (ends_at)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS block_addresses (
  addr_hash  BINARY(32) NOT NULL PRIMARY KEY,
  distinct_n INT NOT NULL DEFAULT 0,
  until_ms   BIGINT NOT NULL DEFAULT 0,
  KEY idx_block_addresses_until (until_ms)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS block_identities (
  addr_hash  BINARY(32) NOT NULL,
  ident_hash BINARY(32) NOT NULL,
  PRIMARY KEY (addr_hash, ident_hash)
) ENGINE=InnoDB;
