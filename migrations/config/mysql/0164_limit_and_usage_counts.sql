-- The shared rate limits and period counts for a gateway with no Redis (DAL
-- plan of 29 Oct 2026, step 3: "MySQL also does what Redis does"). With Redis
-- set, these tables are not used: the counters stay in the main Redis.
--
-- limit_buckets: one row per key and second, as Redis's rl:<key>:<second>.
--   Each process takes a CHUNK of a bucket in one statement and hands it out
--   from memory (internal/ratelimit Chunked), so the accept path asks this
--   table a few times a second, not once a message. `used` is what all
--   processes together have taken of that second. A second that is over is
--   a row nobody asks again; the counters' sweeper removes it (by `sec`).
--
-- usage_counts: quota and per-destination counts, as Redis's qu:<key>. The
--   caller stamps the period into the key; `expires_at` (unix seconds) is
--   when the key ends, set when it is made (the life the caller gives). An
--   ended key that is counted again starts from zero; the sweeper removes
--   ended keys.
--
-- Keys are kept byte for byte (VARBINARY: no collation folds two keys into
-- one); a key longer than 255 bytes is kept as '#' and its SHA-256.
CREATE TABLE IF NOT EXISTS limit_buckets (
  bkey  VARBINARY(255) NOT NULL,
  sec   BIGINT NOT NULL,
  used  BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (bkey, sec),
  KEY idx_limit_buckets_sec (sec)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS usage_counts (
  ukey       VARBINARY(255) NOT NULL PRIMARY KEY,
  n          BIGINT NOT NULL DEFAULT 0,
  expires_at BIGINT NOT NULL,
  KEY idx_usage_counts_expires (expires_at)
) ENGINE=InnoDB;
