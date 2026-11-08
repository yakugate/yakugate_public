-- The live campaign window and the abuse monitor's counters for a gateway
-- with no Redis (DAL plan of 29 Oct 2026, step 3). With Redis set these
-- tables are not used. Keyed and bucketed as the Redis keys are; each row's
-- `expires_at` (unix seconds) is set again by every write, as Redis's EXPIRE
-- after each flush, and the counters' sweeper removes ended rows.
--
-- campaign_counts: cw:<hash>:<bucket>, field <account>.
-- abuse_counts:    ab:<kind>:<bucket>, field as the package names it; kind
--                  'm' minute, 'h' hour, 'p' prefix (minutes), 'd' UTC day
--                  (the bucket is the unix day; Redis names it yyyymmdd).
-- abuse_numbers:   ab:u:<account>:<minute>, the numbers an account sent to.
--                  Redis keeps a HyperLogLog; here each process (`writer`)
--                  keeps its own sketch of the minute and writes it whole, so
--                  two processes never write one row; a reader merges them.
-- abuse_texts:     ab:t:<account>, the newest texts. Each process writes a
--                  ring of `keep` rows (`slot`); a reader takes the newest by
--                  (at, seq).
-- abuse_marks:     ab:epoch (since when the counters are whole) and ab:cfg
--                  (prefix_len, since).
CREATE TABLE IF NOT EXISTS campaign_counts (
  hash       VARBINARY(64) NOT NULL,
  bucket     BIGINT NOT NULL,
  account    BIGINT NOT NULL,
  n          BIGINT NOT NULL DEFAULT 0,
  expires_at BIGINT NOT NULL,
  PRIMARY KEY (hash, bucket, account),
  KEY idx_campaign_counts_expires (expires_at)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS abuse_counts (
  kind       BINARY(1) NOT NULL,
  bucket     BIGINT NOT NULL,
  field      VARBINARY(191) NOT NULL,
  n          BIGINT NOT NULL DEFAULT 0,
  expires_at BIGINT NOT NULL,
  PRIMARY KEY (kind, bucket, field),
  KEY idx_abuse_counts_expires (expires_at)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS abuse_numbers (
  account    BIGINT NOT NULL,
  minute     BIGINT NOT NULL,
  writer     BIGINT NOT NULL,
  sketch     BLOB NOT NULL,
  expires_at BIGINT NOT NULL,
  PRIMARY KEY (account, minute, writer),
  KEY idx_abuse_numbers_expires (expires_at)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS abuse_texts (
  account    BIGINT NOT NULL,
  writer     BIGINT NOT NULL,
  slot       INT NOT NULL,
  at         BIGINT NOT NULL,
  seq        BIGINT NOT NULL,
  sample     TEXT NOT NULL,
  expires_at BIGINT NOT NULL,
  PRIMARY KEY (account, writer, slot),
  KEY idx_abuse_texts_newest (account, at, seq),
  KEY idx_abuse_texts_expires (expires_at)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS abuse_marks (
  name  VARCHAR(32) NOT NULL PRIMARY KEY,
  value VARCHAR(64) NOT NULL
) ENGINE=InnoDB;
