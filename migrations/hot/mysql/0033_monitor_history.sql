-- Monitoring history — every role's snapshot, kept and replayable.
--
-- The PO asked for full snapshots every 5 seconds with the portal able to show
-- past moments alongside live. This is where they land.
--
-- SIZE, because it is the thing that decides whether this design survives.
-- A snapshot is tens of kilobytes. At one every 5 seconds that is 17,280 a day
-- per role, and raw JSON would reach roughly 315 GB a year and fill a 373 GB
-- disk inside fourteen months. So:
--
--   * body is COMPRESSED (gzip, written by the engine, not by MySQL). This
--     data compresses exceptionally well because consecutive samples are
--     nearly identical - most of a snapshot is idle sessions whose counters
--     have not moved. Roughly 10:1, taking a year to about 31 GB.
--   * three TIERS, per the PO's ruling of 2026-09-09. Full 5-second detail for
--     the recent window, then one-minute rollups, then five-minute. Old data
--     stays queryable quickly instead of forcing a scan over millions of rows
--     nobody will look at.
--
-- Partitioned by day so expiry is a partition DROP rather than a DELETE of
-- millions of rows - the same reasoning as the mtlog families, and the same
-- reason the retention process exists at all.
--
-- WHY NOT A TIME-SERIES DATABASE. Because this has to be queryable beside the
-- records it explains: "show me the gateway at the moment that message was
-- refused" is one join here and a correlation exercise anywhere else. Prometheus
-- remains the right answer for graphs and alerting, and /metrics already exists
-- on every role for it - the two are complementary, not alternatives.

CREATE TABLE monitor_samples (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  sampled_at  DATETIME(3) NOT NULL,
  role        VARCHAR(32) NOT NULL,
  -- host:pid. Several instances of one role are normal, and "which instance is
  -- wedged" has to be answerable.
  instance    VARCHAR(96) NOT NULL,
  -- tier 0 = raw 5s, 1 = 1-minute rollup, 2 = 5-minute rollup.
  tier        TINYINT UNSIGNED NOT NULL DEFAULT 0,
  -- The columns below are LIFTED OUT of the JSON so the common dashboard
  -- queries never decompress anything. Everything else stays in body.
  state       ENUM('running','dormant','inactive') NOT NULL DEFAULT 'inactive',
  accepted    BIGINT UNSIGNED NOT NULL DEFAULT 0,
  rejected    BIGINT UNSIGNED NOT NULL DEFAULT 0,
  tps         DECIMAL(10,2) NOT NULL DEFAULT 0,
  latency_p95_ms DECIMAL(10,2) NOT NULL DEFAULT 0,
  queue_depth BIGINT UNSIGNED NOT NULL DEFAULT 0,
  sessions_up SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  sessions_total SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  cpu_pct     DECIMAL(5,2) NOT NULL DEFAULT 0,
  mem_pct     DECIMAL(5,2) NOT NULL DEFAULT 0,
  -- Any dependency reporting not-OK. Lifted out because "was anything unhealthy
  -- at 3am" must not require decompressing a day of samples.
  unhealthy   TINYINT(1) NOT NULL DEFAULT 0,
  -- gzip'd JSON of the full Snapshot. MEDIUMBLOB: a compressed snapshot is
  -- kilobytes, but a gateway with hundreds of vendor sessions could exceed
  -- BLOB's 64 KB and silently truncating monitoring data would be its own bug.
  body        MEDIUMBLOB NULL,
  PRIMARY KEY (id, sampled_at),
  KEY idx_role_time (role, sampled_at),
  KEY idx_time (sampled_at),
  KEY idx_tier_time (tier, sampled_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
PARTITION BY RANGE (TO_DAYS(sampled_at)) (
  PARTITION p_start VALUES LESS THAN (0),
  PARTITION p_max   VALUES LESS THAN MAXVALUE
);
