-- ============================================================================
-- 0056_monitor_stores.sql — MySQL's and Redis's own figures with each sample
-- (Saleem 2026-10-05, D-129: "build monitoring system for redis and mysql,
-- whatever they natively produce")
--
-- The http-server of each deployment reads every store its settings name
-- (internal/storewatch) each time it reports: MySQL's SHOW GLOBAL STATUS /
-- VARIABLES and replica status, Redis's INFO and its Sentinels. The whole
-- reading rides in the sample's body; the figures a store's history and its
-- events need are lifted here, one entry per store:
--
--   {"<store key>": {"k":"mysql|redis", "a":"127.0.0.1:3306", "ok":1, "w":0,
--                    "p":0.4, "c":20.5, "n":123, "m":45.1, "cl":27, "q":346,
--                    "s":0, "e":0, "h":99.9, "l":0, "u":21149, "x":"…"}}
--
--   ok reachable (1/0), w health (0 ok, 1 warning, 2 down), p round trip ms,
--   c MySQL connections % of max_connections, n MySQL connections, m Redis
--   memory % (of maxmemory, else of the machine), cl Redis clients, q queries
--   (MySQL) or commands (Redis) a second, s slow queries a second, e keys
--   evicted a second, h cache hit %, l replica lag s, u uptime s, x the first
--   reason it is not ok. NULL: the sample says nothing about stores.
-- ============================================================================
ALTER TABLE monitor_samples
  ADD COLUMN stores JSON NULL
    COMMENT 'per-store figures of this sample (D-129): reachable, health, round trip, connections, memory, rates';
