-- GW-02 expansion (PO 2026-08-27): central FX rates store.
-- Fetched conversion rates live here with a TTL instead of in each process's
-- private memory: every engine instance shares one fetch, the Admin API can
-- show "current fetched conversion rates from any provider", and a restart
-- does not lose the cache. expires_at is written AND compared on the SQL
-- session clock (NOW()) — never mixed with Go/UTC time (same clock rule as
-- vendor_sessions, D-103).
--
-- endpoint_id references the http_endpoints crp provider row (GW-02 repointed
-- the provider catalog there; cr_providers was dropped by config/0052).
CREATE TABLE IF NOT EXISTS fx_rates (
  endpoint_id BIGINT UNSIGNED NOT NULL,
  from_ccy    VARCHAR(8)      NOT NULL,
  to_ccy      VARCHAR(8)      NOT NULL,
  rate        DECIMAL(18,8)   NOT NULL,
  fetched_at  DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  expires_at  DATETIME        NOT NULL,
  PRIMARY KEY (endpoint_id, from_ccy, to_ccy),
  KEY idx_expires (expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
