-- Where the Admin API reaches each deployment (Saleem 2026-10-02, D-123).
--
-- The Admin API used to know ONE gateway (ENGINE_CACHE_REFRESH_URL in its
-- .env): its cache-refresh nudge after a config write, its Tools calls and
-- its vendor-delete queue check all went there. With several deployments that
-- one address is a single point, and without Redis only the gateway it names
-- reloads at once. The register now carries each deployment's address: the
-- Admin API nudges every registered deployment and sends a single call to the
-- first that answers. Keys stay in the Admin API's .env — every gateway uses
-- the same YAKU_ADMIN_KEY and YAKU_STATUS_KEY. NULL = not called; with no
-- deployment carrying one, the .env address is used as before.
--
-- The engine never reads this column (it never reads the register).
ALTER TABLE deployments
  ADD COLUMN admin_url VARCHAR(255) NULL
      COMMENT 'Base URL the Admin API calls for this deployment /admin/* (its http-server or the load balancer before them), e.g. http://10.0.1.5:8080'
      AFTER base_url;
