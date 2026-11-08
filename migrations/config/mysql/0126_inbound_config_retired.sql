-- ============================================================================
-- 0126_inbound_config_retired.sql — vendors.inbound_config is RETIRED
-- (2026-09-24, PO: "delete older settings")
--
-- The pre-0121 shared inbound setting — one URL token, one header secret and
-- one IP list for BOTH callback kinds, stored in plain text. Every vendor's
-- callbacks are now guarded by its two doors (inbound_endpoints, config/0121),
-- set up under Vendors → Inbounds.
--
-- The dlr and mo roles move whatever a vendor still has here onto its doors
-- when they start (inboundauth.MoveLegacy: the secret becomes the door's
-- token, as a digest; the IP list its address list; a door that already has
-- a credential keeps it) and then clear it — so the plain-text secrets leave
-- the database. The Admin API refuses a new value and the console no longer
-- writes one. The column itself is kept, like every retired column.
-- ============================================================================

ALTER TABLE vendors MODIFY COLUMN inbound_config JSON DEFAULT NULL
  COMMENT 'RETIRED 2026-09-24 (config/0126): moved onto inbound_endpoints by the dlr/mo roles at start; refused by the Admin API';
