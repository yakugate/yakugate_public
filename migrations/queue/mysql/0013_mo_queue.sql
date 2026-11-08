-- ============================================================================
-- 0013_mo_queue.sql — single shared MO staging table (2026-07-19)
-- P6/D-032: mobile-originated messages are staged in ONE shared mo_queue (cloned
-- from mo_template), claimed by the owning client account for delivery — mirrors
-- the client_dlr smsc-leg model, not the per-vendor MT queue (MO volume is low
-- and the consumer is per-client). mo_template shape: canonical ids + claim/lease
-- columns + SMPP capture fields; sender=subscriber MSISDN, destination=shortcode.
-- ============================================================================

CREATE TABLE IF NOT EXISTS mo_queue LIKE mo_template;
