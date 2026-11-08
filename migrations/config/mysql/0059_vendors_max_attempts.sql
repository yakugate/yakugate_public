-- 0059_vendors_max_attempts.sql — restore vendors.max_attempts (found
-- 2026-08-10 by the P7 test campaign; same class as 0058). 0002 added it,
-- 0006_schema_v2 dropped it with the vendors rebuild, and the 2026-07-30
-- TC-P7-08 code (retry's perVendorMaxSnapshot) reads it unconditionally —
-- so the per-vendor dead-letter override has been silently broken on every
-- v2-provisioned config store (query errored each 30s reload; only the
-- global YAKU_MAX_ATTEMPTS applied). DEFAULT 0 = "no override, use the
-- global" (DeadLetter ignores non-positive overrides), so live behaviour
-- is unchanged until an operator sets a real value.
ALTER TABLE vendors
  ADD COLUMN max_attempts INT NOT NULL DEFAULT 0;
