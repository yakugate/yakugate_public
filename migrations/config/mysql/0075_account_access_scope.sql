-- RENUMBERED 0061 -> 0075 on the main-salman merge (2026-08-31).
-- This file was written as config/0061_* on a branch taken before main
-- reached 0061; main already had a different migration at that number, so two
-- files would have shared it. A deployment that applied the OLD name needs its
-- schema_migrations row updated to this one, or the migration re-runs and fails.
-- 0061_account_access_scope.sql — 3-way access scope (P1 scope finalize
-- 2026-08-09 §7/§9): per-account access enforcement selectable as portal+API
-- ('both'), API-only ('api'), or portal-only ('portal'), chosen at account
-- creation. The same value scopes the allowed_ip_list: 'portal' means the IP
-- list gates portal access only, so the engine's ip-allow stage skips it.
--
-- Engine enforcement: access_scope='portal' refuses every API submit after
-- credential proof (403 ACCOUNT_DISABLED family, uniform with the other
-- account-state gates); 'api'/'both' behave exactly as before. Default 'both'
-- keeps every existing row's behavior — this is additive.
ALTER TABLE sms_service_config
  ADD COLUMN access_scope ENUM('both','api','portal') NOT NULL DEFAULT 'both' AFTER smpp;
