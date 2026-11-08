-- Rules for every account are gone (Saleem, 2026-10-01).
--
-- Since routing plans (D-120, 2026-09-30, config/0131) every outgoing (MT)
-- rule belongs to one account: the engine reads no MT rule whose account_id is
-- NULL, and the Admin API and the console refuse to save one. The ones still
-- stored were only shown, read-only, as "Retired: rules for every account" on
-- the Routing Plans screen. That concept is ended, so they leave `routes`.
--
-- Retired, not destroyed: each row is copied to routes_retired_global first,
-- so an old message's route_id (mtlog/mo_log) can still be traced to the rule
-- that routed it. Nothing reads the copy. Incoming (MO) rules are untouched.
CREATE TABLE IF NOT EXISTS routes_retired_global LIKE routes;

INSERT IGNORE INTO routes_retired_global
SELECT * FROM routes WHERE account_id IS NULL AND (direction = 'MT' OR direction IS NULL);

DELETE FROM routes WHERE account_id IS NULL AND (direction = 'MT' OR direction IS NULL);
