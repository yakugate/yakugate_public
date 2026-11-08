-- Incoming (MO) rules for every account leave `routes` too (Saleem,
-- 2026-10-01), completing config/0132, which moved the outgoing (MT) ones.
--
-- The engine never read an MO rule without an account (internal/mo loads
-- only `account_id IS NOT NULL`, joined to its account), and since routing
-- plans (D-120) the Admin API and the console refuse to save one. Any still
-- stored are moved, not destroyed: copied to routes_retired_global first, so
-- an old message's route_id can still be traced. Nothing reads the copy.
CREATE TABLE IF NOT EXISTS routes_retired_global LIKE routes;

INSERT IGNORE INTO routes_retired_global
SELECT * FROM routes WHERE account_id IS NULL AND direction = 'MO';

DELETE FROM routes WHERE account_id IS NULL AND direction = 'MO';
