-- Routes review, part 2 (PO 2026-07-28): "delete 7th and 8th" — remove
-- `blackhole` and `hq` from route_type, superseding the earlier "no removal
-- for now" ruling for these two specifically (`failover` stays, despite
-- having no distinct code path today — see stage_route.go's tierType).
--
-- blackhole: moved to vendors.transport='blackhole' per D-016 — a route_type
-- value of the same name is a leftover from before that decision and does
-- nothing distinct (falls into the same first-match default as direct).
-- hq: dropped per D-017, no surviving explanation of intended behavior;
-- also does nothing distinct today.
--
-- Confirmed safe: 0 live rows use either value (checked before writing this
-- migration).
ALTER TABLE routes
  MODIFY COLUMN route_type ENUM('direct','failover','loadbalance','lcr','capacity','quality') NOT NULL DEFAULT 'direct';
