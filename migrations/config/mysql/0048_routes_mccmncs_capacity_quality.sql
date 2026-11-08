-- Routes review (PO 2026-07-28): consolidate MCC/MNC matching into one
-- combined-network array; drop the unused priority-forcing column; add two
-- new route_type strategies.
--
-- 1) match_mcc/match_mnc/match_mccs/match_mncs -> ONE match_mccmncs JSON
--    array of combined "mccmnc" strings (e.g. ["41001","41003"]), same
--    convention already used by operators.mccmnc and the request's
--    `operator` field ("41001" = MCC 410 MNC 01) — avoids the ambiguity of
--    separate MCC/MNC arrays (does mccs=[410,420]+mncs=[01,03] mean any
--    combination, or specific pairs?). NULL/empty = matches any network,
--    same wildcard semantics the old blank match_mcc/match_mnc had.
--
-- 2) forced_priority dropped — PO: "not needed" (template-level priority
--    forcing, S-11, already covers this). priority_regex is KEPT but
--    repurposed: PO "keep it, repurpose as a route match condition" — it's
--    now a plain match filter (does the message's priority match this
--    pattern?), same shape/treatment as sender_regex/receiver_regex/
--    keyword_regex, no forcing behavior anywhere.
--
-- 3) route_type gains 'capacity' (route to whichever healthy vendor in the
--    tier currently has the lowest live queue depth — reuses the existing
--    QueueDepths mechanism, refreshed into the reference cache periodically,
--    same pattern as routes/rates) and 'quality' (intended: route by best
--    recent delivery/ASR ratio — schema/enum lands now, but there is NO
--    per-vendor delivery-ratio tracking anywhere in the codebase yet; the
--    Go code for this type falls back to first-match until that tracking is
--    built — an open item, not silently faked).
--
-- hq/blackhole/failover are NOT removed this round (PO: "no removal for
-- now") despite hq/blackhole being dead (D-017/D-016) and failover having no
-- distinct code path today (priority-tiering already provides real failover
-- — see stage_route.go) — flagged, not acted on.
--
-- Existing data backfilled before the old columns are dropped so live
-- routing behavior doesn't silently change.
ALTER TABLE routes ADD COLUMN match_mccmncs JSON DEFAULT NULL AFTER direction;

UPDATE routes
SET match_mccmncs = JSON_ARRAY(CONCAT(match_mcc, match_mnc))
WHERE match_mcc IS NOT NULL AND match_mcc <> '' AND match_mnc IS NOT NULL AND match_mnc <> '';

ALTER TABLE routes
  DROP INDEX idx_lookup,
  DROP COLUMN match_mcc,
  DROP COLUMN match_mnc,
  DROP COLUMN match_mccs,
  DROP COLUMN match_mncs,
  DROP COLUMN forced_priority,
  MODIFY COLUMN route_type ENUM('direct','failover','loadbalance','blackhole','hq','lcr','capacity','quality') NOT NULL DEFAULT 'direct',
  ADD KEY idx_lookup (account_id, priority);
