-- ============================================================================
-- Hadal reconciliation, routing group (PO 2026-07-27, Salman): routes keeps
-- our own structure as the base ("in routes we will take ours") — explicit
-- named columns over Hadal's generic condition1-8/modify1-8 slots, which we
-- deliberately did NOT adopt (see the discussion this migration follows from:
-- generic numbered columns are exactly the anti-pattern the original Hadal
-- review itself warned against). routes.account_id is unchanged — no scoping
-- change needed there.
--
-- Purely additive. Nothing here changes existing route-matching behavior yet
-- — match_mcc/match_mnc (singular) are UNTOUCHED and remain what
-- stage_route.go actually reads; match_mccs/match_mncs (plural, array) are
-- new columns backfilled alongside them, wired up in a follow-up code pass
-- (same expand/contract shape as the sms_service_config migration).
-- ============================================================================

ALTER TABLE routes
  -- routing_rules.type turned out to mean "which traffic direction this rule
  -- applies to" (MO/MT/DLR) — direction already exists here as MT/MO; adding
  -- DLR as a third value. Additive to the enum; inert until something sets or
  -- reads 'DLR' (P5/DLR routing doesn't select routes today).
  MODIFY COLUMN direction ENUM('MT','MO','DLR') NOT NULL DEFAULT 'MT',
  -- Hadal's hlr_info ({"410":["01","07"]}) maps to our match_mcc/match_mnc,
  -- but as ARRAYS (one route can apply to multiple MCC/MNC combos) — named
  -- match_mccs/match_mncs to make the plural/array shape explicit, per
  -- Salman: "name it accordingly."
  ADD COLUMN match_mccs JSON DEFAULT NULL AFTER match_mnc,
  ADD COLUMN match_mncs JSON DEFAULT NULL AFTER match_mccs,
  -- Hadal's accept_skipped_hlr, renamed per Salman ("need to change the
  -- name, accept_undetected_network") — accept a message even when HLR
  -- couldn't resolve the destination network, same idea as backlog B-11 but
  -- per-route rather than only a global flag.
  ADD COLUMN accept_undetected_network TINYINT(1) NOT NULL DEFAULT 0 AFTER match_mncs,
  -- Route-forced priority: when this route's priority_regex matches the
  -- message body AND the client didn't set an explicit priority (the same
  -- 0-sentinel the template-forced-priority feature already uses, S-11),
  -- forced_priority overrides the generic default — a route-level sibling of
  -- the template-level feature, same {match} JSON shape as sender_regex/
  -- receiver_regex/keyword_regex for consistency (replace is unused here).
  ADD COLUMN priority_regex JSON DEFAULT NULL AFTER keyword_regex,
  ADD COLUMN forced_priority INT UNSIGNED DEFAULT NULL AFTER priority_regex,
  -- Four additional match/find-replace conditions on the HTTP-request custom
  -- pass-through parameters (S-06's ap1/ap2/apg1/apg2) — same {match,replace}
  -- JSON shape as sender_regex/receiver_regex/keyword_regex, so these get
  -- both route-matching AND find-and-replace (via .replace) for free, exactly
  -- like the existing three. (Route-level find-and-replace itself already
  -- exists today via sender_regex/receiver_regex/keyword_regex's .replace,
  -- applied in stage_route.go's applyRoute — no new mechanism was needed for
  -- that part, just these four extra fields to apply it to.)
  ADD COLUMN ap1_regex JSON DEFAULT NULL AFTER forced_priority,
  ADD COLUMN ap2_regex JSON DEFAULT NULL AFTER ap1_regex,
  ADD COLUMN apg1_regex JSON DEFAULT NULL AFTER ap2_regex,
  ADD COLUMN apg2_regex JSON DEFAULT NULL AFTER apg1_regex;

-- Backfill match_mccs/match_mncs as single-element arrays from the existing
-- scalar columns, so the new columns already reflect today's real config
-- once a follow-up code pass starts reading them.
UPDATE routes
   SET match_mccs = JSON_ARRAY(match_mcc)
 WHERE match_mcc IS NOT NULL AND match_mcc <> '';
UPDATE routes
   SET match_mncs = JSON_ARRAY(match_mnc)
 WHERE match_mnc IS NOT NULL AND match_mnc <> '';
