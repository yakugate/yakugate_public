-- S-03 — reject a route that would sell below its cost (PO 2026-09-03).
--
-- "Approved in principle, not built" per the backlog — but the route stage
-- only ever had BUY-side rate access (LCR cost comparison). Sell pricing,
-- and the account-default resolution around it, lived entirely inside P9's
-- async billing.Rater. Comparing the two honestly means converting both
-- into one currency, which is why this ships with a LIVE FX dip in the
-- route stage (PO's explicit choice, not the safer pinned-rate-only
-- default: "Live FX dip, same as billing") — the same FXResolver P9
-- already trusts, reused rather than duplicated.
--
-- PO rulings, all folded into this design:
--   - only a PRICED loss rejects; a route with no sell price at all stays
--     exactly as it is today (sent, unrated, uncharged)
--   - only the ALREADY-CHOSEN route is checked, not every candidate in a
--     tier — route selection itself is unchanged
--   - per-company opt-in, default OFF, so no live traffic is refused by a
--     policy nobody asked for
--
-- No portal/Admin API surface exists for `companies` at all yet (a
-- pre-existing gap — see CLAUDE.md), so this can only be turned on via
-- direct SQL today, the same limitation every other company-level setting
-- already has.
ALTER TABLE companies
  ADD COLUMN loss_detection_enabled TINYINT(1) NOT NULL DEFAULT 0
    COMMENT 'refuse 403 LOSS_PREVENTED when the chosen route would sell below its buy cost';
