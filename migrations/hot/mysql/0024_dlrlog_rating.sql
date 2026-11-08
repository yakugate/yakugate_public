-- 0024_dlrlog_rating.sql — rating columns on the DLR log (GW-38, PO 2026-08-28).
--
-- Delivery receipts have never been billable: P9 skips every dlrlog_ table
-- because they carry no rating columns ("dlrlog_/rejects_: no rating columns,
-- never charged", S9-03), and a DLR rate card could be created in the portal
-- but priced nothing — the rater does not even read rate_cards.type.
--
-- PO ruling 2026-08-28: DLRs ARE charged, and the card comes from the MT route
-- the original message went out on (config/0072 adds those columns), falling
-- back to the account's default_dlr_rate_card_id. `dlr_charge_mode` on the
-- account decides whether every receipt is charged or only DELIVRD ones — a
-- column that has existed, and been settable in the portal, since before
-- anything read it.
--
-- Same column shapes as mtlog/mo_log so one rating path serves all three.
-- Existing dlrlog_<user>_<DD_MM_YYYY> clones are NOT named here — they cannot
-- be listed portably (same constraint as hot/0018 and hot/0020, and the reason
-- hot/0015-0016 hardcoded one developer's tables, GW-09). Bootstrap's
-- template-to-clone schema sync adds the missing columns to every existing
-- clone on the next start, and new clones come from the fixed template.
ALTER TABLE dlrlog_template
  ADD COLUMN rate_cards JSON DEFAULT NULL,
  ADD COLUMN buy_price  DECIMAL(12,6) DEFAULT NULL,
  ADD COLUMN sell_price DECIMAL(12,6) DEFAULT NULL,
  ADD COLUMN currency   CHAR(3) DEFAULT NULL,
  ADD COLUMN rated_at   DATETIME DEFAULT NULL,
  ADD KEY idx_unrated (rated_at);
