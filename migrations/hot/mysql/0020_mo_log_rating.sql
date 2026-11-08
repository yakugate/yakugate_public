-- 0020_mo_log_rating.sql — restore rating columns on the MO log (P9 FX round,
-- PO 2026-08-10 / D-105).
--
-- The Hadal traffic-log split (hot/0013) carried rated_at/buy_price/
-- sell_price/currency/rate_cards over to mtlog ONLY — mo_log lost them, which
-- silently made MO traffic unbillable and P9's sweep error on every mo_log
-- partition ("Unknown column 'rated_at'"), contradicting D-033's all-accepted
-- MT+MO rating policy (register TC-P9-13). Same column shapes as mtlog.
--
-- Existing mo_log_YYYY_MM clones cannot be named portably here (same
-- constraint as hot/0018) — they are altered out-of-band; this migration
-- fixes the template every future clone comes from.
ALTER TABLE mologtemplate
  ADD COLUMN rate_cards JSON DEFAULT NULL,
  ADD COLUMN buy_price  DECIMAL(12,6) DEFAULT NULL,
  ADD COLUMN sell_price DECIMAL(12,6) DEFAULT NULL,
  ADD COLUMN currency   CHAR(3) DEFAULT NULL,
  ADD COLUMN rated_at   DATETIME DEFAULT NULL,
  ADD KEY idx_unrated (rated_at);
