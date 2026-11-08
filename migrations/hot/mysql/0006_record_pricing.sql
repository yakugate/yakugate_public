-- ============================================================================
-- 0006_record_pricing.sql — rating columns on MDR records (2026-07-19)
-- P9/D-033: the billing role rates every accepted record (all-accepted charge
-- policy) and stamps the buy (vendor cost) and sell (client price) amounts plus
-- when it was rated. The pre-existing rate_cards JSON column keeps the audit
-- detail (which card + rate row matched). Prices are total for the message
-- (price_per_msg x segments), currency from the selling card.
--
-- NOTE: ALTERs the template only (new partitions clone it). Existing
-- records_<...> partitions are backfilled with the same columns at deploy.
-- ============================================================================

ALTER TABLE records_template
  ADD COLUMN buy_price  DECIMAL(12,6) NULL AFTER rate_cards,
  ADD COLUMN sell_price DECIMAL(12,6) NULL AFTER buy_price,
  ADD COLUMN currency   CHAR(3) NULL AFTER sell_price,
  ADD COLUMN rated_at   DATETIME NULL AFTER currency,
  ADD KEY idx_unrated (rated_at);
