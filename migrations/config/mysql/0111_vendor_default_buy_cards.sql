-- A vendor's own price list (PO 2026-09-16), the buy-side twin of the account
-- defaults that have priced the sell side since D-082.4.
--
-- Until now a buy rate could be named in ONE place: the route. Sell already
-- had a ladder — route card, then the account's default_mt/mo/dlr card, then
-- unrated — while buy had no fallback at all. So a route added without a
-- buying card produced traffic that was rated but had no cost, and the margin
-- for it quietly vanished from the money screens: "no card" and "zero cost"
-- look identical downstream.
--
-- A vendor normally has ONE price list, repeated on every route that uses it.
-- These columns hold it once. The route still wins wherever it names its own
-- card — a per-route deal, a promotional corridor — and the charge audit
-- records source="vendor_default" when the fallback applied, so every price
-- stays explainable.
--
-- Three, because the three kinds of traffic are bought separately and often at
-- different prices. NULL means what it meant before: no buy price from here.
--
-- int unsigned to match routes.buying_rate_card_id (rate_cards.id); no foreign
-- key, following the routes table, which references rate cards the same way.
ALTER TABLE vendors
  ADD COLUMN default_mt_buying_rate_card_id  INT UNSIGNED NULL DEFAULT NULL
    COMMENT 'buy card for outgoing messages when the route names none'
    AFTER submit_throughput,
  ADD COLUMN default_mo_buying_rate_card_id  INT UNSIGNED NULL DEFAULT NULL
    COMMENT 'buy card for incoming messages when the route names none'
    AFTER default_mt_buying_rate_card_id,
  ADD COLUMN default_dlr_buying_rate_card_id INT UNSIGNED NULL DEFAULT NULL
    COMMENT 'buy card for delivery receipts when the route names none'
    AFTER default_mo_buying_rate_card_id;
