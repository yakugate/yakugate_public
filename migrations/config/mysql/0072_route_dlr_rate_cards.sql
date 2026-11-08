-- 0072_route_dlr_rate_cards.sql — the DLR rate card lives on the MT route
-- (GW-38, PO 2026-08-28: "reomve dlr rule and take dlr rate card in mt rule").
--
-- Rate cards hang off routes: an MT route names the cards that price the
-- messages it carries, an MO route the cards for the messages it receives.
-- DLRs had neither, because a DLR route would make no routing decision at all
-- (a receipt comes back on the bind or webhook we used, and record_index
-- already names the account) — so `direction='DLR'` was dead config that no
-- engine code ever read, and it is being removed from the portal.
--
-- A receipt is therefore priced by the MT route its ORIGINAL message went out
-- on, which is exactly the granularity a DLR route would have offered: per
-- vendor and per destination, without inventing a route that decides nothing.
--
-- Both sides, mirroring buying_rate_card_id/selling_rate_card_id: a vendor may
-- charge us for receipts (buy) as well as us charging the customer (sell), and
-- P9 prices both sides of every other record family. NULL on either side falls
-- back to the account's default_dlr_rate_card_id, then to unrated — the same
-- route-card -> account-default -> unrated ladder as MT and MO (D-082.4).
ALTER TABLE routes
  ADD COLUMN dlr_buying_rate_card_id  BIGINT UNSIGNED DEFAULT NULL AFTER buying_rate_card_id,
  ADD COLUMN dlr_selling_rate_card_id BIGINT UNSIGNED DEFAULT NULL AFTER selling_rate_card_id;
