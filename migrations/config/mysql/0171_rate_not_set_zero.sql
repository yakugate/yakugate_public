-- A conversion rate of 1 is a real rate (3 Nov 2026, Saleem: "there are few
-- cases where one currency's value is equal to another currency's value").
-- The twin of migrations/config/mssql/0171_rate_not_set_zero.sql.
--
-- companies.base_conversion_rate and rate_card_rates.conversion_rate defaulted
-- to 1, so the gateway read a stored 1 as "not set" and no one could pin a
-- rate of exactly 1. From here 0 is "not set" and 1 is a rate like any other.
--
-- Every stored 1 becomes 0. Nothing is priced differently by it: the code of
-- the same day reads 0 exactly where it read 1 — "not set" where 1 was "not
-- set" (ToBase, buyToOwn, companyToBase, the card's own rate in billing), 1
-- where a stored rate was multiplied in as it was (the sell side's card ->
-- company step). A company that keeps the base currency is fixed at 1 in code,
-- whatever is stored.
UPDATE companies SET base_conversion_rate = 0 WHERE base_conversion_rate = 1;
ALTER TABLE companies ALTER COLUMN base_conversion_rate SET DEFAULT 0.0000000000;
UPDATE rate_card_rates SET conversion_rate = 0 WHERE conversion_rate = 1;
ALTER TABLE rate_card_rates ALTER COLUMN conversion_rate SET DEFAULT 0.0000000000;
