-- A conversion rate of 1 is a real rate (3 Nov 2026). The twin of
-- migrations/config/mysql/0171_rate_not_set_zero.sql, which says why: 0 is
-- "not set" from here, every stored 1 becomes 0, and the default is 0.
-- The default constraints keep their names (0001_base). One transaction.
SET XACT_ABORT ON;
SET NOCOUNT ON;
BEGIN TRAN;

UPDATE [companies] SET [base_conversion_rate] = 0 WHERE [base_conversion_rate] = 1;
ALTER TABLE [companies] DROP CONSTRAINT [DF_companies_base_conversion_rate];
ALTER TABLE [companies] ADD CONSTRAINT [DF_companies_base_conversion_rate] DEFAULT (0.0000000000) FOR [base_conversion_rate];

UPDATE [rate_card_rates] SET [conversion_rate] = 0 WHERE [conversion_rate] = 1;
ALTER TABLE [rate_card_rates] DROP CONSTRAINT [DF_rate_card_rates_conversion_rate];
ALTER TABLE [rate_card_rates] ADD CONSTRAINT [DF_rate_card_rates_conversion_rate] DEFAULT (0.0000000000) FOR [conversion_rate];

COMMIT;
