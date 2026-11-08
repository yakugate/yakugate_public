-- How many tries an account's receipts and incoming messages get before the
-- gateway gives one up (5 Nov 2026): the twin of
-- migrations/config/mysql/0173_client_max_attempts.sql, which says what it
-- means. NULL = the gateway's default.
IF COL_LENGTH(N'sms_service_config', N'dlr_max_attempts') IS NULL
  ALTER TABLE [sms_service_config] ADD [dlr_max_attempts] INT NULL;
IF COL_LENGTH(N'sms_service_config', N'mo_max_attempts') IS NULL
  ALTER TABLE [sms_service_config] ADD [mo_max_attempts] INT NULL;
