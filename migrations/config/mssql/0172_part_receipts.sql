-- An account's receipts for a long message: after the final, or each part's
-- as it comes (4 Nov 2026). The twin of
-- migrations/config/mysql/0172_part_receipts.sql, which says what each means.
SET XACT_ABORT ON;
SET NOCOUNT ON;
BEGIN TRAN;

-- The check is part of the ADD: the file is one batch, and a second statement
-- naming the new column would not compile.
ALTER TABLE [sms_service_config] ADD [part_receipts] VARCHAR(5) COLLATE Latin1_General_100_CI_AS_SC NOT NULL
  CONSTRAINT [DF_sms_service_config_part_receipts] DEFAULT (N'final')
  CONSTRAINT [CK_sms_service_config_part_receipts] CHECK ([part_receipts] IN ('final', 'each'));

COMMIT;
