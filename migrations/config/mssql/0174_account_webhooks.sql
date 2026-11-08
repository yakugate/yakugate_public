-- One MO or DLR webhook may serve many accounts (6 Nov 2026): the twin of
-- migrations/config/mysql/0174_account_webhooks.sql, which says what it
-- means. NULL = no webhook of that kind.
SET XACT_ABORT ON;
SET NOCOUNT ON;
BEGIN TRAN;

IF COL_LENGTH(N'sms_service_config', N'dlr_webhook_id') IS NULL
  ALTER TABLE [sms_service_config] ADD [dlr_webhook_id] BIGINT NULL;
IF COL_LENGTH(N'sms_service_config', N'mo_webhook_id') IS NULL
  ALTER TABLE [sms_service_config] ADD [mo_webhook_id] BIGINT NULL;

-- The file is one batch: a statement naming the new columns would not
-- compile beside their ADD, so the copy runs as its own batch.
EXEC (N'
UPDATE a SET dlr_webhook_id = (SELECT TOP (1) e.id FROM [http_endpoints] e
                                WHERE e.type = ''dlr'' AND e.account_id = a.id
                                ORDER BY e.is_active DESC, e.id DESC)
  FROM [sms_service_config] a WHERE a.dlr_webhook_id IS NULL;
UPDATE a SET mo_webhook_id = (SELECT TOP (1) e.id FROM [http_endpoints] e
                               WHERE e.type = ''mo'' AND e.account_id = a.id
                               ORDER BY e.is_active DESC, e.id DESC)
  FROM [sms_service_config] a WHERE a.mo_webhook_id IS NULL;');

COMMIT;
