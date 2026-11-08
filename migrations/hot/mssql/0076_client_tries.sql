-- Every try to give a client an incoming message or a receipt (D-140, 5 Nov
-- 2026): the twin of migrations/hot/mysql/0076_client_tries.sql, which says
-- what each column holds. Text widths doubled as hot/0075 does (n characters
-- of any kind); a unique key over a nullable column is a filtered index, as
-- the base makes tpl_day_dlr.uq_final.
IF OBJECT_ID(N'tpl_day_mo_sub', N'U') IS NULL
BEGIN
CREATE TABLE [tpl_day_mo_sub] (
  [id] BIGINT IDENTITY(1,1) NOT NULL,
  [try_id] CHAR(26) COLLATE Latin1_General_100_BIN2 NOT NULL,
  [msg_id] CHAR(26) COLLATE Latin1_General_100_BIN2 NOT NULL,
  [account_id] BIGINT NULL,
  [attempt] INT NULL,
  [channel] VARCHAR(4) COLLATE Latin1_General_100_CI_AS_SC NULL,
  [result] VARCHAR(9) COLLATE Latin1_General_100_CI_AS_SC NOT NULL,
  [final_of] CHAR(26) COLLATE Latin1_General_100_BIN2 NULL,
  [command_status] BIGINT NULL,
  [http_status] INT NULL,
  [error_code] NVARCHAR(64) COLLATE Latin1_General_100_CI_AS_SC NULL,
  [error] NVARCHAR(1024) COLLATE Latin1_General_100_CI_AS_SC NULL,
  [endpoint_id] BIGINT NULL,
  [session] NVARCHAR(128) COLLATE Latin1_General_100_CI_AS_SC NULL,
  [method] VARCHAR(8) COLLATE Latin1_General_100_CI_AS_SC NULL,
  [url] NVARCHAR(4000) COLLATE Latin1_General_100_CI_AS_SC NULL,
  [req_headers] NVARCHAR(MAX) COLLATE Latin1_General_100_CI_AS_SC NULL,
  [req_body] NVARCHAR(MAX) COLLATE Latin1_General_100_CI_AS_SC NULL,
  [req_cut] TINYINT NOT NULL CONSTRAINT [DF_tpl_day_mo_sub_req_cut] DEFAULT (0),
  [resp_headers] NVARCHAR(MAX) COLLATE Latin1_General_100_CI_AS_SC NULL,
  [resp_body] NVARCHAR(MAX) COLLATE Latin1_General_100_CI_AS_SC NULL,
  [resp_cut] TINYINT NOT NULL CONSTRAINT [DF_tpl_day_mo_sub_resp_cut] DEFAULT (0),
  [sealed] TINYINT NOT NULL CONSTRAINT [DF_tpl_day_mo_sub_sealed] DEFAULT (0),
  [sent_at] DATETIME2(3) NOT NULL,
  [took_ms] BIGINT NULL,
  [deployment_id] NVARCHAR(128) COLLATE Latin1_General_100_CI_AS_SC NULL,
  [created_at] DATETIME2(0) NOT NULL CONSTRAINT [DF_tpl_day_mo_sub_created_at] DEFAULT (SYSUTCDATETIME()),
  CONSTRAINT [PK_tpl_day_mo_sub] PRIMARY KEY ([id]),
  CONSTRAINT [CK_tpl_day_mo_sub_channel] CHECK ([channel] IN ('smpp', 'http')),
  CONSTRAINT [CK_tpl_day_mo_sub_result] CHECK ([result] IN ('accepted', 'refused', 'no_answer', 'expired', 'failed'))
);
CREATE UNIQUE INDEX [uq_try] ON [tpl_day_mo_sub] ([try_id]);
CREATE UNIQUE INDEX [uq_final] ON [tpl_day_mo_sub] ([final_of]) WHERE [final_of] IS NOT NULL;
CREATE INDEX [idx_msg] ON [tpl_day_mo_sub] ([msg_id]);
CREATE INDEX [idx_at] ON [tpl_day_mo_sub] ([sent_at]);
END;

IF OBJECT_ID(N'tpl_day_dlr_sub', N'U') IS NULL
BEGIN
CREATE TABLE [tpl_day_dlr_sub] (
  [id] BIGINT IDENTITY(1,1) NOT NULL,
  [try_id] CHAR(26) COLLATE Latin1_General_100_BIN2 NOT NULL,
  [msg_id] CHAR(26) COLLATE Latin1_General_100_BIN2 NOT NULL,
  [client_msg_id] NVARCHAR(128) COLLATE Latin1_General_100_CI_AS_SC NULL,
  [account_id] BIGINT NULL,
  [status] NVARCHAR(24) COLLATE Latin1_General_100_CI_AS_SC NULL,
  [stat] NVARCHAR(16) COLLATE Latin1_General_100_CI_AS_SC NULL,
  [attempt] INT NULL,
  [channel] VARCHAR(4) COLLATE Latin1_General_100_CI_AS_SC NULL,
  [result] VARCHAR(9) COLLATE Latin1_General_100_CI_AS_SC NOT NULL,
  [command_status] BIGINT NULL,
  [http_status] INT NULL,
  [error_code] NVARCHAR(64) COLLATE Latin1_General_100_CI_AS_SC NULL,
  [error] NVARCHAR(1024) COLLATE Latin1_General_100_CI_AS_SC NULL,
  [endpoint_id] BIGINT NULL,
  [session] NVARCHAR(128) COLLATE Latin1_General_100_CI_AS_SC NULL,
  [method] VARCHAR(8) COLLATE Latin1_General_100_CI_AS_SC NULL,
  [url] NVARCHAR(4000) COLLATE Latin1_General_100_CI_AS_SC NULL,
  [req_headers] NVARCHAR(MAX) COLLATE Latin1_General_100_CI_AS_SC NULL,
  [req_body] NVARCHAR(MAX) COLLATE Latin1_General_100_CI_AS_SC NULL,
  [req_cut] TINYINT NOT NULL CONSTRAINT [DF_tpl_day_dlr_sub_req_cut] DEFAULT (0),
  [resp_headers] NVARCHAR(MAX) COLLATE Latin1_General_100_CI_AS_SC NULL,
  [resp_body] NVARCHAR(MAX) COLLATE Latin1_General_100_CI_AS_SC NULL,
  [resp_cut] TINYINT NOT NULL CONSTRAINT [DF_tpl_day_dlr_sub_resp_cut] DEFAULT (0),
  [sealed] TINYINT NOT NULL CONSTRAINT [DF_tpl_day_dlr_sub_sealed] DEFAULT (0),
  [sent_at] DATETIME2(3) NOT NULL,
  [took_ms] BIGINT NULL,
  [deployment_id] NVARCHAR(128) COLLATE Latin1_General_100_CI_AS_SC NULL,
  [created_at] DATETIME2(0) NOT NULL CONSTRAINT [DF_tpl_day_dlr_sub_created_at] DEFAULT (SYSUTCDATETIME()),
  CONSTRAINT [PK_tpl_day_dlr_sub] PRIMARY KEY ([id]),
  CONSTRAINT [CK_tpl_day_dlr_sub_channel] CHECK ([channel] IN ('smpp', 'http')),
  CONSTRAINT [CK_tpl_day_dlr_sub_result] CHECK ([result] IN ('accepted', 'refused', 'no_answer', 'expired', 'failed'))
);
CREATE UNIQUE INDEX [uq_try] ON [tpl_day_dlr_sub] ([try_id]);
CREATE INDEX [idx_msg] ON [tpl_day_dlr_sub] ([msg_id]);
CREATE INDEX [idx_at] ON [tpl_day_dlr_sub] ([sent_at]);
END;
