-- The HLR provider's call kept with its lookup (6 Nov 2026): the twin of the
-- mysql file of the same name, which says what it holds. The day tables made
-- before get the columns at start (schema_clones).
IF COL_LENGTH(N'tpl_day_hlr', N'method') IS NULL ALTER TABLE [tpl_day_hlr] ADD [method] VARCHAR(8) COLLATE Latin1_General_100_CI_AS_SC NULL;
IF COL_LENGTH(N'tpl_day_hlr', N'url') IS NULL ALTER TABLE [tpl_day_hlr] ADD [url] NVARCHAR(4000) COLLATE Latin1_General_100_CI_AS_SC NULL;
IF COL_LENGTH(N'tpl_day_hlr', N'req_headers') IS NULL ALTER TABLE [tpl_day_hlr] ADD [req_headers] NVARCHAR(MAX) COLLATE Latin1_General_100_CI_AS_SC NULL;
IF COL_LENGTH(N'tpl_day_hlr', N'req_body') IS NULL ALTER TABLE [tpl_day_hlr] ADD [req_body] NVARCHAR(MAX) COLLATE Latin1_General_100_CI_AS_SC NULL;
IF COL_LENGTH(N'tpl_day_hlr', N'req_cut') IS NULL ALTER TABLE [tpl_day_hlr] ADD [req_cut] TINYINT NOT NULL CONSTRAINT [DF_tpl_day_hlr_req_cut] DEFAULT (0);
IF COL_LENGTH(N'tpl_day_hlr', N'http_status') IS NULL ALTER TABLE [tpl_day_hlr] ADD [http_status] SMALLINT NULL;
IF COL_LENGTH(N'tpl_day_hlr', N'resp_headers') IS NULL ALTER TABLE [tpl_day_hlr] ADD [resp_headers] NVARCHAR(MAX) COLLATE Latin1_General_100_CI_AS_SC NULL;
IF COL_LENGTH(N'tpl_day_hlr', N'resp_body') IS NULL ALTER TABLE [tpl_day_hlr] ADD [resp_body] NVARCHAR(MAX) COLLATE Latin1_General_100_CI_AS_SC NULL;
IF COL_LENGTH(N'tpl_day_hlr', N'resp_cut') IS NULL ALTER TABLE [tpl_day_hlr] ADD [resp_cut] TINYINT NOT NULL CONSTRAINT [DF_tpl_day_hlr_resp_cut] DEFAULT (0);
IF COL_LENGTH(N'tpl_day_hlr', N'took_ms') IS NULL ALTER TABLE [tpl_day_hlr] ADD [took_ms] BIGINT NULL;
IF COL_LENGTH(N'tpl_day_hlr', N'call_error') IS NULL ALTER TABLE [tpl_day_hlr] ADD [call_error] NVARCHAR(1000) COLLATE Latin1_General_100_CI_AS_SC NULL;
