-- An HTTP vendor's call kept raw with its answer (5 Nov 2026): the twin of
-- migrations/hot/mysql/0077_sub_vendor_call.sql, which says what each column
-- holds. Text widths doubled as hot/0075 does. The day tables made before get
-- the columns from the template at start (schema_clones).
IF COL_LENGTH(N'tpl_day_sub', N'method') IS NULL ALTER TABLE [tpl_day_sub] ADD [method] VARCHAR(8) COLLATE Latin1_General_100_CI_AS_SC NULL;
IF COL_LENGTH(N'tpl_day_sub', N'url') IS NULL ALTER TABLE [tpl_day_sub] ADD [url] NVARCHAR(4000) COLLATE Latin1_General_100_CI_AS_SC NULL;
IF COL_LENGTH(N'tpl_day_sub', N'req_headers') IS NULL ALTER TABLE [tpl_day_sub] ADD [req_headers] NVARCHAR(MAX) COLLATE Latin1_General_100_CI_AS_SC NULL;
IF COL_LENGTH(N'tpl_day_sub', N'req_body') IS NULL ALTER TABLE [tpl_day_sub] ADD [req_body] NVARCHAR(MAX) COLLATE Latin1_General_100_CI_AS_SC NULL;
IF COL_LENGTH(N'tpl_day_sub', N'req_cut') IS NULL ALTER TABLE [tpl_day_sub] ADD [req_cut] TINYINT NOT NULL CONSTRAINT [DF_tpl_day_sub_req_cut] DEFAULT (0);
IF COL_LENGTH(N'tpl_day_sub', N'resp_headers') IS NULL ALTER TABLE [tpl_day_sub] ADD [resp_headers] NVARCHAR(MAX) COLLATE Latin1_General_100_CI_AS_SC NULL;
IF COL_LENGTH(N'tpl_day_sub', N'resp_body') IS NULL ALTER TABLE [tpl_day_sub] ADD [resp_body] NVARCHAR(MAX) COLLATE Latin1_General_100_CI_AS_SC NULL;
IF COL_LENGTH(N'tpl_day_sub', N'resp_cut') IS NULL ALTER TABLE [tpl_day_sub] ADD [resp_cut] TINYINT NOT NULL CONSTRAINT [DF_tpl_day_sub_resp_cut] DEFAULT (0);
IF COL_LENGTH(N'tpl_day_sub', N'sealed') IS NULL ALTER TABLE [tpl_day_sub] ADD [sealed] TINYINT NOT NULL CONSTRAINT [DF_tpl_day_sub_sealed] DEFAULT (0);
IF COL_LENGTH(N'tpl_day_sub', N'took_ms') IS NULL ALTER TABLE [tpl_day_sub] ADD [took_ms] BIGINT NULL;
