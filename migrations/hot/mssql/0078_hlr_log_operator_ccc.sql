-- The operator's name and the country calling code on a lookup's log row
-- (6 Nov 2026): the twin of the mysql file of the same name. Text widths
-- doubled. The day tables made before get the columns at start (schema_clones).
IF COL_LENGTH(N'tpl_day_hlr', N'operator_name') IS NULL ALTER TABLE [tpl_day_hlr] ADD [operator_name] NVARCHAR(128) COLLATE Latin1_General_100_CI_AS_SC NULL;
IF COL_LENGTH(N'tpl_day_hlr', N'ccc') IS NULL ALTER TABLE [tpl_day_hlr] ADD [ccc] NVARCHAR(12) COLLATE Latin1_General_100_CI_AS_SC NULL;
