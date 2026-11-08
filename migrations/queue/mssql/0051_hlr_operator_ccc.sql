-- The operator's name and the country calling code on an HLR answer (6 Nov
-- 2026): the twin of the mysql file of the same name, which says what they
-- hold. Text widths doubled as text_widths does. The shards made before get
-- the columns from the template at start (schema_clones).
IF COL_LENGTH(N'hlr_template', N'operator_name') IS NULL ALTER TABLE [hlr_template] ADD [operator_name] NVARCHAR(128) COLLATE Latin1_General_100_CI_AS_SC NULL;
IF COL_LENGTH(N'hlr_template', N'ccc') IS NULL ALTER TABLE [hlr_template] ADD [ccc] NVARCHAR(12) COLLATE Latin1_General_100_CI_AS_SC NULL;
