-- A refused message's lookup on the HLR log (6 Nov 2026): the twin of the
-- mysql file of the same name, which says what it holds. Width doubled. The
-- day tables made before get the column at start (schema_clones).
IF COL_LENGTH(N'tpl_day_hlr', N'refused_reason') IS NULL ALTER TABLE [tpl_day_hlr] ADD [refused_reason] NVARCHAR(80) COLLATE Latin1_General_100_CI_AS_SC NULL;
