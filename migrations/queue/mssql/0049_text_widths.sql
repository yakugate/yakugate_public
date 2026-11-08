-- Text widths (2 Nov 2026, Saleem: "fix these"). MySQL's VARCHAR(n) holds n
-- characters; SQL Server's NVARCHAR(n) holds n UTF-16 units, so a character
-- outside the BMP (an emoji) takes two, and 32 of them did not fit a
-- 32-character spam list name (QA API-034). Every NVARCHAR(n) with n up to
-- 2000 becomes NVARCHAR(2n): n characters of any kind. The app still checks
-- lengths in characters.
--
-- Kept narrow: the text of an index whose key would pass SQL Server's key size
-- once widened (900 bytes clustered, 1700 otherwise) — those hold ASCII: key
-- ids, tokens, migration names — and the other side of a foreign key on such
-- a column. Every table of the store, the per-company day tables and their
-- templates (tpl_day_*) included, so a day table made later is wide too.
--
-- SQL Server refuses to widen a column that a foreign key uses, or that a
-- filtered index names (in its columns or its WHERE — the base makes a unique
-- key over nullable columns that way): those are dropped and made again around
-- the change, as they were (a key's actions, trusted or not, on or off; an
-- index's columns, order, included columns and filter). Any other index, key,
-- default or check is not in the way, and making a column longer is a change
-- of its description only.
--
-- The same rule makes a fresh base: internal/dal/mssql/schemagen/widths.go
-- (MapSchema), which the schema parity test reads. One batch, one transaction.
-- The same text is in every store (config 0170, queue 0049, hot 0075, hlr 0002).
SET XACT_ABORT ON;
SET NOCOUNT ON;
BEGIN TRAN;

-- The columns to widen: every NVARCHAR(n), n <= 2000, of a user table.
DECLARE @w TABLE (object_id INT NOT NULL, column_id INT NOT NULL, PRIMARY KEY (object_id, column_id));
INSERT INTO @w (object_id, column_id)
SELECT c.object_id, c.column_id
  FROM sys.columns c
  JOIN sys.types t ON t.user_type_id = c.user_type_id
  JOIN sys.tables tb ON tb.object_id = c.object_id
 WHERE t.name = N'nvarchar' AND c.max_length BETWEEN 2 AND 4000 AND tb.is_ms_shipped = 0;

-- An index whose key would pass its size once widened keeps its text narrow.
DELETE w FROM @w w
 WHERE EXISTS (
   SELECT 1
     FROM sys.index_columns ic
     JOIN sys.indexes i ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    WHERE ic.object_id = w.object_id AND ic.column_id = w.column_id AND ic.is_included_column = 0
      AND (SELECT SUM(CASE WHEN x.object_id IS NOT NULL THEN 2 * c2.max_length ELSE c2.max_length END)
             FROM sys.index_columns ic2
             JOIN sys.columns c2 ON c2.object_id = ic2.object_id AND c2.column_id = ic2.column_id
             LEFT JOIN @w x ON x.object_id = c2.object_id AND x.column_id = c2.column_id
            WHERE ic2.object_id = i.object_id AND ic2.index_id = i.index_id AND ic2.is_included_column = 0)
          > CASE WHEN i.type = 1 THEN 900 ELSE 1700 END);

-- Both sides of a foreign key alike: a side kept narrow keeps the other narrow.
DECLARE @kept INT = 1;
WHILE @kept > 0
BEGIN
  DELETE w FROM @w w
   WHERE EXISTS (SELECT 1 FROM sys.foreign_key_columns fc
                  WHERE (fc.parent_object_id = w.object_id AND fc.parent_column_id = w.column_id
                         AND NOT EXISTS (SELECT 1 FROM @w x WHERE x.object_id = fc.referenced_object_id AND x.column_id = fc.referenced_column_id))
                     OR (fc.referenced_object_id = w.object_id AND fc.referenced_column_id = w.column_id
                         AND NOT EXISTS (SELECT 1 FROM @w x WHERE x.object_id = fc.parent_object_id AND x.column_id = fc.parent_column_id)));
  SET @kept = @@ROWCOUNT;
END;

-- The foreign keys in the way, as they are.
DECLARE @fk TABLE (name SYSNAME NOT NULL, child NVARCHAR(600) NOT NULL, parent NVARCHAR(600) NOT NULL,
  cols NVARCHAR(MAX) NOT NULL, refs NVARCHAR(MAX) NOT NULL, on_delete NVARCHAR(60) NOT NULL, on_update NVARCHAR(60) NOT NULL,
  untrusted BIT NOT NULL, disabled BIT NOT NULL);
INSERT INTO @fk
SELECT f.name,
       QUOTENAME(OBJECT_SCHEMA_NAME(f.parent_object_id)) + N'.' + QUOTENAME(OBJECT_NAME(f.parent_object_id)),
       QUOTENAME(OBJECT_SCHEMA_NAME(f.referenced_object_id)) + N'.' + QUOTENAME(OBJECT_NAME(f.referenced_object_id)),
       (SELECT STRING_AGG(CAST(QUOTENAME(COL_NAME(fc.parent_object_id, fc.parent_column_id)) AS NVARCHAR(MAX)), N', ')
               WITHIN GROUP (ORDER BY fc.constraint_column_id)
          FROM sys.foreign_key_columns fc WHERE fc.constraint_object_id = f.object_id),
       (SELECT STRING_AGG(CAST(QUOTENAME(COL_NAME(fc.referenced_object_id, fc.referenced_column_id)) AS NVARCHAR(MAX)), N', ')
               WITHIN GROUP (ORDER BY fc.constraint_column_id)
          FROM sys.foreign_key_columns fc WHERE fc.constraint_object_id = f.object_id),
       REPLACE(f.delete_referential_action_desc, N'_', N' '),
       REPLACE(f.update_referential_action_desc, N'_', N' '),
       f.is_not_trusted, f.is_disabled
  FROM sys.foreign_keys f
 WHERE EXISTS (SELECT 1 FROM sys.foreign_key_columns fc
                 JOIN @w w ON (w.object_id = fc.parent_object_id AND w.column_id = fc.parent_column_id)
                           OR (w.object_id = fc.referenced_object_id AND w.column_id = fc.referenced_column_id)
                WHERE fc.constraint_object_id = f.object_id);

-- The filtered indexes in the way, as they are.
DECLARE @ix TABLE (drop_sql NVARCHAR(MAX) NOT NULL, make_sql NVARCHAR(MAX) NOT NULL);
INSERT INTO @ix (drop_sql, make_sql)
SELECT N'DROP INDEX ' + QUOTENAME(i.name) + N' ON ' + tbl.n + N';',
       N'CREATE ' + CASE WHEN i.is_unique = 1 THEN N'UNIQUE ' ELSE N'' END + N'NONCLUSTERED INDEX ' + QUOTENAME(i.name)
       + N' ON ' + tbl.n + N' ('
       + (SELECT STRING_AGG(CAST(QUOTENAME(COL_NAME(ic.object_id, ic.column_id))
                  + CASE WHEN ic.is_descending_key = 1 THEN N' DESC' ELSE N'' END AS NVARCHAR(MAX)), N', ')
                 WITHIN GROUP (ORDER BY ic.key_ordinal)
            FROM sys.index_columns ic
           WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0) + N')'
       + ISNULL(N' INCLUDE (' + (SELECT STRING_AGG(CAST(QUOTENAME(COL_NAME(ic.object_id, ic.column_id)) AS NVARCHAR(MAX)), N', ')
                                    WITHIN GROUP (ORDER BY ic.index_column_id)
                               FROM sys.index_columns ic
                              WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 1) + N')', N'')
       + N' WHERE ' + i.filter_definition + N';'
  FROM sys.indexes i
 CROSS APPLY (SELECT QUOTENAME(OBJECT_SCHEMA_NAME(i.object_id)) + N'.' + QUOTENAME(OBJECT_NAME(i.object_id)) AS n) tbl
 WHERE i.has_filter = 1
   AND EXISTS (SELECT 1 FROM @w w
                 JOIN sys.columns c ON c.object_id = w.object_id AND c.column_id = w.column_id
                WHERE w.object_id = i.object_id
                  AND (CHARINDEX(QUOTENAME(c.name), i.filter_definition) > 0
                       OR EXISTS (SELECT 1 FROM sys.index_columns ic
                                   WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.column_id = c.column_id)));

DECLARE @sql NVARCHAR(MAX) = N'';
SELECT @sql = @sql + N'ALTER TABLE ' + child + N' DROP CONSTRAINT ' + QUOTENAME(name) + N';' + NCHAR(10) FROM @fk;
SELECT @sql = @sql + drop_sql + NCHAR(10) FROM @ix;
IF @sql <> N'' EXEC sys.sp_executesql @sql;

-- Widen: NVARCHAR(n) is max_length 2n bytes, so NVARCHAR(max_length) is 2n characters.
SET @sql = N'';
SELECT @sql = @sql + N'ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(c.object_id)) + N'.' + QUOTENAME(OBJECT_NAME(c.object_id))
     + N' ALTER COLUMN ' + QUOTENAME(c.name) + N' NVARCHAR(' + CAST(c.max_length AS NVARCHAR(10)) + N')'
     + ISNULL(N' COLLATE ' + c.collation_name, N'')
     + CASE WHEN c.is_nullable = 1 THEN N' NULL' ELSE N' NOT NULL' END + N';' + NCHAR(10)
  FROM @w w
  JOIN sys.columns c ON c.object_id = w.object_id AND c.column_id = w.column_id;
IF @sql <> N'' EXEC sys.sp_executesql @sql;

-- The filtered indexes and the foreign keys again, as they were.
SET @sql = N'';
SELECT @sql = @sql + make_sql + NCHAR(10) FROM @ix;
SELECT @sql = @sql + N'ALTER TABLE ' + child + CASE WHEN untrusted = 1 THEN N' WITH NOCHECK' ELSE N' WITH CHECK' END
     + N' ADD CONSTRAINT ' + QUOTENAME(name) + N' FOREIGN KEY (' + cols + N') REFERENCES ' + parent + N' (' + refs + N')'
     + N' ON DELETE ' + on_delete + N' ON UPDATE ' + on_update + N';' + NCHAR(10)
     + CASE WHEN disabled = 1 THEN N'ALTER TABLE ' + child + N' NOCHECK CONSTRAINT ' + QUOTENAME(name) + N';' + NCHAR(10) ELSE N'' END
  FROM @fk;
IF @sql <> N'' EXEC sys.sp_executesql @sql;

COMMIT;
