-- Text widths (2 Nov 2026): the twin of migrations/queue/mssql/0049_text_widths.sql.
-- On SQL Server every NVARCHAR(n) up to 2000 became NVARCHAR(2n), so a VARCHAR(n)
-- holds n characters of any kind there, as it does here (an emoji is two UTF-16
-- units on SQL Server). MySQL counts characters already: nothing to change.
SELECT 1;
