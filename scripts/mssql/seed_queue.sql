-- QUEUE store seed on SQL Server — scripts/seed/queue.sql written in T-SQL. Idempotent.
-- Keep the two in step: a row added to one is added to the other.

-- ---- per-vendor pending queue and an HLR shard, made from their templates ----
-- (SQL Server has no CREATE TABLE … LIKE: the MSSQL DAL's Seed makes them
-- with its CreateLike, as the gateway does at run time.)
-- yaku:create-like queue_template queue_testvendor
-- yaku:create-like hlr_template hlr_9230
GO

-- ---- HLR shard (first-4 digits of 923001234567 = 9230) ----
UPDATE hlr_9230 SET mcc = '410', mnc = '01', last_check = SYSUTCDATETIME() WHERE msisdn = '923001234567';
INSERT INTO hlr_9230 (msisdn, mcc, mnc)
SELECT '923001234567', '410', '01'
WHERE NOT EXISTS (SELECT 1 FROM hlr_9230 WHERE msisdn = '923001234567');

-- ---- auto-replace rule for aracct (account id 11) ----
-- replace_regex {match,replace}; literal 'x' -> 'y'.
INSERT INTO auto_replace_text (account_id, replace_regex, seq)
SELECT 11, N'{"match":"x","replace":"y"}', 1
WHERE NOT EXISTS (SELECT 1 FROM auto_replace_text WHERE account_id = 11 AND seq = 1);
GO
