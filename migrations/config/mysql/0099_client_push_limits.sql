-- 0099_client_push_limits.sql — customer push endpoints get push-sized limits.
--
-- From 2026-09-11 the engine honours a mo/dlr endpoint's rate_limit (pushes
-- per second) and submit_concurrency (pushes in flight), one lane per account
-- (internal/pushpool). Before, it ignored both and pushed everyone from one
-- loop at 100/s, one POST at a time — and the portal hid the two fields for
-- these types and saved 50/1 on every save.
--
-- So every existing mo/dlr row holds 50/1 because nobody could set anything
-- else. Honoured as-is that would halve today's rate, so the rows still on
-- that untouchable default move to the new push default (PO 2026-09-11:
-- 200/s, 4 at once). A row with any other value was set on purpose and is
-- left alone. Vendor rows keep 50/1: there the columns mean claim batch and
-- parallel calls (D-088), and the column defaults stay theirs.
UPDATE http_endpoints
   SET rate_limit = 200, submit_concurrency = 4
 WHERE type IN ('mo', 'dlr') AND rate_limit = 50 AND submit_concurrency = 1;
