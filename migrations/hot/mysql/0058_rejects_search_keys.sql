-- ============================================================================
-- 0058_rejects_search_keys.sql — the Rejected list answers in milliseconds
-- (Saleem 2026-10-08: the list took over 20 seconds on a month of 6.7 million
-- refusals)
--
-- rejects_YYYYMM had two keys, both led by account_id. The screen an operator
-- opens asks three things of a whole month with no account named:
--   the newest refusals            ORDER BY rejected_at DESC   -> sorted 6.7M rows (17 s)
--   how many there are             COUNT(*) over the dates      -> read the month (7 s)
--   how many of each reason        GROUP BY reason_code         -> read the month (13 s)
-- and a search by the number a message was sent to read the month again.
--
--   idx_at          newest first, and the count over a date range
--   idx_reason_at   one reason's refusals newest first, and the counts by reason
--   idx_msisdn_at   "what happened to my message to 92300…"
--
-- One key per statement: the bootstrap treats "that key is already there" as
-- done and skips the WHOLE statement. Months that already exist gain the keys
-- AFTER start, in the background (bootstrap latekeys.go) — about half a minute
-- per million rows, once, with nothing waiting for it.
-- ============================================================================
ALTER TABLE rejects_template ADD KEY idx_at (rejected_at);
ALTER TABLE rejects_template ADD KEY idx_reason_at (reason_code, rejected_at);
ALTER TABLE rejects_template ADD KEY idx_msisdn_at (msisdn, rejected_at);
