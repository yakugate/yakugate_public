-- 0023_client_dlr_give_up.sql — an END STATE for client receipt pushes
-- (P5-B02, PO ruling 2026-08-31).
--
-- client_dlr already carried `attempts` and `next_attempt_at`, but nothing ever
-- read them as a policy: every claim set a flat NOW()+60s and bumped the count
-- forever. An unreachable customer endpoint was therefore retried about once a
-- minute for ever, with no backoff, no cap, and no way to say "we stopped".
--
-- given_up_at is that missing end state. The row is KEPT (PO ruling) rather
-- than deleted: it records exactly what a customer lost and when, it can be
-- replayed if they ask, and retention cleans it up like anything else. A row
-- with given_up_at set is never claimed again.
--
-- mo_queue needs no equivalent column — its `status` enum already carries
-- 'expired', which nothing has ever set.
ALTER TABLE client_dlr
  ADD COLUMN given_up_at DATETIME DEFAULT NULL,
  ADD KEY idx_given_up (given_up_at);
