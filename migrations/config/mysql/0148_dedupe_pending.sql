-- Go-live test of 18 Oct 2026 (P1 E2, P2 E2): a duplicate claim is HELD
-- until its message is durably queued. A claim made by a process that then
-- died before the queue write used to answer every resend with the id of a
-- message nobody had (8 of 16 cut calls lost with MySQL queues). pending = 1
-- while the claim's message is not queued yet: it lives a short while
-- (dedupe.PendingFor), kept alive while the queue write runs, and a resend
-- meanwhile is told to try again; the queued message makes it the full claim
-- (pending = 0, the account's dedupe window). A row written by an older
-- build reads as 0: a full claim, as it always was.
ALTER TABLE dedupe_keys ADD COLUMN pending TINYINT(1) NOT NULL DEFAULT 0;
