-- ============================================================================
-- 0047_client_claim_keys.sql — the client receipt and MO claims read only
-- what they take (step 3 of the DAL plan, 30 Oct 2026; plan section 9).
--
-- A claim asks for an account's due rows, oldest id first, a page at a time:
--   client_dlr: account_id IN (…) AND given_up_at IS NULL AND due … ORDER BY id
--   mo_queue:   status='queued' AND account_id IN (…) AND due … ORDER BY id
-- The keys there were (queue_owner, account_id) and (queue_owner, status,
-- account_id): every claim read ALL the account's rows — the ones out on a
-- lease and, for receipts, every one given up (kept a day and more) — and
-- sorted the due ones by id to hand out the first page. With these keys a
-- claim for one account reads its rows in id order and stops when the page
-- is full: no sort, and the rows it locks are the ones it takes. The rows
-- handed out are the same (same conditions, same order).
--
-- One key per ALTER statement (the bootstrap's rule: an "already exists"
-- skips the whole statement).
-- ============================================================================
ALTER TABLE client_dlr ADD KEY idx_cdlr_claim (account_id, given_up_at, id);

ALTER TABLE mo_queue ADD KEY idx_mo_claim (account_id, status, id);
