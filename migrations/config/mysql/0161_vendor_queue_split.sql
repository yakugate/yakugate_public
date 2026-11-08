-- ============================================================================
-- 0161_vendor_queue_split.sql — how a vendor's rate is shared between its
-- queues, and which of its senders have a queue of their own (issue 22 of the
-- design audit; Saleem and the PO, 23–26 Oct 2026).
--
-- A vendor has a common queue and one queue per short code; its rate is split
-- between those that have work. queue_split is how:
--   fair      each queue with work an equal share; a queue that needs less
--             gives the rest to the others (default)
--   priority  priority 1 from all the vendor's queues first, then 2, …
-- short_codes is the vendor's short codes (a JSON list of strings, each 1–16
-- of A–Z a–z 0–9 + _ -, at most 50; NULL or [] = none): a message whose sender
-- is one of them waits in that short code's queue, every other one in the
-- common queue. The same rule at both doors: internal/provision (console) and
-- the Admin API (services/vendorQueue.js), shared cases in
-- sqa/e2e/shared/vendor-queue-vectors.json.
-- One column per ALTER (the bootstrap rule).
-- ============================================================================
ALTER TABLE vendors ADD COLUMN queue_split ENUM('fair','priority') NOT NULL DEFAULT 'fair';
ALTER TABLE vendors ADD COLUMN short_codes JSON NULL;
