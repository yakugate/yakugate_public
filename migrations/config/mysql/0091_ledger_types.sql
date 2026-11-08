-- What a ledger row MEANS, said in the type rather than in free text (P9 gap 5).
--
-- Two problems, both about money.
--
-- 1. A postpaid charge and a prepaid charge were both written as 'commit'.
--    They are not the same event: a prepaid commit TOOK money from a balance;
--    a postpaid one moved nothing and is a line for a bill not yet sent
--    (previous_balance = new_balance on every one of them). Anyone filtering
--    the ledger by type — and the Admin API offers exactly that filter — got
--    the two mixed together.
--
--    Worse, the invoice export already needed the distinction and had to get
--    it by string-matching the free-text reason:
--
--      CASE WHEN reason LIKE '%postpaid%' THEN 'postpaid' ELSE 'prepaid' END
--
--    So an edit to a log message in charge.go would silently reclassify every
--    postpaid row on an invoice. Money should not be categorised by matching
--    prose.
--
--    'invoice_item' is the new value: a priced message awaiting invoicing, no
--    balance movement.
--
-- 2. 'reserve' is removed. It belonged to the reserve/commit design that was
--    built, measured and REVERTED for a 40-80x throughput regression under
--    concurrency (see process-9-architecture-notes.md). Nothing has ever
--    written it — verified zero rows before this migration — and leaving it
--    invites someone to rebuild a known regression because the schema implies
--    it exists.
--
--    'refund' STAYS despite also being unwritten. Unlike reserve it is not
--    tied to the rejected design: reversing a charge on a disputed or parked
--    record is a plausible thing to want, and it needs rules (who may issue
--    one, whether it may exceed the original, what it does to a sent invoice)
--    rather than deletion.

ALTER TABLE balance_history
  MODIFY COLUMN type ENUM('topup','commit','invoice_item','refund','adjust') NOT NULL
    COMMENT 'topup/adjust: written by the portal. commit: a prepaid debit, money actually taken. invoice_item: a postpaid charge awaiting invoicing, no balance movement. refund: reserved, nothing writes it yet — it needs its own rules, not a schema slot alone.';
