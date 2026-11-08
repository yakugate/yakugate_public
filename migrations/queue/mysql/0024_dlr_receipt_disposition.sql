-- registered_delivery is honoured (PO 2026-09-03; see hot/0026 for the full
-- design note — that migration carries what P5's later, message-less receipt
-- correlation needs). This one is what the message's OWN egress needs: the
-- vendor claim (queue_<vendor>, via Claim/scanQueueRow) is where P3/P4 read
-- the model.Message back to decide whether to call PushSubmitDLR (the
-- intermediate "sent" notice) and PushTerminalDLR (a locally-synthesized
-- failure) — both of which already have the full Message in scope and gate on
-- it directly, no query needed, PROVIDED the fields survived the queue round
-- trip. Without these columns every claimed message reads back false/false —
-- the zero value — which is silently wrong for want_intermediate_receipt (see
-- model.Message's doc comment on why that field's zero value differs from the
-- other two).
--
-- Default 0 on all three: an HTTP-submitted message (no registered_delivery
-- concept) and any row enqueued before this migration are unaffected.
--
-- Bootstrap's missingCloneColumns sync (2026-07-26) propagates ADD COLUMN
-- from queue_template to every EXISTING queue_<vendor> table automatically at
-- next startup — this migration only needs to touch the template plus the
-- seeded dev vendor, matching the established convention (queue/0018).
ALTER TABLE queue_template
  ADD COLUMN no_receipt TINYINT(1) NOT NULL DEFAULT 0,
  ADD COLUMN failure_receipt_only TINYINT(1) NOT NULL DEFAULT 0,
  ADD COLUMN want_intermediate_receipt TINYINT(1) NOT NULL DEFAULT 0;

ALTER TABLE queue_testvendor
  ADD COLUMN no_receipt TINYINT(1) NOT NULL DEFAULT 0,
  ADD COLUMN failure_receipt_only TINYINT(1) NOT NULL DEFAULT 0,
  ADD COLUMN want_intermediate_receipt TINYINT(1) NOT NULL DEFAULT 0;
