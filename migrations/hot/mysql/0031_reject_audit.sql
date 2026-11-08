-- Operator-only detail on a refused submit.
--
-- reason_code says WHAT refused the message ("SPAM_KEYWORD"). It could not say
-- WHICH rule, and once an operator files their spam rules by category —
-- gambling, adult, loans, crypto, pharma — every one of them refuses under the
-- same code. "How much gambling traffic did we turn away this month" was
-- unanswerable from the records.
--
-- A separate column rather than appending to reason_code, deliberately:
-- reason_code is the field an operator groups and counts by, so
-- "SPAM_KEYWORD (spam rule: universal/gambling)" would quietly stop matching
-- WHERE reason_code = 'SPAM_KEYWORD' and every existing report would drift.
--
-- And separate from the client's view entirely (PO 2026-09-09): the sender
-- keeps seeing the bare SPAM_KEYWORD it always saw. A sender who learns which
-- category they tripped can map out the rule set one message at a time.
--
-- NULL for every refusal that has no extra detail, which is most of them.
--
-- NOTE for whoever writes the next one: MySQL wants COMMENT BEFORE the
-- FIRST/AFTER clause in a column definition. Putting it after AFTER is a plain
-- syntax error, and the message points at the COMMENT text rather than at the
-- ordering, which sends you looking in the wrong place.

ALTER TABLE rejects_template
  ADD COLUMN reason_audit VARCHAR(190) NULL
    COMMENT 'Operator-only detail such as which spam category matched - never shown to the sender'
    AFTER reason_detail;
