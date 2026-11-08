-- Carry a soft-flag decision from accept to the record (config/0098).
--
-- A flagged message is SENT, so it is not a reject and never reaches the
-- rejects table. Its record is written later, by whichever vendor process
-- reaches a terminal state (D-030) - and that process has no reference cache
-- and no idea a spam rule was involved. So the decision has to travel on the
-- queue row, the same "carry the flag" pattern as no_receipt (queue/0024),
-- bind_slot (queue/0025) and mask_body (queue/0026).
--
-- NULL for every message no flag rule matched, which is all of them until an
-- operator sets a rule to flag.
ALTER TABLE queue_template
  ADD COLUMN spam_flag VARCHAR(190) NULL
    COMMENT 'a spam rule with action=flag matched at accept - which rule, for the record'
    AFTER mask_body;
