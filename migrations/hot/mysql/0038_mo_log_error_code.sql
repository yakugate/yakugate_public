-- 0038_mo_log_error_code.sql — why an incoming message was not delivered.
--
-- Incoming multi-part messages are joined before routing (2026-09-14). One
-- whose parts did not all arrive within the wait is dropped and recorded (PO:
-- "drop and record", as for outgoing): it goes on the owning account's MO log
-- as expired with error INCOMPLETE_PARTS and the text that did arrive. The MO
-- log had no column for a reason — record_index keeps only the status — so the
-- "why" would have been lost.
--
-- NULL for every message delivered normally. Existing mo_log_YYYY_MM clones
-- follow the template at start-up (bootstrap syncCloneColumns).
ALTER TABLE mologtemplate
  ADD COLUMN error_code VARCHAR(32) NULL
    COMMENT 'why the message was not delivered (INCOMPLETE_PARTS); NULL = normal';
