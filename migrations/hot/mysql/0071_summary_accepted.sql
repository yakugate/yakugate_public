-- The three-table design (27 Oct 2026): a message is counted ACCEPTED when
-- its mt_ row is written (at acceptance, before any vendor is known: on the
-- account's row with vendor 0 and buy 0), and SUBMITTED when a vendor took
-- it (submitted_parts / submitted_msgs, on the vendor's row, written with
-- its sub_ answers). NULL = written before this was counted.
ALTER TABLE summary_mt ADD COLUMN accepted_parts INT UNSIGNED NULL;
ALTER TABLE summary_mt ADD COLUMN accepted_msgs INT UNSIGNED NULL;
