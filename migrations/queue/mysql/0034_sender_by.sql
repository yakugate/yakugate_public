-- ============================================================================
-- 0034_sender_by.sql — who chose the sender (Saleem, 2026-09-29)
--
-- A message's sender is the client's own, or — when the client's cannot be
-- used — a matched template's own sender, or the account's default sender
-- (config/0129), in that fixed order. "Two defaults are difficult to debug"
-- unless the record says which one acted, so the accept pipeline stamps it
-- here and the vendor role copies it onto the record (hot/0049).
--
--   sender_by      NULL = the client's own sender; 'template:<id>'; 'account'
--   client_sender  what the client sent when it was replaced (may be empty)
--
-- One column per statement: the bootstrap skips a whole statement whose first
-- column already exists.
-- ============================================================================
ALTER TABLE queue_template ADD COLUMN sender_by VARCHAR(32) NULL;
ALTER TABLE queue_template ADD COLUMN client_sender VARCHAR(32) NULL;
