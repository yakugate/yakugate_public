-- ============================================================================
-- 0049_mtlog_sender_by.sql — who chose the sender, on the record (Saleem,
-- 2026-09-29; carried from the queue row, queue/0034)
--
--   sender_by      NULL = the client's own sender; 'template:<id>' = that
--                  template's own sender; 'account' = the account's default
--   client_sender  what the client sent when it was replaced (may be empty)
-- ============================================================================
ALTER TABLE mtlog_template ADD COLUMN sender_by VARCHAR(32) NULL;
ALTER TABLE mtlog_template ADD COLUMN client_sender VARCHAR(32) NULL;
