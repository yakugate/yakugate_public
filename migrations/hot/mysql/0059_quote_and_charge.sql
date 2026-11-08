-- ============================================================================
-- 0059_quote_and_charge.sql — the price of a message on its record, and the
-- mark of the billing batch that charged it (Saleem 2026-10-13: "we don't
-- need to record per message")
--
-- No row is written to balance_history for a message, a receipt, an incoming
-- message or a lookup any more. What they did is done by three columns here:
--
--   quote         (mtlog) what the message costs, fixed at acceptance and
--                 carried on the queue row (queue/0038): internal/billing.Quote.
--                 NULL = not known (a record from before this column).
--   charge_batch  the billing batch this record was put in. Written BEFORE
--                 the balance is moved; rated_at is written after. A record
--                 with a batch and no rated_at was in a batch that may or may
--                 not have been applied: the batch's one row in the config
--                 store (usage_batches, config/0144) says which, so a batch
--                 that is tried again can never charge twice.
--   charge        JSON: what this record adds to the account's usage, by kind
--                 (mt, hlr, dlr, mo), and what it moved on a prepaid balance.
--                 The message detail screen reads it.
--
-- One column per ALTER (the bootstrap skips a whole statement when its first
-- column is already there).
-- ============================================================================
ALTER TABLE mtlog_template ADD COLUMN quote TEXT NULL;
ALTER TABLE mtlog_template ADD COLUMN charge_batch CHAR(26) NULL;
ALTER TABLE mtlog_template ADD COLUMN charge TEXT NULL;
ALTER TABLE dlrlog_template ADD COLUMN charge_batch CHAR(26) NULL;
ALTER TABLE dlrlog_template ADD COLUMN charge TEXT NULL;
ALTER TABLE mologtemplate ADD COLUMN charge_batch CHAR(26) NULL;
ALTER TABLE mologtemplate ADD COLUMN charge TEXT NULL;
-- The legacy family is still rated when it has unrated rows: the same marks.
ALTER TABLE records_template ADD COLUMN charge_batch CHAR(26) NULL;
ALTER TABLE records_template ADD COLUMN charge TEXT NULL;
