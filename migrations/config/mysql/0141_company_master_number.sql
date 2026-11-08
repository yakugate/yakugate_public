-- A company's master number (Saleem 2026-10-06).
--
-- The number the operator knows this customer by outside the gateway — the
-- customer's number in the operator's own billing or CRM. Typed by an
-- operator, optional, and not unique: several gateway companies may be billed
-- under one master number.
--
-- It is shown wherever a company is named: invoices and their CSV, the audit
-- log, message logs and their exports, and the gateway's own log files (each
-- account-scoped line carries it). The engine never decides anything by it.
--
-- One column per ALTER (the bootstrap skips a whole statement when its first
-- column is already there).
ALTER TABLE companies
  ADD COLUMN master_number VARCHAR(64) NULL
      COMMENT 'The operator''s own number for this customer (billing/CRM); optional, typed by an operator, not unique'
      AFTER name;

ALTER TABLE companies
  ADD KEY idx_companies_master (master_number);
