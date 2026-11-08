-- An account's receipts for a long message: after the final, or each part's
-- as it comes (Saleem, 4 Nov 2026). The twin of
-- migrations/config/mssql/0172_part_receipts.sql.
--
-- 'final' (the default, as before): the client is sent one final receipt for
-- the message, when the message closes (its vendor's rule: every part, or the
-- first final when the vendor has dlr_config.first_receipt_closes).
-- 'each': the client is also sent each part's final receipt as it comes, on
-- every part id it was given (an HTTP client: on its one id). The message's
-- own status and its bill still follow the vendor's rule, booked once.
ALTER TABLE sms_service_config
  ADD COLUMN part_receipts ENUM('final','each') NOT NULL DEFAULT 'final';
