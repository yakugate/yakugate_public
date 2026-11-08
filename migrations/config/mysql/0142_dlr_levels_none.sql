-- No receipts for an account (Saleem 2026-10-06: "how to disable receipts for
-- an account — they want none").
--
-- dlr_levels (config/0120) chose which PROGRESS receipts an account's clients
-- get; a final receipt always went, subject to the client's own
-- registered_delivery. A customer that wants no receipts at all had no
-- setting: an SMPP client could ask for none per message (registered_delivery
-- 0), an HTTP client with a receipt URL could not, and neither could be made
-- to by the operator.
--
-- `none` = nothing is sent to this account's clients about a message's fate:
-- no progress notice, no final receipt, by bind or by URL. Each message is
-- accepted as if the client had asked for no receipt (Message.NoReceipt, set
-- in the enqueue stage), so every existing rule follows: nothing is pushed,
-- and a receipt that is not sent is not charged. The message's real outcome
-- is still recorded and still answered by GET /messages/{id}.
ALTER TABLE sms_service_config
  MODIFY COLUMN dlr_levels ENUM('registered','all','finals_only','none') NOT NULL DEFAULT 'registered'
    COMMENT 'which receipts reach this account''s clients: registered (what the client asked for), all, finals_only, none (no receipt at all)';
