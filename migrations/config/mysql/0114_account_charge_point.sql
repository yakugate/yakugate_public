-- Charge point per account (PO 2026-09-17). When an accepted message is
-- charged to a prepaid balance or ledgered for a postpaid invoice:
--   acceptance  every accepted message, whatever becomes of it (D-033, the
--               behaviour until now — so it is the default)
--   submit      only once the vendor took it (the record carries a vendor id)
--   delivery    only when it was delivered
-- Refusal happens only at acceptance (the credit gate); everything accepted
-- is processed, so a company balance may go below zero. Inbound messages
-- and receipts are charged as before; this covers MT only.
ALTER TABLE sms_service_config
  ADD COLUMN charge_point ENUM('acceptance','submit','delivery') NOT NULL DEFAULT 'acceptance'
    COMMENT 'when an MT message is charged: acceptance (default), submit (vendor took it), delivery';
