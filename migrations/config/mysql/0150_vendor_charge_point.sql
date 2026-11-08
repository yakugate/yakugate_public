-- When a vendor charges us (Saleem, 26 Oct 2026: "ask vendor as well
-- charging, i.e on submission, on acceptance, on delivery"). The buy side of
-- a message is booked at this moment, as the sell side is at the account's
-- charge point (sms_service_config.charge_point):
--   submission  every submit we send, even one the vendor refused or that
--               timed out — per part, per try
--   acceptance  the vendor accepted it (SMPP submit_sm_resp OK, HTTP
--               accepted) — what vendor billing has always counted, so the
--               default
--   delivery    only a message the vendor reports delivered
ALTER TABLE vendors
  ADD COLUMN charge_point ENUM('submission','acceptance','delivery') NOT NULL DEFAULT 'acceptance'
    COMMENT 'when this vendor charges us: submission, acceptance (default), delivery';
