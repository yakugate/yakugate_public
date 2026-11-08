-- Capture the SYSTEM's own logs, not just one tenant's traffic
-- (PO 2026-09-04: "cant find any setting to capture system logs").
--
-- 0084 let a capture target an account or a vendor. Everything the engine
-- says about ITSELF — start-up, cache refreshes, queue and vendor state,
-- security events like a lockout or a spray block — belongs to neither, so it
-- was invisible to a capture. That is the half of the picture an operator
-- wants during an incident, when the question is "what is the gateway doing",
-- not "what happened to this customer's message".
--
-- scope_id stays NOT NULL, so a system capture stores 0: there is no row to
-- point at, and a nullable column would make every reader handle a case that
-- only ever means "system".
ALTER TABLE log_captures
  MODIFY COLUMN scope_type ENUM('account','vendor','system') NOT NULL;
