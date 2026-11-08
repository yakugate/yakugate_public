-- The form field a client wraps its whole request in (PO 2026-09-16:
-- "0% hardcoding"), companion to config/0041's sms_service_dialects.
--
-- Some legacy APIs post their entire request as ONE form field holding an XML
-- document — Jazz/Mobilink's `xmldoc=<SMSRequest>…</SMSRequest>` is the one we
-- were built against, and its name was a literal in the Go source. Every such
-- API picks its own name (data, request, payload, xml), so the name belongs to
-- the account's dialect, beside the field and value maps that describe the
-- rest of that client's vocabulary.
--
-- NULL or empty keeps what the engine did before: a field literally called
-- `xmldoc`, or failing that a lone form field holding an obvious document.
--
-- Read before authentication, like the request body itself, so it is resolved
-- from the URL the request arrived on — which means it only applies to an
-- account with its own submit path. On the shared native path no account is
-- known that early, and the built-in behaviour stands.
ALTER TABLE sms_service_dialects
  ADD COLUMN request_wrapper VARCHAR(64) NULL DEFAULT NULL
    COMMENT 'form field holding the whole request as one string; NULL = xmldoc, then shape'
    AFTER url;
