-- P5-B03 — where an account's receipts are delivered.
--
-- An account that has BOTH a live SMPP bind and a `dlr_url` could receive the
-- same receipt twice: `client_dlr` is one queue, the bind claims from it with
-- no age filter, and the HTTP poster claims anything older than 5 seconds. If
-- the bind's own delivery lagged, both legs delivered the same row. D-029
-- accepted that trade-off; this replaces it with an explicit per-account
-- choice (PO 2026-08-31: "ask on portal, that where to submit, and gateway
-- should submit there").
--
--   bind  -> the SMPP bind delivers; the HTTP poster never claims
--   url   -> the HTTP poster delivers; the bind never claims
--   both  -> the bind tries FIRST, the url takes over when it has failed
--            dlr_bind_attempts times. Not a fan-out — a fallback.
--
-- One number decides all three at claim time: the bind claims while
-- `attempts < dlr_bind_attempts`, the poster claims once `attempts >=` it.
-- 'bind' is that number at the retry ceiling, 'url' is it at zero.
--
-- dlr_bind_attempts must stay BELOW the endpoint's total retry attempts (PO:
-- "they must be less then total retry attempts") or the fallback never gets a
-- turn — the bind would still be trying when the whole job is given up. The
-- Admin API enforces that; the default 3 sits well under the 30-attempt
-- default of YAKU_PUSH_MAX_ATTEMPTS.
--
-- DEFAULT 'both' with the bind first is exactly today's behaviour minus the
-- duplicate, so no live account changes what it receives.

ALTER TABLE sms_service_config
  ADD COLUMN dlr_channel ENUM('bind','url','both') NOT NULL DEFAULT 'both'
    COMMENT 'where delivery receipts go: SMPP bind, HTTP url, or bind then url',
  ADD COLUMN dlr_bind_attempts INT NOT NULL DEFAULT 3
    COMMENT 'dlr_channel=both: bind attempts before the HTTP url takes over';
