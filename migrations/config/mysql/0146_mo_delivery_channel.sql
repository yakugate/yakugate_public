-- Where an account's incoming messages (MO) go — the twin of dlr_channel
-- (config/0077), 2026-10-15.
--
-- Saleem: "i should have capability to set http only for any one setting
-- along with fallback to http from smpp after some attempts, but what if i
-- want mo to be only sent to http". Until now MO had no choice at all: the
-- smpp-server pushed an MO to any live receive bind at once, and the mo role's
-- webhook poster took only what no bind had taken within 5 seconds. So an
-- account with a bind up could not have its MO on the webhook, and its MO and
-- its receipts could not go to different places.
--
--   bind  -> the SMPP bind delivers; the webhook never claims
--   url   -> the webhook delivers; the bind never claims
--   both  -> the bind tries FIRST, the webhook takes over when it has failed
--            mo_bind_attempts times, or when no bind is up. A fallback, not a
--            fan-out.
--
-- The same numbers decide it at claim time as for receipts: the bind claims
-- while `attempts < mo_bind_attempts`, the webhook once `attempts >=` it (or
-- at 0 once the message is 5 s old — nobody has tried it).
--
-- DEFAULT 'both' is what every account had until today, so no live account
-- changes what it receives. One column per ALTER (bootstrap rule).

ALTER TABLE sms_service_config
  ADD COLUMN mo_channel ENUM('bind','url','both') NOT NULL DEFAULT 'both'
    COMMENT 'where incoming messages go: SMPP bind, HTTP webhook, or bind then webhook';

ALTER TABLE sms_service_config
  ADD COLUMN mo_bind_attempts INT NOT NULL DEFAULT 3
    COMMENT 'mo_channel=both: bind attempts before the webhook takes over';
