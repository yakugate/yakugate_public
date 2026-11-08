-- Tell a customer their webhook is down within minutes, not a day
-- (PO 2026-09-16), companion to config/0074.
--
-- The row used to be written only when the engine ABANDONED a message, which
-- is 30 attempts or 24 hours after the endpoint stopped answering. Until then
-- nothing existed for the Admin API's poller to read, so an outage that began
-- at nine in the morning was first mailed about the following morning.
--
-- The engine now writes the row on the FIRST failed attempt and deletes it
-- again the moment any message reaches that endpoint, so a row present means
-- "failing right now". Two counters ride along:
--
--   failed_count   every failed attempt since the outage began, so the mail
--                  can say how much is piling up; given_up_count stays what
--                  it always was, the messages actually discarded.
--   notified_count how many mails this outage has already produced, so the
--                  poller can stop after its limit instead of mailing every
--                  interval for as long as the endpoint stays down. Deleting
--                  the row on success is what resets it — a later outage is a
--                  new one and is mailed about again.
ALTER TABLE webhook_delivery_alerts
  ADD COLUMN failed_count   BIGINT UNSIGNED NOT NULL DEFAULT 0
    COMMENT 'failed delivery attempts since first_failed_at'
    AFTER given_up_count,
  ADD COLUMN notified_count INT UNSIGNED NOT NULL DEFAULT 0
    COMMENT 'emails sent about this outage; the poller stops at its limit'
    AFTER notified_at;
