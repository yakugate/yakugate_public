-- GW-06 (PO approved 2026-08-27): config/0045 added
--   FOREIGN KEY fk_ssc_user (user_id) REFERENCES users(user_id)
-- but `users` is created by the NODE Admin API's migrations, so an
-- engine-first bootstrap on a clean machine could never satisfy it. The link
-- is advisory (portal display), not integrity-critical to the engine, and
-- the Admin API validates it at write time — so the constraint goes. The
-- column and its index stay.
ALTER TABLE sms_service_config DROP FOREIGN KEY fk_ssc_user;
