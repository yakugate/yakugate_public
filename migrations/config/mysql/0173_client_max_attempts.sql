-- How many tries an account's receipts and incoming messages get before the
-- gateway gives one up (Saleem, 5 Nov 2026: "i need to set max attempts for
-- dlr/mo to client … provide input fields for that … account wise"). The twin
-- of migrations/config/mssql/0173_client_max_attempts.sql.
--
-- Every try counts, on a bind or at the webhook: the number is the try (a
-- claim of the receipt or MO). One that reaches it is given up: taken off the
-- queue and kept as a row with no try (mo_sub_ / dlr_sub_, result expired,
-- MAX_ATTEMPTS_REACHED). NULL = the gateway's default: the webhook's own retry
-- setting, else YAKU_PUSH_MAX_ATTEMPTS; on a bind only the age limit
-- (YAKU_PUSH_GIVE_UP_AFTER_SEC). The webhook's own limit still applies:
-- whichever comes first.
ALTER TABLE sms_service_config ADD COLUMN dlr_max_attempts SMALLINT UNSIGNED NULL;
ALTER TABLE sms_service_config ADD COLUMN mo_max_attempts SMALLINT UNSIGNED NULL;
