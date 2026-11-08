-- Trim sms_service_config (PO 2026-07-28), companion to 0044/0046.
--
-- customer_id -> user_id, FK to users.user_id: the external UUID identity
-- concept is replaced by a real link to a portal login. Existing customer_id
-- values (e.g. "demo-customer") do not correspond to any real users.user_id
-- (users are portal staff/customer logins, an entirely different id space
-- populated separately by the Admin API) -- there is no honest backfill, so
-- existing rows go to NULL rather than a fabricated match. Linking a real
-- portal user to an sms_service_config row going forward is an Admin API/
-- onboarding-flow concern, not something this migration can infer.
--
-- Dropped outright (confirmed dead in Go/Admin API before removal):
--   name/email/phone        -- never read into internal/refdata.Account
--   password_hash           -- dead column; real auth uses api_password
--                              (HTTP) / smpp_config (SMPP bind), never this
--   is_reseller              -- landed schema-only (Hadal reconciliation),
--                              never read by any code
--   enforce_ip_check         -- redundant with allowed_ip_list: enforcement
--                              is now IMPLIED by a non-empty list (same
--                              pattern already used for whitelist below)
--   is_whitelist_number       -- same pattern: implied by non-empty
--                              whitelist_numbers
--   prepaid_billing/balance/quota/timezone/portal_data_retention/
--   archival_data_retention   -- moved to companies (migration 0044);
--                              already company-shadowed for balance/prepaid
--                              via EffectiveBalance()/Prepaid(), the rest
--                              had no such fallback before this pass
--
-- mo_method dropped outright ("method is not needed" as its own column);
-- dlr_url/mo_url become JSON dlr_config/mo_config (folding mo_method's value
-- into mo_config.method) -- room to carry more per-target settings later
-- without another column-per-setting migration.
--
-- api_password -> password: plain rename, same semantics (bcrypt hash for
-- the HTTP username/password scheme), select:false on the Admin API side
-- unchanged.
ALTER TABLE sms_service_config
  CHANGE COLUMN customer_id user_id VARCHAR(64) NULL DEFAULT NULL;

UPDATE sms_service_config SET user_id = NULL;

ALTER TABLE sms_service_config
  ADD CONSTRAINT fk_ssc_user FOREIGN KEY (user_id) REFERENCES users(user_id),
  ADD COLUMN dlr_config JSON DEFAULT NULL AFTER dlr_url,
  ADD COLUMN mo_config JSON DEFAULT NULL AFTER mo_url;

UPDATE sms_service_config SET dlr_config = JSON_OBJECT('url', dlr_url) WHERE dlr_url IS NOT NULL;
UPDATE sms_service_config SET mo_config = JSON_OBJECT('url', mo_url, 'method', COALESCE(mo_method, 'GET'))
 WHERE mo_url IS NOT NULL;

ALTER TABLE sms_service_config
  DROP COLUMN name,
  DROP COLUMN email,
  DROP COLUMN phone,
  DROP COLUMN password_hash,
  DROP COLUMN is_reseller,
  DROP COLUMN enforce_ip_check,
  DROP COLUMN is_whitelist_number,
  DROP COLUMN prepaid_billing,
  DROP COLUMN balance,
  DROP COLUMN quota,
  DROP COLUMN timezone,
  DROP COLUMN portal_data_retention,
  DROP COLUMN archival_data_retention,
  DROP COLUMN dlr_url,
  DROP COLUMN mo_url,
  DROP COLUMN mo_method,
  CHANGE COLUMN api_password password VARCHAR(100) DEFAULT NULL;
