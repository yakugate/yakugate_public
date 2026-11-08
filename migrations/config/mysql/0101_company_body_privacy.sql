-- 0101_company_body_privacy.sql — one message-privacy setting per company
-- (PO 2026-09-13, D-108).
--
-- Until now privacy was set in two places that did not agree:
--   * sms_service_config.body_privacy, per ACCOUNT: plain_text / pii_masked
--     were live; all_encrypted was offered on the screen but never read by
--     the engine;
--   * tenant_kms_config, per COMPANY: an active assignment meant "encrypt
--     this company's messages with its own key".
-- Now the company decides, with one setting: plain_text, pii_masked or
-- all_encrypted. The KMS assignment only says WHICH key an encrypted company
-- uses (no assignment = the gateway key). Only operators may change it, and
-- a change applies to new messages only.
--
-- Carried over, so no company's messages are treated differently on the day:
--   * an active KMS assignment                -> all_encrypted
--   * otherwise any account on pii_masked     -> pii_masked
--   * everything else                         -> plain_text
-- An account that showed all_encrypted got plain text from the engine, so it
-- carries over as what it actually had. sms_service_config.body_privacy stays
-- in the schema (migrations are additive) but the engine no longer reads it
-- for linked accounts.
ALTER TABLE companies
  ADD COLUMN body_privacy ENUM('plain_text','pii_masked','all_encrypted') NOT NULL DEFAULT 'plain_text'
  AFTER loss_detection_enabled;

UPDATE companies c
   SET c.body_privacy = 'pii_masked'
 WHERE EXISTS (SELECT 1 FROM sms_service_config a
                WHERE a.company_id = c.id AND a.body_privacy = 'pii_masked');

UPDATE companies c
   SET c.body_privacy = 'all_encrypted'
 WHERE EXISTS (SELECT 1 FROM tenant_kms_config t
                WHERE t.company_id = c.id AND t.status = 'active');

-- The assignment's on/off switch is gone: whether a company is encrypted is
-- body_privacy's job now. A disabled assignment becomes active so that an
-- encrypted company always uses the key it was given, never quietly the
-- gateway key. (It only takes effect if the company is set to encrypted,
-- which the carry-over above did not do for disabled assignments.)
UPDATE tenant_kms_config SET status = 'active' WHERE status = 'disabled';
