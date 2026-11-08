-- Per-account body masking (PO 2026-09-08), closing the P1 "Masking" row.
--
-- `sms_service_config.body_privacy` (plain_text | pii_masked | all_encrypted)
-- has existed and been loaded into the reference cache for months, and was
-- read by NOTHING — including `all_encrypted`, which never drove encryption
-- either (that is decided separately, by the crypto service's per-company KMS
-- onboarding). This gives the middle value its meaning: `pii_masked` stores
-- and shows the body with digit runs hidden.
--
-- Why the flag rides the queue row rather than being looked up where it is
-- needed: the processes that WRITE the record — httpvendor and smppvendor —
-- have no reference cache at all, so they cannot ask what an account's policy
-- is. The decision is made at accept time, where the account is in scope, and
-- travels with the message. Same reasoning as no_receipt (queue/0024) and
-- bind_slot (queue/0025).
--
-- 0 = store and show the body as sent, which is what every account does today
-- (all 26 are on plain_text), so this migration changes nothing by itself.
ALTER TABLE queue_template
  ADD COLUMN mask_body TINYINT(1) NOT NULL DEFAULT 0
    COMMENT 'the account asked for pii_masked: hide digit runs in the stored record, the response echo and any vendor receipt text';

ALTER TABLE queue_testvendor
  ADD COLUMN mask_body TINYINT(1) NOT NULL DEFAULT 0
    COMMENT 'the account asked for pii_masked: hide digit runs in the stored record, the response echo and any vendor receipt text';
