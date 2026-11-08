-- 0055_template_default_sender.sql — a matched template may set the sender-id
-- (P1 scope finalize 2026-08-09), mirroring the existing default_priority
-- override. NULL / empty = leave the submitted sender unchanged. The
-- template-set sender is still re-validated against the account's sender-id
-- allowlist at pipeline stage 12, so a template cannot introduce an unlisted
-- sender.
ALTER TABLE templates
  ADD COLUMN default_sender VARCHAR(20) NULL AFTER default_priority;
