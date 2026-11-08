-- BUG-2 fix (2026-08-09): widen the reject-log reason_code.
--
-- rejects_template.reason_code was VARCHAR(32) — too small for full reason
-- strings such as "QUEUE_UNAVAILABLE: could not durably queue, retry later"
-- (~50 chars), so the fire-and-forget reject-log write failed with
-- 1406 "Data too long for column 'reason_code'" and the refusal went
-- unrecorded. Widen so any reason (code + short detail) fits.
--
-- The migration fixes the TEMPLATE, so every rejects_<YYYYMM> table cloned
-- from here onward is correct. Already-materialized monthly tables in a live
-- environment are widened out-of-band (they are date-specific and cannot be
-- named portably in a migration).
ALTER TABLE rejects_template
  MODIFY reason_code VARCHAR(191) NOT NULL;
