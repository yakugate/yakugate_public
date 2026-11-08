-- B-11 — what to do when HLR cannot identify the destination's operator.
--
-- The HLR stage fills MCC/MNC so the route stage can pick an operator-specific
-- vendor and the rater can price against a per-operator card. Until now the
-- stage NEVER rejected: a number it could not resolve was stamped
-- `hlr_source='miss'` and sent anyway, routing as "undetected network". Every
-- account got that, and none could ask for anything else.
--
-- For a customer priced or contracted per operator, routing blind can mean a
-- dearer vendor, worse delivery, or traffic sent outside a commercial
-- agreement. This gives them the choice:
--
--   proceed -> send undetected (exactly today's behaviour)
--   reject  -> refuse with 403 HLR_UNRESOLVED
--
-- PO 2026-09-03: 'reject' means ANY unresolved number, not only a definitive
-- not-found. A provider timeout, a provider outage and the in-flight ceiling
-- all count as a miss and all refuse. That is the strict reading and it was
-- chosen deliberately, so be aware of the operational consequence: while an
-- HLR provider is down, an account set to 'reject' sends NOTHING, even though
-- its numbers may be perfectly valid. The engine logs the difference between
-- "the provider said no" and "we could not ask", so an outage is diagnosable
-- rather than mysterious.
--
-- DEFAULT 'proceed' keeps every existing account exactly as it is (PO's
-- original B-11 ruling: "adopt a config flag, default proceed"). The column
-- only bites for an account that opts in.
--
-- Sits beside enforce_hlr_db_check, the flag that decides whether HLR is
-- consulted at all — with that off, this column is never reached.

ALTER TABLE sms_service_config
  ADD COLUMN hlr_miss_policy ENUM('proceed','reject') NOT NULL DEFAULT 'proceed'
    COMMENT 'HLR could not identify the operator: send undetected, or refuse 403 HLR_UNRESOLVED';
