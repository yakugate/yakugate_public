-- ============================================================================
-- 0123_throughput_per_deployment.sql — throughput is always per deployment
--
-- PO 2026-09-24, following config/0122: "deployment selection is mandatory,
-- add an option of all deployments if you want, throughputs are always
-- deployment based, also show the total throughput set to an account or
-- vendor."
--
-- So `sms_service_config.deployments[].mt/mo/dlr_throughput` becomes the ONLY
-- place a rate is configured, and the three account-level columns are retired
-- as configuration. The TOTAL an operator is quoted is the sum across the
-- deployments an account is assigned to; what is enforced on each box is that
-- box's own entry.
--
-- The columns are NOT dropped. A dropped column cannot be rolled back, cannot
-- be read by a binary from last week, and takes its history with it — and
-- this migration has to be safe on a running gateway. They stop being READ
-- (internal/refdata no longer loads them into the rate limiter) and stop
-- being offered on any screen. mt_throughput additionally keeps its old value
-- as the record of what the account used to be allowed.
--
-- ----------------------------------------------------------------------------
-- The one thing that could not simply be retired: the abuse monitor.
--
-- P10's throttle action cut `mt_throughput` to a percentage to make an abusive
-- account hurt, and the rate-limit stage read that column back. Stop reading
-- the column and the throttle silently becomes a no-op — the worst kind of
-- security regression, because the audit trail still says "throttled".
--
-- It cannot write the per-deployment entries instead: an account assigned to
-- ALL deployments has no entry to cut (the All row carries no rate, by the
-- same ruling), so there would be nothing to write for exactly the accounts
-- most likely to be abusing.
--
-- So enforcement gets its own field, which is the honest shape anyway:
-- abuse_throttle_tps is not configuration an operator sets, it is a penalty
-- the gateway applies. It caps the account gateway-wide, across every
-- deployment, on top of whatever the per-deployment entries allow, and only
-- an operator clears it (P10 only ever downgrades — that rule is unchanged).
-- ============================================================================

ALTER TABLE sms_service_config
  ADD COLUMN abuse_throttle_tps INT NULL
    COMMENT 'Abuse monitor penalty: segments/sec cap across every deployment. NULL = none. Set by P10, cleared only by an operator';

-- The three retired columns keep their data and gain a comment saying so, in
-- case somebody reads the schema before they read this file.
ALTER TABLE sms_service_config
  MODIFY mt_throughput INT NOT NULL DEFAULT 0
    COMMENT 'RETIRED config/0123 — throughput is per deployment (deployments[].mt_throughput). Kept for history; not read';

ALTER TABLE sms_service_config
  MODIFY mo_throughput INT NOT NULL DEFAULT 0
    COMMENT 'RETIRED config/0123 — see deployments[].mo_throughput. Kept for history; not read';

ALTER TABLE sms_service_config
  MODIFY dlr_throughput INT NOT NULL DEFAULT 0
    COMMENT 'RETIRED config/0123 — see deployments[].dlr_throughput. Kept for history; not read';
