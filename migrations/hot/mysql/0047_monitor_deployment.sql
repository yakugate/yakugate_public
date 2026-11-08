-- ============================================================================
-- 0047_monitor_deployment.sql — group monitoring by deployment (PO 2026-09-24)
--
-- Every snapshot already carries the deployment block (internal/monitor
-- .Deployment: id, host, env, version, ports). It rides inside `body`, which
-- is exactly where the columns of this table are lifted OUT of so the common
-- dashboard queries never have to open it. "Show me this deployment" is a
-- common dashboard query, so its key belongs beside the others.
--
-- One column, one index, and the whole per-deployment view is a WHERE clause
-- on a table the monitoring screens already read. Nothing new is collected:
-- the roles have been reporting this since the deployment block was added.
--
-- We keep PUSH, deliberately. The reference gateway polls each deployment's
-- hosts over HTTP and aggregates the answers, which makes a dead deployment a
-- hanging request — the dashboard is slowest at the exact moment it matters.
-- A deployment that has stopped pushing simply goes stale here, instantly and
-- without blocking anything.
-- ============================================================================
ALTER TABLE monitor_samples
  ADD COLUMN deployment_id VARCHAR(64) NULL
    COMMENT 'YAKU_DEPLOYMENT_ID of the reporting process; NULL = reported before this';

ALTER TABLE monitor_samples
  ADD KEY idx_ms_deployment (deployment_id, role, sampled_at);
