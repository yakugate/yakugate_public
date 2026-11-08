-- Hadal reconciliation, deployments (PO 2026-07-27, Salman): schema-only
-- adoption of Hadal's `deployments` table — his own multi-instance ops
-- metadata (host, ports, per-component log level, SSL flag). Explicit PO
-- scoping: "lets adopt deployment for now at database level, but we will
-- develop deployments and scalability, in phase 2, once phase 1 is live" —
-- table lands now, nothing reads/writes it, no seed data (Phase 1 is
-- single-instance, so there is nothing yet to track). See D-073.
--
-- Same "our column conventions, his table name" rule as the rest of this
-- reconciliation: `has_ssl` -> `is_ssl` (matches `is_disabled`/`is_reseller`
-- elsewhere); `*_system_log_level` -> `*_log_level` (matches Process 13's
-- info/warning/error/debug/none vocabulary, D-072 — this is that same
-- vocabulary at the ops/instance level rather than per-account/per-vendor);
-- added `updated_at`/`created_by_user_id` for parity with every other config
-- table (Hadal's original has neither).
CREATE TABLE deployments (
  id                  BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  deployment_id       VARCHAR(255) NOT NULL,
  host                VARCHAR(255) NOT NULL,
  server_port         INT NOT NULL,
  client_port         INT NOT NULL,
  database_port       INT NOT NULL,
  server_log_level    VARCHAR(32) NOT NULL DEFAULT 'none',
  client_log_level    VARCHAR(32) NOT NULL DEFAULT 'none',
  database_log_level  VARCHAR(32) NOT NULL DEFAULT 'none',
  is_ssl              TINYINT(1) NOT NULL DEFAULT 1,
  created_by_user_id  VARCHAR(64) DEFAULT NULL,
  created_at          DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at          DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_deployments_deployment_id (deployment_id),
  UNIQUE KEY uq_deployments_host (host)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
