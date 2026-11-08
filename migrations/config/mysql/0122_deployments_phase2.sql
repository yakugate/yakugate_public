-- ============================================================================
-- 0122_deployments_phase2.sql — deployments become real (PO 2026-09-24)
--
-- config/0040 adopted Hadal's `deployments` table schema-only, with the PO's
-- own scoping: "lets adopt deployment for now at database level, but we will
-- develop deployments and scalability, in phase 2, once phase 1 is live"
-- (D-073). This is that phase 2, at gateway and portal level only — there is
-- deliberately NO load balancer / front-door process in it. The register
-- records what SHOULD be running where; nothing acts on a deployment.
--
-- A DEPLOYMENT is the set of role processes reporting the same
-- YAKU_DEPLOYMENT_ID (internal/monitor/deployment.go, already pushed in every
-- monitoring snapshot). Hosts are a detail inside it. That is why the engine
-- never reads this table: it knows its own id from its own settings file, and
-- must keep running with no Admin API at all (PO 2026-09-18).
--
-- Three fixes to 0040's Hadal shape, which assumed his three services on one
-- host and cannot describe ours:
--
--   1. UNIQUE (host) is dropped. It forbids a deployment spanning several
--      hosts — our normal case, fifteen roles spread over a node group — AND
--      forbids two deployments on one box, which is every dev machine.
--   2. server_port/client_port/database_port become optional. They name
--      Hadal's three services; ours are per ROLE and live in deployment_roles.
--   3. A deployment gains the things an operator needs on a screen: a name,
--      an environment, a site, its public base URL (what a vendor is told to
--      call for /dlr/<cid>) and an off switch.
--
-- Assignment mirrors Hadal exactly — an account and a vendor each carry a
-- JSON list of the deployments they live on, with that deployment's own
-- throughput. Keys are snake_case like every other JSON config we own
-- (receipt_policy, dlr_config), not Hadal's camelCase.
--
--   [{"deployment_id":"lhr-1","mt_throughput":100,
--     "mo_throughput":50,"dlr_throughput":50}, ...]
--
-- NULL or [] means EVERYWHERE. That is what makes this migration safe on a
-- live gateway: every existing account and vendor keeps working on every
-- deployment until somebody deliberately narrows it.
-- ============================================================================

-- 1. The register itself ------------------------------------------------------

ALTER TABLE deployments DROP INDEX uq_deployments_host;

ALTER TABLE deployments
  MODIFY server_port   INT NOT NULL DEFAULT 0,
  MODIFY client_port   INT NOT NULL DEFAULT 0,
  MODIFY database_port INT NOT NULL DEFAULT 0;

-- ONE COLUMN PER STATEMENT, deliberately. The bootstrap treats an ALTER that
-- fails because its column is already there (MySQL 1060) as satisfied and
-- moves on — which is what lets an engine-owned migration re-state a shape
-- something else already applied. Put six ADDs in one statement and a single
-- pre-existing column makes the whole thing "satisfied", silently losing the
-- other five. Six statements, six verdicts.

ALTER TABLE deployments
  ADD COLUMN name VARCHAR(190) NULL
      COMMENT 'What an operator calls it on screen; deployment_id is the machine key'
      AFTER deployment_id;

ALTER TABLE deployments
  ADD COLUMN env VARCHAR(32) NULL
      COMMENT 'production|staging|… — mirrors YAKU_ENV, so a screen can never confuse the two'
      AFTER name;

ALTER TABLE deployments
  ADD COLUMN site VARCHAR(190) NULL
      COMMENT 'Data centre / region, for grouping only'
      AFTER env;

ALTER TABLE deployments
  ADD COLUMN base_url VARCHAR(255) NULL
      COMMENT 'Public base a VENDOR is given for /dlr/<cid> and /mo/<cid> on THIS deployment';

ALTER TABLE deployments
  ADD COLUMN is_disabled TINYINT(1) NOT NULL DEFAULT 0
      COMMENT 'Retired/parked: kept for its history, excluded from assignment pickers';

ALTER TABLE deployments
  ADD COLUMN notes TEXT NULL;

-- 2. Which roles are MEANT to run there ---------------------------------------
--
-- Without this the register can say a host is quiet but not WHAT is missing.
-- Drift is "billing is expected on lhr-1 and nothing has reported for five
-- minutes", and that sentence needs a row per role.
--
-- Keyed by the STRING deployment_id, not deployments.id, for the same reason
-- the engine never reads the register: the id an operator sets in sms.config
-- is the identity, and a row here may legitimately describe a deployment that
-- has not been registered yet.

CREATE TABLE IF NOT EXISTS deployment_roles (
  id            BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  deployment_id VARCHAR(255) NOT NULL,
  role          VARCHAR(32)  NOT NULL
                COMMENT 'One of the engine roles: http-server, smpp-vendor, billing, …',
  instances     SMALLINT UNSIGNED NOT NULL DEFAULT 1
                COMMENT 'How many processes of this role should be reporting',
  admin_addr    VARCHAR(255) NULL
                COMMENT 'host:port of this role admin listener, for monitoring and captures',
  log_level     VARCHAR(32)  NULL
                COMMENT 'Intended level (info|warning|error|debug|none); sms.config remains the truth',
  is_singleton  TINYINT(1) NOT NULL DEFAULT 0
                COMMENT 'Gateway-wide role that must run on ONE deployment only (billing, retention, backup, abuse-monitor)',
  created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_deployment_role (deployment_id, role)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 3. Assignment ---------------------------------------------------------------

ALTER TABLE sms_service_config
  ADD COLUMN deployments JSON NULL
      COMMENT 'Deployments this account may submit to, with per-deployment throughput. NULL/[] = every deployment';

ALTER TABLE vendors
  ADD COLUMN deployments JSON NULL
      COMMENT 'Deployments whose vendor roles claim this vendor queue and hold its bind. NULL/[] = every deployment';
