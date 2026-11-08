-- ============================================================================
-- 0046_log_deployment.sql — which gateway handled this message (PO 2026-09-24)
--
-- With more than one deployment, "where did this happen?" is the first
-- question of every incident and the logs could not answer it. The portal has
-- expected the field for a long time: p13/src/config/Constants.js already
-- offers "Deployment ID" as a search filter on the message, incoming and
-- rejected log screens.
--
-- MT carries TWO, because ours can genuinely differ and Hadal's cannot. A
-- message accepted at one deployment is sent by whichever deployment holds
-- that carrier bind — that separation is the whole point of assigning vendors
-- to deployments. One column would force a false choice:
--
--   deployment_id       — who ACCEPTED it (the submit landed here)
--   sent_deployment_id  — who SENT it (this box held the vendor connection)
--
-- Without the second, "is the far site's carrier link the slow one?" is
-- unanswerable, which is exactly the question a split deployment creates.
-- Receipts, MO and rejects have one side only.
--
-- VARCHAR(64), not the register's VARCHAR(255): this lands on every row of
-- the hottest tables we own, at 4,000/s. Sixty-four is a generous name and
-- the Admin API refuses anything longer when the register is written.
--
-- Deliberately NOT indexed. Every log screen already filters by account and
-- day, and the partition IS the account-day, so a scan inside one day's table
-- is cheap. An index here costs write throughput on the hot path for a filter
-- that is always applied second. Add one only if it proves needed.
--
-- Day tables made from now on have the columns. Tables already cloned from
-- the template get them from the store on first write — the same self-repair
-- as mtlog.sent_parts (hot/0043) and dlrlog.stat (hot/0045). NULL means a row
-- written before this, or a gateway that never set YAKU_DEPLOYMENT_ID.
-- ============================================================================

-- ONE COLUMN PER STATEMENT, deliberately.
--
-- The bootstrap treats an ALTER that fails because its column is already
-- there as satisfied, and moves on — which is what lets an engine-owned
-- migration re-state a shape the Admin API applied years ago. Combined with
-- the store's self-repair that is a trap: the first write after this file
-- lands adds deployment_id on its own, this ALTER then fails on THAT column,
-- the whole statement is declared satisfied, and sent_deployment_id is never
-- added at all. Two statements, two verdicts.

ALTER TABLE mtlog_template
  ADD COLUMN deployment_id VARCHAR(64) NULL
      COMMENT 'YAKU_DEPLOYMENT_ID of the gateway that ACCEPTED the submit';

ALTER TABLE mtlog_template
  ADD COLUMN sent_deployment_id VARCHAR(64) NULL
      COMMENT 'YAKU_DEPLOYMENT_ID of the gateway that sent it to the vendor';

ALTER TABLE dlrlog_template
  ADD COLUMN deployment_id VARCHAR(64) NULL
      COMMENT 'YAKU_DEPLOYMENT_ID of the gateway that received this receipt';

ALTER TABLE mologtemplate
  ADD COLUMN deployment_id VARCHAR(64) NULL
      COMMENT 'YAKU_DEPLOYMENT_ID of the gateway that received this MO';

ALTER TABLE rejects_template
  ADD COLUMN deployment_id VARCHAR(64) NULL
      COMMENT 'YAKU_DEPLOYMENT_ID of the gateway that refused the submit';
