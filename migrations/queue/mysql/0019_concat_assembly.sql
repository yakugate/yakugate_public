-- 0019_concat_assembly.sql — SMPP multipart assemble-first buffer (D-096,
-- PO 2026-08-04: "first we will collect all segments from smpp, then we will
-- run rules that required message in one place" + "it must reside into the
-- db, as multiple smpp binds may be transmitting segments").
--
-- Each arriving concat part is acked immediately after a durable INSERT here
-- (Q3 ruling) and held until its group completes; the assembled whole message
-- then runs the pipeline ONCE. Incomplete groups past the timeout are
-- released as individual parts through the legacy per-part path.
--
-- group_key = <account_id>|<src>|<dst>|<ref>|<total> — the SMPP-standard
-- group identity, account-scoped so tenants can never collide on the 8-bit ref.
CREATE TABLE IF NOT EXISTS concat_assembly (
  id          BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
  account_id  BIGINT       NOT NULL,
  group_key   VARCHAR(191) NOT NULL,
  seq         INT          NOT NULL,
  total       INT          NOT NULL,
  payload     JSON         NOT NULL,
  received_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_group_seq (group_key, seq),
  KEY idx_age (received_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
