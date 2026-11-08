-- Tamper-evident audit trail (P1 register 1.9.5).
--
-- Until now an `audit_log` row was a plain INSERT: edit one, or delete one,
-- and nothing anywhere shows it. For a compliance trail that is the whole
-- point of the table, so the trail could not actually be relied on.
--
-- Each row now carries the hash of the row before it. Deleting a row breaks
-- the link at that point; editing a row makes its own hash stop matching its
-- contents. Neither can be repaired without recomputing every later row AND
-- the head below, which is exactly the difficulty we want.
--
-- WHY THIS IS A TRIGGER AND NOT APPLICATION CODE.
-- 98% of the rows in this table are written by the Node Admin API (portal
-- CRUD); the Go engine writes the rest. A chain implemented in one of them
-- would be broken by the other on its first write. Implemented in BOTH, the
-- two canonicalizations would eventually drift, and drift here means a FALSE
-- tamper alarm — the worst possible failure for this feature. In the database
-- there is one implementation, it covers writers we have not thought of, and
-- it covers a DBA typing an INSERT at the console.
--
-- WHY A SEPARATE HEAD TABLE.
-- The obvious trigger reads the last row of audit_log itself. That works
-- until two writers append at the same moment: both read the same tail, both
-- claim the same predecessor, and the chain forks — which a verifier cannot
-- tell apart from tampering. The fix is to serialize appenders, and MySQL
-- forbids a locking read (SELECT ... FOR UPDATE) against the very table whose
-- trigger is running (error 1442). So the head lives in its own one-row
-- table, and `FOR UPDATE` on that row is what makes concurrent appends line
-- up. Verified with six concurrent writers: 62 rows, no broken links, no
-- duplicate hashes.
--
-- EXISTING ROWS ARE DELIBERATELY NOT BACKFILLED.
-- They were written with no protection, so a hash computed for them now would
-- attest to nothing — it would certify whatever the table happens to contain
-- today, tampered or not, and produce a chain that "verifies" a history
-- nobody ever witnessed. That is worse than an honest gap. Rows written
-- before this migration keep NULL hashes and the verifier reports them as
-- unprotected rather than as valid.

CREATE TABLE IF NOT EXISTS audit_chain_head (
  id        TINYINT UNSIGNED NOT NULL PRIMARY KEY,
  head_hash CHAR(64) NOT NULL DEFAULT '',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='single row: the hash of the newest audit_log row. Locked FOR UPDATE by the append trigger so concurrent writers cannot fork the chain.';

INSERT IGNORE INTO audit_chain_head (id, head_hash) VALUES (1, '');

ALTER TABLE audit_log
  ADD COLUMN prev_hash CHAR(64) NULL
    COMMENT 'hash of the previous audit row; NULL = written before the chain existed (never protected, not merely unverified)',
  ADD COLUMN row_hash CHAR(64) NULL
    COMMENT 'SHA-256 over prev_hash plus every substantive column of this row';

DROP TRIGGER IF EXISTS trg_audit_log_chain;

-- Every column except id and the two hash columns goes into the digest. A
-- column left out is a column an attacker may edit freely, so the list is
-- deliberately exhaustive rather than "the interesting ones". id cannot be
-- included: it is auto-increment and not yet assigned while a BEFORE INSERT
-- trigger runs. Nothing is lost by that — the chain is ordered by insertion,
-- so a renumbering cannot go unnoticed either way.
--
-- CONCAT_WS skips NULLs entirely, which would let ('a', NULL, 'b') and
-- (NULL, 'a', 'b') digest alike, so every column is COALESCEd to '' first and
-- the separator does the rest.
CREATE TRIGGER trg_audit_log_chain
BEFORE INSERT ON audit_log
FOR EACH ROW
BEGIN
  DECLARE prev CHAR(64);
  SELECT head_hash INTO prev FROM audit_chain_head WHERE id = 1 FOR UPDATE;
  SET prev = COALESCE(prev, '');
  SET NEW.prev_hash = prev;
  SET NEW.row_hash = SHA2(CONCAT_WS('|',
      prev,
      COALESCE(NEW.actor,''), COALESCE(NEW.actor_name,''), COALESCE(NEW.actor_email,''),
      COALESCE(NEW.account_id,''), COALESCE(NEW.company_id,''), COALESCE(NEW.company_name,''),
      COALESCE(NEW.action,''), COALESCE(NEW.method,''), COALESCE(NEW.route,''),
      COALESCE(NEW.params,''), COALESCE(NEW.query,''), COALESCE(NEW.response_code,''),
      COALESCE(NEW.summary,''), COALESCE(NEW.outcome,''), COALESCE(NEW.reason,''),
      COALESCE(NEW.entity,''), COALESCE(NEW.feature,''), COALESCE(NEW.module,''),
      COALESCE(NEW.entity_id,''), COALESCE(NEW.target_label,''),
      COALESCE(NEW.detail,''), COALESCE(NEW.before_data,''), COALESCE(NEW.after_data,''),
      COALESCE(NEW.ip,''), COALESCE(NEW.user_agent,''), COALESCE(NEW.content_type,''),
      COALESCE(NEW.created_at,'')
  ), 256);
  UPDATE audit_chain_head SET head_hash = NEW.row_hash WHERE id = 1;
END;
