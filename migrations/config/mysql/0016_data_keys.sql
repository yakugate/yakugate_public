-- ============================================================================
-- 0016_data_keys.sql — envelope-encryption keyring (2026-07-20)
-- D-043 / [SCOPE P1-CRYPTO]. Data at rest is encrypted with a per-tenant-month
-- DATA key (DEK); the DEK itself is never stored in the clear — it is wrapped by
-- a KEY-ENCRYPTION key (KEK) that lives outside the database, resolved through
-- internal/secrets (YAKU_CRYPTO_KEK_REF).
--
-- Why per tenant-month: it bounds the blast radius of any single key, and makes
-- crypto-erasure cheap — revoking one account-month makes exactly that data
-- unreadable without touching anything else (set revoked=1; Decrypt then returns
-- ErrKeyRevoked). Deleting the row would also work but loses the audit trail.
--
-- key_ref format: dek:<account_id>:<YYYYMM>
-- kek_ref records WHICH KEK wrapped this DEK, so a KEK rotation can tell which
-- rows still need re-wrapping.
--
-- NOTE the wrapped DEK is binary (nonce||ciphertext), hence VARBINARY.
-- ============================================================================

CREATE TABLE data_keys (
  key_ref     VARCHAR(64)  NOT NULL PRIMARY KEY,
  wrapped_dek VARBINARY(512) NOT NULL,
  kek_ref     VARCHAR(128) NOT NULL,
  revoked     TINYINT(1)   NOT NULL DEFAULT 0,
  created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_kek (kek_ref),
  KEY idx_revoked (revoked)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
