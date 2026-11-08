-- 0003  JWT revocation blocklist — QUEUE / HLR store (MySQL). Hot-path check (cached).
-- A JWT is rejected if its `jti` is present here (and not yet past expires_at).
CREATE TABLE jwt_revocations (
  jti        VARCHAR(128) PRIMARY KEY,               -- JWT ID claim
  account_id BIGINT UNSIGNED,
  expires_at DATETIME NOT NULL,                      -- token's own exp; row prunable after this
  revoked_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  reason     VARCHAR(64),
  KEY idx_expires (expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
