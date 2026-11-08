-- ============================================================================
-- 0121_inbound_endpoints.sql — inbound vendor callbacks get an account's own
-- security (2026-09-23, PO: "same auth management system as for accounts")
--
-- What a vendor calling US could prove until now (vendors.inbound_config):
-- a secret in the URL, a secret in a header, an IP allow-list. Three flat
-- values, shared by both callback kinds, with no rate limit, no lockout, no
-- rotation and no audit of their own — while a CLIENT calling us has JWT with
-- a key-rotation window, bcrypt passwords, per-account throughput, an IP
-- allow-list, an escalating lockout ladder, a credential-spray guard and a
-- security event for every refusal.
--
-- An inbound callback deserves the stricter half of that, not the looser: a
-- forged receipt rewrites a real message's outcome, and a forged MO is BILLED,
-- reaches the customer's log as genuine and can trigger their auto-reply into
-- sending real traffic.
--
-- So: one row per vendor PER KIND (PO 2026-09-23) — receipts and inbound
-- messages are separate doors, with separate credentials, separate limits and
-- separate switches. A vendor can carry a token for one and a JWT for the
-- other, and MO can be closed without touching receipts.
--
-- Credentials mirror the account columns one for one, deliberately: the same
-- shapes, read by the same verifier (internal/auth), so there is one answer in
-- this gateway to "is this JWT good" and one to "is this password right".
--
--   jwt          {alg, secret_ref, secret_ref_prev, public_key, sub_claim,
--                 require_exp, max_lifetime_sec, issuer, audience, leeway_sec}
--   token_hash   HMAC-SHA256 of the token under YAKU_TOKEN_PEPPER, like
--                accounts.token_hash — never the token itself
--   username +   bcrypt, like accounts.password, verified through the same
--   password     cache so a busy vendor does not pay bcrypt per callback
--   hmac         {alg, secret_ref, header, prefix, tolerance_sec} — the vendor
--                signs the body; the only scheme here that stops a replay
--
-- And the account-grade controls around them:
--
--   allowed_ip_list  addresses/CIDRs, resolved through the same trusted-proxy
--                    rule as the submit edge
--   max_throughput   calls a second for this door, 0 = no limit
--   is_disabled      close one door without deleting its configuration
--
-- Every existing vendors.inbound_config is copied into BOTH kinds below, so
-- nothing configured today stops working the moment this lands.
-- ============================================================================

CREATE TABLE IF NOT EXISTS inbound_endpoints (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  vendor_id     BIGINT UNSIGNED NOT NULL,
  kind          ENUM('dlr','mo') NOT NULL,
  name          VARCHAR(100)    NULL,
  is_disabled   TINYINT(1)      NOT NULL DEFAULT 0,

  -- credentials (every one optional; every one CONFIGURED is enforced)
  jwt           JSON            NULL,
  token_hash    VARCHAR(128)    NULL COMMENT 'HMAC-SHA256 of the callback token under YAKU_TOKEN_PEPPER',
  token_source  VARCHAR(16)     NOT NULL DEFAULT 'any' COMMENT 'where the token may be read: any|header|query|path',
  token_header  VARCHAR(64)     NULL COMMENT 'header/query name carrying the token; empty = the gateway default',
  username      VARCHAR(64)     NULL,
  password      VARCHAR(100)    NULL COMMENT 'bcrypt hash, like an account password',
  hmac_config   JSON            NULL COMMENT '{alg, secret_ref, header, prefix, tolerance_sec} — signed-body verification',

  -- the same controls an account has
  allowed_ip_list JSON          NULL,
  max_throughput  INT UNSIGNED  NOT NULL DEFAULT 0 COMMENT 'callbacks a second for this door; 0 = no limit',

  created_at    DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_inbound_vendor_kind (vendor_id, kind),
  KEY idx_inbound_vendor (vendor_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Carry every vendor's current inbound_config into both doors. The URL token
-- is copied as a HASH placeholder of nothing — it cannot be hashed in SQL
-- under the server pepper — so the legacy JSON stays authoritative until an
-- operator sets a real credential here: the engine reads a row first and falls
-- back to vendors.inbound_config while the row has no credential of its own.
-- That is what makes this migration safe to apply to a live gateway.
INSERT INTO inbound_endpoints (vendor_id, kind, name, allowed_ip_list)
SELECT v.id, k.kind,
       CONCAT(v.cid, ' ', k.kind),
       JSON_EXTRACT(v.inbound_config, '$.ip_allow')
  FROM vendors v
  JOIN (SELECT 'dlr' AS kind UNION ALL SELECT 'mo') k
 WHERE NOT EXISTS (
       SELECT 1 FROM inbound_endpoints e WHERE e.vendor_id = v.id AND e.kind = k.kind);
