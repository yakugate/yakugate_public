-- ============================================================================
-- 0003  Auth = x-api-key + JWT only (Basic/username-password removed) — CONFIG (MySQL)
-- Adds per-account auth_type and JWT verification config. Submit-API auth is now
-- either an opaque API key (api_tokens.token_hash) or a verified JWT bearer token.
-- ============================================================================
ALTER TABLE accounts
  ADD COLUMN auth_type      ENUM('apikey','jwt') NOT NULL DEFAULT 'apikey' AFTER password_hash,
  ADD COLUMN jwt_alg        VARCHAR(8)   NULL,        -- HS256 | RS256 | ES256 (allow-list; alg:none rejected)
  ADD COLUMN jwt_secret_ref VARCHAR(128) NULL,        -- KMS ref for the HS256 shared secret (never plaintext)
  ADD COLUMN jwt_public_key TEXT         NULL,        -- PEM public key for RS256/ES256
  ADD COLUMN jwks_uri       VARCHAR(512) NULL,        -- JWKS endpoint (alternative to jwt_public_key; cached)
  ADD COLUMN jwt_issuer     VARCHAR(190) NULL,        -- required `iss` claim
  ADD COLUMN jwt_audience   VARCHAR(190) NULL,        -- required `aud` claim
  ADD COLUMN jwt_sub_claim  VARCHAR(32)  NOT NULL DEFAULT 'sub',  -- claim that maps to the account (username/customer_id)
  ADD COLUMN jwt_leeway_sec INT          NOT NULL DEFAULT 60;     -- clock-skew tolerance (exp/nbf/iat)

-- DEPRECATED: HTTP Basic (username/password) auth is removed from the Submit API.
-- accounts.password_hash is retained ONLY for the future Admin Portal (Process 12)
-- login; it is no longer consulted on the message hot path.
