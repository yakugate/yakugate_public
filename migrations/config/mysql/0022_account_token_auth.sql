-- ============================================================================
-- 0022_account_token_auth.sql — per-account opaque static-token auth (2026-07-23)
--
-- PO decision 2026-07-23: a migrating legacy client whose credential is
-- neither a JWT nor a username+password pair -- one opaque value, in a
-- header/query/body location and under a key name THEY chose, not us (e.g.
-- header "X-Custom-Auth", or an XML tag "ApiKey" inside the body).
--
-- auth_config carries WHERE to look (dialect.AuthConfig: scheme/token_key/
-- token_source) -- the same per-account-dialect-JSON pattern request_map/
-- response_map already use, so internal/dialect stays free of any database
-- dependency (loader_mysql.go round-trips this column through dialect.Parse
-- exactly like it already does for request_map/response_map).
--
-- token_hash is NOT bcrypt. Bcrypt is salted and deliberately non-
-- deterministic, so it cannot be looked up BY VALUE -- only compared against
-- one already-known row (which is exactly how api_password/username auth
-- works: the username finds the row, bcrypt only verifies it). An opaque
-- token has no separate "username half" to find it by, so token_hash instead
-- stores a DETERMINISTIC keyed digest (HMAC-SHA256 with a server-side pepper,
-- internal/auth's tokenPepper -- never stored in this table, env-configured
-- only) of the raw token. Deterministic + high-entropy input is the standard
-- API-key pattern (same approach Stripe/GitHub/AWS use for their own keys) --
-- it's what makes the in-memory refdata.Mem.byTokenHash map a direct O(1)
-- lookup instead of a per-row compare loop.
-- ============================================================================

ALTER TABLE account_dialects
  ADD COLUMN auth_config JSON NULL AFTER response_map,
  ADD COLUMN token_hash  CHAR(64) NULL AFTER auth_config,  -- hex HMAC-SHA256, or NULL = token scheme disabled
  ADD KEY idx_token_hash (token_hash);
