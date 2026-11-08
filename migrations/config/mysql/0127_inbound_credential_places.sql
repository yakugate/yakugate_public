-- ============================================================================
-- 0127_inbound_credential_places.sql — where each inbound credential is, and
-- the signature scheme RETIRED (2026-09-28, PO: "in inbound we don't need
-- signature ... they can send any value at any place of http, header, body,
-- query")
--
-- credential_places says, per door, where in the vendor's callback each
-- credential is read from:
--
--   {"token":"header:X-Api-Key", "jwt":"query:access_token",
--    "username":"body:auth.user", "password":"body:auth.pass"}
--
--   header:NAME | query:NAME | body:FIELD (JSON / XML / form, dot path)
--   path, any[:NAME]   token only (the URL segment; header→query→path)
--   bearer             JWT only (Authorization: Bearer)
--   basic              username and password together (HTTP Basic)
--
-- NULL, or a credential left out, keeps the reading every door had before:
-- the token by token_source + token_header, a JWT from Authorization: Bearer
-- or the token header, username/password from Basic auth or the query.
-- internal/inboundauth/places.go is the engine's reading; the Admin API's
-- services/inboundPlaces.js checks the same rules.
--
-- hmac_config is no longer read by the engine or offered by the Admin API.
-- A door that was guarded ONLY by a signature is now unguarded, which means
-- REFUSED ("nothing configured means refused"), never open. The column is
-- kept, like every retired column.
-- ============================================================================

ALTER TABLE inbound_endpoints ADD COLUMN credential_places JSON NULL
  COMMENT 'where each credential is read: {token,jwt,username,password} -> header:NAME | query:NAME | body:FIELD | path | any[:NAME] | bearer | basic (config/0127)'
  AFTER password;

ALTER TABLE inbound_endpoints MODIFY COLUMN hmac_config JSON NULL
  COMMENT 'RETIRED 2026-09-28 (config/0127): the signature scheme is no longer read or offered';
