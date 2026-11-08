-- ============================================================================
-- 0152_inbound_custom_path.sql — a vendor's inbound door may answer on a path
-- of its own (issue 20, 2026-10-27; Saleem: "dialects for MO and DLR should
-- also have the custom endpoints feature, and the DLR and MO servers should
-- also support it")
--
-- A door answered /dlr/<cid> and /mo/<cid> only, so a vendor that can post
-- only to a URL of its own shape (/callback/status, /api/receipts.php …)
-- could not be served, and every vendor's URL showed our cid. Now a door may
-- carry its own path beside its default one, as an account carries its own
-- submit path. On the DOOR, not the dialect: a dialect is shared by several
-- vendors, and a path must name one vendor's door.
--
-- The rule is written twice — internal/inboundauth/paths.go (the console's
-- door, internal/provision, and what the dlr and mo roles load) and the Admin
-- API's services/inboundPlaces.js cleanCustomPath: "/" then letters, digits
-- and . _ ~ - between single slashes, at most 190 characters and 10 parts,
-- never under /dlr /mo /sms /messages /admin /status /metrics /healthz
-- /readyz, never another door's path (either kind) nor under one, never an
-- account's custom submit path. The key below holds the first of those
-- whatever writes the row. It compares as the column's collation does —
-- letter case ignored — and NULL (no path) may repeat.
--
-- One column per ALTER (the bootstrap rule).
-- ============================================================================

ALTER TABLE inbound_endpoints ADD COLUMN custom_path VARCHAR(190) NULL
  COMMENT 'a path of the door''s own beside /<kind>/<cid>, e.g. /acme/receipts; NULL = none (config/0152, issue 20)'
  AFTER dialect_id;

ALTER TABLE inbound_endpoints ADD UNIQUE KEY uq_inbound_custom_path (custom_path);
