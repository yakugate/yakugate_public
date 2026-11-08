-- Replace admin_users/admin_roles with users/roles; trim sms_service_dialects
-- (PO 2026-07-28, table-review Group 3): "delete these two tables. admin_users,
-- admin_roles) / delete url_exclusive, send_sms_cost, send_remaining_balance,
-- account_id / change password_hash to password."
--
-- Ownership-convention exception, acknowledged: admin/migrations/0003 already
-- established that users/roles/admin_users/admin_roles are NODE-owned tables
-- (applied via admin/src/db/migrate.js, not this Go runner) -- this migration
-- deliberately breaks that split anyway, for one reason: it needs to DROP
-- admin_users/admin_roles in the SAME transaction-ordered sequence as backfilling
-- sms_service_config.user_id FROM admin_users. Two separate migration runners
-- (Go's bootstrap.Ensure on gateway startup, Node's migrate.js on admin/index.js
-- startup) have no guaranteed relative ordering across processes -- splitting
-- this into "Go backfills user_id" + "Node drops admin_users later" risks the
-- Node side running first and leaving the Go side with nothing to backfill
-- from. Doing both here, once, atomically, avoids that hazard. admin/migrations/
-- 0003's own backfill INTO users/roles ran once already, against a database
-- that had zero rows in admin_users/admin_roles at the time (before the
-- bootstrap admin + demo users existed) -- it is a permanent no-op now, which
-- is why this migration re-does that copy itself, from the real current data.
--
-- admin_users/admin_roles were the ORIGINAL Node Admin API auth tables, live
-- and in active use (bootstrap admin login + 2 real portal users). users/roles
-- are the newer Hadal-reconciled pair -- confirmed empty before this migration
-- (nothing had ever written to them). This migration is the actual cutover:
-- copy the live data across (explicit ids, so users.role_id keeps resolving
-- correctly against the freshly-copied roles rows) before dropping the old
-- pair, so the real admin@yakugate.local login (and the two seeded customer
-- logins) keep working.
--
-- sms_service_config.user_id was set NULL for every row by migration 0045
-- (no honest mapping existed at the time). admin_users.account_id WAS exactly
-- that mapping, just in the reverse direction -- recovered here, one
-- migration late, before the source table disappears.
--
-- users.account_id dropped outright: the relationship is one-directional now
-- (sms_service_config.user_id -> users.user_id), so resolving "which account
-- does this login belong to" means querying sms_service_config by user_id,
-- not reading a stored column on the user row. The Admin API's tenant-scoping
-- code (internal/http/security.js `authenticate()`) now does exactly that
-- live lookup instead of trusting a stored/JWT-carried account_id.
--
-- url_exclusive dropped from sms_service_dialects: this ALSO removes the
-- DB-enforced uniqueness on a shared dialect's custom path (it was a
-- generated column carrying the UNIQUE key). No code depends on the
-- constraint being DB-enforced today (refdata's DialectForPath already
-- tolerates a same-path collision as a "defensive, last-write-wins" case per
-- its own comment) -- but a genuine duplicate custom path across two dialect
-- rows is now possible without an error. Flagged, not silently absorbed.
--
-- send_sms_cost/send_remaining_balance dropped outright (not moved anywhere):
-- the accept-time cost/balance disclosure (S-05) loses its per-account/
-- per-dialect toggle entirely. internal/refdata now treats it as always-on
-- for every account, matching these columns' original DEFAULT 1 -- there is
-- no longer any way to turn it off. Flagged as a real feature-configurability
-- loss, not a silent one.
-- roles/users must be populated BEFORE the sms_service_config.user_id backfill
-- below — that column has an FK to users.user_id (migration 0045), which
-- would reject any value that doesn't exist in users yet.
INSERT INTO roles (id, name, description, mappings, scope, created_by_user_id, created_at, updated_at)
SELECT id, name, description, mappings, scope, created_by_user_id, created_at, updated_at
FROM admin_roles;

INSERT INTO users (id, user_id, email, name, password_hash, role_id, is_active,
                    last_login_at, failed_logins, locked_until, created_by_user_id,
                    created_at, updated_at)
SELECT id, user_id, email, name, password_hash, role_id, is_active,
       last_login_at, failed_logins, locked_until, created_by_user_id,
       created_at, updated_at
FROM admin_users;

UPDATE sms_service_config s
JOIN admin_users au ON au.account_id = s.id
SET s.user_id = au.user_id
WHERE s.user_id IS NULL;

DROP TABLE admin_users;
DROP TABLE admin_roles;

ALTER TABLE users
  DROP COLUMN account_id,
  CHANGE COLUMN password_hash password VARCHAR(100) NOT NULL;

ALTER TABLE sms_service_dialects
  DROP COLUMN url_exclusive,
  DROP COLUMN send_sms_cost,
  DROP COLUMN send_remaining_balance;
