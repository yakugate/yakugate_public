-- Dissolve accounts + account_dialects, the CONTRACT half (PO 2026-07-28:
-- "can you delete the not required ones now?"). By this point nothing reads
-- or writes either table on either side: the Go engine's loader (config/0031
-- cutover) and the Node Admin API's entities (PO 2026-07-27 cutover) both
-- target sms_service_config exclusively. Confirmed live before this
-- migration was written: both tables held 0 rows, and the only FK in the
-- schema referencing them was account_dialects -> accounts itself (dropped
-- along with the table, nothing else pointed at either).
--
-- Also drops the six dialect-SHAPE columns sms_service_config carried
-- temporarily during the 0041 transition (url/url_exclusive/request_map/
-- response_map/auth_config/keep_unmapped) — now that they live on
-- sms_service_dialects (referenced via dialect_id) and nothing reads the
-- copies here (Go: internal/refdata/loader_mysql.go already reads via the
-- dialect_id join; Node: admin/src/db/entities.js's Account entity updated
-- in the same change to drop these five and add dialect_id, plus a new
-- Dialect entity mirroring sms_service_dialects). api_password/token_hash
-- are UNCHANGED — genuine per-account secrets, never moved.
--
-- NOT dropped this migration (separate, not requested): the known-stale
-- `sqa/e2e/custompath_e2e.py` harness still writes account_dialects directly
-- via raw SQL — it predates the 0031 cutover and was already broken by that
-- migration, not by this one. Flagged, not fixed, here.
-- url_exclusive is a generated column derived from url, so it must drop
-- first (a generated column cannot outlive the column it's generated from
-- being dropped in the same statement's evaluation order). dialect_id and
-- its FK (added in 0041) are untouched -- unrelated to these six columns.
ALTER TABLE sms_service_config
  DROP COLUMN url_exclusive,
  DROP COLUMN url,
  DROP COLUMN request_map,
  DROP COLUMN response_map,
  DROP COLUMN auth_config,
  DROP COLUMN keep_unmapped;

DROP TABLE IF EXISTS account_dialects;
DROP TABLE IF EXISTS accounts;
