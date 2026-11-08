-- ============================================================================
-- 0128_dialect_kinds.sql — a vendor's inbound door reads its callbacks through
-- a DIALECT (2026-09-28, PO: "move them there, and let operators choose the
-- dialects ... tag dialects for mo, mt and dlr")
--
-- A dialect was one client's API shape: their field names, our reply in their
-- shape, where their credentials are. A vendor calling US has the same three
-- things the other way round, and until now they lived on each door
-- (inbound_endpoints.request_map / response_map / credential_places, config/
-- 0125 + 0127), mapped by hand per vendor even when ten vendors run one
-- aggregator platform. Now they are dialects, shared, and a door points at
-- one — as an account does.
--
--   kind  mt   a client's submit API — every dialect that existed before this
--              (request_map = [field…], response_map = our reply, auth_config
--              = token/username/password names); assigned to accounts
--         dlr  a vendor's delivery-receipt callback, assigned to DLR doors
--         mo   a vendor's incoming-message callback, assigned to MO doors
--
-- For dlr/mo the columns hold the callback shapes internal/callbackmap reads:
--   request_map   {list, fields{ours: their path}, status_map, time_format,
--                  time_zone, text_format}
--   response_map  {ok{status, content_type, body}, fail{…}}
--   auth_config   where each credential is: {token, jwt, username, password}
--                 → header:NAME | query:NAME | body:FIELD | path | any[:NAME]
--                   | bearer | basic   (internal/inboundauth/places.go)
-- url stays the shared default: a vendor calls /dlr/<cid> or /mo/<cid>.
--
-- The secrets stay on the door — each vendor's own token, JWT and password.
-- A dialect only says where they are and what the vendor's fields are called.
--
-- Every door that had a mapping or places gets a dialect of its own, named
-- "<cid> DLR (door N)" / "<cid> MO (door N)", and points at it. The door's
-- three columns are kept, like every retired column, and no longer read.
-- One column per ALTER (the bootstrap rule: a statement whose first column
-- already exists is skipped whole).
-- ============================================================================

ALTER TABLE sms_service_dialects ADD COLUMN kind ENUM('mt','dlr','mo') NOT NULL DEFAULT 'mt'
  COMMENT 'mt = a client submit API (accounts); dlr / mo = a vendor callback (inbound doors) — config/0128'
  AFTER name;

ALTER TABLE inbound_endpoints ADD COLUMN dialect_id BIGINT UNSIGNED NULL
  COMMENT 'the dlr/mo dialect this door reads callbacks with; NULL = the default reading (config/0128)'
  AFTER kind;

ALTER TABLE inbound_endpoints ADD CONSTRAINT fk_inbound_endpoints_dialect
  FOREIGN KEY (dialect_id) REFERENCES sms_service_dialects (id);

INSERT INTO sms_service_dialects (name, kind, request_map, response_map, auth_config)
SELECT CONCAT(v.cid, ' ', UPPER(e.kind), ' (door ', e.id, ')'), e.kind,
       e.request_map, e.response_map, e.credential_places
  FROM inbound_endpoints e
  JOIN vendors v ON v.id = e.vendor_id
 WHERE e.dialect_id IS NULL
   AND (e.request_map IS NOT NULL OR e.response_map IS NOT NULL OR e.credential_places IS NOT NULL)
   AND NOT EXISTS (SELECT 1 FROM sms_service_dialects d
                    WHERE d.name = CONCAT(v.cid, ' ', UPPER(e.kind), ' (door ', e.id, ')') AND d.kind = e.kind);

UPDATE inbound_endpoints e
  JOIN vendors v ON v.id = e.vendor_id
  JOIN sms_service_dialects d ON d.name = CONCAT(v.cid, ' ', UPPER(e.kind), ' (door ', e.id, ')') AND d.kind = e.kind
   SET e.dialect_id = d.id
 WHERE e.dialect_id IS NULL;

ALTER TABLE inbound_endpoints MODIFY COLUMN request_map JSON NULL
  COMMENT 'RETIRED 2026-09-28 (config/0128): moved to the door''s dialect (sms_service_dialects.request_map)';

ALTER TABLE inbound_endpoints MODIFY COLUMN response_map JSON NULL
  COMMENT 'RETIRED 2026-09-28 (config/0128): moved to the door''s dialect (sms_service_dialects.response_map)';

ALTER TABLE inbound_endpoints MODIFY COLUMN credential_places JSON NULL
  COMMENT 'RETIRED 2026-09-28 (config/0128): moved to the door''s dialect (sms_service_dialects.auth_config)';
