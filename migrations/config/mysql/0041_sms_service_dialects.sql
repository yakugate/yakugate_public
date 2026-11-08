-- Dissolve accounts + account_dialects, part 2 (PO 2026-07-28): "we will
-- dissolve two tables, accounts and account_dialects. make sure everything
-- is available in sms_service_config and sms_service_dialects. new dialects
-- table will have only url related stuff and not the credentials. and
-- dialects will be referenced in config to which one to use to avoid
-- repetition, we may use one dialect in multiple sms config."
--
-- sms_service_config already carries every accounts/account_dialects field
-- (migration 0031) -- every accounts field maps 1:1 except throughput, which
-- was already split mt/mo/dlr there. This migration is the second half:
-- splitting the per-account DIALECT columns (url/url_exclusive/request_map/
-- response_map/auth_config/keep_unmapped) OUT into a new, shareable table,
-- so N accounts using the same third-party API shape (e.g. a reseller
-- migrating 50 clients off one legacy gateway, all with identical field
-- mapping and wire format) reference ONE row instead of repeating identical
-- JSON blobs N times -- the actual "avoid repetition" ask.
--
-- api_password/token_hash stay on sms_service_config -- genuinely per-account
-- secrets that can never be shared, exactly the split the PO asked for
-- ("url related stuff... not the credentials"). Only url and the four
-- shape/mapping columns move; nothing else on sms_service_config changes.
--
-- dialect_id NULL = native contract, no shared row needed at all -- actually
-- SIMPLER than today's "every account gets an explicit row" model
-- (config/0021), since a plain native account now needs zero dialect rows
-- rather than one redundant one.
--
-- Expand, not contract: `accounts`/`account_dialects` and the old dialect
-- columns on sms_service_config are left in place by this migration. Dropping
-- them is a follow-up once the Admin API's own account-management code (not
-- audited this session) is confirmed to not read them directly -- the same
-- expand/contract discipline as every other Hadal-reconciliation migration
-- in this series.
CREATE TABLE sms_service_dialects (
  id            BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  name          VARCHAR(128) DEFAULT NULL,          -- human label, e.g. "jazz-xml-v1"; not an identity key
  url           VARCHAR(190) NOT NULL DEFAULT '/sms/v4/send',
  url_exclusive VARCHAR(190) GENERATED ALWAYS AS (NULLIF(url, '/sms/v4/send')) STORED,
  request_map   JSON DEFAULT NULL,
  response_map  JSON DEFAULT NULL,
  auth_config   JSON DEFAULT NULL,
  keep_unmapped TINYINT(1) DEFAULT NULL,
  created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_ssd_url_exclusive (url_exclusive)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

ALTER TABLE sms_service_config
  ADD COLUMN dialect_id BIGINT UNSIGNED DEFAULT NULL AFTER token_hash,
  ADD KEY idx_ssc_dialect (dialect_id),
  ADD CONSTRAINT fk_ssc_dialect FOREIGN KEY (dialect_id) REFERENCES sms_service_dialects(id);

-- Backfill: one sms_service_dialects row per DISTINCT non-native shape
-- already on sms_service_config, then point every matching row's dialect_id
-- at it -- this is the real de-duplication, not just a schema add. A row
-- whose shape is the plain native default (url='/sms/v4/send' and every
-- other dialect field NULL) gets dialect_id=NULL; no row is created for it.
INSERT INTO sms_service_dialects (url, request_map, response_map, auth_config, keep_unmapped)
SELECT DISTINCT url, request_map, response_map, auth_config, keep_unmapped
FROM sms_service_config
WHERE NOT (url = '/sms/v4/send' AND request_map IS NULL AND response_map IS NULL
           AND auth_config IS NULL AND keep_unmapped IS NULL);

UPDATE sms_service_config s
JOIN sms_service_dialects d
  ON d.url <=> s.url
 AND d.request_map <=> s.request_map
 AND d.response_map <=> s.response_map
 AND d.auth_config <=> s.auth_config
 AND d.keep_unmapped <=> s.keep_unmapped
SET s.dialect_id = d.id
WHERE NOT (s.url = '/sms/v4/send' AND s.request_map IS NULL AND s.response_map IS NULL
           AND s.auth_config IS NULL AND s.keep_unmapped IS NULL);
