-- 0102_account_hlr_order.sql — each account decides how a number's operator
-- is found (2026-09-14, accounts-flow review).
--
-- Until now the order was fixed in code: the client's own operator code, then
-- our HLR data (hlr_<first4>), then one gateway-wide live provider. The
-- operators table's number ranges (349k rows) were never used for sending.
--
-- Now an account lists the steps it wants, first answer wins:
--   client   the operator code the client sent (the `operator` field)
--   data     our HLR data: numbers we know, imported or saved from live lookups
--   range    the operator the number's range was issued to (operators table)
--   live     a live lookup, trying the account's hlr_provider_ids in order
--   country  the country code only (MCC from the calling code, no network)
-- NULL = the gateway default in gateway_settings.hlr_default_order, which
-- starts as client, data, range, live.
--
-- hlr_cache_ttl_days: how long a saved answer counts as known. NULL = the
-- gateway default (gateway_settings.hlr_default_cache_ttl_days, 30); 0 = never
-- expires. An expired answer counts as unknown and the account's order goes on
-- to its next step. The answer itself stays in hlr_<first4> and is updated
-- when new data arrives.
--
-- charge_failed_hlr: bill a live lookup that found nothing (off by default).
ALTER TABLE sms_service_config
  ADD COLUMN hlr_order JSON NULL,
  ADD COLUMN hlr_cache_ttl_days INT UNSIGNED NULL,
  ADD COLUMN charge_failed_hlr TINYINT(1) NOT NULL DEFAULT 0;

-- Gateway-wide settings operators change from the portal. One row per name,
-- the value is JSON so a setting can be a number, a word or a list.
CREATE TABLE IF NOT EXISTS gateway_settings (
  name       VARCHAR(64) NOT NULL PRIMARY KEY,
  value      JSON        NOT NULL,
  updated_at TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT IGNORE INTO gateway_settings (name, value) VALUES
  ('hlr_default_order', JSON_ARRAY('client', 'data', 'range', 'live')),
  ('hlr_default_cache_ttl_days', CAST('30' AS JSON));

-- An account's HLR provider list now decides its live lookups, and an empty
-- list means none. So nothing changes on the day, every account with HLR
-- check on and no list gets today's provider: the first active hlr endpoint
-- that has a URL (the one http-server has been using).
UPDATE sms_service_config a
  JOIN (SELECT id FROM http_endpoints
         WHERE type = 'hlr' AND is_active = 1 AND url IS NOT NULL AND url <> ''
         ORDER BY id LIMIT 1) p
   SET a.hlr_provider_ids = JSON_ARRAY(p.id)
 WHERE a.enforce_hlr_db_check = 1
   AND (a.hlr_provider_ids IS NULL OR JSON_LENGTH(a.hlr_provider_ids) = 0);
