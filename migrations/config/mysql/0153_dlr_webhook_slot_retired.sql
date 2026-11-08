-- ============================================================================
-- 0153_dlr_webhook_slot_retired.sql — a vendor's receipts are read by its
-- Inbound DLR door, never by a vendor "DLR webhook" call (issue 19,
-- 2026-10-27; Saleem 2026-10-23: "if it does not standard format then i will
-- create dialect for it not http endpoint, and it has nothing to do with
-- vendor" · "it will be defined in inbounds" · "no dialect should use default,
-- but creating inbound is necessary")
--
-- Until now two things said how a vendor's receipt post is read:
--   1. the vendor's "DLR webhook" slot, adapter_config.endpoints.webhook — an
--      http_endpoints row the gateway never called; its response values
--      operator_id / status / error_code / to named the vendor's fields and
--      its config.statusMap the vendor's status words (and, before calls
--      existed, a hand-written profile's status.extract patterns and
--      status.map, with status.mode "webhook");
--   2. the vendor's Inbound DLR door (inbound_endpoints) with a dlr dialect.
-- Now only the door. A door with no dialect reads the usual names
-- (internal/callbackmap DefaultDLR); a door with one reads with it.
--
-- What this moves, for every HTTP / SOAP vendor:
--   * a webhook call (switched on, naming operator_id) becomes a dlr dialect
--     "<cid> DLR (webhook call N)": operator_id -> message_id, status,
--     error_code and to by the same source names; the status words in the
--     eight states (the gateway's own words — delivered, failed, … — are
--     read as dlr.ParseStat reads them; a word that means nothing is left
--     out, which reads the same); a status with no source of its own reads
--     "status", as the default reading would;
--   * a hand-written profile with status.mode "webhook" becomes
--     "<cid> DLR (profile)": each status.extract pattern a regex: source
--     (read over the query string and the body, as before), status.map the
--     status words;
--   * the vendor's DLR door is made when it has none (empty: no credential
--     yet, so it refuses every callback until one is set — as a vendor with
--     no door was refused before), and points at the new dialect when it has
--     no dialect. A door that HAD a dialect keeps it, and no dialect is made
--     for its vendor: the webhook call was not read beside a dialect;
--   * the slot is taken out of every vendor's adapter_config, and every
--     webhook call a vendor owned is nobody's (filed under the vendor's
--     company when it had none). The calls themselves are kept, switched as
--     they were; nothing reads them any more.
-- A profile's status block is left in place: the http-vendor role reads its
-- mode (a webhook vendor is not polled).
--
-- Two work tables are made and dropped here; nothing else reads them. Every
-- statement may run again: names are checked, rows are kept once.
-- ============================================================================

CREATE TABLE IF NOT EXISTS mig0153_fields (
  vendor_id BIGINT UNSIGNED NOT NULL,
  call_id   BIGINT UNSIGNED NULL,
  n         INT NOT NULL,
  ours      VARCHAR(16) NOT NULL,
  src       VARCHAR(255) NOT NULL,
  PRIMARY KEY (vendor_id, ours)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS mig0153_words (
  vendor_id BIGINT UNSIGNED NOT NULL,
  word      VARCHAR(190) COLLATE utf8mb4_bin NOT NULL,
  state     VARCHAR(8) NOT NULL,
  PRIMARY KEY (vendor_id, word)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- The webhook calls' fields, the first of each name.
INSERT IGNORE INTO mig0153_fields (vendor_id, call_id, n, ours, src)
SELECT v.id, h.id, p.n,
       CASE p.alias WHEN 'operator_id' THEN 'message_id' ELSE p.alias END,
       TRIM(p.source)
  FROM vendors v
  JOIN http_endpoints h
    ON h.id = CAST(JSON_UNQUOTE(JSON_EXTRACT(v.adapter_config, '$.endpoints.webhook')) AS UNSIGNED)
   AND h.is_active = 1,
       JSON_TABLE(h.config, '$.response.params[*]' COLUMNS (
         n      FOR ORDINALITY,
         alias  VARCHAR(64)  PATH '$.alias',
         source VARCHAR(255) PATH '$.source')) p
 WHERE v.transport IN ('http','soap')
   AND p.alias IN ('operator_id','status','error_code','to')
   AND TRIM(COALESCE(p.source,'')) <> ''
 ORDER BY v.id, p.n;

-- A hand-written profile's patterns, for a vendor with no webhook call.
INSERT IGNORE INTO mig0153_fields (vendor_id, call_id, n, ours, src)
SELECT v.id, NULL, x.n,
       CASE x.k WHEN 'id' THEN 'message_id' ELSE x.k END,
       CONCAT('regex:', JSON_UNQUOTE(JSON_EXTRACT(v.adapter_config, CONCAT('$.status.extract.', JSON_QUOTE(x.k)))))
  FROM vendors v,
       JSON_TABLE(JSON_KEYS(JSON_EXTRACT(v.adapter_config, '$.status.extract')), '$[*]' COLUMNS (
         n FOR ORDINALITY,
         k VARCHAR(64) PATH '$')) x
 WHERE v.transport IN ('http','soap')
   AND JSON_UNQUOTE(JSON_EXTRACT(v.adapter_config, '$.status.mode')) = 'webhook'
   AND NOT JSON_CONTAINS_PATH(v.adapter_config, 'one', '$.endpoints.webhook')
   AND x.k IN ('id','status','error_code','to')
   AND JSON_TYPE(JSON_EXTRACT(v.adapter_config, CONCAT('$.status.extract.', JSON_QUOTE(x.k)))) = 'STRING'
   AND TRIM(JSON_UNQUOTE(JSON_EXTRACT(v.adapter_config, CONCAT('$.status.extract.', JSON_QUOTE(x.k))))) <> ''
 ORDER BY v.id, x.n;

-- A dialect must say where the status is: one that named none reads "status".
INSERT IGNORE INTO mig0153_fields (vendor_id, call_id, n, ours, src)
SELECT f.vendor_id, f.call_id, 999, 'status', 'status'
  FROM mig0153_fields f
 WHERE f.ours = 'message_id';

-- The status words: a webhook call's statusMap, else a profile's status.map,
-- each word in one of the eight states.
INSERT IGNORE INTO mig0153_words (vendor_id, word, state)
SELECT s.vendor_id, s.word, s.state
  FROM (
    SELECT m.vendor_id, w.word,
           CASE UPPER(TRIM(JSON_UNQUOTE(JSON_EXTRACT(m.words, CONCAT('$.', JSON_QUOTE(w.word))))))
             WHEN 'DELIVRD' THEN 'DELIVRD' WHEN 'DELIVERED' THEN 'DELIVRD'
             WHEN 'UNDELIV' THEN 'UNDELIV' WHEN 'UNDELIVERABLE' THEN 'UNDELIV' WHEN 'FAILED' THEN 'UNDELIV'
             WHEN 'EXPIRED' THEN 'EXPIRED'
             WHEN 'REJECTD' THEN 'REJECTD' WHEN 'REJECTED' THEN 'REJECTD'
             WHEN 'DELETED' THEN 'DELETED' WHEN 'CANCELLED' THEN 'DELETED' WHEN 'CANCELED' THEN 'DELETED'
             WHEN 'ACCEPTD' THEN 'ACCEPTD' WHEN 'ACCEPTED' THEN 'ACCEPTD' WHEN 'SENT' THEN 'ACCEPTD'
             WHEN 'ENROUTE' THEN 'ENROUTE' WHEN 'EN_ROUTE' THEN 'ENROUTE' WHEN 'QUEUED' THEN 'ENROUTE'
             WHEN 'UNKNOWN' THEN 'UNKNOWN'
           END AS state
      FROM (SELECT f.vendor_id,
                   IF(f.call_id IS NULL,
                      JSON_EXTRACT(v.adapter_config, '$.status.map'),
                      JSON_EXTRACT(h.config, '$.statusMap')) AS words
              FROM (SELECT DISTINCT vendor_id, call_id FROM mig0153_fields WHERE ours = 'message_id') f
              JOIN vendors v ON v.id = f.vendor_id
              LEFT JOIN http_endpoints h ON h.id = f.call_id) m,
           JSON_TABLE(JSON_KEYS(m.words), '$[*]' COLUMNS (word VARCHAR(190) PATH '$')) w
  ) s
 WHERE s.state IS NOT NULL AND TRIM(s.word) <> '';

-- One dlr dialect for each vendor that names where its message id is.
INSERT INTO sms_service_dialects (name, kind, request_map)
SELECT IF(g.call_id IS NULL, CONCAT(v.cid, ' DLR (profile)'), CONCAT(v.cid, ' DLR (webhook call ', g.call_id, ')')),
       'dlr', g.fields
  FROM (
    SELECT f.vendor_id, MAX(f.call_id) AS call_id,
           JSON_ARRAYAGG(IF(f.ours = 'status' AND sm.words IS NOT NULL,
                            JSON_OBJECT('alias', f.ours, 'source', f.src, 'map', sm.words),
                            JSON_OBJECT('alias', f.ours, 'source', f.src))) AS fields,
           SUM(f.ours = 'message_id') AS has_id
      FROM mig0153_fields f
      LEFT JOIN (SELECT vendor_id, JSON_OBJECTAGG(word, state) AS words FROM mig0153_words GROUP BY vendor_id) sm
        ON sm.vendor_id = f.vendor_id
     GROUP BY f.vendor_id
  ) g
  JOIN vendors v ON v.id = g.vendor_id
 WHERE g.has_id > 0
   AND NOT EXISTS (SELECT 1 FROM inbound_endpoints e
                    WHERE e.vendor_id = v.id AND e.kind = 'dlr' AND e.dialect_id IS NOT NULL)
   AND NOT EXISTS (SELECT 1 FROM sms_service_dialects d
                    WHERE d.kind = 'dlr'
                      AND d.name = IF(g.call_id IS NULL, CONCAT(v.cid, ' DLR (profile)'), CONCAT(v.cid, ' DLR (webhook call ', g.call_id, ')')));

-- Its DLR door, made when it has none.
INSERT INTO inbound_endpoints (vendor_id, kind, dialect_id)
SELECT v.id, 'dlr', d.id
  FROM (SELECT DISTINCT vendor_id, call_id FROM mig0153_fields WHERE ours = 'message_id') f
  JOIN vendors v ON v.id = f.vendor_id
  JOIN sms_service_dialects d
    ON d.kind = 'dlr'
   AND d.name = IF(f.call_id IS NULL, CONCAT(v.cid, ' DLR (profile)'), CONCAT(v.cid, ' DLR (webhook call ', f.call_id, ')'))
 WHERE NOT EXISTS (SELECT 1 FROM inbound_endpoints e WHERE e.vendor_id = v.id AND e.kind = 'dlr');

-- A door with no dialect reads with the new one.
UPDATE inbound_endpoints e
  JOIN (SELECT DISTINCT vendor_id, call_id FROM mig0153_fields WHERE ours = 'message_id') f ON f.vendor_id = e.vendor_id
  JOIN vendors v ON v.id = f.vendor_id
  JOIN sms_service_dialects d
    ON d.kind = 'dlr'
   AND d.name = IF(f.call_id IS NULL, CONCAT(v.cid, ' DLR (profile)'), CONCAT(v.cid, ' DLR (webhook call ', f.call_id, ')'))
   SET e.dialect_id = d.id
 WHERE e.kind = 'dlr' AND e.dialect_id IS NULL;

-- The slot is gone from every vendor.
UPDATE vendors
   SET adapter_config = JSON_REMOVE(adapter_config, '$.endpoints.webhook')
 WHERE JSON_CONTAINS_PATH(adapter_config, 'one', '$.endpoints.webhook');

-- A webhook call is nobody's: no vendor uses one now.
UPDATE http_endpoints h
  LEFT JOIN vendors v ON v.id = h.vendor_id
   SET h.company_id = COALESCE(h.company_id, v.company_id),
       h.vendor_id = NULL
 WHERE h.type = 'vendor' AND h.name = 'webhook' AND h.vendor_id IS NOT NULL;

DROP TABLE IF EXISTS mig0153_words;

DROP TABLE IF EXISTS mig0153_fields;
