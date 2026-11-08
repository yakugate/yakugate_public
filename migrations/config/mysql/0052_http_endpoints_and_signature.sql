-- Signature pass, config-store half (PO 2026-07-30: "implement all discussion
-- in mysql... delete not required tables, and confirm"). Implements the
-- config-side deltas of D-082/D-083/D-084/D-087/D-088 in one coherent set.

-- ============================================================
-- 1. http_endpoints (D-087): ONE row per outbound HTTP target.
--    type covers all six ruled kinds; explicit nullable owners
--    (account for mo/dlr client webhooks, company for scoped
--    providers, vendor for vendor send/login/poll endpoints).
--    Policy columns are "the requirements": what internal/httpx
--    consumes, scope-agnostic. rate_limit doubles as the claim
--    batch size for vendor endpoints (D-088: "50 records must be
--    configurable at rate_limit"); submit_concurrency is the
--    HTTP-specific parallelism knob (D-088, NOT submit_throughput).
-- ============================================================
CREATE TABLE http_endpoints (
  id                 BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  type               ENUM('hlr','crp','kms','mo','dlr','vendor') NOT NULL,
  name               VARCHAR(128) NOT NULL,               -- e.g. 'send'/'login'/'poll' for vendor rows; provider name otherwise
  account_id         BIGINT UNSIGNED DEFAULT NULL,        -- mo/dlr: owning account
  company_id         BIGINT UNSIGNED DEFAULT NULL,        -- providers: company-scoped (NULL = shared)
  vendor_id          BIGINT UNSIGNED DEFAULT NULL,        -- vendor rows: owning vendor
  url                VARCHAR(512) DEFAULT NULL,           -- NULL for non-URL providers (kms gcp-kms:// refs live in config)
  method             VARCHAR(8) NOT NULL DEFAULT 'POST',
  headers            JSON DEFAULT NULL,                   -- templated name -> value (adapter Call shape)
  auth               JSON DEFAULT NULL,                   -- adapter AuthStep shape (basic/bearer/header/query/session)
  body_template      TEXT,                                -- raw body template (xml/soap/custom json)
  timeout_ms         INT NOT NULL DEFAULT 15000,
  retry              JSON DEFAULT NULL,                   -- {"max":N,"backoff_ms":..} — idempotency-aware in httpx
  tls                JSON DEFAULT NULL,
  circuit_breaker    JSON DEFAULT NULL,                   -- same shape P3 uses in protocol_config.circuit_breaker
  rate_limit         INT NOT NULL DEFAULT 50,             -- vendor rows: claim batch / rate (D-088)
  submit_concurrency INT NOT NULL DEFAULT 1,              -- vendor rows: parallel in-flight requests (D-088)
  config             JSON DEFAULT NULL,                   -- type-specific extras (provider_type, kms refs, ...)
  cache_ttl_sec      INT NOT NULL DEFAULT 0,              -- crp: rate cache TTL (absorbed from cr_providers)
  is_active          TINYINT(1) NOT NULL DEFAULT 1,
  created_by_user_id VARCHAR(64) DEFAULT NULL,
  created_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_endpoint_owner (type, account_id, vendor_id, name),
  KEY idx_endpoints_type (type, is_active),
  KEY idx_endpoints_vendor (vendor_id),
  KEY idx_endpoints_account (account_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Absorb the provider-catalog trio (D-087). Confirmed live before writing:
-- every reference (sms_service_config.hlr_provider_ids/kms_provider_ids,
-- tenant_kms_config rows) is NULL/empty in this environment, so no id
-- remapping is needed — rows migrate on their (type,name) identity alone.
INSERT INTO http_endpoints (type, name, config, is_active)
SELECT 'hlr', name, JSON_OBJECT('provider_type', provider_type), 1 FROM hlr_providers;
INSERT INTO http_endpoints (type, name, config, is_active)
SELECT 'kms', name, JSON_OBJECT('provider_type', provider_type), 1 FROM kms_providers;
INSERT INTO http_endpoints (type, name, config, cache_ttl_sec, is_active)
SELECT 'crp', name, JSON_OBJECT('provider_type', provider_type), cache_ttl_sec, is_active FROM cr_providers;

-- tenant_kms_config referenced kms_providers by FK; drop the FK (table is
-- empty live — the column now points at http_endpoints ids, FK re-added when
-- the KMS code pass lands).
ALTER TABLE tenant_kms_config DROP FOREIGN KEY fk_tenant_kms_provider;

DROP TABLE hlr_providers;
DROP TABLE kms_providers;
DROP TABLE cr_providers;

-- mo_config/dlr_config move to http_endpoints rows (D-087, superseding the
-- D-078 fold — acknowledged reversal). Confirmed live: both columns NULL on
-- every row, nothing to migrate.
ALTER TABLE sms_service_config
  DROP COLUMN mo_config,
  DROP COLUMN dlr_config;

-- ============================================================
-- 2. D-082/D-083 deltas
-- ============================================================
ALTER TABLE rate_cards
  MODIFY COLUMN type ENUM('MT','MO','DLR','HLR') DEFAULT NULL;

ALTER TABLE sms_service_config
  ADD COLUMN default_hlr_rate_card_id INT UNSIGNED DEFAULT NULL AFTER default_dlr_rate_card_id,
  ADD COLUMN dlr_charge_mode ENUM('all_receipts','delivered_only') NOT NULL DEFAULT 'all_receipts' AFTER default_hlr_rate_card_id;

ALTER TABLE vendors
  ADD COLUMN billing_cycle_day INT NOT NULL DEFAULT 1 AFTER submit_throughput;

-- Invoice lifecycle vocabulary (D-083): DRAFT -> FINALIZED (never edited;
-- corrections VOID + regenerate with next seq). status stays VARCHAR; only
-- the default changes to the new entry state.
ALTER TABLE invoices
  MODIFY COLUMN status VARCHAR(32) NOT NULL DEFAULT 'DRAFT';

-- ============================================================
-- 3. vendor_status_codes (D-084): dictionary + expanded actions.
--    code becomes VARCHAR (HTTP/SOAP codes are strings); action
--    gains the ruled vocabulary; alert flag on any row.
-- ============================================================
ALTER TABLE vendor_status_codes
  MODIFY COLUMN code VARCHAR(32) NOT NULL,
  MODIFY COLUMN action ENUM('success','retry','permanent','hlr_retry','failover','throttle','block_destination','pause_vendor') NOT NULL,
  ADD COLUMN alert TINYINT(1) NOT NULL DEFAULT 0 AFTER action;

-- ============================================================
-- 4. Sample vendors (PO: "create all sample" — the reference API
--    styles as ready-to-copy fixtures). service_status='paused' so
--    none affects routing until an operator activates + edits URLs.
--    adapter_config carries the FULL Profile (today's code reads it);
--    http_endpoints rows carry the same targets in the D-087 shape
--    (send/login/poll) for the slimmed-Profile code pass later.
-- ============================================================
INSERT INTO vendors (cid, name, transport, service_status, submit_throughput, queue_config, adapter_config, dlr_config, log_config) VALUES
('sample_ufone','Sample: Ufone BSMS (GET query, XML resp)','http','paused',0,'{}',
 '{"kind":"rest","vars":{"id":"ACCT","password":"PASS"},"send":{"method":"GET","url":"https://bsms.example-ufone.test/ufone/send","encoding":"query","params":{"id":"{id}","password":"{password}","msisdn":"{to}","message":"{text}"},"ok_match":"<status>OK</status>","extract":{"msgid":"<messageId>(.+?)</messageId>","error":"<status>(ERR[^<]*)</status>"}},"status":{"mode":"poll","interval_sec":30,"call":{"method":"GET","url":"https://bsms.example-ufone.test/ufone/report","encoding":"query","params":{"id":"{id}","password":"{password}","messageId":"{msgid}"},"extract":{"status":"<status>(.+?)</status>"}},"map":{"DELIVERED":"delivered","UNDELIV":"failed","EXPIRED":"expired"}}}',
 '{}','{}'),
('sample_telenor','Sample: Telenor (session login, GET send)','http','paused',0,'{}',
 '{"kind":"rest","vars":{"msisdn":"ACCT","password":"PASS"},"auth":{"method":"GET","url":"https://corp.example-telenor.test/tel/auth","encoding":"query","params":{"msisdn":"{msisdn}","password":"{password}"},"extract":{"session":"<session_id>(.+?)</session_id>"},"ttl_sec":1800},"send":{"method":"GET","url":"https://corp.example-telenor.test/tel/send","encoding":"query","params":{"session":"{session}","to":"{to}","message":"{text}","mask":"{from}"},"extract":{"msgid":"<messageId>(.+?)</messageId>","error":"<err>(.+?)</err>"}},"status":{"mode":"poll","interval_sec":30,"call":{"method":"GET","url":"https://corp.example-telenor.test/tel/query","encoding":"query","params":{"session":"{session}","id":"{msgid}"},"extract":{"status":"<status>(.+?)</status>"}},"map":{"DELIVERED":"delivered","FAILED":"failed"}}}',
 '{}','{}'),
('sample_zong','Sample: ZONG Corporate (SOAP/XML)','soap','paused',0,'{}',
 '{"kind":"soap","vars":{"loginId":"ACCT","loginPassword":"PASS"},"send":{"method":"POST","url":"https://cbs.example-zong.test/soap/send","encoding":"xml","headers":{"Content-Type":"text/xml; charset=utf-8","SOAPAction":"urn:QuickSMS"},"body":"<soapenv:Envelope xmlns:soapenv=\\"http://schemas.xmlsoap.org/soap/envelope/\\"><soapenv:Body><QuickSMS><loginId>{loginId}</loginId><loginPassword>{loginPassword}</loginPassword><Dest>{to}</Dest><Mask>{from}</Mask><Message>{text}</Message></QuickSMS></soapenv:Body></soapenv:Envelope>","ok_match":"<Status>Submitted</Status>","extract":{"msgid":"<TransactionId>(.+?)</TransactionId>","error":"<Status>((?!Submitted)[^<]+)</Status>"}},"status":{"mode":"poll","interval_sec":60,"call":{"method":"POST","url":"https://cbs.example-zong.test/soap/status","encoding":"xml","headers":{"Content-Type":"text/xml; charset=utf-8"},"body":"<soapenv:Envelope><soapenv:Body><QueryStatus><TransactionId>{msgid}</TransactionId></QueryStatus></soapenv:Body></soapenv:Envelope>","extract":{"status":"<DeliveryStatus>(.+?)</DeliveryStatus>"}},"map":{"DELIVERED":"delivered","UNDELIVERED":"failed"}}}',
 '{}','{}'),
('sample_cmt','Sample: CMT/Mobilink (raw XML POST, webhook DLR)','http','paused',0,'{}',
 '{"kind":"rest","vars":{"user":"ACCT","pass":"PASS"},"send":{"method":"POST","url":"https://cmt.example-mobilink.test/xml/send","encoding":"xml","headers":{"Content-Type":"application/xml"},"body":"<sms><user>{user}</user><pass>{pass}</pass><to>{to}</to><from>{from}</from><text>{text}</text><clientRef>{msgid}</clientRef></sms>","ok_match":"<code>0</code>","extract":{"msgid":"<smsId>(.+?)</smsId>","error":"<code>([1-9][0-9]*)</code>"}},"status":{"mode":"webhook","extract":{"id":"id=([A-Za-z0-9-]+)","status":"status=([A-Z]+)"},"map":{"DELIVRD":"delivered","UNDELIV":"failed","EXPIRED":"expired"}}}',
 '{}','{}'),
('sample_jazz','Sample: Jazz (form-encoded xmldoc POST)','http','paused',0,'{}',
 '{"kind":"rest","vars":{"user":"ACCT","password":"PASS"},"send":{"method":"POST","url":"https://sms.example-jazz.test/sendsms_xml.html","encoding":"form","params":{"xmldoc":"<SMSRequest><user>{user}</user><password>{password}</password><oa>{from}</oa><da>{to}</da><message>{text}</message></SMSRequest>"},"ok_match":"<response>OK</response>","extract":{"msgid":"<messageid>(.+?)</messageid>","error":"<response>((?!OK)[^<]+)</response>"}},"status":{"mode":"webhook","extract":{"id":"messageid=([A-Za-z0-9-]+)","status":"status=([A-Z]+)"},"map":{"DELIVERED":"delivered","REJECTED":"failed"}}}',
 '{}','{}'),
('sample_v3','Sample: generic REST/JSON v3','http','paused',0,'{}',
 '{"kind":"rest","vars":{"apikey":"KEY"},"send":{"method":"POST","url":"https://api.example-v3.test/v3/send","encoding":"json","headers":{"Authorization":"Bearer {apikey}"},"params":{"to":"{to}","from":"{from}","text":"{text}","clientRef":"{msgid}"},"extract":{"msgid":"\\"messageId\\"\\\\s*:\\\\s*\\"([^\\"]+)\\"","error":"\\"error\\"\\\\s*:\\\\s*\\"([^\\"]+)\\""}},"status":{"mode":"poll","interval_sec":30,"call":{"method":"GET","url":"https://api.example-v3.test/v3/status/{msgid}","headers":{"Authorization":"Bearer {apikey}"},"extract":{"status":"\\"status\\"\\\\s*:\\\\s*\\"([^\\"]+)\\""}},"map":{"delivered":"delivered","failed":"failed","expired":"expired"}}}',
 '{}','{}');

-- The same targets as D-087 endpoint rows (send + login/poll where the style
-- has them), owned by the sample vendors just inserted.
INSERT INTO http_endpoints (type, name, vendor_id, url, method, timeout_ms, rate_limit, submit_concurrency)
SELECT 'vendor','send', id, 'https://bsms.example-ufone.test/ufone/send','GET',15000,50,4 FROM vendors WHERE cid='sample_ufone';
INSERT INTO http_endpoints (type, name, vendor_id, url, method, timeout_ms, rate_limit, submit_concurrency)
SELECT 'vendor','poll', id, 'https://bsms.example-ufone.test/ufone/report','GET',15000,50,1 FROM vendors WHERE cid='sample_ufone';
INSERT INTO http_endpoints (type, name, vendor_id, url, method, timeout_ms, rate_limit, submit_concurrency)
SELECT 'vendor','login', id, 'https://corp.example-telenor.test/tel/auth','GET',15000,50,1 FROM vendors WHERE cid='sample_telenor';
INSERT INTO http_endpoints (type, name, vendor_id, url, method, timeout_ms, rate_limit, submit_concurrency)
SELECT 'vendor','send', id, 'https://corp.example-telenor.test/tel/send','GET',15000,50,4 FROM vendors WHERE cid='sample_telenor';
INSERT INTO http_endpoints (type, name, vendor_id, url, method, timeout_ms, rate_limit, submit_concurrency)
SELECT 'vendor','poll', id, 'https://corp.example-telenor.test/tel/query','GET',15000,50,1 FROM vendors WHERE cid='sample_telenor';
INSERT INTO http_endpoints (type, name, vendor_id, url, method, timeout_ms, rate_limit, submit_concurrency)
SELECT 'vendor','send', id, 'https://cbs.example-zong.test/soap/send','POST',20000,50,4 FROM vendors WHERE cid='sample_zong';
INSERT INTO http_endpoints (type, name, vendor_id, url, method, timeout_ms, rate_limit, submit_concurrency)
SELECT 'vendor','poll', id, 'https://cbs.example-zong.test/soap/status','POST',20000,50,1 FROM vendors WHERE cid='sample_zong';
INSERT INTO http_endpoints (type, name, vendor_id, url, method, timeout_ms, rate_limit, submit_concurrency)
SELECT 'vendor','send', id, 'https://cmt.example-mobilink.test/xml/send','POST',15000,50,4 FROM vendors WHERE cid='sample_cmt';
INSERT INTO http_endpoints (type, name, vendor_id, url, method, timeout_ms, rate_limit, submit_concurrency)
SELECT 'vendor','send', id, 'https://sms.example-jazz.test/sendsms_xml.html','POST',15000,50,4 FROM vendors WHERE cid='sample_jazz';
INSERT INTO http_endpoints (type, name, vendor_id, url, method, timeout_ms, rate_limit, submit_concurrency)
SELECT 'vendor','send', id, 'https://api.example-v3.test/v3/send','POST',15000,50,8 FROM vendors WHERE cid='sample_v3';
INSERT INTO http_endpoints (type, name, vendor_id, url, method, timeout_ms, rate_limit, submit_concurrency)
SELECT 'vendor','poll', id, 'https://api.example-v3.test/v3/status/{msgid}','GET',15000,50,1 FROM vendors WHERE cid='sample_v3';
