-- CONFIG store seed on SQL Server — scripts/seed/config.sql written in T-SQL. Idempotent.
-- Keep the two in step: a row added to one is added to the other. The fixtures,
-- accounts and their order are the same (aracct must land id 11: the queue seed's
-- auto_replace_text rule names it). One batch: T-SQL variables live for a batch.
--
-- MySQL's INSERT IGNORE is "insert unless the unique key is taken"; here each
-- insert says so (IF NOT EXISTS / WHERE NOT EXISTS on that key).

DECLARE @jwt NVARCHAR(400) = N'{"alg":"HS256","secret":"yaku-dev-secret","sub_claim":"sub","leeway_sec":60}';
DECLARE @acme BIGINT, @prepaid BIGINT, @vendor BIGINT, @demo BIGINT, @quotaacct BIGINT,
        @tplacct BIGINT, @countryacct BIGINT, @loadco BIGINT;

-- ---- companies (the tenant: owns balance + prepaid flag) --------------------
INSERT INTO companies (company_id, name, type, currency, billing_config, balance, max_throughput, timezone)
SELECT v.company_id, v.name, v.type, v.currency, v.billing_config, v.balance, v.max_throughput, v.timezone
FROM (VALUES
  ('00000000-0000-0000-0000-000000000001', N'Acme Retail (test)', N'enterprise', 'USD', N'{"prepaid":0}', 5000.0000, 100, N'UTC'),
  ('00000000-0000-0000-0000-000000000002', N'Prepaid Zero (test)', N'smb', 'USD', N'{"prepaid":1}', 0.0000, 100, N'UTC')
) AS v (company_id, name, type, currency, billing_config, balance, max_throughput, timezone)
WHERE NOT EXISTS (SELECT 1 FROM companies c WHERE c.company_id = v.company_id);
SELECT @acme = id FROM companies WHERE company_id = '00000000-0000-0000-0000-000000000001';
SELECT @prepaid = id FROM companies WHERE company_id = '00000000-0000-0000-0000-000000000002';

-- ---- vendor + operators + global catch-all route ---------------------------
IF NOT EXISTS (SELECT 1 FROM vendors WHERE cid = N'testvendor')
  INSERT INTO vendors (cid, name, transport, service_status, queue_config, dlr_config, log_config) VALUES
  (N'testvendor', N'Test Vendor', 'http', 'active',
   N'{"poll_interval_ms":1000,"batch_size":50,"max_attempts":5,"retry_interval_sec":10}',
   N'{"enabled":true,"mode":"webhook","msgid_transform":"none"}',
   N'{"enabled":true,"level":"info","payloads":false}');
SELECT @vendor = id FROM vendors WHERE cid = N'testvendor';

INSERT INTO operators (ccc, mcc, mnc, mccmnc, operator_name, country, from_series, to_series, hlr_shard_key)
SELECT v.ccc, v.mcc, v.mnc, v.mccmnc, v.operator_name, v.country, v.from_series, v.to_series, v.hlr_shard_key
FROM (VALUES
  (N'92', N'410', N'01', N'41001', N'Op A', N'Pakistan', N'92300', N'92300', N'92300'),
  (N'92', N'410', N'01', N'41001', N'Op A', N'Pakistan', N'92301', N'92301', N'92301'),
  (N'92', N'410', N'03', N'41003', N'Op B', N'Pakistan', N'92303', N'92303', N'92303'),
  (N'92', N'410', N'04', N'41004', N'Op C', N'Pakistan', N'92304', N'92304', N'92304'),
  (N'92', N'410', N'06', N'41006', N'Op D', N'Pakistan', N'92306', N'92306', N'92306')
) AS v (ccc, mcc, mnc, mccmnc, operator_name, country, from_series, to_series, hlr_shard_key)
WHERE NOT EXISTS (SELECT 1 FROM operators o WHERE o.ccc = v.ccc AND o.mcc = v.mcc AND o.mnc = v.mnc
                    AND o.from_series = v.from_series AND o.to_series = v.to_series);
-- International form, with the calling code: a number the gateway reads never
-- starts with 0 (QA BUG-YK-OPS-001).

-- global catch-all route (match_mccmncs is JSON, NULL = every network)
INSERT INTO routes (priority, route_type, account_id, match_mccmncs, vendor_id, is_disabled)
SELECT 200, 'direct', NULL, NULL, @vendor, 0
WHERE NOT EXISTS (SELECT 1 FROM routes WHERE account_id IS NULL AND match_mccmncs IS NULL AND vendor_id = @vendor);

-- ---- accounts (sms_service_config) — FIXED ORDER (aracct must land id 11) ---
IF NOT EXISTS (SELECT 1 FROM sms_service_config WHERE username = 'demo')
  INSERT INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config)
  VALUES (@acme, 'demo', @jwt, N'["*"]', '$2a$10$/jlZXVRU1hsDeGo7f/Z3eO5gXNpogB5eugxz49yCnamMHiCMAzcJ2', 1, N'{"system_id":"demo","password_hash":"$2a$10$/jlZXVRU1hsDeGo7f/Z3eO5gXNpogB5eugxz49yCnamMHiCMAzcJ2","allowed_modes":["TX","RX","TRX"],"max_binds":0,"enquire_link_sec":30,"idle_timeout_sec":90}');
SELECT @demo = id FROM sms_service_config WHERE username = 'demo';
IF NOT EXISTS (SELECT 1 FROM sms_service_config WHERE username = 'spamacct')
  INSERT INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config)
  VALUES (@acme, 'spamacct', @jwt, N'["*"]', '$2a$10$NlkuGf3xdqWthms3ej4J.OirXJaD0SFxIHrzCeKeznXEtVg3tXHlq', 1, N'{"system_id":"spamacct","password_hash":"$2a$10$NlkuGf3xdqWthms3ej4J.OirXJaD0SFxIHrzCeKeznXEtVg3tXHlq","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}');
IF NOT EXISTS (SELECT 1 FROM sms_service_config WHERE username = 'sidacct')
  INSERT INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config)
  VALUES (@acme, 'sidacct', @jwt, N'["BRAND"]', '$2a$10$R2iqGluVqbrlrciN/2u8lefT/bTlxU/pJokXEBdBQIfaQenerKFtK', 1, N'{"system_id":"sidacct","password_hash":"$2a$10$R2iqGluVqbrlrciN/2u8lefT/bTlxU/pJokXEBdBQIfaQenerKFtK","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}');
IF NOT EXISTS (SELECT 1 FROM sms_service_config WHERE username = 'prepaidacct')
  INSERT INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config)
  VALUES (@prepaid, 'prepaidacct', @jwt, N'["*"]', '$2a$10$IcTTztd/qXC4CPY5WWWYZOGKfMFHUlQe.OIk/P3xFC7rM9myGICIO', 1, N'{"system_id":"prepaidacct","password_hash":"$2a$10$IcTTztd/qXC4CPY5WWWYZOGKfMFHUlQe.OIk/P3xFC7rM9myGICIO","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}');
IF NOT EXISTS (SELECT 1 FROM sms_service_config WHERE username = 'quotaacct')
  INSERT INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config)
  VALUES (@acme, 'quotaacct', @jwt, N'["*"]', '$2a$10$PQZ2QZ/nbTaYtje0DXQ3KOD5LIkHBDNibZIkAhr3Qz/xxeIAA.fbK', 1, N'{"system_id":"quotaacct","password_hash":"$2a$10$PQZ2QZ/nbTaYtje0DXQ3KOD5LIkHBDNibZIkAhr3Qz/xxeIAA.fbK","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}');
SELECT @quotaacct = id FROM sms_service_config WHERE username = 'quotaacct';
IF NOT EXISTS (SELECT 1 FROM sms_service_config WHERE username = 'tplacct')
  INSERT INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config, enforce_template_check)
  VALUES (@acme, 'tplacct', @jwt, N'["*"]', '$2a$10$kZWad9Ncyh.q/1zDVvylk.NtDYDqWF8bHNjT1x0qqX.3EiEwvoFRK', 1, N'{"system_id":"tplacct","password_hash":"$2a$10$kZWad9Ncyh.q/1zDVvylk.NtDYDqWF8bHNjT1x0qqX.3EiEwvoFRK","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}', 1);
SELECT @tplacct = id FROM sms_service_config WHERE username = 'tplacct';
IF NOT EXISTS (SELECT 1 FROM sms_service_config WHERE username = 'disabledacct')
  INSERT INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config, is_disabled)
  VALUES (@acme, 'disabledacct', @jwt, N'["*"]', '$2a$10$qBcEjgiIN0rvnj.UUwS2r.jl56lIE8nqPnPNDMQ3D9JmSjzqDum2e', 1, N'{"system_id":"disabledacct","password_hash":"$2a$10$qBcEjgiIN0rvnj.UUwS2r.jl56lIE8nqPnPNDMQ3D9JmSjzqDum2e","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}', 1);
IF NOT EXISTS (SELECT 1 FROM sms_service_config WHERE username = 'ipacct')
  INSERT INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config, allowed_ip_list)
  VALUES (@acme, 'ipacct', @jwt, N'["*"]', '$2a$10$I.1JCq7dQ8Rx9BitGaoF.u.0Pk04SBMDjwNTIWubCOBekGs0mTvA2', 1, N'{"system_id":"ipacct","password_hash":"$2a$10$I.1JCq7dQ8Rx9BitGaoF.u.0Pk04SBMDjwNTIWubCOBekGs0mTvA2","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}', N'["10.0.0.0/32"]');
IF NOT EXISTS (SELECT 1 FROM sms_service_config WHERE username = 'quietacct')
  INSERT INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config, quiet_range)
  VALUES (@acme, 'quietacct', @jwt, N'["*"]', '$2a$10$qeVU36/xPNbN4Tn/flUF1u1hf.26NrrpkWUrNmdVpTTmcLi.75oZi', 1, N'{"system_id":"quietacct","password_hash":"$2a$10$qeVU36/xPNbN4Tn/flUF1u1hf.26NrrpkWUrNmdVpTTmcLi.75oZi","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}', '00:00-23:59');
IF NOT EXISTS (SELECT 1 FROM sms_service_config WHERE username = 'countryacct')
  INSERT INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config)
  VALUES (@acme, 'countryacct', @jwt, N'["*"]', '$2a$10$BbMMudkGBQI2Sq9Ab3OaEuHsRMXe5GEWbchugghHBF7O0nbqEspCi', 1, N'{"system_id":"countryacct","password_hash":"$2a$10$BbMMudkGBQI2Sq9Ab3OaEuHsRMXe5GEWbchugghHBF7O0nbqEspCi","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}');
SELECT @countryacct = id FROM sms_service_config WHERE username = 'countryacct';
IF NOT EXISTS (SELECT 1 FROM sms_service_config WHERE username = 'aracct')
  INSERT INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config, enforce_auto_replace)
  VALUES (@acme, 'aracct', @jwt, N'["*"]', '$2a$10$1/9UvtFs9MrxDZRkg/uAUOrpClLY1kHoS8BSbqBFYka4wNjZStFXO', 1, N'{"system_id":"aracct","password_hash":"$2a$10$1/9UvtFs9MrxDZRkg/uAUOrpClLY1kHoS8BSbqBFYka4wNjZStFXO","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}', 1);
IF NOT EXISTS (SELECT 1 FROM sms_service_config WHERE username = 'rlacct')
  INSERT INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config, mt_throughput)
  VALUES (@acme, 'rlacct', @jwt, N'["*"]', '$2a$10$ZDdqJ8NxodvKEkSKjZdOT.ghXUu.7YjRAwNiR5UADmIGFMtcx/8Si', 1, N'{"system_id":"rlacct","password_hash":"$2a$10$ZDdqJ8NxodvKEkSKjZdOT.ghXUu.7YjRAwNiR5UADmIGFMtcx/8Si","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}', 1);
IF NOT EXISTS (SELECT 1 FROM sms_service_config WHERE username = 'smpp_txonly')
  INSERT INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config)
  VALUES (@acme, 'smpp_txonly', @jwt, N'["*"]', '$2a$10$0ohj018g0NnaV.0RpdyjJuiQzJSHOtmJb2NyjdXl1gV0EK4cLFIiu', 1, N'{"system_id":"smpp_txonly","password_hash":"$2a$10$0ohj018g0NnaV.0RpdyjJuiQzJSHOtmJb2NyjdXl1gV0EK4cLFIiu","allowed_modes":["TX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}');
IF NOT EXISTS (SELECT 1 FROM sms_service_config WHERE username = 'smpp_rxonly')
  INSERT INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config)
  VALUES (@acme, 'smpp_rxonly', @jwt, N'["*"]', '$2a$10$/hSs6xiytnz/veSS7UmzHOEg5M9U9vZyjjJOBI.YsoRdI.R09Szj2', 1, N'{"system_id":"smpp_rxonly","password_hash":"$2a$10$/hSs6xiytnz/veSS7UmzHOEg5M9U9vZyjjJOBI.YsoRdI.R09Szj2","allowed_modes":["RX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}');
IF NOT EXISTS (SELECT 1 FROM sms_service_config WHERE username = 'smpp_maxbind1')
  INSERT INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config)
  VALUES (@acme, 'smpp_maxbind1', @jwt, N'["*"]', '$2a$10$rzBBV0F3cnRlHDuUnr58wOK/.zOvUbvVBROREhUFmJpT5zSRvF4jW', 1, N'{"system_id":"smpp_maxbind1","password_hash":"$2a$10$rzBBV0F3cnRlHDuUnr58wOK/.zOvUbvVBROREhUFmJpT5zSRvF4jW","allowed_modes":["TX","RX","TRX"],"max_binds":1,"enquire_link_sec":30,"idle_timeout_sec":90}');
IF NOT EXISTS (SELECT 1 FROM sms_service_config WHERE username = 'smpp_fastka')
  INSERT INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config)
  VALUES (@acme, 'smpp_fastka', @jwt, N'["*"]', '$2a$10$bOmz6bz93gdpJUcUkZHDiel4TUhfJ863xWI5.nu2vcXGqZ85OkHR6', 1, N'{"system_id":"smpp_fastka","password_hash":"$2a$10$bOmz6bz93gdpJUcUkZHDiel4TUhfJ863xWI5.nu2vcXGqZ85OkHR6","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":5,"idle_timeout_sec":15}');

-- demo direct route to testvendor for MCC/MNC 410/01
INSERT INTO routes (priority, route_type, account_id, match_mccmncs, vendor_id, is_disabled)
SELECT 50, 'direct', @demo, N'["41001"]', @vendor, 0
WHERE NOT EXISTS (SELECT 1 FROM routes WHERE account_id = @demo AND match_mccmncs = N'["41001"]');

-- ---- policy fixtures -------------------------------------------------------
-- spam: global blocked keyword (company_id NULL = applies to all tenants)
INSERT INTO spam_keywords (company_id, destination_country_code, list_type, category, keywords)
SELECT NULL, NULL, 'blocked', N'general', N'["loanoffer"]'
WHERE NOT EXISTS (SELECT 1 FROM spam_keywords WHERE company_id IS NULL AND destination_country_code IS NULL
                    AND list_type = 'blocked' AND category = N'general');

-- quotaacct: per-country daily quota already exhausted (1/1)
INSERT INTO quota_by_countries (account_id, destination_country_code, mode, quota)
SELECT @quotaacct, N'92', 'allow', N'{"daily_quota":1,"daily_used":1,"monthly_quota":null,"monthly_used":0}'
WHERE NOT EXISTS (SELECT 1 FROM quota_by_countries WHERE account_id = @quotaacct AND destination_country_code = N'92');

-- countryacct: country '44' denied
INSERT INTO quota_by_countries (account_id, destination_country_code, mode)
SELECT @countryacct, N'44', 'deny'
WHERE NOT EXISTS (SELECT 1 FROM quota_by_countries WHERE account_id = @countryacct AND destination_country_code = N'44');

-- tplacct: approved template '^OTP [0-9]{4}$'
INSERT INTO templates (account_id, original_text, regex, priority, status)
SELECT @tplacct, N'OTP ####', N'{"match":"^OTP [0-9]{4}$"}', 'TRANSACTIONAL', 'approved'
WHERE NOT EXISTS (SELECT 1 FROM templates WHERE account_id = @tplacct);

-- ---- global fixtures -------------------------------------------------------
INSERT INTO blocklist (msisdn, account_id, source, reason)
SELECT N'923009999999', NULL, 'manual', N'demo DND'
WHERE NOT EXISTS (SELECT 1 FROM blocklist WHERE msisdn = N'923009999999' AND account_id IS NULL);

-- ---- SQA test fixtures (bind negatives / whitelist / dedupe-ttl / ip-wildcard / perf) ----
INSERT INTO companies (company_id, name, type, currency, billing_config, balance, max_throughput, timezone)
SELECT '00000000-0000-0000-0000-0000000000ff', N'Loadtest Co', N'enterprise', 'USD', N'{"prepaid":0}', 1000000000.0000, 2000000000, N'UTC'
WHERE NOT EXISTS (SELECT 1 FROM companies WHERE company_id = '00000000-0000-0000-0000-0000000000ff');
SELECT @loadco = id FROM companies WHERE company_id = '00000000-0000-0000-0000-0000000000ff';
-- smpp_off: channel disabled (smpp=0) -> bind ESME_RBINDFAIL; smpp_wl: whitelist-only;
-- smpp_ttl: short dedupe window; smpp_ipall: 0.0.0.0 allow-all; loadtest: perf harness
IF NOT EXISTS (SELECT 1 FROM sms_service_config WHERE username = 'smpp_off')
  INSERT INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config)
  VALUES (@acme,'smpp_off',@jwt,N'["*"]','$2a$10$.l4KoS5L.dnXKzHelbenj.eQ2oghWy6gAv4toxh5sBuOKqxXgIEZK',0,N'{"system_id":"smpp_off","password_hash":"$2a$10$.l4KoS5L.dnXKzHelbenj.eQ2oghWy6gAv4toxh5sBuOKqxXgIEZK","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}');
IF NOT EXISTS (SELECT 1 FROM sms_service_config WHERE username = 'smpp_wl')
  INSERT INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config, whitelist_numbers)
  VALUES (@acme,'smpp_wl',@jwt,N'["*"]','$2a$10$Rs0HVPFX9KSW5pfJ2LYevO8aI7P4XTj5y5af5QXn4MRDHug7wqNWK',1,N'{"system_id":"smpp_wl","password_hash":"$2a$10$Rs0HVPFX9KSW5pfJ2LYevO8aI7P4XTj5y5af5QXn4MRDHug7wqNWK","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}',N'["923009998888"]');
IF NOT EXISTS (SELECT 1 FROM sms_service_config WHERE username = 'smpp_ttl')
  INSERT INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config, dedupe_ttl_sec)
  VALUES (@acme,'smpp_ttl',@jwt,N'["*"]','$2a$10$ZI8pBzv4zZp23Iu3pPSAq.iZ.kuOnt/UohrVJc923ek3i9PTGnq7y',1,N'{"system_id":"smpp_ttl","password_hash":"$2a$10$ZI8pBzv4zZp23Iu3pPSAq.iZ.kuOnt/UohrVJc923ek3i9PTGnq7y","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}',2);
IF NOT EXISTS (SELECT 1 FROM sms_service_config WHERE username = 'smpp_ipall')
  INSERT INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config, allowed_ip_list)
  VALUES (@acme,'smpp_ipall',@jwt,N'["*"]','$2a$10$EGhvCjK6o9mdSAJDUT3pYuw8.C.VXQsnt7bvBXDUIUz1xm2A5suz.',1,N'{"system_id":"smpp_ipall","password_hash":"$2a$10$EGhvCjK6o9mdSAJDUT3pYuw8.C.VXQsnt7bvBXDUIUz1xm2A5suz.","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}',N'["0.0.0.0"]');
IF NOT EXISTS (SELECT 1 FROM sms_service_config WHERE username = 'loadtest')
  INSERT INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config, enforce_duplicate_check, enforce_spam_check)
  VALUES (@loadco,'loadtest',@jwt,N'["*"]','$2a$10$Js4g9v0k2AwTQA4Go7ir8uenPjhz67SI6xSBkSMJ3gtuJDggrL29K',1,N'{"system_id":"loadtest","password_hash":"$2a$10$Js4g9v0k2AwTQA4Go7ir8uenPjhz67SI6xSBkSMJ3gtuJDggrL29K","allowed_modes":["TX","RX","TRX"],"max_binds":0,"enquire_link_sec":300,"idle_timeout_sec":600}',0,0);
