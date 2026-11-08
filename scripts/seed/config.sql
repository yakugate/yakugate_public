-- CONFIG store seed (yakugate_config) — reconciled to the Hadal schema 2026-08-09. Idempotent.
-- Rewritten from the pre-reconciliation seed (which referenced dropped columns
-- customer_id/password_hash/balance/prepaid_billing/enforce_ip_check and would
-- FAIL on a fresh provision). Now seeds companies (tenant: balance/prepaid) ->
-- accounts (JWT + username/password + SMPP bind) with the same 12 policy test
-- fixtures, PLUS 4 dedicated SMPP config-variation accounts.
--
-- Auth: JWT (shared dev HS256 secret 'yaku-dev-secret', sub=username) OR
-- username+password (bcrypt, password == username). SMPP bind: system_id ==
-- username, password == username. Accounts insert in a FIXED order so aracct
-- keeps id 11 (the queue seed's auto_replace_text FK).
--
--   demo         clean happy-path account (Acme; unlimited SMPP binds)
--   spamacct     global spam keyword 'loanoffer' blocked
--   sidacct      sender-id locked to 'BRAND'
--   prepaidacct  prepaid company, zero balance (-> 402)
--   quotaacct    per-country daily quota exhausted
--   tplacct      template-locked to '^OTP [0-9]{4}$'
--   disabledacct disabled (-> 403)
--   ipacct       IP allowlist 10.0.0.0/32
--   quietacct    quiet hours all day
--   countryacct  country '44' denied
--   aracct       auto-replace x->y (rule seeded in the QUEUE store; id MUST be 11)
--   rlacct       mt_throughput 1 tps
--   smpp_txonly / smpp_rxonly   bind-mode restricted (TX-only / RX-only)
--   smpp_maxbind1               max_binds=1
--   smpp_fastka                 fast keepalive (enquire_link 5s / idle 15s)

SET @jwt    := '{"alg":"HS256","secret":"yaku-dev-secret","sub_claim":"sub","leeway_sec":60}';

-- ---- companies (the tenant: owns balance + prepaid flag) --------------------
INSERT IGNORE INTO companies (company_id, name, type, currency, billing_config, balance, max_throughput, timezone) VALUES
  ('00000000-0000-0000-0000-000000000001','Acme Retail (test)','enterprise','USD','{"prepaid":0}',5000.0000,100,'UTC'),
  ('00000000-0000-0000-0000-000000000002','Prepaid Zero (test)','smb','USD','{"prepaid":1}',0.0000,100,'UTC');
SELECT id INTO @acme    FROM companies WHERE company_id='00000000-0000-0000-0000-000000000001' LIMIT 1;
SELECT id INTO @prepaid FROM companies WHERE company_id='00000000-0000-0000-0000-000000000002' LIMIT 1;

-- ---- vendor + operators + global catch-all route ---------------------------
INSERT IGNORE INTO vendors (cid, name, transport, service_status, queue_config, dlr_config, log_config) VALUES
  ('testvendor','Test Vendor','http','active',
   '{"poll_interval_ms":1000,"batch_size":50,"max_attempts":5,"retry_interval_sec":10}',
   '{"enabled":true,"mode":"webhook","msgid_transform":"none"}',
   '{"enabled":true,"level":"info","payloads":false}');
SELECT id INTO @vendor FROM vendors WHERE cid='testvendor' LIMIT 1;

INSERT IGNORE INTO operators (ccc, mcc, mnc, mccmnc, operator_name, country, from_series, to_series, hlr_shard_key) VALUES
  ('92','410','01','41001','Op A','Pakistan','92300','92300','92300'),
  ('92','410','01','41001','Op A','Pakistan','92301','92301','92301'),
  ('92','410','03','41003','Op B','Pakistan','92303','92303','92303'),
  ('92','410','04','41004','Op C','Pakistan','92304','92304','92304'),
  ('92','410','06','41006','Op D','Pakistan','92306','92306','92306');
-- International form, with the calling code: a number the gateway reads never
-- starts with 0, so the national 0300 matched nothing (QA BUG-YK-OPS-001;
-- config/0140 corrects boxes seeded before).

-- global catch-all route (match_mccmncs is JSON now, not match_mcc/match_mnc)
INSERT INTO routes (priority, route_type, account_id, match_mccmncs, vendor_id, is_disabled)
SELECT 200,'direct',NULL,NULL,@vendor,0 FROM dual
WHERE NOT EXISTS (SELECT 1 FROM routes WHERE account_id IS NULL AND match_mccmncs IS NULL AND vendor_id=@vendor);

-- ---- accounts (sms_service_config) — FIXED ORDER (aracct must land id 11) ---
INSERT IGNORE INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config)
  VALUES (@acme, 'demo', @jwt, '["*"]', '$2a$10$/jlZXVRU1hsDeGo7f/Z3eO5gXNpogB5eugxz49yCnamMHiCMAzcJ2', 1, '{"system_id":"demo","password_hash":"$2a$10$/jlZXVRU1hsDeGo7f/Z3eO5gXNpogB5eugxz49yCnamMHiCMAzcJ2","allowed_modes":["TX","RX","TRX"],"max_binds":0,"enquire_link_sec":30,"idle_timeout_sec":90}');
SELECT id INTO @demo FROM sms_service_config WHERE username='demo' LIMIT 1;
INSERT IGNORE INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config)
  VALUES (@acme, 'spamacct', @jwt, '["*"]', '$2a$10$NlkuGf3xdqWthms3ej4J.OirXJaD0SFxIHrzCeKeznXEtVg3tXHlq', 1, '{"system_id":"spamacct","password_hash":"$2a$10$NlkuGf3xdqWthms3ej4J.OirXJaD0SFxIHrzCeKeznXEtVg3tXHlq","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}');
INSERT IGNORE INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config)
  VALUES (@acme, 'sidacct', @jwt, '["BRAND"]', '$2a$10$R2iqGluVqbrlrciN/2u8lefT/bTlxU/pJokXEBdBQIfaQenerKFtK', 1, '{"system_id":"sidacct","password_hash":"$2a$10$R2iqGluVqbrlrciN/2u8lefT/bTlxU/pJokXEBdBQIfaQenerKFtK","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}');
INSERT IGNORE INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config)
  VALUES (@prepaid, 'prepaidacct', @jwt, '["*"]', '$2a$10$IcTTztd/qXC4CPY5WWWYZOGKfMFHUlQe.OIk/P3xFC7rM9myGICIO', 1, '{"system_id":"prepaidacct","password_hash":"$2a$10$IcTTztd/qXC4CPY5WWWYZOGKfMFHUlQe.OIk/P3xFC7rM9myGICIO","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}');
INSERT IGNORE INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config)
  VALUES (@acme, 'quotaacct', @jwt, '["*"]', '$2a$10$PQZ2QZ/nbTaYtje0DXQ3KOD5LIkHBDNibZIkAhr3Qz/xxeIAA.fbK', 1, '{"system_id":"quotaacct","password_hash":"$2a$10$PQZ2QZ/nbTaYtje0DXQ3KOD5LIkHBDNibZIkAhr3Qz/xxeIAA.fbK","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}');
SELECT id INTO @quotaacct FROM sms_service_config WHERE username='quotaacct' LIMIT 1;
INSERT IGNORE INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config, enforce_template_check)
  VALUES (@acme, 'tplacct', @jwt, '["*"]', '$2a$10$kZWad9Ncyh.q/1zDVvylk.NtDYDqWF8bHNjT1x0qqX.3EiEwvoFRK', 1, '{"system_id":"tplacct","password_hash":"$2a$10$kZWad9Ncyh.q/1zDVvylk.NtDYDqWF8bHNjT1x0qqX.3EiEwvoFRK","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}', 1);
SELECT id INTO @tplacct FROM sms_service_config WHERE username='tplacct' LIMIT 1;
INSERT IGNORE INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config, is_disabled)
  VALUES (@acme, 'disabledacct', @jwt, '["*"]', '$2a$10$qBcEjgiIN0rvnj.UUwS2r.jl56lIE8nqPnPNDMQ3D9JmSjzqDum2e', 1, '{"system_id":"disabledacct","password_hash":"$2a$10$qBcEjgiIN0rvnj.UUwS2r.jl56lIE8nqPnPNDMQ3D9JmSjzqDum2e","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}', 1);
INSERT IGNORE INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config, allowed_ip_list)
  VALUES (@acme, 'ipacct', @jwt, '["*"]', '$2a$10$I.1JCq7dQ8Rx9BitGaoF.u.0Pk04SBMDjwNTIWubCOBekGs0mTvA2', 1, '{"system_id":"ipacct","password_hash":"$2a$10$I.1JCq7dQ8Rx9BitGaoF.u.0Pk04SBMDjwNTIWubCOBekGs0mTvA2","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}', '["10.0.0.0/32"]');
INSERT IGNORE INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config, quiet_range)
  VALUES (@acme, 'quietacct', @jwt, '["*"]', '$2a$10$qeVU36/xPNbN4Tn/flUF1u1hf.26NrrpkWUrNmdVpTTmcLi.75oZi', 1, '{"system_id":"quietacct","password_hash":"$2a$10$qeVU36/xPNbN4Tn/flUF1u1hf.26NrrpkWUrNmdVpTTmcLi.75oZi","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}', '00:00-23:59');
INSERT IGNORE INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config)
  VALUES (@acme, 'countryacct', @jwt, '["*"]', '$2a$10$BbMMudkGBQI2Sq9Ab3OaEuHsRMXe5GEWbchugghHBF7O0nbqEspCi', 1, '{"system_id":"countryacct","password_hash":"$2a$10$BbMMudkGBQI2Sq9Ab3OaEuHsRMXe5GEWbchugghHBF7O0nbqEspCi","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}');
SELECT id INTO @countryacct FROM sms_service_config WHERE username='countryacct' LIMIT 1;
INSERT IGNORE INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config, enforce_auto_replace)
  VALUES (@acme, 'aracct', @jwt, '["*"]', '$2a$10$1/9UvtFs9MrxDZRkg/uAUOrpClLY1kHoS8BSbqBFYka4wNjZStFXO', 1, '{"system_id":"aracct","password_hash":"$2a$10$1/9UvtFs9MrxDZRkg/uAUOrpClLY1kHoS8BSbqBFYka4wNjZStFXO","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}', 1);
INSERT IGNORE INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config, mt_throughput)
  VALUES (@acme, 'rlacct', @jwt, '["*"]', '$2a$10$ZDdqJ8NxodvKEkSKjZdOT.ghXUu.7YjRAwNiR5UADmIGFMtcx/8Si', 1, '{"system_id":"rlacct","password_hash":"$2a$10$ZDdqJ8NxodvKEkSKjZdOT.ghXUu.7YjRAwNiR5UADmIGFMtcx/8Si","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}', 1);
INSERT IGNORE INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config)
  VALUES (@acme, 'smpp_txonly', @jwt, '["*"]', '$2a$10$0ohj018g0NnaV.0RpdyjJuiQzJSHOtmJb2NyjdXl1gV0EK4cLFIiu', 1, '{"system_id":"smpp_txonly","password_hash":"$2a$10$0ohj018g0NnaV.0RpdyjJuiQzJSHOtmJb2NyjdXl1gV0EK4cLFIiu","allowed_modes":["TX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}');
INSERT IGNORE INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config)
  VALUES (@acme, 'smpp_rxonly', @jwt, '["*"]', '$2a$10$/hSs6xiytnz/veSS7UmzHOEg5M9U9vZyjjJOBI.YsoRdI.R09Szj2', 1, '{"system_id":"smpp_rxonly","password_hash":"$2a$10$/hSs6xiytnz/veSS7UmzHOEg5M9U9vZyjjJOBI.YsoRdI.R09Szj2","allowed_modes":["RX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}');
INSERT IGNORE INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config)
  VALUES (@acme, 'smpp_maxbind1', @jwt, '["*"]', '$2a$10$rzBBV0F3cnRlHDuUnr58wOK/.zOvUbvVBROREhUFmJpT5zSRvF4jW', 1, '{"system_id":"smpp_maxbind1","password_hash":"$2a$10$rzBBV0F3cnRlHDuUnr58wOK/.zOvUbvVBROREhUFmJpT5zSRvF4jW","allowed_modes":["TX","RX","TRX"],"max_binds":1,"enquire_link_sec":30,"idle_timeout_sec":90}');
INSERT IGNORE INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config)
  VALUES (@acme, 'smpp_fastka', @jwt, '["*"]', '$2a$10$bOmz6bz93gdpJUcUkZHDiel4TUhfJ863xWI5.nu2vcXGqZ85OkHR6', 1, '{"system_id":"smpp_fastka","password_hash":"$2a$10$bOmz6bz93gdpJUcUkZHDiel4TUhfJ863xWI5.nu2vcXGqZ85OkHR6","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":5,"idle_timeout_sec":15}');

-- demo direct route to testvendor for MCC/MNC 410/01
INSERT INTO routes (priority, route_type, account_id, match_mccmncs, vendor_id, is_disabled)
SELECT 50,'direct',@demo,'["41001"]',@vendor,0 FROM dual
WHERE NOT EXISTS (SELECT 1 FROM routes WHERE account_id=@demo AND match_mccmncs='["41001"]');

-- ---- policy fixtures -------------------------------------------------------
-- spam: global blocked keyword (company_id NULL = applies to all tenants)
INSERT IGNORE INTO spam_keywords (company_id, destination_country_code, list_type, category, keywords)
  VALUES (NULL, NULL, 'blocked', 'general', '["loanoffer"]');

-- quotaacct: per-country daily quota already exhausted (1/1)
INSERT INTO quota_by_countries (account_id, destination_country_code, mode, quota)
SELECT @quotaacct,'92','allow','{"daily_quota":1,"daily_used":1,"monthly_quota":null,"monthly_used":0}' FROM dual
WHERE NOT EXISTS (SELECT 1 FROM quota_by_countries WHERE account_id=@quotaacct AND destination_country_code='92');

-- countryacct: country '44' denied
INSERT INTO quota_by_countries (account_id, destination_country_code, mode)
SELECT @countryacct,'44','deny' FROM dual
WHERE NOT EXISTS (SELECT 1 FROM quota_by_countries WHERE account_id=@countryacct AND destination_country_code='44');

-- tplacct: approved template '^OTP [0-9]{4}$'
INSERT INTO templates (account_id, original_text, regex, priority, status)
SELECT @tplacct,'OTP ####','{"match":"^OTP [0-9]{4}$"}','TRANSACTIONAL','approved' FROM dual
WHERE NOT EXISTS (SELECT 1 FROM templates WHERE account_id=@tplacct);

-- ---- global fixtures -------------------------------------------------------
INSERT INTO blocklist (msisdn, account_id, source, reason)
SELECT '923009999999', NULL, 'manual', 'demo DND' FROM dual
WHERE NOT EXISTS (SELECT 1 FROM blocklist WHERE msisdn='923009999999' AND account_id IS NULL);

-- ---- SQA test fixtures (bind negatives / whitelist / dedupe-ttl / ip-wildcard / perf) ----
INSERT IGNORE INTO companies (company_id, name, type, currency, billing_config, balance, max_throughput, timezone) VALUES
  ('00000000-0000-0000-0000-0000000000ff','Loadtest Co','enterprise','USD','{"prepaid":0}',1000000000.0000,2000000000,'UTC');
SELECT id INTO @loadco FROM companies WHERE company_id='00000000-0000-0000-0000-0000000000ff' LIMIT 1;
-- smpp_off: channel disabled (smpp=0) -> bind ESME_RBINDFAIL
INSERT IGNORE INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config)
  VALUES (@acme,'smpp_off',@jwt,'["*"]','$2a$10$.l4KoS5L.dnXKzHelbenj.eQ2oghWy6gAv4toxh5sBuOKqxXgIEZK',0,'{"system_id":"smpp_off","password_hash":"$2a$10$.l4KoS5L.dnXKzHelbenj.eQ2oghWy6gAv4toxh5sBuOKqxXgIEZK","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}');
-- smpp_wl: whitelist-only (any other dest -> NOT_WHITELISTED)
INSERT IGNORE INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config, whitelist_numbers)
  VALUES (@acme,'smpp_wl',@jwt,'["*"]','$2a$10$Rs0HVPFX9KSW5pfJ2LYevO8aI7P4XTj5y5af5QXn4MRDHug7wqNWK',1,'{"system_id":"smpp_wl","password_hash":"$2a$10$Rs0HVPFX9KSW5pfJ2LYevO8aI7P4XTj5y5af5QXn4MRDHug7wqNWK","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}','["923009998888"]');
-- smpp_ttl: short dedupe window (dedupe_ttl_sec=2)
INSERT IGNORE INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config, dedupe_ttl_sec)
  VALUES (@acme,'smpp_ttl',@jwt,'["*"]','$2a$10$ZI8pBzv4zZp23Iu3pPSAq.iZ.kuOnt/UohrVJc923ek3i9PTGnq7y',1,'{"system_id":"smpp_ttl","password_hash":"$2a$10$ZI8pBzv4zZp23Iu3pPSAq.iZ.kuOnt/UohrVJc923ek3i9PTGnq7y","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}',2);
-- smpp_ipall: 0.0.0.0 allow-all IP wildcard
INSERT IGNORE INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config, allowed_ip_list)
  VALUES (@acme,'smpp_ipall',@jwt,'["*"]','$2a$10$EGhvCjK6o9mdSAJDUT3pYuw8.C.VXQsnt7bvBXDUIUz1xm2A5suz.',1,'{"system_id":"smpp_ipall","password_hash":"$2a$10$EGhvCjK6o9mdSAJDUT3pYuw8.C.VXQsnt7bvBXDUIUz1xm2A5suz.","allowed_modes":["TX","RX","TRX"],"max_binds":10,"enquire_link_sec":30,"idle_timeout_sec":90}','["0.0.0.0"]');
-- loadtest: perf harness account (dedup OFF, spam OFF, unlimited binds/throughput)
INSERT IGNORE INTO sms_service_config (company_id, username, jwt, allowed_sender_ids, password, smpp, smpp_config, enforce_duplicate_check, enforce_spam_check)
  VALUES (@loadco,'loadtest',@jwt,'["*"]','$2a$10$Js4g9v0k2AwTQA4Go7ir8uenPjhz67SI6xSBkSMJ3gtuJDggrL29K',1,'{"system_id":"loadtest","password_hash":"$2a$10$Js4g9v0k2AwTQA4Go7ir8uenPjhz67SI6xSBkSMJ3gtuJDggrL29K","allowed_modes":["TX","RX","TRX"],"max_binds":0,"enquire_link_sec":300,"idle_timeout_sec":600}',0,0);
