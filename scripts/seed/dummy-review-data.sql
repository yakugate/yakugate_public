-- ============================================================================
-- DUMMY REVIEW DATA (2026-07-27, ad hoc — NOT part of bootstrap auto-seed).
-- Purpose: give the PO a realistic-looking database to browse (companies,
-- varied accounts, vendors/routes, rate cards, templates in every state,
-- traffic history across MT/MO/DLR/rejected, ledger + audit history, and the
-- Node admin-plane/portal tables) without touching the 12 functional test
-- fixtures scripts/seed/config.sql already seeds (those stay exactly as they
-- are — the SQA harness depends on them).
--
-- Not idempotent-guarded by a bootstrap marker — safe to re-run against a
-- freshly migrated+seeded DB, but running it twice on the SAME already-
-- populated DB will hit UNIQUE-key collisions on the second pass by design
-- (it's meant to run once per fresh reseed, not accumulate).
-- ============================================================================

-- ---- companies + link a few existing test accounts to them -----------------
INSERT INTO companies (company_id, name, type, currency, prepaid_billing, balance, low_balance_alert, max_throughput, billing_email, support_email)
VALUES (UUID(), 'Acme Retail', 'enterprise', 'USD', 0, 5000.0000, 200.0000, 100, 'billing@acme-retail.example', 'support@acme-retail.example');
SET @acme := (SELECT id FROM companies WHERE name='Acme Retail' LIMIT 1);

INSERT INTO companies (company_id, name, type, currency, prepaid_billing, balance, low_balance_alert, max_throughput, billing_email, support_email)
VALUES (UUID(), 'Globex Marketing', 'smb', 'USD', 1, 250.0000, 50.0000, 20, 'billing@globex-mkt.example', 'support@globex-mkt.example');
SET @globex := (SELECT id FROM companies WHERE name='Globex Marketing' LIMIT 1);

UPDATE sms_service_config SET company_id=@acme WHERE username IN ('demo','sidacct');
UPDATE sms_service_config SET company_id=@globex WHERE username IN ('aracct','quietacct');

SET @demo   := (SELECT id FROM sms_service_config WHERE username='demo' LIMIT 1);
SET @spam   := (SELECT id FROM sms_service_config WHERE username='spamacct' LIMIT 1);
SET @sid    := (SELECT id FROM sms_service_config WHERE username='sidacct' LIMIT 1);
SET @prepaid:= (SELECT id FROM sms_service_config WHERE username='prepaidacct' LIMIT 1);
SET @quota  := (SELECT id FROM sms_service_config WHERE username='quotaacct' LIMIT 1);
SET @tpl    := (SELECT id FROM sms_service_config WHERE username='tplacct' LIMIT 1);
SET @disabled:=(SELECT id FROM sms_service_config WHERE username='disabledacct' LIMIT 1);
SET @country:= (SELECT id FROM sms_service_config WHERE username='countryacct' LIMIT 1);
SET @ar     := (SELECT id FROM sms_service_config WHERE username='aracct' LIMIT 1);
SET @quiet  := (SELECT id FROM sms_service_config WHERE username='quietacct' LIMIT 1);

-- ---- vendors (2 more, alongside the seeded testvendor) ----------------------
INSERT INTO vendors (cid, name, transport, service_status, submit_throughput, queue_config, adapter_config, encodings, dlr_config, log_config, smpp_config)
VALUES ('smppvendor1', 'SMPP Direct Connect', 'smpp', 'active', 200,
        '{"poll_interval_ms":500,"batch_size":100,"max_attempts":5,"retry_interval_sec":10}',
        NULL, '["gsm7","ucs2"]',
        '{"enabled":true,"mode":"session","msgid_transform":"none"}',
        '{"enabled":true,"level":"info","payloads":false}',
        '{"host":"smpp.example.com","port":2775,"system_id":"yakudemo","bind_type":"TRX"}');
SET @smppv := (SELECT id FROM vendors WHERE cid='smppvendor1' LIMIT 1);

INSERT INTO vendors (cid, name, transport, service_status, submit_throughput, queue_config, dlr_config, log_config)
VALUES ('backup-http-vendor', 'Backup HTTP Vendor', 'http', 'paused', 50,
        '{"poll_interval_ms":1000,"batch_size":25,"max_attempts":3,"retry_interval_sec":15}',
        '{"enabled":true,"mode":"poll","msgid_transform":"none"}',
        '{"enabled":true,"level":"warn","payloads":false}');
SET @backupv := (SELECT id FROM vendors WHERE cid='backup-http-vendor' LIMIT 1);
SET @testv := (SELECT id FROM vendors WHERE cid='testvendor' LIMIT 1);

-- ---- rate cards + rates ------------------------------------------------------
INSERT INTO rate_cards (name, kind, type, currency, company_id) VALUES ('Pakistan Buy v1', 'buy', 'MT', 'USD', NULL);
SET @rc_buy := (SELECT id FROM rate_cards WHERE name='Pakistan Buy v1' LIMIT 1);
INSERT INTO rate_cards (name, kind, type, currency, company_id) VALUES ('Pakistan Sell v1', 'sell', 'MT', 'USD', @acme);
SET @rc_sell := (SELECT id FROM rate_cards WHERE name='Pakistan Sell v1' LIMIT 1);

INSERT INTO rate_card_rates (rate_card_id, match_mcc, match_mnc, destination_country_code, price_per_segment, tax_percentage) VALUES
  (@rc_buy, '410', '01', NULL, 0.008000, 0.00),
  (@rc_buy, '410', '03', NULL, 0.009000, 0.00),
  (@rc_buy, NULL, NULL, '44',  0.015000, 0.00),
  (@rc_sell,'410', '01', NULL, 0.012000, 5.00),
  (@rc_sell,'410', '03', NULL, 0.013500, 5.00),
  (@rc_sell,NULL, NULL, '44',  0.022000, 5.00);

-- ---- routes (failover + lcr, beyond the seeded direct routes) ---------------
INSERT INTO routes (name, priority, route_type, account_id, vendor_id, match_mcc, match_mnc, buying_rate_card_id, selling_rate_card_id, is_disabled)
VALUES ('demo failover to SMPP', 55, 'failover', @demo, @smppv, '410', '01', @rc_buy, @rc_sell, 0);
INSERT INTO routes (name, priority, route_type, account_id, vendor_id, buying_rate_card_id, selling_rate_card_id, is_disabled)
VALUES ('aracct LCR', 70, 'lcr', @ar, @smppv, @rc_buy, @rc_sell, 0);

-- ---- templates in every workflow state --------------------------------------
INSERT INTO templates (account_id, company_id, original_text, regex, priority, status, current_approval_level, required_approval_level)
VALUES (@demo, @acme, 'Your order #### has shipped', '{"match":"^Your order [0-9]{4} has shipped$"}', 'TRANSACTIONAL', 'pending', 'NONE', 'NONE');
INSERT INTO templates (account_id, original_text, regex, priority, status, rejection_reason, rejected_by_user_id, rejected_at)
VALUES (@sid, 'WIN A FREE PRIZE CLICK HERE', '{"match":"^WIN A FREE PRIZE"}', 'PROMOTIONAL', 'rejected', 'promotional content violates content policy', 'admin', NOW());

-- ---- spam / blocklist / quota extras -----------------------------------------
INSERT INTO spam_keywords (account_id, destination_country_code, list_type, category, keywords)
VALUES (NULL, NULL, 'blocked', 'financial', '[{"word":"freemoney","enabled":true},{"word":"winbig","enabled":true}]');
INSERT INTO blocklist (msisdn, account_id, source, reason) VALUES ('923001112222', NULL, 'unsub', 'user replied STOP');
INSERT INTO quota_by_countries (account_id, destination_country_code, mode, quota, rate_limit)
VALUES (@sid, '1', 'allow', '{"daily_quota":500,"daily_used":42,"monthly_quota":10000,"monthly_used":1200}', 10);

-- ---- provider catalogs -------------------------------------------------------
INSERT INTO cr_providers (name, provider_type, config, cache_ttl_sec) VALUES ('default-fx-provider', 'fixed', JSON_OBJECT(), 3600);
INSERT INTO hlr_providers (name, provider_type, config, company_id) VALUES ('third-party-hlr-dip', 'third_party_api', JSON_OBJECT('url','https://hlr.example.com/lookup'), @acme);
INSERT INTO kms_providers (name, provider_type, config) VALUES ('vault-kms', 'vault', JSON_OBJECT('addr','https://vault.example.com'));

-- ---- balance ledger variety ---------------------------------------------------
INSERT INTO balance_history (account_id, company_id, type, amount, previous_balance, new_balance, reference, reason, created_by_user_id)
VALUES (@sid, @acme, 'topup', 500.0000, 1000.0000, 1500.0000, 'admin:demo-seed:1', 'manual topup (review data)', 'admin');
UPDATE sms_service_config SET balance = 1500.0000 WHERE id=@sid;
INSERT INTO balance_history (account_id, company_id, type, amount, previous_balance, new_balance, reference, reason, created_by_user_id)
VALUES (@ar, @globex, 'adjust', -100.0000, 1000.0000, 900.0000, 'admin:demo-seed:2', 'correction (review data)', 'admin');
UPDATE sms_service_config SET balance = 900.0000 WHERE id=@ar;

-- ---- audit log (simulated portal actions) ------------------------------------
INSERT INTO audit_log (actor, account_id, company_id, action, entity, entity_id, before_data, after_data, ip)
VALUES ('admin', @disabled, NULL, 'accounts.update', 'account', @disabled, JSON_OBJECT('is_disabled',0), JSON_OBJECT('is_disabled',1), '127.0.0.1');
INSERT INTO audit_log (actor, account_id, company_id, action, entity, entity_id, before_data, after_data, ip)
VALUES ('admin', @sid, @acme, 'templates.reject', 'template', (SELECT id FROM templates WHERE account_id=@sid LIMIT 1),
        JSON_OBJECT('status','pending'), JSON_OBJECT('status','rejected','rejection_reason','promotional content violates content policy'), '127.0.0.1');
INSERT INTO audit_log (actor, account_id, company_id, action, entity, entity_id, before_data, after_data, ip)
VALUES ('admin', @sid, @acme, 'balances.adjust', 'account', @sid, JSON_OBJECT('balance',1000.0000), JSON_OBJECT('balance',1500.0000), '127.0.0.1');

-- ---- portal plane (admin_users/admin_roles — the tables the Node Admin API
-- ACTUALLY reads today; `users`/`roles` from admin/migrations/0003 exist but
-- are still unwired — auth/login/seeding all still target admin_users/
-- admin_roles, so dummy portal users go there to be visible in the running
-- Admin API, not into the empty new tables) ------------------------------------
SET @pwhash := '$2a$10$placeholderplaceholderplaceholderplaceholderpla';
INSERT INTO admin_users (user_id, email, name, password_hash, role_id, account_id, is_active)
VALUES ('acme.ops', 'ops@acme-retail.example', 'Acme Ops', @pwhash, 2, @demo, 1);
INSERT INTO admin_users (user_id, email, name, password_hash, role_id, account_id, is_active)
VALUES ('globex.mktg', 'mktg@globex-mkt.example', 'Globex Marketing User', @pwhash, 2, @ar, 1);

-- ---- portal tables (contacts/groups/exports/invoices) -----------------------
INSERT INTO contacts (msisdn, first_name, last_name, company_id, email_address, dnd_enabled) VALUES
  ('923001234561', 'Ali', 'Khan', @acme, 'ali.khan@example.com', 0),
  ('923001234562', 'Sara', 'Ahmed', @acme, 'sara.ahmed@example.com', 0),
  ('447911123456', 'James', 'Smith', @acme, 'james.smith@example.com', 0),
  ('14155550111', 'Maria', 'Garcia', @globex, 'maria.garcia@example.com', 1),
  ('923001234563', 'Bilal', 'Hussain', @globex, NULL, 0);

INSERT INTO `groups` (group_id, description, company_id, is_disabled) VALUES
  (UUID(), 'VIP customers', @acme, 0),
  (UUID(), 'Newsletter subscribers', @globex, 0);
SET @grp_vip := (SELECT group_id FROM `groups` WHERE description='VIP customers' LIMIT 1);
SET @grp_news := (SELECT group_id FROM `groups` WHERE description='Newsletter subscribers' LIMIT 1);

INSERT INTO contact_group_mapping (group_id, contact_id)
SELECT @grp_vip, id FROM contacts WHERE msisdn IN ('923001234561','923001234562');
INSERT INTO contact_group_mapping (group_id, contact_id)
SELECT @grp_news, id FROM contacts WHERE msisdn IN ('14155550111','923001234563');

INSERT INTO exports (user_id, company_id, export_type, module, file_path, total_records, progress, status, completed_at) VALUES
  ('acme.ops', @acme, 'csv', 'records', '/exports/acme-mt-2026-07.csv', 12, 100, 'COMPLETED', NOW()),
  ('globex.mktg', @globex, 'csv', 'contacts', '/exports/globex-contacts.csv', 5, 100, 'COMPLETED', NOW());

INSERT INTO invoices (invoice_number, company_id, transaction_type, billing_cycle_day, period_from, period_to, amount_total, currency, status, payment_status) VALUES
  ('INV-2026-07-ACME-001', @acme, 'mt', 1, '2026-07-01', '2026-07-31', 128.4500, 'USD', 'GENERATED', 'UNPAID'),
  ('INV-2026-06-GLOBEX-001', @globex, 'mt', 1, '2026-06-01', '2026-06-30', 42.1000, 'USD', 'GENERATED', 'PAID');

-- ==============================================================================
-- HOT STORE traffic history (mtlog/dlrlog per-tenant-daily, mologtemplate
-- shared-per-month, rejectedlog per-tenant-daily) — switch database.
-- ==============================================================================
