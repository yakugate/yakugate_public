-- ============================================================================
-- DUMMY REVIEW DATA — HOT STORE (yakugate_hot), companion to
-- dummy-review-data.sql. Fabricated MT/DLR/MO/rejected traffic history across
-- 2026-07-26/27 so record_index, the traffic-log tables, and the (still
-- writer-less) summary tables have something real to look at. Not part of
-- bootstrap auto-seed — run once against a freshly migrated+seeded DB.
--
-- Account ids used (from sms_service_config, this environment):
--   demo=1  sidacct=3  prepaidacct=4  countryacct=10  aracct=11
-- Vendor ids used (from vendors):
--   testvendor=1  smppvendor1=2  backup-http-vendor=3
-- ============================================================================

-- ---- clone the per-tenant-daily / per-month partitions ----------------------
CREATE TABLE mtlog_demo_26_07_2026    LIKE mtlog_template;
CREATE TABLE dlrlog_demo_26_07_2026   LIKE dlrlog_template;
CREATE TABLE mtlog_demo_27_07_2026    LIKE mtlog_template;
CREATE TABLE dlrlog_demo_27_07_2026   LIKE dlrlog_template;
CREATE TABLE mtlog_sidacct_26_07_2026  LIKE mtlog_template;
CREATE TABLE dlrlog_sidacct_26_07_2026 LIKE dlrlog_template;
CREATE TABLE mtlog_sidacct_27_07_2026  LIKE mtlog_template;
CREATE TABLE dlrlog_sidacct_27_07_2026 LIKE dlrlog_template;
CREATE TABLE mtlog_aracct_27_07_2026   LIKE mtlog_template;
CREATE TABLE dlrlog_aracct_27_07_2026  LIKE dlrlog_template;
CREATE TABLE mo_log_2026_07           LIKE mologtemplate;
CREATE TABLE rejectedlog_prepaidacct_27_07_2026  LIKE rejectedlog_template;
CREATE TABLE rejectedlog_countryacct_26_07_2026  LIKE rejectedlog_template;
CREATE TABLE rejectedlog_demo_27_07_2026         LIKE rejectedlog_template;

-- ---- demo, 2026-07-26 (2 delivered, 1 failed) --------------------------------
INSERT INTO mtlog_demo_26_07_2026
  (msg_id, account_id, sender, destination, body, encoding, segments, protocol, ingress, mcc, mnc, vendor_id, transport, accepted_at, buy_price, sell_price, currency, rated_at, charging_mode)
VALUES
  ('RVWMT00000000000000000001', 1, 'BIDUPS', '923001234561', 'Your OTP is 4821', 'gsm7', 1, 'http', 'http', '410', '01', 1, 'http', '2026-07-26 09:12:03', 0.008000, 0.012000, 'USD', '2026-07-26 09:12:05', 'prepaid'),
  ('RVWMT00000000000000000002', 1, 'BIDUPS', '447911123456', 'Reminder: your appointment is tomorrow at 3pm', 'gsm7', 1, 'http', 'http', '234', '15', 1, 'http', '2026-07-26 14:40:11', 0.015000, 0.022000, 'USD', '2026-07-26 14:40:13', 'prepaid'),
  ('RVWMT00000000000000000003', 1, 'BIDUPS', '923009999998', 'Test message failure case', 'gsm7', 1, 'http', 'http', '410', '01', 1, 'http', '2026-07-26 18:02:47', 0.008000, 0.012000, 'USD', '2026-07-26 18:02:49', 'prepaid');

INSERT INTO dlrlog_demo_26_07_2026 (msg_id, account_id, vendor_id, vendor_msg_id, status, error_code, raw, delivered_at) VALUES
  ('RVWMT00000000000000000001', 1, 1, 'VEND-0001', 'DELIVRD', '000', 'id:VEND-0001 sub:001 dlvrd:001 stat:DELIVRD err:000', '2026-07-26 09:12:40'),
  ('RVWMT00000000000000000002', 1, 1, 'VEND-0002', 'DELIVRD', '000', 'id:VEND-0002 sub:001 dlvrd:001 stat:DELIVRD err:000', '2026-07-26 14:41:02'),
  ('RVWMT00000000000000000003', 1, 1, 'VEND-0003', 'UNDELIV', '021', 'id:VEND-0003 sub:001 dlvrd:000 stat:UNDELIV err:021', '2026-07-26 18:03:30');

-- ---- demo, 2026-07-27 (2 delivered, 1 still in flight) -----------------------
INSERT INTO mtlog_demo_27_07_2026
  (msg_id, account_id, sender, destination, body, encoding, segments, protocol, ingress, mcc, mnc, vendor_id, transport, accepted_at, buy_price, sell_price, currency, rated_at, charging_mode)
VALUES
  ('RVWMT00000000000000000004', 1, 'BIDUPS', '923001234562', 'Your order has been confirmed', 'gsm7', 1, 'http', 'http', '410', '01', 1, 'http', '2026-07-27 08:05:20', 0.008000, 0.012000, 'USD', '2026-07-27 08:05:22', 'prepaid'),
  ('RVWMT00000000000000000005', 1, 'BIDUPS', '14155550111', 'Welcome to our service!', 'gsm7', 1, 'http', 'http', '310', '260', 1, 'http', '2026-07-27 10:22:00', 0.015000, 0.022000, 'USD', '2026-07-27 10:22:02', 'prepaid'),
  ('RVWMT00000000000000000006', 1, 'BIDUPS', '923001234561', 'Your verification code is 9273', 'gsm7', 1, 'http', 'http', '410', '01', 1, 'http', '2026-07-27 13:55:10', NULL, NULL, NULL, NULL, NULL);

INSERT INTO dlrlog_demo_27_07_2026 (msg_id, account_id, vendor_id, vendor_msg_id, status, error_code, raw, delivered_at) VALUES
  ('RVWMT00000000000000000004', 1, 1, 'VEND-0004', 'DELIVRD', '000', 'id:VEND-0004 sub:001 dlvrd:001 stat:DELIVRD err:000', '2026-07-27 08:05:58'),
  ('RVWMT00000000000000000005', 1, 1, 'VEND-0005', 'DELIVRD', '000', 'id:VEND-0005 sub:001 dlvrd:001 stat:DELIVRD err:000', '2026-07-27 10:22:41');

-- ---- sidacct (sender-id locked to BRAND), 2026-07-26 (2 delivered) -----------
INSERT INTO mtlog_sidacct_26_07_2026
  (msg_id, account_id, sender, destination, body, encoding, segments, protocol, ingress, mcc, mnc, vendor_id, transport, accepted_at, buy_price, sell_price, currency, rated_at, charging_mode)
VALUES
  ('RVWMT00000000000000000007', 3, 'BRAND', '923001234563', 'BRAND: Flash sale today only, 30% off!', 'gsm7', 1, 'http', 'http', '410', '03', 1, 'http', '2026-07-26 11:00:00', 0.009000, 0.013500, 'USD', '2026-07-26 11:00:02', 'prepaid'),
  ('RVWMT00000000000000000008', 3, 'BRAND', '447911123456', 'BRAND: New arrivals just dropped', 'gsm7', 1, 'http', 'http', '234', '15', 1, 'http', '2026-07-26 16:30:00', 0.015000, 0.022000, 'USD', '2026-07-26 16:30:02', 'prepaid');

INSERT INTO dlrlog_sidacct_26_07_2026 (msg_id, account_id, vendor_id, vendor_msg_id, status, error_code, raw, delivered_at) VALUES
  ('RVWMT00000000000000000007', 3, 1, 'VEND-0007', 'DELIVRD', '000', 'id:VEND-0007 sub:001 dlvrd:001 stat:DELIVRD err:000', '2026-07-26 11:00:35'),
  ('RVWMT00000000000000000008', 3, 1, 'VEND-0008', 'DELIVRD', '000', 'id:VEND-0008 sub:001 dlvrd:001 stat:DELIVRD err:000', '2026-07-26 16:30:40');

-- ---- sidacct, 2026-07-27 (1 delivered, 1 failed) -----------------------------
INSERT INTO mtlog_sidacct_27_07_2026
  (msg_id, account_id, sender, destination, body, encoding, segments, protocol, ingress, mcc, mnc, vendor_id, transport, accepted_at, buy_price, sell_price, currency, rated_at, charging_mode)
VALUES
  ('RVWMT00000000000000000009', 3, 'BRAND', '14155550199', 'BRAND: Thanks for your purchase!', 'gsm7', 1, 'http', 'http', '310', '260', 1, 'http', '2026-07-27 09:15:00', 0.015000, 0.022000, 'USD', '2026-07-27 09:15:02', 'prepaid'),
  ('RVWMT00000000000000000010', 3, 'BRAND', '923001234561', 'BRAND: Limited time offer inside', 'gsm7', 1, 'http', 'http', '410', '01', 1, 'http', '2026-07-27 15:45:00', 0.008000, 0.012000, 'USD', '2026-07-27 15:45:02', 'prepaid');

INSERT INTO dlrlog_sidacct_27_07_2026 (msg_id, account_id, vendor_id, vendor_msg_id, status, error_code, raw, delivered_at) VALUES
  ('RVWMT00000000000000000009', 3, 1, 'VEND-0009', 'DELIVRD', '000', 'id:VEND-0009 sub:001 dlvrd:001 stat:DELIVRD err:000', '2026-07-27 09:15:38'),
  ('RVWMT00000000000000000010', 3, 1, 'VEND-0010', 'UNDELIV', '021', 'id:VEND-0010 sub:001 dlvrd:000 stat:UNDELIV err:021', '2026-07-27 15:46:10');

-- ---- aracct (SMPP vendor), 2026-07-27 (2 delivered) --------------------------
INSERT INTO mtlog_aracct_27_07_2026
  (msg_id, account_id, sender, destination, body, encoding, segments, protocol, ingress, mcc, mnc, vendor_id, transport, accepted_at, buy_price, sell_price, currency, rated_at, charging_mode)
VALUES
  ('RVWMT00000000000000000011', 11, 'BIDUPS', '923001234562', 'Order #5521 confirmed, arriving Jul 30', 'gsm7', 1, 'smpp', 'smpp', '410', '01', 2, 'smpp', '2026-07-27 12:00:00', 0.008000, 0.012000, 'USD', '2026-07-27 12:00:02', 'prepaid'),
  ('RVWMT00000000000000000012', 11, 'BIDUPS', '923001234563', 'Order #5522 confirmed, arriving Aug 1', 'gsm7', 1, 'smpp', 'smpp', '410', '01', 2, 'smpp', '2026-07-27 12:05:00', 0.008000, 0.012000, 'USD', '2026-07-27 12:05:02', 'prepaid');

INSERT INTO dlrlog_aracct_27_07_2026 (msg_id, account_id, vendor_id, vendor_msg_id, status, error_code, raw, delivered_at) VALUES
  ('RVWMT00000000000000000011', 11, 2, 'VEND-0011', 'DELIVRD', '000', 'id:VEND-0011 sub:001 dlvrd:001 stat:DELIVRD err:000', '2026-07-27 12:00:33'),
  ('RVWMT00000000000000000012', 11, 2, 'VEND-0012', 'DELIVRD', '000', 'id:VEND-0012 sub:001 dlvrd:001 stat:DELIVRD err:000', '2026-07-27 12:05:29');

-- ---- record_index: the correlation pointer for every MT message above -------
INSERT INTO record_index (msg_id, account_id, ym, vendor_id, vendor_msg_id, status, rec_table) VALUES
  ('RVWMT00000000000000000001', 1,  '202607', 1, 'VEND-0001', 'delivered', 'mtlog_demo_26_07_2026'),
  ('RVWMT00000000000000000002', 1,  '202607', 1, 'VEND-0002', 'delivered', 'mtlog_demo_26_07_2026'),
  ('RVWMT00000000000000000003', 1,  '202607', 1, 'VEND-0003', 'failed',    'mtlog_demo_26_07_2026'),
  ('RVWMT00000000000000000004', 1,  '202607', 1, 'VEND-0004', 'delivered', 'mtlog_demo_27_07_2026'),
  ('RVWMT00000000000000000005', 1,  '202607', 1, 'VEND-0005', 'delivered', 'mtlog_demo_27_07_2026'),
  ('RVWMT00000000000000000006', 1,  '202607', 1, NULL,        'sent',      'mtlog_demo_27_07_2026'),
  ('RVWMT00000000000000000007', 3,  '202607', 1, 'VEND-0007', 'delivered', 'mtlog_sidacct_26_07_2026'),
  ('RVWMT00000000000000000008', 3,  '202607', 1, 'VEND-0008', 'delivered', 'mtlog_sidacct_26_07_2026'),
  ('RVWMT00000000000000000009', 3,  '202607', 1, 'VEND-0009', 'delivered', 'mtlog_sidacct_27_07_2026'),
  ('RVWMT00000000000000000010', 3,  '202607', 1, 'VEND-0010', 'failed',    'mtlog_sidacct_27_07_2026'),
  ('RVWMT00000000000000000011', 11, '202607', 2, 'VEND-0011', 'delivered', 'mtlog_aracct_27_07_2026'),
  ('RVWMT00000000000000000012', 11, '202607', 2, 'VEND-0012', 'delivered', 'mtlog_aracct_27_07_2026');

-- ---- MO inbound (shared per-month table, no tenant suffix) -------------------
INSERT INTO mo_log_2026_07 (msg_id, account_id, sender, destination, body, encoding, segments, vendor_id, mcc, mnc, received_at) VALUES
  ('RVWMO00000000000000000001', 1,  '923001234561', 'BIDUPS', 'STOP', 'gsm7', 1, 1, '410', '01', '2026-07-27 09:30:00'),
  ('RVWMO00000000000000000002', 3,  '447911123456', 'BRAND',  'HELP', 'gsm7', 1, 1, '234', '15', '2026-07-27 17:00:00'),
  ('RVWMO00000000000000000003', 1,  '923001234562', 'BIDUPS', 'YES CONFIRM', 'gsm7', 1, 1, '410', '01', '2026-07-26 20:10:00'),
  ('RVWMO00000000000000000004', 11, '923001234563', 'BIDUPS', 'Order status please?', 'gsm7', 1, 2, '410', '01', '2026-07-27 13:00:00');

-- ---- rejected submits ---------------------------------------------------------
INSERT INTO rejectedlog_prepaidacct_27_07_2026
  (msg_id, account_id, sender, destination, body, encoding, segments, channel, protocol, ingress, mcc, mnc, reject_reason, status_code, is_charged, accepted_at)
VALUES
  ('RVWRJ00000000000000000001', 4, 'TEST', '923001234561', 'Cannot send, out of funds', 'gsm7', 1, NULL, 'http', 'http', '410', '01', 'INSUFFICIENT_BALANCE', 402, 0, '2026-07-27 07:40:00');

INSERT INTO rejectedlog_countryacct_26_07_2026
  (msg_id, account_id, sender, destination, body, encoding, segments, channel, protocol, ingress, mcc, mnc, reject_reason, status_code, is_charged, accepted_at)
VALUES
  ('RVWRJ00000000000000000002', 10, 'TEST', '447911123456', 'Blocked destination test', 'gsm7', 1, NULL, 'http', 'http', '234', '15', 'COUNTRY_DENIED', 400, 0, '2026-07-26 12:50:00');

INSERT INTO rejectedlog_demo_27_07_2026
  (msg_id, account_id, sender, destination, body, encoding, segments, channel, protocol, ingress, mcc, mnc, reject_reason, status_code, is_charged, accepted_at)
VALUES
  ('RVWRJ00000000000000000003', 1, 'BIDUPS', '923001234561', 'Your verification code is 9273', 'gsm7', 1, NULL, 'http', 'http', '410', '01', 'DUPLICATE', 409, 0, '2026-07-27 13:55:12');
