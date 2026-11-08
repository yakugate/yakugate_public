-- QUEUE store seed (yakugate_queue) — schema v2.0. Idempotent.
-- JWT-only auth: no api_tokens table. auto_replace_text + HLR shards live here.

-- ---- per-vendor pending queue (cloned from template) ----
CREATE TABLE IF NOT EXISTS queue_testvendor LIKE queue_template;

-- ---- HLR shard (first-4 digits of 923001234567 = 9230) ----
CREATE TABLE IF NOT EXISTS hlr_9230 LIKE hlr_template;
INSERT INTO hlr_9230 (msisdn, mcc, mnc) VALUES ('923001234567','410','01')
  ON DUPLICATE KEY UPDATE mcc=VALUES(mcc), mnc=VALUES(mnc);

-- ---- auto-replace rule for aracct (account id 11) ----
-- replace_regex {match,replace}; literal 'x' -> 'y'.
INSERT INTO auto_replace_text (account_id, replace_regex, seq)
SELECT 11, '{"match":"x","replace":"y"}', 1 FROM dual
WHERE NOT EXISTS (SELECT 1 FROM auto_replace_text WHERE account_id=11 AND seq=1);
