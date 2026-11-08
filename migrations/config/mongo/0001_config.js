// STORE 2: CONFIGURATIONS — MongoDB collections + indexes.
// Mongo is allowed for config and hot stores (NOT for the queue store).
db.createCollection("accounts");
db.accounts.createIndex({ username: 1 }, { unique: true });
db.accounts.createIndex({ customer_id: 1 }, { unique: true });
// fields: _id, customer_id, username, password_hash, secret_key, name, email, phone,
//   http, smpp, soap, allowed_ip_list[], ip_check, is_disabled, spam_check, template_check,
//   auto_replace_check, prepaid_billing, throughput (Int TPS, 0=unlimited), balance,
//   quiet_start, quiet_end, is_whitelist_number, whitelist_numbers[], retention_days,
//   timezone, mo_url, mo_method, created_at, updated_at

db.createCollection("sender_ids"); db.sender_ids.createIndex({ account_id: 1, mask: 1 }, { unique: true });
db.createCollection("vendors");    db.vendors.createIndex({ cid: 1 }, { unique: true });
db.createCollection("operators"); db.operators.createIndex({ mcc: 1, mnc: 1 }, { unique: true });  // fields: mcc, mnc, numbering_plan, country_code, country_name
db.createCollection("routes");     db.routes.createIndex({ account_id: 1, match_mcc: 1, match_mnc: 1, match_prefix: 1, priority: 1 });
db.createCollection("templates");  db.templates.createIndex({ account_id: 1, status: 1 });
db.createCollection("spam_keywords"); db.spam_keywords.createIndex({ scope: 1, account_id: 1, traffic: 1, list_type: 1 });
db.createCollection("blocklist");  db.blocklist.createIndex({ msisdn: 1, account_id: 1 }, { unique: true });
db.createCollection("country_rules"); db.country_rules.createIndex({ account_id: 1, cc: 1 }, { unique: true });
// country_rules doc: { account_id, cc, mode:'allow'|'deny', daily_quota, daily_used, monthly_quota, monthly_used }
db.createCollection("auto_replace"); db.auto_replace.createIndex({ account_id: 1, status: 1, seq: 1 });
db.createCollection("audit_log");  db.audit_log.createIndex({ created_at: 1 }); db.audit_log.createIndex({ account_id: 1 });
