// 0004  Routes v2 + rate-card catalog — CONFIG (MongoDB)
// Matches MySQL/MSSQL 0004. Mongo is schemaless, so this creates the rate-card
// collections and backfills the v2 fields onto existing route docs.

db.createCollection("rate_cards");
db.rate_cards.createIndex({ kind: 1, status: 1 });
// rate_cards doc: { _id, name, kind: 'buy'|'sell', currency, status, created_at, updated_at }

db.createCollection("rate_card_rates");
db.rate_card_rates.createIndex({ rate_card_id: 1, match_mcc: 1, match_mnc: 1, match_prefix: 1 });
// rate_card_rates doc: { _id, rate_card_id, match_mcc, match_mnc, match_prefix,
//   price_per_msg, effective_from }   // most specific (mcc/mnc then prefix) wins

// Backfill v2 fields on existing route docs (defaults mirror the SQL side).
db.routes.updateMany({}, { $set: {
  name: null,
  route_type: 'direct',
  weight: 100,
  match_sender: null,
  require_hlr: false,
  accept_skipped_hlr: false,
  buying_rate_card_id: null,
  selling_rate_card_id: null,
  loss_detection: false,
  created_at: new Date(),
  updated_at: new Date()
}});

// route doc (v2): { _id, name, priority, route_type, weight, account_id,
//   match_mcc, match_mnc, match_prefix, match_sender,
//   vendor_id (null when route_type='blackhole'),
//   require_hlr, accept_skipped_hlr, buying_rate_card_id, selling_rate_card_id,
//   loss_detection, status, created_at, updated_at }
