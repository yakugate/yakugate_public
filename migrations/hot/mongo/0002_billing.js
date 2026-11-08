// STORE 3 (HOT) — MongoDB collections and indexes Process 9 needs.
//
// The document model mirrors the relational one on purpose: a collection per
// hot partition, a record_index locator, and a materialised collection per
// summary. The reasoning is in internal/store/mongo/billing.go's header — in
// short, the portal and Admin API read the summary tables BY NAME, retention
// drops a partition by dropping it, and P9's contract is expressed in
// partitions. A cleverer Mongo-native shape would make the same deployment
// behave differently depending on its store, which is the failure this parity
// work exists to remove.
//
// Only the shared collections are created here. Partition collections
// (mtlog_*, dlrlog_*, mo_log_*, rejects_*) are created on first write, the way
// their MySQL clones are created on first use.

const summaries = [
  // Unique on the full grouping key, so a rebuild upserts the same row rather
  // than duplicating it. This is what makes delete-then-rebuild safe.
  { name: "summary_mt",
    key: { bucket_start: 1, account_id: 1, vendor_id: 1, mcc: 1, mnc: 1,
           sender_id: 1, sell_card_id: 1, buy_card_id: 1 } },
  { name: "summary_mo",
    key: { bucket_start: 1, account_id: 1, vendor_id: 1, mcc: 1, mnc: 1,
           sender_id: 1, sell_card_id: 1, buy_card_id: 1 } },
  { name: "summary_lookup",
    key: { bucket_start: 1, service: 1, account_id: 1, vendor_id: 1, mcc: 1,
           mnc: 1, sell_card_id: 1, buy_card_id: 1 } },
  { name: "summary_dlr_flow", key: { bucket_start: 1, vendor_id: 1, sender_id: 1 } },
  { name: "summary_err_daily",
    key: { day: 1, service: 1, vendor_id: 1, mcc: 1, mnc: 1, err_code: 1 } },
  { name: "summary_live", key: { minute_start: 1, service: 1, vendor_id: 1 } },
];

for (const s of summaries) {
  db.createCollection(s.name);
  db[s.name].createIndex(s.key, { unique: true, name: "uq_grouping" });
}

// The locator. msg_id+ym is the primary key on the SQL backends; here it is a
// unique index for the same reason — one message, one locator row.
db.createCollection("record_index");
db.record_index.createIndex({ msg_id: 1, ym: 1 }, { unique: true, name: "uq_msg_ym" });
db.record_index.createIndex({ updated_at: 1 }, { name: "idx_updated" });   // dirty-bucket scan
db.record_index.createIndex({ vendor_msg_id: 1 }, { name: "idx_vendor_msg" });
db.record_index.createIndex({ client_ref: 1 }, { name: "idx_client_ref" });
db.record_index.createIndex({ request_id: 1 }, { name: "idx_request" });

// Watermarks, keyed by name so _id IS the name.
db.createCollection("summary_state");

// Billing leases.
//
// Mongo has no session-scoped advisory lock, so a lease is a document with an
// expiry. The TTL index is what makes that safe: a billing process that dies
// holding one would otherwise block every other instance forever, because
// there is no session end for the server to notice. expireAfterSeconds 0 means
// "expire AT the time in the field", not "immediately".
db.createCollection("billing_leases");
db.billing_leases.createIndex({ expires_at: 1 }, { expireAfterSeconds: 0, name: "ttl_expires" });
