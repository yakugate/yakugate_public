// 0002  Process 3 + anti-abuse — CONFIG store additions (MongoDB)
// vendor_status_codes collection; new vendor fields.
db.createCollection("vendor_status_codes");
db.vendor_status_codes.createIndex({ vendor_id: 1, scope: 1, code: 1 }, { unique: true });
// doc: { vendor_id, scope: 'submit'|'dlr', code:Int, action:'success'|'retry'|'permanent', description }

// New per-vendor Process 3 fields (applied to existing vendor docs; schemaless add).
db.vendors.updateMany({}, { $set: {
  sessions: 1, window_size: 10, retry_interval_sec: 10, enquire_link_sec: 30,
  response_timeout_sec: 30, poll_interval_ms: 1000, batch_size: 50,
  max_attempts: 5, throttle_pause_sec: 10, msgid_transform: null
}});
