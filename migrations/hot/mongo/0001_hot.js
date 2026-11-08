// STORE 3: HOT BACKUP — MongoDB. Per-tenant isolation via per-tenant-month
// collections: records_<account_id>_<YYYYMM> (created on first write).
//
// ONE collection shape only. The former record_index and dlr_log collections are
// REMOVED; their data folds into the record document as embedded fields:
//   messageids — every id (gateway ULID, client messageId, per-part vendor ids)
//                with part-wise sequencing (seq / total).
//   raw_dlr    — an array of the raw receipts, ONE ENTRY PER PART, each tagged
//                with its segment seq. Holds multiple DLRs per logical message.
// DLR correlation runs through the queue/HLR delivery_reports (Process 5); the
// Finalizer (Process 8) then updates the owning record and pushes onto raw_dlr.
// No global routing collection is required.
//
// Per-tenant record collection doc (records_<account_id>_<YYYYMM>):
//   {
//     _id, msg_id, client_msg_id, account_id, username, sender, destination,
//     mcc, mnc, operator_id, vendor_id, transport, channel, segments,
//     status,                 // 'queued'|'sent'|'delivered'|'partial'|'failed'|'expired'
//     error_code, service_day, submit_date, delivery_date,
//     vendor_msg_id,          // first/primary part vendor id (also in messageids.parts)
//     original_message, charge, rate_cards, created_at,
//
//     messageids: {
//       gateway: "01J29XYZ8QK4T2R7VMBGA9WNPE",
//       client:  "order-7842-otp-1",
//       parts: [
//         { seq: 1, total: 3, vendor_msg_id: "9f2c.1" },
//         { seq: 2, total: 3, vendor_msg_id: "9f2c.2" },
//         { seq: 3, total: 3, vendor_msg_id: "9f2c.3" }
//       ]
//     },
//
//     raw_dlr: [
//       { seq: 1, direction: "esme", vendor_msg_id: "9f2c.1", stat: "DELIVRD",
//         err: "000", done_date: "2607081230", raw: "id:9f2c.1 ... stat:DELIVRD err:000",
//         tlv: { message_state: 2, receipted_message_id: "9f2c.1" },
//         received_at: ISODate("2026-07-08T12:30:05Z"), delivered_at: null },
//       { seq: 2, direction: "esme", vendor_msg_id: "9f2c.2", stat: "DELIVRD", ... },
//       { seq: 3, direction: "esme", vendor_msg_id: "9f2c.3", stat: "UNDELIV", ... }
//     ]
//     // direction: "esme" = received from vendor SMSC; "smsc" = owed/pushed to client.
//     // status = 'delivered' when all parts DELIVRD; 'partial' when mixed.
//   }
//
// Indexes are created per tenant-month collection on first write:
//   { msg_id: 1 }, { account_id: 1, client_msg_id: 1 } (client status lookup),
//   { vendor_id: 1, vendor_msg_id: 1 } (primary-part DLR correlation),
//   { "messageids.parts.vendor_msg_id": 1 } (any-part correlation), { service_day: 1 }.
