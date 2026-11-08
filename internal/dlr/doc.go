// Package dlr is the delivery-receipt (DLR) service: correlation of vendor
// receipts to records, the client push (bind or webhook), receipt rendering.
//
// Yakugate sits between the client and the vendor and plays a different SMPP role
// on each side, so every receipt has a DIRECTION. Receipts are NOT stored in a
// separate table: they live on the message record itself, in the hot store's
// single records_template shape, inside the raw_dlr JSON array (one entry per
// message part). Each entry carries its own "direction":
//
//	esme — a receipt RECEIVED FROM a vendor. Here Yakugate is the ESME and the
//	       vendor is the SMSC. It arrives first (deliver_sm on the vendor bind, or
//	       the vendor's HTTP callback), keyed by vendor_id + vendor_msg_id. It is
//	       correlated to the owning record through the queue/HLR delivery_reports
//	       table (Process 5); the Finalizer (Process 8) then updates the owning
//	       records_<account>_<YYYYMM> row and appends the receipt to raw_dlr.
//
//	smsc — a receipt OWED TO a client. Here Yakugate is the SMSC and the client is
//	       the ESME. It is produced once the esme leg is correlated and is pushed
//	       back to the client (deliver_sm on the client bind, or an API webhook),
//	       keyed by account_id + client_msg_id, with a per-entry delivered_at
//	       tracking the outbound delivery.
//
// A multi-segment SMS therefore holds several raw_dlr entries (one per part, keyed
// by seq); the record's per-part vendor message ids live alongside them in the
// messageids JSON. The record's aggregate status is 'delivered' when every part is
// DELIVRD and 'partial' when some parts fail.
package dlr
