// Package billingstore is the hot-store surface Process 9 needs, and the
// types that cross it.
//
// It exists because P9 was the one process wired straight to *mysql.Store —
// twenty concrete methods, no interface anywhere — so a deployment on any
// other backend got a billing role that started, reported healthy, and rated
// nothing. Every other process already talks to its stores through
// internal/store's interfaces; this is the missing equivalent.
//
// The types live HERE rather than in the MySQL package so that MSSQL and
// Mongo can satisfy the interface without importing a sibling backend. The
// MySQL package keeps aliases so its existing callers and tests are untouched.
//
// Honest note on scope: the two non-MySQL implementations cannot be run
// end-to-end today, because the gateway itself does not boot on those config
// stores. They are written against the same contract and the same tests the
// MySQL one passes, and they will need a live pass the day that changes.
package billingstore

import (
	"context"
	"time"
)

// RecordTable is one log table (D-136; by day since 26 Oct 2026):
// <kind>_<company>_<YYYYMMDD>.
type RecordTable struct {
	Name      string    // the table's name
	Kind      string    // mt | sub | dlr | mo | mo_out | hlr | rejects | orphans
	CompanyID int64     // the customer company; 0 = none
	Day       string    // YYYYMMDD, UTC
	Date      time.Time // that day, 00:00 UTC
}

// UnratedRecord is one MT record awaiting a price.
type UnratedRecord struct {
	// RowID is the record's place in its table (the table's own id), so a
	// reader can go on after it (mysql.Store.UnratedRecordsAfter). 0 on
	// stores that do not give it.
	RowID       int64
	MsgID       string
	AccountID   int64
	RouteID     int64
	Destination string
	MCC, MNC    string
	Segments    int
	// SentParts is how many PDUs the vendor was sent (mtlog.sent_parts,
	// hot/0043): the vendor side is priced by it. 0 = not known, use Segments.
	SentParts int
	// HLRSource is how the network was found (mtlog.hlr_source): "db" and
	// "dip" are charged an HLR lookup, "dip_miss" only when the account
	// charges failed lookups (2026-09-14). Empty on stores that do not record
	// it (MSSQL, Mongo) and on MO / legacy records.
	HLRSource string
	// Status and VendorMsgID decide the account's charge point (PO
	// 2026-09-17): "submit" charges once the vendor took the message (a
	// vendor id is present), "delivery" only a delivered one. Empty on MO
	// and on stores that do not read them.
	Status      string
	VendorMsgID string
	// VendorID is the vendor that sent it (mtlog.vendor_id): a rule's vendor
	// may carry its own buy and sell card, and the vendor's default buy card
	// is the one of the vendor that really sent it (config/0131). 0 = not
	// known (MO, legacy, stores that do not read it).
	VendorID int64
	// BillingModel is how the company paid when the message was accepted
	// (mtlog.billing_model, hot/0051): "prepaid" or "postpaid". The charge
	// follows it, not the company's model at rating time (QA BUG-024).
	// "" = not known (older rows, MO, other stores): the company's current
	// model decides, as before.
	BillingModel string
	// Quote is the price fixed when the message was accepted (mtlog.quote,
	// hot/0059; the JSON of internal/billing.Quote). The charge follows it.
	// "" = not known (an older record, MO, other stores): the record is
	// priced by the rates of its accept time, as before.
	Quote string
	// ChargeBatch is set on a record an earlier sweep put in a billing batch
	// and did not finish (see ChargeMarker): its prices and its charge mark
	// are already written, only the batch's outcome is open.
	ChargeBatch string
}

// UnratedDLR is one receipt awaiting a price. A message has MANY receipts, so
// the row id is the key here, not the message id.
type UnratedDLR struct {
	ID          int64
	MsgID       string
	Status      string // as received; `delivered_only` charging compares on this
	AccountID   int64
	RouteID     int64 // the MT route that carried the original message
	Destination string
	MCC, MNC    string
	// The client's receipt preference, carried from record_index: a receipt
	// the client was never SHOWN is not one they should be billed for.
	NoReceipt, FailureReceiptOnly bool
	// NotPushed says this receipt never reached the client: it is a vendor
	// receipt that arrived AFTER the account's receipt policy had already
	// given the client a final answer (record_index.told_status), so it was
	// recorded and not pushed. The same "no receipt, no charge" rule that
	// covers registered_delivery covers it too (PO 2026-09-22).
	//
	// Stated negatively on purpose: false is "the client got it", so every
	// backend and every test fake that does not know about receipt policies
	// keeps charging receipts exactly as it did before.
	NotPushed bool
	// IntermediateUnwanted says the client did NOT ask for progress notices
	// (record_index.want_intermediate = 0: an SMPP submit without
	// registered_delivery bit 4). GatewayNotice says the row is the gateway's
	// own at-submit record (status `sent`, no raw receipt) — the notice the
	// vendor roles send when the vendor takes the message — rather than a
	// receipt the vendor sent. VendorID is the vendor that carried it. With
	// them the billing role can tell whether an INTERMEDIATE receipt ever
	// reached the client under dlr_charge_mode=all_levels ("no receipt, no
	// charge", review of QA BILL-003). All three are stated so that the zero
	// value keeps the old reading: a backend that does not fill them charges
	// as before.
	IntermediateUnwanted bool
	GatewayNotice        bool
	VendorID             int64
	// Quote and BillingModel are the MESSAGE's (mtlog.quote, billing_model):
	// a receipt is priced by what its message was quoted at acceptance and
	// settled by the model the message was accepted under. "" = not known.
	Quote        string
	BillingModel string
	// ChargeBatch: as on UnratedRecord.
	ChargeBatch string
	// AfterFinal says an EARLIER row of the same message is a final receipt
	// the client was given (not one kept from it by a receipt policy). The
	// client is told a message's outcome once, and the quote prices ONE
	// receipt (D-130): a final receipt after the first is never charged —
	// the parts of a long message sent through an SMPP vendor (one receipt
	// row each, one receipt to an HTTP client), and a final the vendor sent
	// again for a message already closed ("recorded only"). Go-live test of
	// 18 Oct 2026, P2 B3, P5 C8, P9 B8, P4 D6, P14 C5.
	AfterFinal bool
	// MsgStatus and MsgVendorMsgID are the MESSAGE's outcome as billing reads
	// it for its charge point (COALESCE(told_status, status), vendor_msg_id):
	// a receipt of a message its account's charge point did not charge is
	// not charged either (P9 C2/C3, P7 D2). "" = not known: charged as before.
	MsgStatus, MsgVendorMsgID string
}

// SummaryBucket names one 15-minute rollup bucket of one account.
type SummaryBucket struct {
	Bucket    string // DATETIME string, 15-min aligned, on the store's own clock
	AccountID int64
}

// RatingWrite is one priced record for a BatchRater: MsgID keys an MT/MO row,
// ID a receipt row. A nil price persists as NULL, as with ApplyRating.
type RatingWrite struct {
	MsgID            string
	ID               int64
	Buy, Sell        *float64
	Currency, Detail string
	// Charge is the record's charge mark (hot/0059 `charge`, JSON): what it
	// adds to the account's usage and what it moves on a prepaid balance.
	// Written by ClaimRatings; "" on a record nothing is charged for.
	Charge string
}

// PendingCharge is one record of a billing batch that was started and not
// finished: its key, whose it is, and the charge mark written when the batch
// was claimed.
type PendingCharge struct {
	MsgID     string
	ID        int64
	AccountID int64
	Charge    string
}

// ChargeMarker is how a batch is charged exactly once WITHOUT a ledger row
// for each record (Saleem 2026-10-13: "we don't need to record per message").
//
// The balance is in the config store and the records are in the hot store, so
// "charge" and "mark as rated" cannot be one transaction. The order is:
//
//  1. ClaimRatings — each record's prices and its charge mark are written and
//     the record is put in the batch. rated_at stays NULL.
//  2. the batch is applied in the config store (billing.SettleBatch): the
//     balance, the day's usage, and ONE row for the batch, in one transaction.
//  3. FinishRatings — rated_at is set.
//
// A stop between 1 and 3 leaves records that are in a batch and not rated.
// The next sweep reads them all (PendingBatch) and applies the batch again
// with the same id and the same amounts: the batch's row says whether it was
// applied, so it is applied once.
type ChargeMarker interface {
	// ClaimRatings answers, for each record, whether it is now in the batch:
	// one that was rated or claimed by someone else meanwhile is not, and
	// must not be charged by this batch.
	ClaimRatings(ctx context.Context, table string, dlr bool, batch string, rs []RatingWrite) ([]bool, error)
	FinishRatings(ctx context.Context, table string, dlr bool, batch string, rs []RatingWrite) error
	PendingBatch(ctx context.Context, table string, dlr bool, batch string) ([]PendingCharge, error)
}

// BatchRater is an OPTIONAL fast path a store may offer: stamp a whole batch
// in one commit instead of one per record. Each separate commit waits for the
// disk, which is what held P9 near 70 records a second (2026-09-11). A store
// without it gets the per-record ApplyRating calls, unchanged.
//
// All or nothing: on error none of the batch is stamped, and the caller falls
// back to per-record calls to find the row that failed.
type BatchRater interface {
	ApplyRatings(ctx context.Context, table string, rs []RatingWrite) error
	ApplyRatingsDLR(ctx context.Context, table string, rs []RatingWrite) error
}

// Store is everything P9 asks of the hot store.
//
// Grouped by what it is for: find work, price it, and coordinate.
type Store interface {
	// --- finding work ---

	// ListRecordTables enumerates the hot partitions that may hold unrated
	// rows.
	ListRecordTables(ctx context.Context) ([]RecordTable, error)
	// UnratedRecords returns up to n MT records with no price yet, OLDEST
	// FIRST, skipping any that have failed maxAttempts times.
	//
	// Oldest-first matters: without an order the store returns whatever is
	// convenient, so a steady arrival of new records can leave older ones
	// unrated indefinitely — and settlement latency for one account then
	// depends on how busy everyone else is.
	//
	// maxAttempts <= 0 means no cap, which is the behaviour before parking
	// existed.
	UnratedRecords(ctx context.Context, table string, n, maxAttempts int) ([]UnratedRecord, error)
	// UnratedDLRs is the same for receipts.
	UnratedDLRs(ctx context.Context, dlrTable string, n, maxAttempts int) ([]UnratedDLR, error)

	// --- pricing ---

	// ApplyRating stamps an MT record with its buy/sell price. A nil price
	// means "rated, but not chargeable" — the row is still stamped so it is
	// never re-examined.
	ApplyRating(ctx context.Context, table, msgID string, buy, sell *float64, currency, detailJSON string) error
	// ApplyRatingDLR does the same for one receipt row.
	ApplyRatingDLR(ctx context.Context, table string, id int64, buy, sell *float64, currency, detailJSON string) error

	// NoteRatingFailure records that a record could not be priced or charged,
	// and returns how many consecutive failures it has now had.
	//
	// The POLICY — how many failures are too many — stays with the caller, so
	// three backends cannot disagree about when a record is written off. All a
	// store does is count and remember why.
	//
	// rated_at is deliberately NOT set: that field means PRICED, and a row
	// nobody could price is not priced. Saying otherwise would hide it from
	// every unrated count and quietly turn a billing failure into a free
	// message.
	NoteRatingFailure(ctx context.Context, table, msgID, reason string) (attempts int, err error)
	// NoteRatingFailureDLR is the same for a receipt row, which is keyed by
	// its row id because one message has many receipts.
	NoteRatingFailureDLR(ctx context.Context, table string, id int64, reason string) (attempts int, err error)

	// The rollups are not the billing role's any more (D-136, 2026-10-14):
	// whoever writes a record adds it to the summaries in the same
	// transaction, and a rating that moves a record's rate ids moves its
	// counts with it.

	// --- coordination ---

	// DBNow is the STORE's clock, not the process's.
	DBNow(ctx context.Context) (string, error)
	// TryLease takes a named cross-process lock so two billing instances do
	// not rate the same rows. ok=false means someone else holds it; the
	// returned release must be called when ok is true.
	TryLease(ctx context.Context, name string) (release func(), ok bool, err error)
}
