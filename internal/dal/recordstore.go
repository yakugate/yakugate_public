package dal

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/billingstore"
	cry "github.com/yakugate/yakugate/internal/crypto"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// The record store (plan of 29 Oct 2026, step 2): the hot store's records of
// every message — the daily tables mt_, sub_, dlr_, mo_ and rejects_, their
// summaries, the receipts matched to their messages, the messages waiting
// for a receipt, the orphans and the lookups. What a record means (when a
// receipt closes its message, what is charged, the receipt policy) stays in
// the roles and in internal/dlr, internal/mo and internal/recordq; what is
// written and read is here. Every method is the one step it was before the
// move: the same statements, the same transaction.
//
// The roles reached it as *mysql.Store until 30 Oct 2026. The interfaces
// below are the methods they really call, by what the data is; RecordStore
// is all of them on one database. Some callers ask the store for a method
// they may do without (internal/dlr's PartTracker, Finalizer, Leaser, …,
// recordq's "Once" writes): those are in RecordStore too, so every DAL that
// serves records gives them and nothing has to fall back.

// ---- the rows ----

// TerminalRecord is one simulated/known-outcome delivery record: the message
// plus its terminal DLR fields, written in ONE pass instead of the live path's
// Write(sent) + UpdateStatus(terminal) (2 + 3 statements per message — the
// measured 75 msg/s blackhole ceiling, 2026-07-26).
type TerminalRecord struct {
	Msg         model.Message // Msg.Status carries the terminal status
	VendorMsgID string
	ErrorCode   string
	// AckAt is when the vendor TOOK the message (its submit answer); zero =
	// it did not (a refusal). Written to mtlog.vendor_ack_at, which the
	// vendor summary counts "taken by the vendor" and the answer time from —
	// the column had been read since hot/0016 and written by nothing, so
	// both always read zero (found by the load test, 2026-10-06).
	AckAt time.Time
	// Rating is what an older DB worker carried for the record's price; its
	// rate ids are read from Detail when the message has no Priced. It is
	// worked out by whoever writes the record, so it does not ride a queued
	// task.
	Rating *RecordRating `json:"-"`
	// Answers are the vendor's answers that came with this outcome — every
	// part of the try that ended the message (issue 25.4) — written to
	// sub_ in the same transaction as the record.
	Answers []model.SubmitAnswer `json:",omitempty"`
}

// RecordRating is a message record's price and what it charges, for a record
// that is written already rated (2026-10-13, D-131): the billing role's read
// of the unrated record and its two updates of it are then not needed.
//
// Charge "" = nothing is charged for it (no price at acceptance, an unlinked
// account, a charge point it did not reach): the record is rated at once.
// Charge set = the record is put in billing batch Batch with rated_at empty,
// exactly as billingstore.ChargeMarker.ClaimRatings leaves one; the batch is
// then applied and finished by the caller (or, after a stop, by the billing
// role's sweep, which finishes any batch it finds open).
type RecordRating struct {
	Buy, Sell        *float64
	Currency, Detail string
	Charge           string
	Batch            string
}

// RatedOutcome is what WriteRated found in one message table.
type RatedOutcome struct {
	Table string
	// Claimed holds the ids of the records whose Rating this write put on
	// the record. A record that was on the log already keeps what it had and
	// is not here: it must not be charged by this write's batch.
	Claimed map[string]bool
	// Open lists other billing batches found on records of this write that
	// were there already and are not finished (an earlier write of the same
	// records that did not live to settle its batch).
	Open []string
}

// MsgStatus is the current-status view of one message, served by GET
// /messages/{id} (P1 scope finalize 2026-08-09).
type MsgStatus struct {
	MsgID       string
	RequestID   string
	Status      string
	VendorMsgID string
	// Reference is the id the CLIENT gave this message at submit — empty when
	// they gave none. Returned so a status lookup answers in the caller's own
	// vocabulary, not only ours.
	Reference string
	// AccountID owns the message. Returned so the status endpoint can refuse
	// to show one account's message to another (PO 2026-09-08).
	AccountID int64
}

// ReceiptRow is one receipt's database work.
type ReceiptRow struct {
	MsgID       string
	AccountID   int64
	VendorID    int64
	VendorMsgID string
	Table       string // the message's own table (mt_<company>_<day>)
	Status      string // the message status the receipt implies
	Stat        string // the receipt's own word
	ErrorCode   string
	Raw         string
	DoneAt      string // the vendor's delivery time (RFC 3339), "" = not given
	Deployment  string // the gateway that received it
	At          time.Time
	// Part is which part of a long message the receipt is about; 0 = the
	// whole message or not known.
	Part int
	// Source is who made the receipt (hot/0072): SourceVendor or
	// SourceSystem; "" = vendor, but a `policy:` row is system.
	Source string
	// Finalize: the receipt closed its message (it took the closing mark),
	// so its row is the message's final one (final_of) unless another
	// receipt's row is already. The status it closes with is FinalStatus
	// when set — a per-vendor rule renamed the outcome — else Status.
	Finalize    bool
	FinalStatus string
	// Recorded, on a row given back as owed: it closes a message whose
	// record is not written yet, and waits for it, whole (the logs are
	// written once: nothing of it is written meanwhile). A row given back
	// without it was refused.
	Recorded bool
	// RecordedOnly: the message was final already when the receipt came, so
	// the client was not sent it. Its row keeps no price: a receipt the
	// client never got is not charged (go-live test 18 Oct 2026).
	RecordedOnly bool
	// What this receipt booked, as rate version ids (issue 16): the receipt
	// fee's selling rate and the vendor's receipt cost; 0 = not booked. FX
	// is the fee's conversion into the company's currency.
	SellRateID, BuyRateID int64
	FX                    float64
	// Charge is what an older build put here. Read by nothing now.
	Charge string
	// Ref is the caller's own mark for the row; it comes back untouched on a
	// row that is still owed.
	Ref int
}

// ClosingStatus is the status this receipt closes its message with.
func (r ReceiptRow) ClosingStatus() string {
	if r.FinalStatus != "" {
		return r.FinalStatus
	}
	return r.Status
}

// Closes reports whether this receipt is to close its message.
func (r ReceiptRow) Closes() bool {
	return r.Finalize && model.Status(r.ClosingStatus()).IsTerminal()
}

// Rows that used to be written by the role that met them, and are now tasks
// of the record queue in worker mode (Saleem, 28 Oct 2026: with the DB worker
// every hot-store log and summary goes through the queue): orphans, orphans
// matched, and lookups with no message. The DB worker writes many at once;
// the one-row methods (WriteOrphanMO, WriteOrphanDLR, MarkOrphanMatched,
// WriteLookup) are these with one row, for a gateway without the worker.
//
// A row carries the moment it happened (At), so a task written late, or
// twice, still says when — and "twice" can be told: once=true (a task that
// may be on a list already) leaves out a row that is there already.

// OrphanRow is one orphan: an incoming message no route owns (Kind "mo") or a
// receipt that matched no message (Kind "dlr").
type OrphanRow struct {
	Kind        string    `json:"k"`
	VendorID    int64     `json:"v,omitempty"`
	VendorMsgID string    `json:"i,omitempty"`
	Source      string    `json:"s,omitempty"`
	Destination string    `json:"d,omitempty"`
	Body        string    `json:"b,omitempty"`
	Encoding    string    `json:"e,omitempty"`
	MCC         string    `json:"mc,omitempty"`
	MNC         string    `json:"mn,omitempty"`
	Status      string    `json:"st,omitempty"`
	ErrorCode   string    `json:"ec,omitempty"`
	Raw         string    `json:"r,omitempty"`
	Reason      string    `json:"why,omitempty"`
	At          time.Time `json:"at"`
}

// OrphanMatch notes that a filed receipt was applied after all.
type OrphanMatch struct {
	Table string `json:"t"`
	ID    int64  `json:"o"`
	MsgID string `json:"m,omitempty"`
}

// LookupRow is one lookup that has no message (GET /sms/v4/operator).
type LookupRow struct {
	CompanyID   int64     `json:"c,omitempty"`
	AccountID   int64     `json:"a,omitempty"`
	Destination string    `json:"d"`
	Source      string    `json:"s,omitempty"`
	MCC         string    `json:"mc,omitempty"`
	MNC         string    `json:"mn,omitempty"`
	Ported      bool      `json:"p,omitempty"`
	At          time.Time `json:"at"`
	// The operator's name and the country calling code the lookup found
	// (6 Nov 2026); "" = not known. An older worker ignores them.
	OperatorName string `json:"on,omitempty"`
	CCC          string `json:"cc,omitempty"`
	// Call is the call the lookup made to its HLR provider (6 Nov 2026,
	// hot/0080); nil = none kept. An older worker ignores it.
	Call *model.LookupCall `json:"call,omitempty"`
}

// LookupCallCols are the columns of an HLR provider's call on a lookup's
// log row (hlr_<company>_<day>, hot/0080), in the order LookupCallArgs gives
// their values.
var LookupCallCols = []string{"method", "url", "req_headers", "req_body", "req_cut", "http_status",
	"resp_headers", "resp_body", "resp_cut", "took_ms", "call_error"}

// LookupCallArgs are c's values for LookupCallCols: NULLs (the cuts 0) when
// no call was kept.
func LookupCallArgs(c *model.LookupCall) []any {
	if c == nil {
		return []any{nil, nil, nil, nil, 0, nil, nil, nil, 0, nil, nil}
	}
	str := func(s string) any {
		if s == "" {
			return nil
		}
		return s
	}
	cut := func(b bool) int {
		if b {
			return 1
		}
		return 0
	}
	var status any
	if c.Status > 0 {
		status = c.Status
	}
	return []any{str(c.Method), str(c.URL), str(c.ReqHeaders), str(c.ReqBody), cut(c.ReqCut), status,
		str(c.RespHeaders), str(c.RespBody), cut(c.RespCut), c.TookMS, str(c.Err)}
}

// AwaitingReceipt is one message an account's no_receipt rule may have to
// answer for: sent to a vendor, nobody told the client anything, and no
// receipt has arrived since.
type AwaitingReceipt struct {
	MsgID              string
	CompanyID          int64
	Sender             string
	Destination        string
	BindSlot           int
	NoReceipt          bool
	FailureReceiptOnly bool
	Owner              string // whose queue it was sent from (D-121)
	// The vendor that took it and its id for it (AwaitingReceiptInDays):
	// with the receipt facts in Redis, the no-receipt rule closes the
	// message's facts by them.
	VendorID    int64
	VendorMsgID string
}

// AwaitCursor is how far the no-receipt sweep has read one account's
// messages in open_msgs: the last one read, by when it was sent.
type AwaitCursor struct {
	SentAt time.Time
	MsgID  string
}

// PendingDLR is one message awaiting a poll-mode status answer. AccountID is
// carried so the poll can be written to the owning account's flow capture.
type PendingDLR struct {
	MsgID       string
	VendorMsgID string
	AccountID   int64
	CompanyID   int64
}

// RecordTable is one of the hot store's record tables, as listed.
type RecordTable = billingstore.RecordTable

// ArchiveResult is what one archive of a record table did.
type ArchiveResult struct {
	Rows       int64 // hot rows found the same in cold (the table's rows when it was dropped)
	Refreshed  int64 // rows cold already had, written again with hot's newer values
	Added      int64 // rows cold did not have
	Renumbered bool  // the hot table was made again after its month moved: its ids were moved above cold's
}

// ---- what the record store keeps, by what it is ----

// AcceptedRecords is an outgoing message's record at acceptance (mt_) and
// the vendor's answers to its sends (sub_): the three-table design, 27 Oct
// 2026.
type AcceptedRecords interface {
	WriteAccepted(ctx context.Context, msgs []model.Message) error
	WriteAnswers(ctx context.Context, as []model.SubmitAnswer) error
}

// RejectRecords is the refused submits (rejects_). The Once form leaves out
// a row that is there already (a task that may have been written before).
type RejectRecords interface {
	WriteRejected(ctx context.Context, m model.Message) error
	WriteRejectedBatch(ctx context.Context, ms []model.Message) error
	WriteRejectedBatchOnce(ctx context.Context, ms []model.Message) error
}

// SubmitRecords is what a vendor role records of a message it sent: the
// record with its outcome, many in one pass, and its status changes.
type SubmitRecords interface {
	Write(ctx context.Context, m model.Message) error
	WriteBatch(ctx context.Context, recs []TerminalRecord) error
	WriteTerminalBatch(ctx context.Context, recs []TerminalRecord) error
	// WriteRated is WriteTerminalBatch (receiptRow) or WriteBatch for
	// records that may carry their price.
	WriteRated(ctx context.Context, recs []TerminalRecord, receiptRow bool) ([]RatedOutcome, error)
	UpdateStatus(ctx context.Context, ref model.RecordRef, st model.Status, vendorMsgID, errorCode string) error
	// FinalizeOnce gives a message its final status only if it has none
	// yet; true = this call did it.
	FinalizeOnce(ctx context.Context, ref model.RecordRef, st model.Status, vendorMsgID, errorCode string) (bool, error)
}

// ReceiptMatching finds the message a vendor's receipt is about, and what
// the receipt path needs to know of it.
type ReceiptMatching interface {
	ResolveVendorMsgID(ctx context.Context, vendorID int64, vendorMsgID string) (msgID string, accountID int64, noReceipt, failureOnly bool, bindSlot int, privacy model.Privacy, mcc, mnc string, ok bool, err error)
	ResolveVendorMsgIDAll(ctx context.Context, vendorID int64, vendorMsgID string) (msgIDs []string, accountID int64, noReceipt, failureOnly bool, bindSlot int, privacy model.Privacy, mcc, mnc string, err error)
	ResolveReceipt(ctx context.Context, vendorIDs []int64, vendorMsgID, number string) (model.ReceiptMatch, error)
	WantsIntermediate(ctx context.Context, ref model.RecordRef) (bool, error)
	MessageQueueOwner(ctx context.Context, ref model.RecordRef) (string, error)
	MessageAddresses(ctx context.Context, ref model.RecordRef) (string, string, error)
	RecordState(ctx context.Context, ref model.RecordRef) (status string, found bool, err error)
	BillNote(ctx context.Context, ref model.RecordRef) (string, error)
}

// ReceiptRecords is the receipts' rows (dlr_) and the close of the message
// a receipt ends. The Once forms leave out a row that is there already.
type ReceiptRecords interface {
	AppendRawDLR(ctx context.Context, ref model.RecordRef, entryJSON string) error
	AppendRawDLROnce(ctx context.Context, ref model.RecordRef, entryJSON string) error
	CloseWithReceipt(ctx context.Context, ref model.RecordRef, st model.Status, vendorMsgID, errorCode, entryJSON string, mayClose bool) (mine bool, err error)
	CloseWithReceiptOnce(ctx context.Context, ref model.RecordRef, st model.Status, vendorMsgID, errorCode, entryJSON string, mayClose bool) (mine bool, err error)
	// CloseWithReceipts closes many messages with their receipts, one
	// transaction a message table (no Redis).
	CloseWithReceipts(ctx context.Context, rs []model.ReceiptClose) (out []model.ReceiptClosed, err error)
	// WriteReceiptBatch writes receipts finished in Redis: as much of a
	// batch as it can, and what is still owed.
	WriteReceiptBatch(ctx context.Context, rows []ReceiptRow) (owed []ReceiptRow, err error)
	WriteReceiptBatchOnce(ctx context.Context, rows []ReceiptRow) (owed []ReceiptRow, err error)
}

// ConcatParts is the parts of a long message sent apart, each with its own
// vendor id, until its receipts are all in (concat_parts).
type ConcatParts interface {
	RecordConcatPart(ctx context.Context, vendorID int64, vendorMsgID string, ref model.RecordRef, seq, total int) error
	MarkPartAndAggregate(ctx context.Context, vendorID int64, vendorMsgID string, st model.Status) (agg model.Status, ready, ok bool, err error)
	// MarkFirstPartFinal is MarkPartAndAggregate for a vendor whose first
	// final closes a long message (dlr_config.first_receipt_closes): ready
	// for the group's first final part, with that part's status.
	MarkFirstPartFinal(ctx context.Context, vendorID int64, vendorMsgID string, st model.Status) (agg model.Status, ready, ok bool, err error)
	FoldConcatParts(ctx context.Context, ref model.RecordRef) error
	PruneConcatParts(ctx context.Context, olderThanHours, limit int) (int, error)
}

// AwaitingReceipts is the messages sent and waiting for a receipt: open_msgs
// with no Redis, else read from the days' tables (AwaitDays).
type AwaitingReceipts interface {
	AwaitingReceiptOlderAfter(ctx context.Context, accountID int64, olderThan time.Duration, after AwaitCursor, limit int) ([]AwaitingReceipt, AwaitCursor, error)
	AwaitingReceiptInDays(ctx context.Context, companyID, accountID int64, olderThan time.Duration, after AwaitCursor, limit, days int) ([]AwaitingReceipt, AwaitCursor, error)
	SentAwaitingDLRPage(ctx context.Context, vendorID int64, after string, limit int) ([]PendingDLR, error)
	SentAwaitingDLRInDays(ctx context.Context, vendorID int64, after string, limit, days int) ([]PendingDLR, error)
	SweepOpenMessages(ctx context.Context, limit int) (int64, error)
}

// Orphans is the incoming messages no route owns and the receipts that
// matched no message, and their later matches.
type Orphans interface {
	WriteOrphanMO(ctx context.Context, vendorID int64, source, destination, body, enc, mcc, mnc, reason string) error
	WriteOrphanDLR(ctx context.Context, vendorID int64, vendorMsgID, status, errorCode, raw, reason string) error
	WriteOrphans(ctx context.Context, rows []OrphanRow, once bool) error
	PendingOrphanDLRs(ctx context.Context, since time.Time, limit int) ([]model.OrphanDLR, error)
	MarkOrphanMatched(ctx context.Context, table string, id int64, msgID string) error
	MarkOrphansMatched(ctx context.Context, ms []OrphanMatch) error
	MarkOrphansPlaced(ctx context.Context, orphans []model.OrphanDLR) error
	PruneOrphansMatched(ctx context.Context, days int) (int64, error)
}

// Lookups is the number lookups made with no message.
type Lookups interface {
	// WriteLookup files one; a zero At is now (6 Nov 2026: one row, not
	// seven values, so a field added to the row needs no new signature).
	WriteLookup(ctx context.Context, r LookupRow) error
	WriteLookups(ctx context.Context, rows []LookupRow, once bool) error
}

// MORecords is the incoming messages (mo_), many in one go, and every try
// to give them — and receipts — to their clients (mo_sub_, dlr_sub_; D-140).
// Nothing of an MO's row is ever changed: what became of it is its final
// mo_sub_ row. once: a message or try already there is left out.
type MORecords interface {
	WriteMOs(ctx context.Context, ms []model.Message, once bool) error
	ClientTries
}

// RecordStatus is a message's status as GET /messages/{id} and the client
// receipts read it.
type RecordStatus interface {
	StatusByID(ctx context.Context, ref model.RecordRef) (MsgStatus, bool, error)
	// StatusByIDFresh reads where the record is written, not on a read
	// instance.
	StatusByIDFresh(ctx context.Context, ref model.RecordRef) (MsgStatus, bool, error)
	StatusByReference(ctx context.Context, accountID, companyID int64, ref string) (MsgStatus, bool, error)
	ClientRefsFor(ctx context.Context, refs []model.RecordRef) (map[string]string, error)
	// ReadsApart says the store's reads go to a read instance of its own.
	ReadsApart() bool
}

// RecordTables is the record tables as retention keeps them: listed, and
// moved to the cold database.
type RecordTables interface {
	ListRecordTables(ctx context.Context) ([]RecordTable, error)
	EnsureColdSchema(ctx context.Context, coldDB string) error
	ArchiveTableToCold(ctx context.Context, coldDB, table string) (ArchiveResult, error)
}

// RecordStore is the whole record store on one database.
type RecordStore interface {
	AcceptedRecords
	RejectRecords
	SubmitRecords
	ReceiptMatching
	ReceiptRecords
	ConcatParts
	AwaitingReceipts
	Orphans
	Lookups
	MORecords
	RecordStatus
	RecordTables
	// TryLease takes a named lock on the record store's database without
	// waiting (the orphans' re-match takes it); ok=false = held elsewhere.
	TryLease(ctx context.Context, name string) (release func(), ok bool, err error)
	Close() error
}

// RecordStoreOn says which DAL keeps the records: the database
// YAKU_HOT_BACKEND names, else the install's SQL database.
func RecordStoreOn(cfg config.Config) config.Backend {
	if b := norm(cfg.HotBackend); b != "" && b != config.BackendRedis {
		return b
	}
	return SQL(cfg)
}

// ServesRecords says the DAL RecordStoreOn picks is in this binary and keeps
// records (a hot store on Mongo has none).
func ServesRecords(cfg config.Config) bool {
	d, ok := Lookup(RecordStoreOn(cfg))
	return ok && d.RecordStore != nil
}

// OpenRecordStore opens the record store on the DAL RecordStoreOn picks:
// the hot store (YAKU_HOT_DSN), on the process's one pool a database.
func OpenRecordStore(cfg config.Config) (RecordStore, error) {
	on := RecordStoreOn(cfg)
	d, ok := Lookup(on)
	if !ok {
		return nil, fmt.Errorf("dal: no %s DAL in this binary (have %v)", on, Registered())
	}
	if d.RecordStore == nil {
		return nil, fmt.Errorf("dal: record store on %s: %w", on, ErrNotServed)
	}
	s, err := d.RecordStore(cfg)
	if err != nil {
		return nil, fmt.Errorf("dal: record store on %s: %w", on, err)
	}
	return s, nil
}

// ---- what is not database work ----

// openInSQL says the messages waiting for a receipt are kept in open_msgs:
// on an install with no Redis. cmd/yakugate sets it off when Redis is set,
// for every role; a store made anywhere else (tests, tools) keeps them here.
var openInSQL atomic.Bool

func init() { openInSQL.Store(true) }

// SetOpenMessagesInSQL says where the messages waiting for a receipt are
// kept: true = open_msgs (no Redis), false = the receipt facts in Redis.
func SetOpenMessagesInSQL(on bool) { openInSQL.Store(on) }

// OpenMessagesInSQL reports SetOpenMessagesInSQL.
func OpenMessagesInSQL() bool { return openInSQL.Load() }

// AwaitDays is how many days of tables a reader of waiting messages looks at
// for a wait of this long: the days it spans, and today.
func AwaitDays(wait time.Duration) int {
	if wait <= 0 {
		wait = 48 * time.Hour
	}
	return int(wait/(24*time.Hour)) + 2
}

// MTLogTableFor is the table a message is filed in — the name the vendor
// role's own record write uses, so a receipt filed later lands beside it.
func MTLogTableFor(m model.Message) string {
	ref := m.Ref()
	return billingstore.TableOf(billingstore.KindMT, ref.CompanyID, ref.MsgID, time.Now())
}

// PrivacyOf is what is kept of a message's text (plain, masked, encrypted):
// a receipt that arrives later is kept the same way (its text: part repeats
// the start of the message). Read off the body itself rather than carried
// as another flag, so it is right whatever decided to encrypt it.
func PrivacyOf(m model.Message) model.Privacy {
	switch {
	case cry.IsCiphertext(m.Body):
		return model.PrivacyEncrypted
	case m.MaskBody:
		return model.PrivacyMasked
	}
	return model.PrivacyPlain
}

// ---- what a failed record write says about itself ----

// RecordAway says a record write failed for a reason that is not its row's:
// the database could not be reached, was busy, is read-only, lacks a table
// or a grant — anything but an error that names the ROW or a message with no
// record yet (ErrNoRows: the task's own matter, parked and tried again).
// Anything unknown counts as away. The DB worker reads it (internal/recordq);
// the DAL whose error it is says which (Driver.RecordFault).
func RecordAway(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrNoRows) {
		return false
	}
	if mine, row, _ := recordFault(err); mine {
		return !row
	}
	return true
}

// RecordDuplicate reports a duplicate-key error of the database it came from.
func RecordDuplicate(err error) bool {
	_, _, dup := recordFault(err)
	return dup
}

// recordFault asks each DAL whether err is its own.
func recordFault(err error) (mine, row, dup bool) {
	if err == nil {
		return false, false, false
	}
	mu.RLock()
	defer mu.RUnlock()
	for _, d := range drivers {
		if d.RecordFault == nil {
			continue
		}
		if mine, row, dup = d.RecordFault(err); mine {
			return mine, row, dup
		}
	}
	return false, false, false
}
