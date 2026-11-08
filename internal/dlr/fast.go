package dlr

import (
	"context"
	"log"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/billingstore"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/mask"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/receiptpolicy"
	"github.com/yakugate/yakugate/internal/vendorquality"
)

// Receipts in Redis: one SET when the vendor takes a message, one GETDEL when
// its final receipt comes (Saleem, 29 Oct 2026: "an receipt can have only 3
// calls at max, 1 set in redis, 2 read from redis, 3 delete from redis" —
// "even the get and del can go in call").
//
//	at SEND time   the vendor role files what a receipt will need about the
//	               message — Facts — under the vendor's own message id, ONE
//	               key whose value is all of it (FactStore.Put: SET … EX the
//	               vendor's wait, pipelined every few milliseconds);
//	on a FINAL     GETDEL: Redis hands the value to exactly one caller, and
//	               that caller closes the message — the client's receipt goes
//	               to its queue, the database work to the record queue (or
//	               the receipts' own list, rcptw), and what it books is booked.
//	               A second copy of the receipt, or the same receipt on the
//	               other gateway, gets nothing;
//	on a PROGRESS  GET: read only, nothing closes.
//
// A receipt whose key is not there is looked for again 5 s later, three looks
// in all (later.go), and is then filed as an orphan. The record store is not
// asked: no MySQL call on the receipt path (Saleem, 29 Oct 2026). That covers
// a receipt that beat its facts by a flush, one for a message sent before the
// upgrade, a copy of a receipt already applied, a progress word after the
// final, and one under an id the vendor never gave us.
//
// The record store is still the system of record: every receipt row and
// every close ends up there, written by the DB worker or the rcptw drain.

// Facts is what a receipt needs to know about its message.
type Facts struct {
	MsgID            string
	AccountID        int64
	NoReceipt        bool // the client asked to hear nothing
	FailureOnly      bool // the client asked for failures only
	WantIntermediate bool // the client asked for progress notices
	BindSlot         int  // the bind it was submitted on (0 = none)
	Privacy          model.Privacy
	MCC, MNC         string
	Src, Dst         string // as the client submitted them
	Owner            string // the deployment its client is on ("" = shared)
	Table            string // the message's own log table; its receipt table follows from it
	// VendorID is the vendor that SENT it: with a TX and an RX row, not the
	// one its receipt comes in on.
	VendorID int64
	// VendorMsgID is the vendor's id for the message, as filed with it.
	VendorMsgID string
	// Wait is how long the facts are kept: its vendor's wait for a receipt
	// (dlr_config.wait_hours, D-136). 0 = the store's own default.
	Wait time.Duration
	// Group says the vendor's id names more than this message: GroupParts,
	// the parts of one long message sent as one call (one receipt closes
	// them all), or GroupCall, several messages of a bulk call the vendor
	// gave one id (each receipt closes one: the one sent to its number, else
	// the newest). "" = the id names this message alone.
	Group string
	// Bill is what the message's receipt may still book (billing.BillNote,
	// worked out when the vendor took it; issues 3 and 18). "" = nothing.
	Bill string
	// Parts is how many PDUs the message went to its vendor as, when more
	// than one: each has its own vendor id, key and receipt, and the
	// message's status is all of them together (PartMarker). 0 or 1 = one PDU.
	Parts int
	// Part is which of them this vendor id is (1-based).
	Part int
	// FirstCloses: its vendor sends one final receipt per long message, on
	// any one part (dlr_config.first_receipt_closes; Saleem 4 Nov 2026,
	// Telenor Direct and Ozeki). The first part's final closes the message
	// with its own status; every later part's is recorded only.
	FirstCloses bool
	// Told is the status the client was already given in the vendor's place
	// (the account's receipt policy answered at submit, MarkTold); "" = none.
	// The vendor's real receipt is then recorded and not sent on.
	Told string
}

// The kinds of Facts.Group.
const (
	GroupParts = "parts"
	GroupCall  = "call"
)

// Company is the message's customer company, read from its table's name
// (D-136). 0 for facts that do not say.
func (f Facts) Company() int64 {
	if rt, ok := billingstore.ParseRecordTable(f.Table); ok {
		return rt.CompanyID
	}
	return 0
}

// Ref is the record of the message the facts are about.
func (f Facts) Ref() model.RecordRef { return model.RecordRef{MsgID: f.MsgID, CompanyID: f.Company()} }

// FactsKey is the one Redis key of a message's facts: rf:<scope>:<vendor's
// message id>. The scope is the vendor's COMPANY when it has one (Saleem
// 2026-10-14: a carrier's TX and RX rows are two vendors; the receipt comes in
// on the other one), given as minus the company id and written c<company>; a
// vendor with no company keeps its own id.
func FactsKey(scope int64, vendorMsgID string) string {
	if scope < 0 {
		return "rf:c" + strconv.FormatInt(-scope, 10) + ":" + vendorMsgID
	}
	return "rf:" + strconv.FormatInt(scope, 10) + ":" + vendorMsgID
}

// FactStore keeps Facts by scope and vendor message id. The scope is the
// receipt's (Service.scope): the vendor's own id, or minus its company's id.
type FactStore interface {
	// Put files the facts of a message a vendor has just taken (a few
	// milliseconds later, with others), replacing whatever an earlier
	// message left under the same id. A group member is added to the id's
	// group.
	Put(ctx context.Context, scope int64, vendorMsgID string, f Facts) error
	// Get reads them, for a progress receipt: nothing is closed. Of a group,
	// the member sent to number (when it is one of them), else the newest.
	Get(ctx context.Context, scope int64, vendorMsgID, number string) (f Facts, ok bool, err error)
	// Take reads them and deletes them in one step (GETDEL): whoever gets
	// them closes their message, and nobody else can. Of a parts group,
	// every member; of a call group, the member sent to number (else the
	// newest), the others filed again. Nothing = nobody to close.
	Take(ctx context.Context, scope int64, vendorMsgID, number string) ([]Facts, error)
	// PutBack files taken facts again, as they were: a receipt that turned
	// out not to close its message (a vendor rule made its final word a
	// status that is not final).
	PutBack(ctx context.Context, scope int64, vendorMsgID string, fs []Facts) error
}

// FactAsk is one receipt's look in TakeMany.
type FactAsk struct {
	Scope       int64
	VendorMsgID string
	Number      string
	// Close: GETDEL (a final receipt); else GET (a progress receipt).
	Close bool
}

// ManyFacts is an optional FactStore capability: many looks in one round
// trip, answered in the order asked. A group answered by Close is settled as
// Take settles it.
type ManyFacts interface {
	TakeMany(ctx context.Context, asks []FactAsk) ([][]Facts, error)
}

// PartMarker is an optional FactStore capability: the part tracker of a
// message sent as several PDUs (Saleem, 28 Oct 2026: "if there are many parts,
// the last part receiving gateway will process it"). Each part's final adds
// to the message's count in ONE call: any part failed or expired → the
// message fails (the first such part closes it); every part delivered → it
// is delivered (the part that completes the count closes it). ready=true goes
// to exactly one caller; finalAlready to every part after the close.
type PartMarker interface {
	MarkPart(ctx context.Context, msgID string, parts int, st model.Status, life time.Duration) (agg model.Status, ready, finalAlready bool, err error)
}

// FirstPartMarker is the part tracker for a vendor whose first final closes
// a long message (Facts.FirstCloses): ready to the first part's final only,
// finalAlready to every one after it.
type FirstPartMarker interface {
	MarkFirstPart(ctx context.Context, msgID string, life time.Duration) (ready, finalAlready bool, err error)
}

// ReceiptWrite is one receipt's database work, to be done later.
type ReceiptWrite struct {
	MsgID       string `json:"m"`
	AccountID   int64  `json:"a"`
	VendorID    int64  `json:"v"`
	VendorMsgID string `json:"i,omitempty"`
	Table       string `json:"t"`           // the message's log table
	Status      string `json:"s,omitempty"` // the message status the receipt's own word implies
	Stat        string `json:"w,omitempty"` // the receipt's own word
	// FinalStatus is the status the MESSAGE is closed with when Finalize is
	// set. Usually Status; a per-vendor code rule can rename the outcome
	// (failed -> expired), and then the receipt's row keeps what the vendor
	// said while the message takes the renamed status. "" = Status.
	FinalStatus string `json:"fs,omitempty"`
	ErrorCode   string `json:"e,omitempty"`
	Raw         string `json:"r,omitempty"`
	DoneAt      string `json:"d,omitempty"` // the vendor's delivery time (RFC 3339), "" = not given
	Deployment  string `json:"g,omitempty"` // the gateway that received it
	At          int64  `json:"at"`          // when it arrived, Unix milliseconds
	// Finalize says this receipt closed the message: its status is to be
	// written on the record (only if the record is not final already).
	Finalize bool `json:"f,omitempty"`
	// RecordedOnly says the message was final already when this receipt
	// came: its row is kept as evidence, the client was not sent it, and so
	// it is not charged.
	RecordedOnly bool `json:"ro,omitempty"`
	// Owner is the deployment the message's record is queued on (its queue
	// owner, "" = shared): a status that waits for that record waits on
	// that deployment's list (receiptfacts.Queue).
	Owner string `json:"o,omitempty"`
	// Recorded says the receipt closes a message whose record is not
	// written yet: it waits, whole, for the record (receiptfacts.Queue).
	Recorded bool `json:"k,omitempty"`
	// Part is which part of a long message the receipt is about; 0 = the
	// whole message or not known.
	Part int `json:"p,omitempty"`
	// Source is who made the receipt (hot/0072): "vendor" or "system";
	// "" = vendor, but a `policy:` row is system.
	Source string `json:"src,omitempty"`
	// What this receipt booked, as rate version ids (issue 16): the receipt
	// fee's selling rate and the vendor's receipt cost; 0 = not booked.
	SellRate int64 `json:"sr,omitempty"`
	BuyRate  int64 `json:"br,omitempty"`
	// Charge is what an older build put here. Read by nothing now.
	Charge string `json:"cg,omitempty"`
	// Tries counts how often this work was put back, and After (Unix
	// milliseconds) is when it may be tried again.
	Tries int   `json:"n,omitempty"`
	After int64 `json:"nb,omitempty"`
}

// setBooked puts on a receipt's row what it booked, as rate version ids.
func (w *ReceiptWrite) setBooked(bs []billing.Booking) {
	w.SellRate, w.BuyRate = billing.BookedRates(bs)
}

// RecordWriter takes a receipt's database work to be done later, in batches.
// It must not lose what it accepted: the vendor has been acknowledged.
type RecordWriter interface {
	Receipt(ctx context.Context, w ReceiptWrite) error
}

// SetFast gives the service a fact store and a record writer, which together
// let it finish a receipt in Redis. Both or neither: with either one nil
// every receipt takes the record store's path, as a gateway without Redis
// does.
func (s *Service) SetFast(f FactStore, w RecordWriter) {
	if s == nil {
		return
	}
	if f == nil || w == nil {
		s.facts, s.writer = nil, nil
		return
	}
	s.facts, s.writer = f, w
}

// Fast reports whether receipts are finished in Redis.
func (s *Service) Fast() bool { return s != nil && s.facts != nil && s.writer != nil }

// HasFacts says the messages waiting for a receipt are filed in a fact store:
// a vendor role files each one it sends (NoteSent). The same as Fast since
// 29 Oct 2026: a fact store is only ever given with a writer.
func (s *Service) HasFacts() bool { return s.Fast() }

// Pusher is where the service queues a receipt for its client (nil = none):
// for a role that tells a client something the service does not decide
// (internal/giveup).
func (s *Service) Pusher() ClientPusher {
	if s == nil {
		return nil
	}
	return s.pusher
}

// fastCounts: receipts finished in Redis, and receipts that found no facts
// there (looked for again, then orphans).
var fastCounts struct{ fast, full, queued atomic.Int64 }

// FastStats returns the two counts for this process.
func FastStats() (fast, full int64) { return fastCounts.fast.Load(), fastCounts.full.Load() }

// QueuedFromRecord was the count of receipts matched in the record store and
// finished through the queues. Since 29 Oct 2026 the record store matches no
// receipt while the facts are in Redis: always 0 there.
func QueuedFromRecord() int64 { return fastCounts.queued.Load() }

// NoteSent files what a receipt for this message will need. Called by a
// vendor role the moment the vendor's answer names its message id — on the
// goroutine that reads the vendor's answers, so the facts are on their way
// before the receipt behind that answer is read.
func (s *Service) NoteSent(ctx context.Context, vendorID int64, vendorMsgID string, f Facts) {
	if !s.HasFacts() || vendorMsgID == "" || f.MsgID == "" {
		return
	}
	if f.VendorID == 0 {
		f.VendorID = vendorID
	}
	if f.VendorMsgID == "" {
		f.VendorMsgID = vendorMsgID
	}
	if err := s.facts.Put(ctx, s.scope(vendorID), vendorMsgID, f); err != nil {
		noteFastTrouble("could not file the facts of %s for its receipt (%v)", f.MsgID, err)
	}
}

// fastTroubleSaid rate-limits the log line below: a Redis that is down would
// otherwise write one per message.
var fastTroubleSaid atomic.Int64

func noteFastTrouble(format string, a ...any) {
	now := time.Now().UnixNano()
	last := fastTroubleSaid.Load()
	if now-last < int64(10*time.Second) || !fastTroubleSaid.CompareAndSwap(last, now) {
		return
	}
	log.Printf("[dlr] "+format, a...)
}

// closingWord says whether a receipt's word is a final one, read with the
// map of the row it came in on (the sending vendor is not known before its
// facts are read): it decides between GETDEL and GET.
func (s *Service) closingWord(r ReceiptIn) bool {
	return ParseStat(r.RawStatus, s.mapFor(r.StatusMap, 0, r.VendorID)).Final()
}

// applyRedis is one receipt's path with the facts in Redis: look (GETDEL for
// a final word, GET for a progress word) under each form of the vendor's id
// the receipt carries, and finish it with what was found. Found nothing: it
// is looked for again lookAgain later (look counts the looks, 1 = the
// first), and after the last look filed as an orphan.
func (s *Service) applyRedis(ctx context.Context, r ReceiptIn, look int) (bool, error) {
	if r.VendorMsgID == "" {
		r.VendorMsgID, r.AltID = r.AltID, ""
	}
	if r.VendorMsgID == "" {
		return false, nil
	}
	k := receiptKey{vendorID: r.VendorID, id: r.VendorMsgID, alt: r.AltID, number: r.Number}
	scope := s.scope(r.VendorID)
	closing := s.closingWord(r)
	flushed := false
again:
	for _, cand := range k.candidates() {
		var fs []Facts
		var err error
		if closing {
			fs, err = s.facts.Take(ctx, scope, cand, r.Number)
		} else {
			var f Facts
			var ok bool
			f, ok, err = s.facts.Get(ctx, scope, cand, r.Number)
			if ok {
				fs = []Facts{f}
			}
		}
		if err != nil {
			// Redis cannot be read: the receipt is kept and looked for again,
			// its look not counted — nothing of it was done.
			noteFastTrouble("the receipt facts could not be read (%v) — receipts are kept and looked up again", err)
			return false, s.lookLater(r, look, true)
		}
		if len(fs) > 0 {
			return s.handleTaken(ctx, r, cand, fs, closing)
		}
	}
	// This process may hold the facts still, waiting for their flush (a
	// vendor that receipts the moment it answers): filed now, and looked for
	// once more at once — not 5 s from now.
	if fl, ok := s.facts.(pendingFacts); ok && !flushed && fl.Unfiled() > 0 {
		fl.Flush(ctx)
		flushed = true
		goto again
	}
	fastCounts.full.Add(1)
	if look < lookTries {
		return false, s.lookLater(r, look+1, false)
	}
	return false, s.fileOrphan(ctx, r, "no message with this vendor_msg_id")
}

// pendingFacts is an optional FactStore capability: facts handed to Put and
// not filed yet (receiptfacts.Store files them every few milliseconds).
type pendingFacts interface {
	Unfiled() int
	Flush(ctx context.Context)
}

// handleTaken finishes a receipt with the facts its look found. closing says
// they were TAKEN (GETDEL): this receipt holds their close.
func (s *Service) handleTaken(ctx context.Context, r ReceiptIn, found string, fs []Facts, closing bool) (bool, error) {
	scope := s.scope(r.VendorID)
	if !closing {
		// Read with the sending vendor's words a progress word may turn out
		// final (an RX row with no map of its own): the facts are taken now.
		sender := fs[0].VendorID
		if sender == 0 {
			sender = r.VendorID
		}
		if ParseStat(r.RawStatus, s.mapFor(r.StatusMap, sender, r.VendorID)).Final() {
			taken, err := s.facts.Take(ctx, scope, found, r.Number)
			if err != nil {
				noteFastTrouble("the receipt facts could not be read (%v) — receipts are kept and looked up again", err)
				return false, s.lookLater(r, 1, true)
			}
			if len(taken) == 0 {
				return false, s.fileOrphan(ctx, r, "no message with this vendor_msg_id")
			}
			fs, closing = taken, true
		}
	}
	var firstErr error
	for _, f := range fs {
		d := s.decide(ctx, r, found, f, closing)
		if err := s.fileDone(ctx, d); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return true, firstErr
}

// done is what a receipt leaves behind for one message, decided before
// anything is filed.
type done struct {
	f      Facts
	r      ReceiptIn
	scope  int64
	found  string // the form of the vendor's id the facts were found under
	sender int64
	st     model.Status
	write  ReceiptWrite
	push   *Receipt
	bs     []billing.Booking
	// tell is the account's receipt policy answering in the vendor's place:
	// told after the receipt's own work is filed.
	tell *receiptpolicy.Outcome
	// closed: this receipt closed the message (its verdicts follow).
	closed bool
	// putBack: the facts were taken and the receipt does not close the
	// message — they go back.
	putBack bool
}

// decide is the check a receipt goes through once its message's facts are
// in hand, and what it leaves behind. closing says this receipt took them.
func (s *Service) decide(ctx context.Context, r ReceiptIn, found string, f Facts, closing bool) *done {
	d := &done{f: f, r: r, scope: s.scope(r.VendorID), found: found}
	// The vendor that SENT the message (groups.go): its receipts, verdicts,
	// quality and code rules these are. r.VendorID is the row it came in on.
	d.sender = f.VendorID
	if d.sender == 0 {
		d.sender = r.VendorID
	}
	stat := ParseStat(r.RawStatus, s.mapFor(r.StatusMap, d.sender, r.VendorID))
	st := stat.Record()
	overrideAction := ""
	if s.overrides != nil && r.ErrorCode != "" {
		if rule, ok := s.overrides.DLR(d.sender, r.ErrorCode); ok {
			overrideAction = rule.Action
			if rule.TargetStatus != "" {
				st = model.Status(rule.TargetStatus)
			}
		}
	}
	fastCounts.fast.Add(1)
	monitor.Verdicts.Receipt(d.sender, true)
	raw := r.Raw
	switch f.Privacy {
	case model.PrivacyMasked:
		raw = mask.ReceiptText(raw, maskMinDigits())
	case model.PrivacyEncrypted:
		raw = s.sealReceiptText(ctx, f.AccountID, trunc(raw, 512)) // its text: part is sealed
	}
	at := time.Now()
	if !r.Arrived.IsZero() {
		at = r.Arrived // the receipt's row says when it ARRIVED
	}
	d.write = ReceiptWrite{
		MsgID: f.MsgID, AccountID: f.AccountID, VendorID: d.sender, VendorMsgID: found, Table: f.Table,
		Status: string(stat.Record()), Stat: string(stat), ErrorCode: r.ErrorCode, Raw: trunc(raw, 512),
		DoneAt: doneText(r.DoneAt), Deployment: deployment.ID(), At: at.UnixMilli(), Owner: f.Owner,
	}
	// A word we do not know: the receipt is kept, the message left as it is.
	if st == "" {
		logging.New("dlr").Warn(logging.SystemScope, "dlr_unknown_status",
			"vendor sent a delivery status we do not recognise; message left unchanged, the receipt kept",
			logging.Fields{"vendorId": r.VendorID, "vendorMsgId": found,
				"rawStatus": r.RawStatus, "errorCode": r.ErrorCode, "messages": []string{f.MsgID}})
		d.write.Status = ""
		d.putBack = closing
		return d
	}
	if c, on := logging.ActiveForAccountID(f.AccountID); on {
		logging.CaptureTo(c, logging.New("dlr"), "receipt", "receipt from vendor", logging.Fields{
			"vendorId": r.VendorID, "vendorMsgId": found, "matchedAs": found,
			"rawStatus": r.RawStatus, "mappedStatus": string(st), "errorCode": r.ErrorCode,
			"vendorRule": overrideAction,
			"raw":        raw,
			"messages":   []string{f.MsgID},
		})
	}
	// A progress receipt moves nothing: kept, and forwarded when the account
	// and the client both want it — and not after a final answer the client
	// was given in the vendor's place.
	if !stat.Final() {
		d.putBack = closing
		levels := s.levelsFor(f.AccountID)
		if s.pusher == nil || f.NoReceipt || f.Told != "" || levels == LevelsFinalsOnly ||
			(levels != LevelsAll && !f.WantIntermediate) {
			return d
		}
		d.push = &Receipt{MsgID: f.MsgID, Status: st, Stat: stat, ErrCode: r.ErrorCode,
			Src: f.Src, Dst: f.Dst, BindSlot: f.BindSlot, DoneAt: r.DoneAt, Owner: f.Owner,
			FactsKey: FactsKey(d.scope, found)}
		d.bs, _ = s.noticeBill(f.Bill, f.MsgID, f.Company(), f.AccountID, d.sender)
		d.write.setBooked(d.bs)
		return d
	}
	// A final word a vendor rule moved to a status that is not final: the
	// receipt's row, and the message stays open — its facts go back.
	if !st.IsTerminal() {
		d.putBack = closing
		return d
	}
	// A part of a message sent as several PDUs: its row, until the part that
	// completes the message — which closes it with the status of all of them.
	if f.Parts > 1 {
		d.write.Part = f.Part
		var agg model.Status
		var ready, already bool
		var err error
		if fm, ok := s.facts.(FirstPartMarker); ok && f.FirstCloses {
			// Its vendor sends one final per long message: the first closes it.
			agg = st
			ready, already, err = fm.MarkFirstPart(ctx, f.MsgID, f.Wait)
		} else if pm, ok := s.facts.(PartMarker); ok {
			agg, ready, already, err = pm.MarkPart(ctx, f.MsgID, f.Parts, st, f.Wait)
		} else {
			return d
		}
		if err != nil {
			noteFastTrouble("the part tracker could not be asked (%v) — the part's receipt is kept, the message stays open", err)
			return d
		}
		// The account that asked for each part's receipt (part_receipts =
		// 'each', 4 Nov 2026) is sent this part's own word now, on every part
		// id, whether or not it closes the message; the close below sends its
		// own as before.
		if (already || !ready) && s.partToClient(f.AccountID, f.Told, f.NoReceipt, f.FailureOnly, st, r.RawStatus, r.ErrorCode) {
			d.push = &Receipt{MsgID: f.MsgID, Status: st, Stat: statSent(stat, st), ErrCode: r.ErrorCode,
				Src: f.Src, Dst: f.Dst, BindSlot: f.BindSlot, DoneAt: r.DoneAt, Owner: f.Owner}
		}
		if already {
			d.write.RecordedOnly = true
			return d
		}
		if !ready {
			return d // the part's row; the message stays open
		}
		if string(agg) != d.write.Status {
			d.write.FinalStatus = string(agg)
		}
		st = agg
	} else if string(st) != d.write.Status {
		d.write.FinalStatus = string(st) // a vendor rule renamed the outcome
	}
	// This receipt closes the message.
	d.write.Finalize = true
	d.closed = true
	d.st = st
	toClient := false
	if f.Told == "" {
		if pol := s.policyFor(f.AccountID); pol != nil {
			if a, rule := pol.ForReceiptRule(st, r.RawStatus, r.ErrorCode); a != nil {
				o := receiptpolicy.Decide(a, rule, nil)
				if o.Err == "" {
					// The rule said nothing about the code: the vendor's goes on.
					o.Err = r.ErrorCode
				}
				if o.Final() {
					d.tell = &o
				}
			}
		}
		if d.tell == nil && s.pusher != nil && model.WantsTerminalReceipt(f.NoReceipt, f.FailureOnly, st) {
			toClient = true
			d.push = &Receipt{MsgID: f.MsgID, Status: st, Stat: statSent(stat, st), ErrCode: r.ErrorCode,
				Src: f.Src, Dst: f.Dst, BindSlot: f.BindSlot, DoneAt: r.DoneAt, Owner: f.Owner}
		}
	}
	// What its final books (billing.go): the receipt fee when the client is
	// sent THIS receipt, a message charged on delivery, the vendor's sides.
	d.bs, _ = s.finalBill(f.Bill, f.MsgID, f.Company(), f.AccountID, d.sender, st, toClient, true)
	d.write.setBooked(d.bs)
	return d
}

// fileDone files what decide decided: the database work and the client's
// receipt (file), then what follows once they are filed. Work that cannot be
// filed is held and filed again (hold.go): its facts were taken, so nothing
// else will finish it.
func (s *Service) fileDone(ctx context.Context, d *done) error {
	if d.putBack {
		if err := s.facts.PutBack(ctx, d.scope, d.found, []Facts{d.f}); err != nil {
			noteFastTrouble("taken receipt facts could not be put back (%v): %s", err, d.f.MsgID)
		}
	}
	if queued, err := s.file(ctx, d.f.AccountID, d.write, d.push); err != nil {
		// The record work is in and only the client's receipt was refused:
		// that is what is filed again, not the work a second time.
		again := func(ctx context.Context) error {
			_, err := s.file(ctx, d.f.AccountID, d.write, d.push)
			return err
		}
		if queued && d.push != nil {
			again = func(ctx context.Context) error { return s.pusher.PushToClient(ctx, d.f.AccountID, *d.push) }
		}
		if herr := s.hold("the receipt of "+d.f.MsgID, err, func(ctx context.Context) error {
			if err := again(ctx); err != nil {
				return err
			}
			s.afterFiled(ctx, d)
			return nil
		}); herr != nil {
			return herr
		}
		return nil
	}
	s.afterFiled(ctx, d)
	return nil
}

// afterFiled is what follows a receipt's filing: what it books, the account
// policy's own answer, and the message's verdicts.
func (s *Service) afterFiled(ctx context.Context, d *done) {
	s.book(ctx, d.f.Company(), d.bs)
	if d.write.RecordedOnly {
		logging.New("dlr").Info(logging.SystemScope, "dlr_late",
			"the message was already final, so this receipt is recorded only",
			logging.Fields{"messageId": d.f.MsgID, "vendorId": d.r.VendorID, "vendorMsgId": d.found,
				"vendorStatus": d.write.Status, "errorCode": d.r.ErrorCode})
	}
	if !d.closed {
		return
	}
	if d.f.Told != "" {
		logging.New("dlr").Info(logging.SystemScope, "dlr_not_pushed",
			"the client was already told about this message, so the vendor's receipt is recorded only",
			logging.Fields{"messageId": d.f.MsgID, "toldStatus": d.f.Told,
				"vendorStatus": string(d.st), "errorCode": d.r.ErrorCode})
	}
	if d.tell != nil {
		f := d.f
		s.Tell(ctx, Told{MsgID: f.MsgID, AccountID: f.AccountID, CompanyID: f.Company(), Src: f.Src, Dst: f.Dst,
			BindSlot: f.BindSlot, NoReceipt: f.NoReceipt, FailureReceiptOnly: f.FailureOnly,
			Owner: f.Owner, Bill: f.Bill, VendorID: d.sender}, *d.tell)
	}
	monitor.Verdicts.Final(d.sender, d.f.AccountID, string(d.st), d.f.MsgID)
	vendorquality.Note(d.sender, d.f.MCC, d.f.MNC, string(d.st))
}

// fileOrphan files a receipt that matched nothing after its last look.
func (s *Service) fileOrphan(ctx context.Context, r ReceiptIn, reason string) error {
	monitor.Verdicts.Receipt(r.VendorID, false)
	rec, ok := s.orphans()
	if !ok {
		return nil
	}
	raw := r.Raw
	if err := rec.WriteOrphanDLR(ctx, r.VendorID, r.VendorMsgID, r.RawStatus, r.ErrorCode, raw, reason); err != nil {
		// Not an error the vendor sees: the orphan's filing waits and is done
		// when the store takes it.
		if herr := s.hold("the receipt "+r.VendorMsgID, err, func(ctx context.Context) error {
			return rec.WriteOrphanDLR(ctx, r.VendorID, r.VendorMsgID, r.RawStatus, r.ErrorCode, raw, reason)
		}); herr != nil {
			log.Printf("[dlr] orphan receipt vendor=%d id=%s not recorded: %v", r.VendorID, r.VendorMsgID, herr)
		}
	}
	return nil
}

// file puts a matched receipt's outputs where they go: its database work in
// the queue the record store is written from, and — when the client is to
// have one — the client's receipt in its queue.
//
// Both in ONE step when the pusher can (BothPusher: the two queues are in the
// same Redis, with the DB worker on), so a stop between them cannot leave the
// message final on its record with its client never told. Otherwise the
// record's work first, then the client's receipt, as it always was.
//
// queued says whether the record work is queued (or kept to be): false with
// an error means nothing of the receipt went anywhere.
func (s *Service) file(ctx context.Context, accountID int64, write ReceiptWrite, toClient *Receipt) (queued bool, err error) {
	if toClient != nil {
		if both, ok := s.pusher.(BothPusher); ok && both.OneStep() {
			err := both.PushWithWrite(ctx, accountID, *toClient, write)
			return err == nil, err
		}
	}
	if err := s.writer.Receipt(ctx, write); err != nil {
		return false, err
	}
	if toClient != nil {
		return true, s.pusher.PushToClient(ctx, accountID, *toClient)
	}
	return true, nil
}
