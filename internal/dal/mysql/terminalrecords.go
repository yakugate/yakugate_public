package mysql

// A message's record: Write and the batch writers, and how its text is
// kept (masked, never over ciphertext).

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/yakugate/yakugate/internal/billingstore"
	cry "github.com/yakugate/yakugate/internal/crypto"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/mask"
	"github.com/yakugate/yakugate/internal/model"
)

// [SCOPE P1-STORE-HOT] Write files one message's record (D-136): an incoming
// message in mo_<company>_<month>, an outgoing one in mt_<company>_<day>
// with its status, both counted in their summary in the same transaction.
//
// An outgoing record goes in the month the message was ACCEPTED (the time in
// its id), stamped with that time — not the moment it is written: a message
// that waited in a queue must not land in the day it was finally written
// (2026-09-13). It is written exactly as a batch of one (writeBatch), with
// the vendor's answer time now for a message a vendor has just taken.
//
// An incoming message is written when it is taken in and again when its
// client has it; whichever comes second only moves it on, and a final
// status is never moved back (2026-10-12, go-live test P3 B7).
func (s *Store) Write(ctx context.Context, m model.Message) error {
	if m.Direction == model.DirectionMO {
		return s.writeMO(ctx, m)
	}
	return s.writeBatch(ctx, []TerminalRecord{{Msg: m, VendorMsgID: m.VendorMsgID, AckAt: sentNow(m)}}, false, nil)
}

// toldAtSQL stamps told_at only for a message whose client was told something
// the vendor did not say. A plain NULL literal keeps the column empty for the
// overwhelming majority of messages without spending a parameter on it.
func toldAtSQL(m model.Message) string {
	if m.ToldStatus == "" {
		return "NULL"
	}
	return "NOW()"
}

// wireProtocol is the value for mtlog's `protocol` column; the vendor leg
// itself goes in `transport`.
//
// Both writers used to put the leg in protocol and never name transport, so
// every MT row read transport='smpp' — the ENUM's first value, MySQL's
// implicit default for a NOT NULL enum — whatever vendor carried it. And
// protocol never gained 'soap' when transport did (hot/0015), so under strict
// mode a SOAP message could not be recorded at all: P4 sent it, the insert
// failed, the message stayed unacked, and the lease handed it out again.
//
// SOAP travels over HTTP, so http is the true wire protocol for it. Mapping
// here, rather than widening the ENUM, keeps every existing daily clone
// writable today: an ENUM change is not one the clone sync applies on its own.
func wireProtocol(t model.Transport) string {
	if t == "soap" {
		return string(model.TransportHTTP)
	}
	return string(t)
}

// rejectBody is the text of a refused message as it may be KEPT (hot/0057).
//
// The edge that records the refusal says whether to keep it at all
// (Message.KeepRejectBody — it knows the account and its company's privacy:
// never for an encrypted company, never before the account is known). Here
// the same two guards the accepted record has: digit runs hidden for a
// masking company, and never a sealed body (a refusal after sealing — the
// queue write failing — has ciphertext in Body, which says nothing a screen
// could show and must not be stored as if it were text).
func rejectBody(m model.Message) string {
	if !m.KeepRejectBody || m.Body == "" || cry.IsCiphertext(m.Body) {
		return ""
	}
	if m.MaskBody {
		return mask.Digits(m.Body, maskMinDigits())
	}
	return m.Body
}

// sentNow is the vendor's answer time for a record written by Write: now for
// a message a vendor has just taken, zero for every other status.
func sentNow(m model.Message) time.Time {
	if m.Status == model.StatusSent {
		return time.Now()
	}
	return time.Time{}
}

// ackAtArg is a vendor's answer time for mtlog.vendor_ack_at, or NULL when
// the vendor did not take the message. Unix seconds with milliseconds, turned
// into a time by MySQL (FROM_UNIXTIME) exactly as accepted_at beside it is,
// so the two always agree whatever time zone the session is in.
func ackAtArg(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	ms := t.UnixMilli()
	return fmt.Sprintf("%d.%03d", ms/1000, ms%1000)
}

// WriteTerminalBatch writes a batch of records already at their terminal
// status: one multi-row INSERT into mtlog_* (submission, immutable), one
// multi-row INSERT into dlrlog_* (the immediate/simulated terminal receipt —
// blackhole and similar synchronous senders never get a separate async DLR,
// so this IS their one-and-only receipt row), plus one multi-row
// record_index upsert — still ONE pass per batch, same reasoning as before
// this Hadal-reconciliation split (the measured 75 msg/s blackhole ceiling,
// 2026-07-26, from Write(sent)+UpdateStatus(terminal) as 2+3 separate
// statements per message). Safe to run again with the same records (a write
// tried again, a DB worker's task seen twice): the message row is INSERT
// IGNORE, the locator an upsert that never re-opens a final record, and the
// receipt row is left out when it is there already (2026-10-13 — it used to
// land a second time). A message really sent a second time has another
// vendor id and gets its own receipt row. [SCOPE BLACKHOLE/EGRESS-BATCH]
func (s *Store) WriteTerminalBatch(ctx context.Context, recs []TerminalRecord) error {
	return s.writeBatch(ctx, recs, true, nil)
}

// WriteRated is WriteTerminalBatch (receiptRow) or WriteBatch for records
// that may carry their price (TerminalRecord.Rating): the price, the charge
// mark and the billing batch are written in the SAME insert as the record.
// It answers, per message table, which ratings were really written — a
// record that was there already keeps its own — and which unfinished batches
// it met on such records.
func (s *Store) WriteRated(ctx context.Context, recs []TerminalRecord, receiptRow bool) ([]RatedOutcome, error) {
	outs := []RatedOutcome{}
	err := s.writeBatch(ctx, recs, receiptRow, &outs)
	return outs, err
}

// WriteBatch writes many records in one pass exactly as Write writes each:
// the mtlog rows and the record_index rows, and NO receipt row.
//
// For the HTTP vendor role's bulk calls (2026-10-12). One call gave the
// vendor up to 500 messages, and the role then wrote their records one at a
// time — four statements a message, on one worker — so a chunk took seven to
// ten seconds to file and Bulk ran at a third of the speed of single sends.
// The role's single path uses Write, which files no receipt row at submit;
// this is its batch twin, so a message is recorded (and later billed) the
// same whichever way it was sent. VendorMsgID comes from the record; AckAt is
// when the vendor took it (zero for a refusal); ErrorCode is not stored.
func (s *Store) WriteBatch(ctx context.Context, recs []TerminalRecord) error {
	return s.writeBatch(ctx, recs, false, nil)
}

// writeBatch is WriteTerminalBatch, with or without the receipt row.
//
// The three-table design (Saleem and the PO, 27 Oct 2026): a vendor role's
// record write is about a message's SUBMISSION. Its message's mt_ row was
// written at acceptance (made here when it is missing); this write adds the
// vendor's answers (sub_), the submitted count, and the receipt the gateway
// makes at submission — the one that closes a message no vendor kept, with
// why (refused with no tries left, out of tries, validity over), in place of
// the rejects_ row of stage SUBMISSION the first build of 27 Oct wrote. Each table's records are
// written in ONE transaction (writeSubmitGroup), so a stop part way leaves
// nothing half counted. A table's write is run again if MySQL makes it a
// deadlock victim (againIfVictim), or when another writer made one of its
// rows meanwhile (errRaced: read again).
func (s *Store) writeBatch(ctx context.Context, recs []TerminalRecord, receiptRow bool, outs *[]RatedOutcome) error {
	if len(recs) == 0 {
		return nil
	}
	// In id order: several sessions write batches at once, and rows locked in
	// one order by all of them cannot deadlock each other.
	recs = append([]TerminalRecord(nil), recs...)
	sort.Slice(recs, func(i, j int) bool { return recs[i].Msg.ID < recs[j].Msg.ID })
	byTable := map[string][]TerminalRecord{}
	var tables []string
	for _, r := range recs {
		if r.Msg.Priced == nil && r.Rating != nil && r.Rating.Detail != "" {
			r.Msg.Priced = &model.Priced{Detail: r.Rating.Detail}
		}
		t := MTTableOf(r.Msg)
		if _, ok := byTable[t]; !ok {
			tables = append(tables, t)
		}
		byTable[t] = append(byTable[t], r)
	}
	sort.Strings(tables)
	for _, table := range tables {
		group := byTable[table]
		if err := s.ensureRecords(ctx, table); err != nil {
			return err
		}
		write := func() error { return s.writeSubmitGroup(ctx, table, group, receiptRow) }
		var err error
		for try := 0; try < 3; try++ {
			err = againIfVictim(ctx, func() error {
				e := write()
				// A table made with ap columns narrower than the message's
				// (an older template) is widened by the write that needs it,
				// as a queue table is (apwide.go, QA templates API-013).
				if isAPTooLong(e) && s.widenAP(ctx, table) {
					e = write()
				}
				return e
			})
			if !errors.Is(err, errRaced) {
				break
			}
		}
		if err != nil {
			return s.forgetIfMissing(err, table, billingstore.SwapKind(table, billingstore.KindDLR),
				billingstore.SwapKind(table, billingstore.KindSub))
		}
		if outs != nil {
			*outs = append(*outs, RatedOutcome{Table: table, Claimed: map[string]bool{}})
		}
	}
	return nil
}

// ---- GW-19.2 (PO approved 2026-08-27): the remaining summary writers ----
// All follow the proven summary_mt pattern: watermark-scoped, recompute-not-
// increment. Families whose dimensions can MUTATE after insert (rating moves
// card ids) recompute as DELETE+reINSERT per bucket (the GW-21 rule); families
// derived from APPEND-ONLY rows (dlrlog receipts) use whole-bucket upserts —
// their keys can never vanish, so ghosts cannot form.

// ---- The refusals' summary (hot/0061, 2026-10-13) ----
//
// summary_rejects is kept by ADDING. A refusal is written once and never
// changed, so there is nothing to rebuild: the rows that came since the last
// look are read by the table's own id and added to their buckets. Where the
// count has read up to is in summary_cursor and moves in the SAME
// transaction as the counts, so a row is counted once whatever stops between.

// privacyOf is what was kept of a message's text, for its locator row: a
// receipt that arrives later is kept the same way (its text: part repeats
// the start of the message). Read off the body itself rather than carried
// as another flag, so it is right whatever decided to encrypt it.
// bodyEncOf is privacyOf as the record's body_enc column spells it. The
// column has existed since the v2 schema and nothing ever wrote it, so every row
// said plain_text, ciphertext included (I-27, PO 2026-09-17: write the
// real value).
func bodyEncOf(m model.Message) string {
	switch privacyOf(m) {
	case model.PrivacyEncrypted:
		return "all_encrypted"
	case model.PrivacyMasked:
		return "pii_masked"
	}
	return "plain_text"
}

func privacyOf(m model.Message) model.Privacy { return dal.PrivacyOf(m) }

// storedBody is the body as it should be KEPT — masked when the account asked
// for pii_masked (PO 2026-09-08). The message that went to the handset is
// never affected: by the time a record is written the send has already
// happened, and every earlier stage (encoding, segments, dedupe fingerprint,
// spam) saw the real text.
func storedBody(m model.Message) string {
	// Never over ciphertext: masking the digit runs of an encrypted body
	// corrupts it for good (it happened when a company on its own KMS had an
	// account set to mask digits). Encryption already hides the text; which
	// of the two an account gets is one choice to make (open PO ruling).
	if !m.MaskBody || cry.IsCiphertext(m.Body) {
		return m.Body
	}
	return mask.Digits(m.Body, maskMinDigits())
}

// maskMinDigits is the shortest digit run that gets hidden
// (YAKU_MASK_MIN_DIGITS, default 4).
func maskMinDigits() int {
	if v := os.Getenv("YAKU_MASK_MIN_DIGITS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return mask.DefaultMinDigits
}
