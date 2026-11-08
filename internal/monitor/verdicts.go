package monitor

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// Verdicts is delivery truth as each process sees it: what it handed to a
// vendor, and what the vendors' receipts said about it (PO 2026-09-11 — from
// memory, never a database query).
//
// Until now nothing reported delivery at all: "success" meant the vendor took
// the message, not that it arrived. A vendor that accepts everything and
// confirms nothing looked perfect.
//
// A message is usually sent by one process and its receipt applied by another
// (P3 sends, P5 takes the webhook; several instances of each), so no process
// can pair the two. Each publishes its own counts; the Admin API adds every
// instance's together. The EXACT number waiting, which does need pairing, is
// read from the receipt facts' waiting sets in Redis (receiptfacts
// WaitingByVendor): they list exactly the messages a receipt is owed for.
var Verdicts = newVerdicts()

type verdicts struct {
	mu       sync.Mutex
	vendors  map[int64]*verdictTally
	accounts map[int64]*verdictTally
}

// verdictTally is one vendor's or one account's last hour.
type verdictTally struct {
	slots       [hourSlots]verdictSlot
	latency     *HourHist // accept -> delivered, seconds
	lastSent    time.Time
	lastReceipt time.Time
}

type verdictSlot struct {
	start                                           int64
	sent, delivered, failed, expired, receipts, unm int64
}

func newVerdicts() *verdicts {
	return &verdicts{vendors: map[int64]*verdictTally{}, accounts: map[int64]*verdictTally{}}
}

func (v *verdicts) tally(m map[int64]*verdictTally, id int64) *verdictTally {
	t := m[id]
	if t == nil {
		t = &verdictTally{latency: NewHourHist(DeliveryBoundsSec)}
		m[id] = t
	}
	return t
}

func (t *verdictTally) slot(now int64) *verdictSlot {
	start := now - now%hourSlotSec
	s := &t.slots[(now/hourSlotSec)%hourSlots]
	if s.start != start {
		*s = verdictSlot{start: start}
	}
	return s
}

// Sent records messages a vendor ACCEPTED — the submit succeeded and a
// receipt is now owed. Called by the senders (P3, P4).
func (v *verdicts) Sent(vendorID, accountID int64, msgIDs ...string) {
	if len(msgIDs) == 0 {
		return
	}
	now := time.Now()
	v.mu.Lock()
	for _, t := range []*verdictTally{v.tally(v.vendors, vendorID), v.tally(v.accounts, accountID)} {
		t.slot(now.Unix()).sent += int64(len(msgIDs))
		t.lastSent = now
	}
	v.mu.Unlock()
}

// Receipt records that a vendor sent a receipt at all, matched or not. "Last
// receipt N minutes ago" is the alarm for a vendor that has gone quiet, and an
// unmatched receipt still proves the vendor is talking.
func (v *verdicts) Receipt(vendorID int64, matched bool) {
	now := time.Now()
	v.mu.Lock()
	t := v.tally(v.vendors, vendorID)
	s := t.slot(now.Unix())
	s.receipts++
	if !matched {
		s.unm++
	}
	t.lastReceipt = now
	v.mu.Unlock()
}

// Final records a message's verdict — "delivered", "failed" or "expired";
// anything else is ignored. A delivered message also records how long it took
// from acceptance, read off its ULID, so it needs no lookup.
func (v *verdicts) Final(vendorID, accountID int64, status, msgID string) {
	now := time.Now()
	var took float64 = -1
	if status == "delivered" {
		if at, ok := ULIDTime(msgID); ok {
			if d := now.Sub(at).Seconds(); d >= 0 {
				took = d
			}
		}
	}
	v.mu.Lock()
	for _, t := range []*verdictTally{v.tally(v.vendors, vendorID), v.tally(v.accounts, accountID)} {
		s := t.slot(now.Unix())
		switch status {
		case "delivered":
			s.delivered++
			if took >= 0 {
				t.latency.Add(took, now.Unix())
			}
		case "failed":
			s.failed++
		case "expired":
			s.expired++
		default:
			continue
		}
	}
	v.mu.Unlock()
}

// VerdictRow is one vendor's or account's last hour, JSON-ready. Ratio is nil
// when nothing was judged: "no verdicts yet" and "0% delivered" call for
// opposite reactions, and Hadal's "0.00%" could not tell them apart.
type VerdictRow struct {
	ID              int64    `json:"id"`
	Sent            int64    `json:"sent"`
	Delivered       int64    `json:"delivered"`
	Failed          int64    `json:"failed"`
	Expired         int64    `json:"expired"`
	Receipts        int64    `json:"receipts,omitempty"`
	Unmatched       int64    `json:"unmatched,omitempty"`
	Ratio           *float64 `json:"ratio"`
	LastSentAt      string   `json:"last_sent_at,omitempty"`
	LastReceiptAt   string   `json:"last_receipt_at,omitempty"`
	SinceReceiptSec *int64   `json:"since_last_receipt_sec,omitempty"`
	LatencyCounts   []int64  `json:"delivery_latency_counts,omitempty"`
	DeliveryP50Sec  *float64 `json:"delivery_p50_sec,omitempty"`
	DeliveryP95Sec  *float64 `json:"delivery_p95_sec,omitempty"`
	DeliveryP95Over bool     `json:"delivery_p95_over,omitempty"`
}

func (v *verdicts) rows(m map[int64]*verdictTally, now time.Time) []VerdictRow {
	out := make([]VerdictRow, 0, len(m))
	for id, t := range m {
		r := VerdictRow{ID: id}
		for i := range t.slots {
			s := &t.slots[i]
			if now.Unix()-s.start >= hourSlots*hourSlotSec {
				continue
			}
			r.Sent += s.sent
			r.Delivered += s.delivered
			r.Failed += s.failed
			r.Expired += s.expired
			r.Receipts += s.receipts
			r.Unmatched += s.unm
		}
		if judged := r.Delivered + r.Failed + r.Expired; judged > 0 {
			ratio := float64(r.Delivered) / float64(judged)
			r.Ratio = &ratio
		}
		if !t.lastSent.IsZero() {
			r.LastSentAt = t.lastSent.UTC().Format(time.RFC3339)
		}
		if !t.lastReceipt.IsZero() {
			r.LastReceiptAt = t.lastReceipt.UTC().Format(time.RFC3339)
			since := int64(now.Sub(t.lastReceipt).Seconds())
			r.SinceReceiptSec = &since
		}
		counts := t.latency.Counts(now.Unix())
		if p50, _, ok := HistQuantile(DeliveryBoundsSec, counts, 0.50); ok {
			p95, over, _ := HistQuantile(DeliveryBoundsSec, counts, 0.95)
			r.LatencyCounts, r.DeliveryP50Sec, r.DeliveryP95Sec, r.DeliveryP95Over = counts, &p50, &p95, over
		}
		// An entry with nothing in the last hour is history, not news.
		if r.Sent+r.Delivered+r.Failed+r.Expired+r.Receipts == 0 {
			continue
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// section is the "verdicts" block of this process's status, nil when it has
// seen nothing in the last hour (most roles, most of the time).
func (v *verdicts) section() map[string]any {
	now := time.Now()
	v.mu.Lock()
	vend, acct := v.rows(v.vendors, now), v.rows(v.accounts, now)
	v.mu.Unlock()
	if len(vend) == 0 && len(acct) == 0 {
		return nil
	}
	out := map[string]any{
		"window_sec":                hourSlots * hourSlotSec,
		"delivery_latency_bounds_s": DeliveryBoundsSec,
		"vendors":                   vend,
		"accounts":                  acct,
	}
	return out
}

// --- receipts owed, per vendor ---------------------------------------------

// AwaitStat is one vendor's receipts owed: how many messages wait for one,
// and how long the oldest has. NOT published since 29 Oct 2026: it was read
// from the receipt facts' waiting sets in the main Redis, which are gone
// (every count on Monitoring comes from the live length of a queue). Kept for
// the monitoring demo's old reports (sqa/e2e/go/monitordemo).
type AwaitStat struct {
	VendorID     int64 `json:"vendor_id"`
	Waiting      int64 `json:"waiting"`
	OldestAgeSec int64 `json:"oldest_age_sec"`
	GaveUp       int64 `json:"gave_up_last_trim,omitempty"`
}

// DropAwaitSets removes the waiting sets an older build kept until
// 2026-10-15 (await:v:<vendor>, and the set of vendor ids that had one). They
// were a second copy of the receipt facts' waiting sets, and a leakier one:
// kept 72 h whatever the vendor's wait, and filled for vendors that send no
// receipts at all (1.2 million entries on the dev box). The live view's
// DropAwaitSets UNLINKs them: a set of a million members is freed off Redis's
// main thread. Safe to call again: nothing writes them any more.
func DropAwaitSets(ctx context.Context, rdb dal.LiveStatus) (int, error) {
	return rdb.DropAwaitSets(ctx)
}

// ULIDTime is when a message id was minted: a ULID's first ten characters are
// its creation time in milliseconds, Crockford base32. Every message id is
// minted at acceptance, so this is the accept time without a lookup.
func ULIDTime(id string) (time.Time, bool) {
	if len(id) < 10 {
		return time.Time{}, false
	}
	var ms uint64
	for i := 0; i < 10; i++ {
		c := id[i]
		var d byte
		switch {
		case c >= '0' && c <= '9':
			d = c - '0'
		case c >= 'A' && c <= 'Z':
			d = crockford[c-'A']
		case c >= 'a' && c <= 'z':
			d = crockford[c-'a']
		default:
			return time.Time{}, false
		}
		if d == 0xFF {
			return time.Time{}, false
		}
		ms = ms<<5 | uint64(d)
	}
	return time.UnixMilli(int64(ms)), true
}

// crockford maps A..Z to digit values 10..31; I, L, O and U are not digits.
var crockford = func() [26]byte {
	var t [26]byte
	const alphabet = "ABCDEFGHJKMNPQRSTVWXYZ"
	for i := range t {
		t[i] = 0xFF
	}
	for i := 0; i < len(alphabet); i++ {
		t[alphabet[i]-'A'] = byte(10 + i)
	}
	return t
}()
