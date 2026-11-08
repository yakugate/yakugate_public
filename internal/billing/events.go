package billing

import (
	"encoding/json"
	"hash/fnv"
	"strconv"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
)

// Money is booked from events (issues 3 and 18; Saleem, 26 Oct 2026).
//
// "we need to retire the billing process, no sweeping at all, buying selling
// events go into queue and all is done there, both bills and invoices are
// generated from there." Every amount is an Event, made at the moment it is
// decided — a balance deduction, or for postpaid wherever the amount applies —
// by the process that decides it. A prepaid event moves the live balance and
// is queued in ONE Redis step (Booker); each deployment's db-worker adds a
// batch of events up into usage_buckets (internal/billingq). A row's
// 15-minute bucket is the event's moment.
//
// One event carries every side decided at its moment: SELL (what the customer
// pays) at the account's charge point, BUY (what we pay the vendor) at the
// vendor's charge point. When the two points meet — say an account charged on
// submit and a vendor that charges on acceptance — it is one event with both
// ("as we decide/apply both buying and selling at same time then why separate
// event?"); when they do not, each side is its own event at its own moment.

// The kinds of an Event, as usage_buckets.kind holds them.
const (
	EventMT  = KindMT
	EventMO  = KindMO
	EventDLR = KindDLR
	EventHLR = KindHLR
)

// The models of a sell side, as usage_buckets.model holds them.
const (
	ModelPrepaid  = "prepaid"
	ModelPostpaid = "postpaid"
)

// Event is one moment's money for one message (or one receipt, one lookup,
// one incoming message).
type Event struct {
	ID      string `json:"id"` // ULID: unique, so two events never collapse into one queue member
	At      int64  `json:"at"` // the moment it was decided, Unix milliseconds
	Kind    string `json:"k"`  // mt | mo | dlr | hlr
	Msg     string `json:"m"`  // the message id
	Company int64  `json:"co"` // the customer company (both sides)
	Account int64  `json:"a"`  // the customer account
	Vendor  int64  `json:"v"`  // the vendor that carried it, 0 = none (a lookup)
	Items   int64  `json:"i"`  // messages; 0 on a correction of parts
	Parts   int64  `json:"p"`  // message parts
	Sell    *Side  `json:"s,omitempty"`
	Buy     *Side  `json:"b,omitempty"`
}

// Side is one side's amount. Amounts are whole ledger units (ten-thousandths,
// toUnits) so a batch adds up exactly; positive = owed, negative = given back.
type Side struct {
	Units    int64  `json:"u"`
	Currency string `json:"c,omitempty"`
	Model    string `json:"o,omitempty"` // sell only: prepaid | postpaid
	RateID   int64  `json:"r,omitempty"`
	// Conv is how the amount converts (27 Oct 2026); nil = an event made
	// before conversions were kept.
	Conv *Conv `json:"k,omitempty"`
}

// Conv is every conversion of one side, fixed when it is decided (Saleem,
// 27 Oct 2026: card -> company -> base, nothing changes later). 0 = not known.
//
//	sell: Units are in the customer company's currency (Currency); CardCur
//	      is the selling card's; FX card -> company; Base company -> base.
//	buy:  Units are in the buying card's currency (Currency); OwnCur is the
//	      vendor's own company's; FX card -> that company; Base that
//	      company -> base.
type Conv struct {
	CardCur string  `json:"cc,omitempty"`
	OwnCur  string  `json:"oc,omitempty"`
	FX      float64 `json:"fx,omitempty"`
	Base    float64 `json:"b,omitempty"`
}

// With gives a side its conversions (nil-safe both ways).
func (s *Side) With(c *Conv) *Side {
	if s != nil && c != nil {
		cc := *c
		s.Conv = &cc
	}
	return s
}

// SellConvOf is a rating's selling conversions.
func SellConvOf(r Rated) *Conv {
	return &Conv{CardCur: strings.ToUpper(r.Currency), FX: r.SellFX, Base: r.SellBase}
}

// BuyConvOf is a rating's buying conversions.
func BuyConvOf(r Rated) *Conv {
	return &Conv{CardCur: strings.ToUpper(r.BuyCurrency), OwnCur: strings.ToUpper(r.BuyOwnCurrency), FX: r.BuyFX, Base: r.BuyBase}
}

// Amount is the side's amount as a decimal.
func (s *Side) Amount() float64 {
	if s == nil {
		return 0
	}
	return float64(s.Units) / moneyScale
}

// NewEvent starts an event for a message decided now.
func NewEvent(kind, msgID string, company, account, vendor int64) Event {
	return Event{ID: ulid.Make().String(), At: time.Now().UnixMilli(), Kind: kind, Msg: msgID,
		Company: company, Account: account, Vendor: vendor}
}

// SellSide is a sell side of amount (in the company's currency).
func SellSide(amount float64, currency, model string, rateID int64) *Side {
	u := toUnits(amount)
	if u == 0 {
		return nil
	}
	return &Side{Units: u, Currency: strings.ToUpper(currency), Model: model, RateID: rateID}
}

// BuySide is a buy side of amount (in the buying card's currency).
func BuySide(amount float64, currency string, rateID int64) *Side {
	u := toUnits(amount)
	if u == 0 {
		return nil
	}
	return &Side{Units: u, Currency: strings.ToUpper(currency), RateID: rateID}
}

// Empty reports an event with neither side: nothing to book.
func (e Event) Empty() bool { return e.Sell == nil && e.Buy == nil }

// Prepaid is what the event moves on a prepaid balance: its sell side when
// that side is prepaid, else 0. In ledger units; positive = taken.
func (e Event) Prepaid() int64 {
	if e.Sell != nil && e.Sell.Model == ModelPrepaid {
		return e.Sell.Units
	}
	return 0
}

// Negated is the event that takes e back (a hold given back when the message
// never reached the queue): a new id, the same moment, every amount negated.
func (e Event) Negated() Event {
	n := e
	n.ID = ulid.Make().String()
	n.Items, n.Parts = -e.Items, -e.Parts
	if e.Sell != nil {
		s := *e.Sell
		s.Units = -s.Units
		n.Sell = &s
	}
	if e.Buy != nil {
		b := *e.Buy
		b.Units = -b.Units
		n.Buy = &b
	}
	return n
}

// Member is the event as it rides the queue (a sorted-set member).
func (e Event) Member() string {
	b, err := json.Marshal(e)
	if err != nil {
		return ""
	}
	return string(b)
}

// ParseEvent reads a queue member back.
func ParseEvent(s string) (Event, bool) {
	var e Event
	if err := json.Unmarshal([]byte(s), &e); err != nil || e.ID == "" || e.Kind == "" {
		return Event{}, false
	}
	return e, true
}

// BucketMS is the length of a usage bucket.
const BucketMS = int64(15 * time.Minute / time.Millisecond)

// BucketOf is the UTC start of the 15-minute bucket a moment falls in.
func BucketOf(atMS int64) time.Time {
	return time.UnixMilli(atMS - atMS%BucketMS).UTC()
}

// --- the queue's keys -------------------------------------------------------

// Every deployment books its own events (issue 25: "queues:<dep>:<what>:<who>
// :ready|delayed"; since 29 Oct 2026 in the deployment's queue Redis).

// QueueScope is the deployment part of the billing queue's keys: the
// deployment id when it is set, else "shared" (an install that never named
// its deployments has one gateway's queues, as before).
func QueueScope(owner string) string {
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return "shared"
	}
	return owner
}

// The sides of the billing queue (Saleem, 27 Oct 2026: "separate billing
// queues for buying and selling events, for safer distributed traffic").
// An event is still decided once, with every side of its moment (26 Oct);
// the booking puts its sell half on the sell queue and its buy half on the
// buy queue in the same Redis step, and each queue is booked by itself, so
// trouble booking one side never holds up the other.
const (
	SideSell = "sell"
	SideBuy  = "buy"
	// SideMixed is the one queue of the build of 26 Oct, both sides in one
	// event: still booked after an upgrade until it is empty, never written.
	SideMixed = ""
)

// Sides are the queues a booking writes to.
var Sides = []string{SideSell, SideBuy}

// The queue's keys (queues:<scope>:billing:<side>:db:<lane>:ready|delayed|
// doing|doing:id|lock, and …:lanes) are the Redis DAL's since 29 Oct 2026:
// internal/dal/redis billing_queue.go (BillingReadyKey, BillingLanesKey, …).

// DefaultLanes is the billing queue's lanes when YAKU_DB_QUEUE_LANES is not set.
const DefaultLanes = 4

// LaneOf is a company's lane: one company's events are always booked in
// order, by one worker at a time.
func LaneOf(company int64, lanes int) int {
	if lanes < 1 {
		lanes = 1
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(strconv.FormatInt(company, 10)))
	return int(h.Sum32() % uint32(lanes))
}
