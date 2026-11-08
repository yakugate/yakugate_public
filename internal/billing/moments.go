package billing

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/monitor"
)

// Moments books the money of the moments a role sees away from the accept
// path (issues 3 and 18; Saleem, 26 Oct 2026): a vendor's answer, a
// message's final, a progress receipt the client was sent, an incoming
// message. It prices what is decided there (the vendor's side from the rate
// cards, the customer's from the message's quote), books it on the billing
// queue (Booker) and gives the record its price, so nothing is priced or
// charged after the fact. One per process; the vendor roles, the receipt
// path, retry, the re-route mover and the mo role share it.
type Moments struct {
	book  *Booker
	terms *Terms
	rater *Rater
	fx    *FXResolver
	logf  func(string, ...any)

	mu       sync.Mutex
	kept     []keptBooking // refused by Redis before it ran: filed again
	keptFrom time.Time
	filing   bool

	booked atomic.Int64 // events booked
	lost   atomic.Int64 // bookings that failed in a way that cannot be retried safely

	// The bookers (bookAll): bookings handed over and not yet booked.
	in        chan handOver
	startOnce sync.Once
	waiting   atomic.Int64
}

// handOver is one moment's bookings handed to the bookers.
type handOver struct {
	company int64
	b       []Booking
}

// keptBooking is one booking that waits for Redis. moved: its balance step
// is done and only its events are left to queue.
type keptBooking struct {
	company int64
	b       Booking
	moved   bool
}

// keptMost is the most bookings kept in memory while Redis is away.
const keptMost = 200000

// NewMoments wires the moments of one process (b from dal.OpenBilling):
// balances on the live counters (the main Redis), the sell and buy queues on
// the deployment's queue, terms and rate cards from the store, read again
// while ctx lives. With no Redis, the balance of record and the queue of the
// SQL database (b.Book, one transaction a booking). No balances to book on
// or no store = no moments.
func NewMoments(ctx context.Context, b *dal.Billing, logf func(string, ...any)) *Moments {
	if !books(b) {
		return nil
	}
	if logf == nil {
		logf = func(string, ...any) {}
	}
	m := newMoments(ctx, b, logf)
	m.rater, m.fx = NewRater(b.Store), NewFXResolver(b.Store).NoWait()
	m.rater.KeepHistory(RateHistory())
	go m.keepRates(ctx)
	return m
}

// NewMomentsBooking is NewMoments for a role that prices nothing itself — it
// books finals and messages no vendor took, from their quotes (the receipt
// role, retry, expiry, the re-route mover): no rate cards are read.
func NewMomentsBooking(ctx context.Context, b *dal.Billing, logf func(string, ...any)) *Moments {
	if !books(b) {
		return nil
	}
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return newMoments(ctx, b, logf)
}

// books says b has somewhere to book on: the live balances (Redis), or the
// SQL database's balance book (no Redis), and the store.
func books(b *dal.Billing) bool {
	return b != nil && b.Store != nil && (b.Live != nil || (b.Book != nil && b.Queue != nil))
}

// bookerOf is the booker of b: on the live balances when it has them (Redis
// set), else on the SQL database's balance book.
func bookerOf(b *dal.Billing) *Booker {
	if b.Live != nil {
		return NewBooker(b.Live, b.Queue, b.Store, deployment.Owner(), LanesFromEnv())
	}
	return NewSQLBooker(b.Book, b.Queue, deployment.Owner(), LanesFromEnv())
}

func newMoments(ctx context.Context, b *dal.Billing, logf func(string, ...any)) *Moments {
	m := &Moments{book: bookerOf(b), terms: NewTerms(b.Store), logf: logf}
	allMu.Lock()
	all = append(all, m)
	allMu.Unlock()
	go m.terms.Run(ctx, logf)
	// On this process's status: what it booked, and what waits on the
	// deployment's billing queue — seen even when no db-worker reports.
	monitor.AddSection("billing_events", func() any {
		out := m.Section()
		c, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if n, oldest, err := m.book.Waiting(c); err == nil {
			out["waiting"], out["oldest_ms"] = n, oldest
		}
		return out
	})
	return m
}

// RateHistory is YAKU_RATE_HISTORY_HOURS (72): how long replaced rate
// versions are kept, so a message is priced by the rates of its accept time.
func RateHistory() time.Duration {
	h := 72
	if v, err := strconv.Atoi(os.Getenv("YAKU_RATE_HISTORY_HOURS")); err == nil && v >= 0 {
		h = v
	}
	return time.Duration(h) * time.Hour
}

// keepRates reads the rate cards now, again when they change (a small read
// every 30 s), and in full every 5 minutes.
func (m *Moments) keepRates(ctx context.Context) {
	if err := m.rater.Reload(ctx); err != nil {
		m.logf("rate cards for billing: %v (tried again every 2 s)", err)
		m.rater.KeepTrying(ctx, 2*time.Second, m.logf)
	}
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for n := 1; ; n++ {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		var err error
		if n%10 == 0 {
			err = m.rater.Reload(ctx)
		} else {
			_, err = m.rater.ReloadIfChanged(ctx)
		}
		if err != nil && ctx.Err() == nil {
			m.logf("rate cards for billing: %v (the last ones read are kept)", err)
		}
	}
}

// Terms is the terms this process books by.
func (m *Moments) Terms() *Terms {
	if m == nil {
		return nil
	}
	return m.terms
}

// VendorAnswered is the moment a vendor answered a submit of msg (accepted
// or not; a timeout is not accepted): the record gets its price
// (msg.Priced), the events decided here are booked (VendorEvents), and the
// note of what its receipt may still book is returned (for its receipt facts).
//
// A message with no quote (accepted by a gateway with no prices wired —
// none since 13 Oct 2026) is priced for its record and books nothing.
func (m *Moments) VendorAnswered(ctx context.Context, msg *model.Message, vendorID int64, accepted bool) BillNote {
	if m == nil || msg == nil || msg.Direction == model.DirectionMO {
		return BillNote{}
	}
	if m.rater == nil {
		return m.Unsent(ctx, msg) // a role that prices nothing (NewMomentsBooking)
	}
	return m.answered(ctx, msg, vendorID, accepted, true)
}

// PriceOnly is VendorAnswered without the booking: msg gets the price its
// record and its receipt facts carry, and nothing is booked. The parts of a
// long message sent as several PDUs have their receipt facts filed as each
// part is answered, before the message is decided (smppvendor
// onSubmitResp); without this their facts carried no note and the message's
// final booked no receipt fee (BUG-RR-01, 2 Nov 2026).
func (m *Moments) PriceOnly(ctx context.Context, msg *model.Message, vendorID int64, accepted bool) BillNote {
	if m == nil || msg == nil || msg.Direction == model.DirectionMO || m.rater == nil {
		return BillNote{}
	}
	return m.answered(ctx, msg, vendorID, accepted, false)
}

// answered prices msg as its vendor's answer does, and books what that
// answer decides when book is set.
func (m *Moments) answered(ctx context.Context, msg *model.Message, vendorID int64, accepted, book bool) BillNote {
	t := m.terms.Account(msg.AccountID)
	company := msg.CompanyID
	if company == 0 {
		company = t.Company
	}
	q, quoted := ParseQuote(msg.Quote)
	cur, _, _ := q.TermsOf(t)
	parts := msg.SentParts
	if parts < 1 {
		parts = msg.Segments
	}
	if parts < 1 {
		parts = 1
	}
	mdl := msg.BillingModel
	if mdl == "" {
		mdl = t.ModelOf()
	}
	at := time.Time{}
	if RateHistory() > 0 {
		at = AcceptedAt(msg.ID)
	}
	rated := m.rater.RateFull(ctx, msg.RouteID, msg.Destination, msg.MCC, msg.MNC, parts, RateOpts{
		CompanyCurrency: cur, CompanyID: company, FX: m.fx, BuySegments: parts, VendorID: vendorID, At: at, DefaultSellCard: t.MTSellCard})
	var buy *Side
	if rated.Buy != nil {
		buy = BuySide(*rated.Buy, rated.BuyCurrency, RateIDOf([]byte(rated.Detail), "buy")).With(BuyConvOf(rated))
	}
	var dlrBuy *Side
	if rc := m.rater.RateFull(ctx, msg.RouteID, msg.Destination, msg.MCC, msg.MNC, 1, RateOpts{
		CompanyCurrency: cur, CompanyID: company, FX: m.fx, BuySegments: 1, VendorID: vendorID, At: at, DLR: true}); rc.Buy != nil {
		dlrBuy = BuySide(*rc.Buy, rc.BuyCurrency, RateIDOf([]byte(rc.Detail), "buy")).With(BuyConvOf(rc))
	}
	vp := m.terms.VendorPoint(vendorID)
	rec := rated
	mark := ChargeMark{Model: mdl}
	if quoted {
		rec = q.MT.OnRating(rated, parts, q.Parts)
		rec.Detail = WithQuoteDetail(rated.Detail, q)
		_, point, _ := q.TermsOf(t)
		if q.MT != nil {
			switch {
			case point == PointAcceptance:
				mark.MT, mark.Held = q.ChargeFor(parts), q.Held
			case point == PointSubmit && accepted:
				mark.MT = q.ChargeFor(parts)
			}
			if q.HLR != nil {
				mark.HLR = RoundLedger(q.HLR.Charge)
			}
		}
	}
	note := NoteFor(mdl, q, quoted, t, parts, vp, buy, dlrBuy)
	note.Took = accepted
	if !note.Empty() {
		mark.Note = &note
	}
	msg.Priced = &model.Priced{Buy: rec.Buy, Sell: rec.Sell, Currency: rec.Currency, Detail: rec.Detail, Charge: mark.String()}
	if book && quoted && company != 0 {
		m.bookAll(ctx, company, VendorEvents(VendorAnswer{MsgID: msg.ID, Company: company, Account: msg.AccountID,
			Vendor: vendorID, Model: mdl, Segments: msg.Segments, SentParts: parts, Accepted: accepted,
			Quote: q, Quoted: true, Terms: t, VendorPoint: vp, Buy: buy}))
	}
	return note
}

// NoteOf reads the note a record's charge carries ("" = none).
func NoteOf(charge string) (BillNote, bool) {
	m, ok := ParseChargeMark(charge)
	if !ok || m.Note == nil {
		return BillNote{}, false
	}
	return *m.Note, true
}

// Book books bookings of one company (the receipt path's, dlr.Biller): kept
// and filed again when Redis refused them before running them.
func (m *Moments) Book(ctx context.Context, company int64, bs []Booking) {
	if m == nil || company == 0 {
		return
	}
	m.bookAll(ctx, company, bs)
}

// Final is the moment a message's final is known: what FinalEvents decides
// is booked.
func (m *Moments) Final(ctx context.Context, f Final) {
	if m == nil || f.Company == 0 || f.Note.Empty() {
		return
	}
	m.bookAll(ctx, f.Company, FinalEvents(f))
}

// Notice is the moment a progress receipt was sent to the client.
func (m *Moments) Notice(ctx context.Context, msgID string, company, account, vendor int64, n BillNote) {
	if m == nil || company == 0 {
		return
	}
	m.bookAll(ctx, company, NoticeEvents(msgID, company, account, vendor, n, n.Took))
}

// MOIn is the moment an incoming message is taken: PriceMO, then its money
// booked at once.
func (m *Moments) MOIn(ctx context.Context, msg *model.Message) {
	company, bs := m.PriceMO(ctx, msg)
	m.Book(ctx, company, bs)
}

// PriceMO prices an incoming message by the account's incoming-message card
// at the rates of now and gives it its record's price (msg.Priced), and
// returns what it books — its sell side, and a buy side when a buying card
// prices it — for the caller to book once the message is taken in. An
// incoming message has no quote and no charge point. Priced BEFORE it is
// queued, so whichever writes its record — the intake, or its delivery when
// the client takes it first — writes the price.
func (m *Moments) PriceMO(ctx context.Context, msg *model.Message) (int64, []Booking) {
	if m == nil || msg == nil || m.rater == nil {
		return 0, nil
	}
	t := m.terms.Account(msg.AccountID)
	company := msg.CompanyID
	if company == 0 {
		company = t.Company
	}
	parts := msg.Segments
	if parts < 1 {
		parts = 1
	}
	rated := m.rater.RateFull(ctx, msg.RouteID, msg.Destination, msg.MCC, msg.MNC, parts, RateOpts{
		CompanyCurrency: t.Currency, CompanyID: company, FX: m.fx, DefaultSellCard: t.MOSellCard, VendorID: msg.VendorID})
	mdl := t.ModelOf()
	mark := ChargeMark{Model: mdl}
	e := NewEvent(EventMO, msg.ID, company, msg.AccountID, msg.VendorID)
	e.Items, e.Parts = 1, int64(parts)
	if sc := rated.SellCharge; sc != nil && *sc > 0 {
		mark.MO = RoundLedger(*sc)
		e.Sell = SellSide(*sc, t.Currency, mdl, RateIDOf([]byte(rated.Detail), "sell")).With(SellConvOf(rated))
	}
	if rated.Buy != nil {
		e.Buy = BuySide(*rated.Buy, rated.BuyCurrency, RateIDOf([]byte(rated.Detail), "buy")).With(BuyConvOf(rated))
	}
	msg.Priced = &model.Priced{Buy: rated.Buy, Sell: rated.Sell, Currency: rated.Currency, Detail: rated.Detail, Charge: mark.String()}
	if company == 0 || e.Empty() {
		return company, nil
	}
	return company, []Booking{{Events: []Event{e}}}
}

// bookAll hands bookings to this process's bookers, off the caller's
// goroutine: a vendor role books on the goroutine that reads its vendor's
// answers, and a round trip to Redis there would slow the reading. Up to
// bookBatch waiting bookings go to Redis in one round trip (BookMany), each
// still one step of its own. A hand-over that is full books on the caller's
// goroutine.
func (m *Moments) bookAll(ctx context.Context, company int64, bs []Booking) {
	if len(bs) == 0 {
		return
	}
	m.startBookers()
	m.waiting.Add(1)
	select {
	case m.in <- handOver{company: company, b: bs}:
	default:
		m.bookNow([]handOver{{company: company, b: bs}})
		m.waiting.Add(-1)
	}
}

// The hand-over to the bookers.
const (
	bookHandOver = 100000 // bookings waiting for a booker
	bookBatch    = 256    // bookings sent to Redis in one round trip
	bookers      = 4
)

func (m *Moments) startBookers() {
	m.startOnce.Do(func() {
		m.in = make(chan handOver, bookHandOver)
		for i := 0; i < bookers; i++ {
			go func() {
				for first := range m.in {
					batch := []handOver{first}
				more:
					for len(batch) < bookBatch {
						select {
						case k := <-m.in:
							batch = append(batch, k)
						default:
							break more
						}
					}
					m.bookNow(batch)
					m.waiting.Add(-int64(len(batch)))
				}
			}()
		}
	})
}

// bookNow books a batch: the balances in one round trip to the main Redis,
// the events in one to the local Redis (BookMany). It keeps what Redis
// refused before running it and files it again until Redis takes it. Events
// left after their balance moved are always kept: queueing them again is
// safe. A balance deduction that may have run (a timeout once the command
// was sent) is not tried again — it could deduct twice — and is said loudly.
func (m *Moments) bookNow(batch []handOver) {
	m.mu.Lock()
	behind := len(m.kept) > 0
	m.mu.Unlock()
	if behind {
		// Redis was away a moment ago: behind what waits, in order.
		for _, k := range batch {
			for _, b := range k.b {
				m.keep(k.company, b, false)
			}
		}
		return
	}
	var items []Item
	var of []int // items[j] belongs to batch[of[j]]
	for i, k := range batch {
		for _, b := range k.b {
			items = append(items, Item{Company: k.company, Booking: b})
			of = append(of, i)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	errs := m.book.BookMany(ctx, items)
	cancel()
	for j, err := range errs {
		k := batch[of[j]]
		switch {
		case err == nil:
			m.booked.Add(int64(len(items[j].Booking.Events)))
		case balanceMoved(err):
			m.keep(k.company, items[j].Booking, true)
		case m.notSent(err):
			m.keep(k.company, items[j].Booking, false)
		default:
			m.lost.Add(1)
			m.logf("BILLING EVENTS MAY BE LOST (company %d, msg %s): %v — not tried again, it may have been booked",
				k.company, firstMsg(items[j].Booking), err)
		}
	}
}

func firstMsg(b Booking) string {
	if len(b.Events) > 0 {
		return b.Events[0].Msg
	}
	return ""
}

// Drain waits until every booking handed over in this process is booked or
// kept, at most d — for a role that is stopping.
func (m *Moments) Drain(d time.Duration) {
	if m == nil {
		return
	}
	until := time.Now().Add(d)
	for m.waiting.Load() > 0 && time.Now().Before(until) {
		time.Sleep(10 * time.Millisecond)
	}
}

func (m *Moments) keep(company int64, b Booking, moved bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.kept) >= keptMost {
		m.lost.Add(1)
		m.logf("BILLING EVENTS LOST (company %d): %d bookings already wait for Redis", company, len(m.kept))
		return
	}
	if len(m.kept) == 0 {
		m.keptFrom = time.Now()
		m.logf("billing events are kept in memory until Redis takes them")
	}
	m.kept = append(m.kept, keptBooking{company: company, b: b, moved: moved})
	if !m.filing {
		m.filing = true
		go m.fileKept()
	}
}

// fileKept files the kept bookings, in order, until none is left.
func (m *Moments) fileKept() {
	var lastSaid time.Time
	for {
		m.mu.Lock()
		if len(m.kept) == 0 {
			m.filing = false
			m.mu.Unlock()
			return
		}
		k := m.kept[0]
		m.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		var err error
		if k.moved {
			err = m.book.Queue(ctx, k.company, k.b.Events)
		} else {
			err = m.book.Book(ctx, k.company, k.b.Events)
		}
		cancel()
		if err != nil && balanceMoved(err) {
			// The balance moved on this try; only the events are left.
			m.mu.Lock()
			m.kept[0].moved = true
			m.mu.Unlock()
		}
		if err != nil && (k.moved || balanceMoved(err) || m.notSent(err)) {
			if time.Since(lastSaid) > 30*time.Second {
				lastSaid = time.Now()
				m.mu.Lock()
				n := len(m.kept)
				m.mu.Unlock()
				m.logf("billing events: %d booking(s) wait for Redis: %v", n, err)
			}
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if err != nil {
			m.lost.Add(1)
			m.logf("BILLING EVENTS MAY BE LOST (company %d): %v", k.company, err)
		} else {
			m.booked.Add(int64(len(k.b.Events)))
		}
		m.mu.Lock()
		m.kept = m.kept[1:]
		if len(m.kept) == 0 {
			m.logf("billing events: Redis took every kept booking (%s after the first)", time.Since(m.keptFrom).Round(time.Second))
		}
		m.mu.Unlock()
	}
}

// Every Moments of this process, for DrainAll.
var (
	allMu sync.Mutex
	all   []*Moments
)

// DrainAll waits until the bookings handed over in this process are booked
// (or kept), at most d in all: called when a role stops, so its last
// moments' money is not left in memory.
func DrainAll(d time.Duration) {
	allMu.Lock()
	ms := append([]*Moments(nil), all...)
	allMu.Unlock()
	until := time.Now().Add(d)
	for _, m := range ms {
		m.Drain(time.Until(until))
	}
}

// Section is this process's booking on its status (billing_events).
func (m *Moments) Section() map[string]any {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	kept := len(m.kept)
	m.mu.Unlock()
	out := map[string]any{"booked": m.booked.Load(), "kept": kept, "maybe_lost": m.lost.Load(),
		"handed_over": m.waiting.Load(), "terms_loaded": m.terms.Loaded()}
	if m.rater != nil {
		out["rates_loaded"] = m.rater.Loaded()
	}
	return out
}

// notSent says Redis certainly did not run the booking: it was refused
// before it was sent, or refused whole (the live store's own reading of the
// error, dal.LiveBalances.Unsent).
func (m *Moments) notSent(err error) bool {
	if err == nil {
		return false
	}
	return m.book.unsent(err)
}

// WithQuoteDetail puts the selling side that was fixed at acceptance into a
// record's audit JSON, in place of whatever the cards say now: the card and
// rate that priced it and the exchange rate used (summary_mt reads
// $.sell.rate_id), and a line saying where the price came from.
func WithQuoteDetail(detail string, q Quote) string {
	m := map[string]any{}
	if detail != "" {
		if json.Unmarshal([]byte(detail), &m) != nil {
			m = map[string]any{}
		}
	}
	delete(m, "sell")
	delete(m, "fx")
	delete(m, "not_priced")
	note := map[string]any{"at": "acceptance", "parts": q.Parts}
	if q.MT == nil {
		note["price"] = "none"
		note["note"] = "no rate priced this message when it was accepted, so it is not charged"
	} else {
		var qd map[string]any
		if len(q.MT.Detail) > 0 && json.Unmarshal(q.MT.Detail, &qd) == nil {
			if v, ok := qd["sell"]; ok {
				m["sell"] = v
			}
			if v, ok := qd["fx"]; ok {
				m["fx"] = v
			}
		}
		if q.Held {
			note["price"] = "taken at acceptance"
		} else {
			note["price"] = "fixed at acceptance"
		}
	}
	m["quote"] = note
	b, err := json.Marshal(m)
	if err != nil {
		return detail
	}
	return string(b)
}

// Unsent is the moment a message leaves without any vendor having taken it
// (it expired in our queue, ran out of tries, had no route left): its record
// gets its price (the customer's side, from its quote), a message accepted
// before the billing queue gets its acceptance events, and the note of what
// its final may still book is returned.
func (m *Moments) Unsent(ctx context.Context, msg *model.Message) BillNote {
	if m == nil || msg == nil || msg.Direction == model.DirectionMO {
		return BillNote{}
	}
	t := m.terms.Account(msg.AccountID)
	company := msg.CompanyID
	if company == 0 {
		company = t.Company
	}
	q, quoted := ParseQuote(msg.Quote)
	if !quoted {
		return BillNote{}
	}
	cur, point, _ := q.TermsOf(t)
	parts := msg.Segments
	if parts < 1 {
		parts = 1
	}
	mdl := msg.BillingModel
	if mdl == "" {
		mdl = t.ModelOf()
	}
	if !q.Booked && q.MT != nil && company != 0 {
		q2 := q
		q2.Cur, q2.Point = cur, point
		if evs := AcceptEvents(Accepted{MsgID: msg.ID, Company: company, Account: msg.AccountID, Vendor: msg.VendorID,
			Model: mdl, Parts: parts, Quote: q2}); len(evs) > 0 {
			m.bookAll(ctx, company, []Booking{{Events: evs}})
		}
	}
	rec := q.MT.OnRating(Rated{}, parts, q.Parts)
	rec.Detail = WithQuoteDetail("", q)
	mark := ChargeMark{Model: mdl}
	if q.MT != nil {
		if point == PointAcceptance {
			mark.MT, mark.Held = q.ChargeFor(parts), q.Held
		}
		if q.HLR != nil {
			mark.HLR = RoundLedger(q.HLR.Charge)
		}
	}
	note := NoteFor(mdl, q, true, t, parts, "", nil, nil)
	if !note.Empty() {
		mark.Note = &note
	}
	msg.Priced = &model.Priced{Sell: rec.Sell, Currency: rec.Currency, Detail: rec.Detail, Charge: mark.String()}
	return note
}

// GiveBackRefused gives back what a message refused at submission was
// charged when it was accepted (issue 22: its validity ended, or its tries
// ran out, before any vendor took it — a refusal is not charged). Only the
// message's own price taken at acceptance: a lookup it had was made. Once per
// message (a message is refused once); nothing for a message charged later than
// acceptance, which was never charged. Call it after Unsent, which books the
// acceptance of a message accepted before the billing queue.
func (m *Moments) GiveBackRefused(ctx context.Context, msg model.Message) {
	if m == nil || msg.Direction == model.DirectionMO {
		return
	}
	t := m.terms.Account(msg.AccountID)
	company := msg.CompanyID
	if company == 0 {
		company = t.Company
	}
	q, quoted := ParseQuote(msg.Quote)
	if !quoted || q.MT == nil || company == 0 {
		return
	}
	cur, point, _ := q.TermsOf(t)
	if NormalPoint(point) != PointAcceptance {
		return
	}
	parts := msg.Segments
	if parts < 1 {
		parts = 1
	}
	mdl := msg.BillingModel
	if mdl == "" {
		mdl = t.ModelOf()
	}
	q.Cur, q.Point = cur, point
	var back []Event
	for _, e := range AcceptEvents(Accepted{MsgID: msg.ID, Company: company, Account: msg.AccountID, Vendor: msg.VendorID,
		Model: mdl, Parts: parts, Quote: q}) {
		if e.Kind == EventMT {
			back = append(back, e.Negated())
		}
	}
	if len(back) > 0 {
		m.bookAll(ctx, company, []Booking{{Events: back}})
	}
}
