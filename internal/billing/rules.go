package billing

import (
	"encoding/json"
	"strings"
)

// The rules of what is booked when (issues 3 and 18; Saleem, 26 Oct 2026).
// They were the billing role's, applied to a record minutes after the fact;
// they are now applied at the moment each fact is known, by the process that
// knows it — and they are one set, here, so every moment books by the same
// rules.

// An account's charge points (sms_service_config.charge_point, config/0114):
// when the SELL side of a message is booked.
const (
	PointAcceptance = "acceptance" // when we accepted it, whatever became of it (D-033)
	PointSubmit     = "submit"     // when a vendor took it
	PointDelivery   = "delivery"   // when it was delivered
)

// A vendor's charge points (vendors.charge_point, config/0150): when the BUY
// side is booked.
const (
	VendorSubmission = "submission" // every submit we send, even refused or timed out
	VendorAcceptance = "acceptance" // the vendor accepted it (the default)
	VendorDelivery   = "delivery"   // the vendor reports it delivered
)

// NormalPoint is an account charge point as the rules read it: one of the
// three, "acceptance" for anything else.
func NormalPoint(p string) string {
	switch strings.ToLower(strings.TrimSpace(p)) {
	case PointSubmit:
		return PointSubmit
	case PointDelivery:
		return PointDelivery
	}
	return PointAcceptance
}

// NormalVendorPoint is a vendor charge point as the rules read it.
func NormalVendorPoint(p string) string {
	switch strings.ToLower(strings.TrimSpace(p)) {
	case VendorSubmission:
		return VendorSubmission
	case VendorDelivery:
		return VendorDelivery
	}
	return VendorAcceptance
}

// CleanVendorPoint is the rule both doors keep for a vendor's charge point
// (the console's provision.UpdateVendor, the Admin API's
// services/vendorChargePoint.js): one of the three words, any case, trimmed;
// "" = the default. ok=false for anything else.
func CleanVendorPoint(v string) (string, bool) {
	switch p := strings.ToLower(strings.TrimSpace(v)); p {
	case "":
		return VendorAcceptance, true
	case VendorSubmission, VendorAcceptance, VendorDelivery:
		return p, true
	}
	return "", false
}

// ChargeDecision applies an account's charge point (PO 2026-09-17) to one MT
// message: charge says whether its sell price is taken; settle says whether
// that can be judged now at all. A message whose outcome is still open under
// the point (a sent message on a "delivery" account) is not decided yet.
//
//	acceptance  charge everything accepted, whatever became of it (D-033)
//	submit      charge once the vendor took it (it has a vendor id); a
//	            message the vendor never took (refused, expired in our
//	            queue) is not charged
//	delivery    charge a delivered message only; failed/expired are not
//	            charged; sent/partial wait for the receipt
func ChargeDecision(point, status, vendorMsgID string) (charge, settle bool) {
	switch NormalPoint(point) {
	case PointSubmit:
		if vendorMsgID != "" {
			return true, true
		}
		switch status {
		case "failed", "expired":
			return false, true // never taken, terminal: no charge
		}
		return false, false // queued/sent without a vendor id yet: wait
	case PointDelivery:
		switch status {
		case "delivered":
			return true, true
		case "failed", "expired":
			return false, true
		}
		return false, false // sent / partial: the receipt decides
	}
	return true, true // acceptance (default): every accepted message
}

// RateIDOf is the rate row that priced one side of a rating's audit JSON
// ({"sell":{"rate_id":…}} / {"buy":{…}}); 0 when it names none.
func RateIDOf(detail []byte, side string) int64 {
	if len(detail) == 0 {
		return 0
	}
	var m map[string]struct {
		RateID int64 `json:"rate_id"`
	}
	if json.Unmarshal(detail, &m) != nil {
		return 0
	}
	return m[side].RateID
}

// Terms of an account that a moment books by, from its quote or (a quote
// written before the billing queue) from the account as it stands now.
type AccountTerms struct {
	Company  int64
	Currency string // the company's
	Point    string // charge point
	DLRMode  string // all_receipts | delivered_only | all_levels
	Prepaid  bool   // the company pays before (the model of an incoming message)
	// The account's own cards, for what has no quote (incoming messages,
	// a message accepted with no prices wired).
	MOSellCard, MTSellCard, DLRSellCard int64
}

// TermsOf fills what the quote leaves out from the account's terms.
func (q Quote) TermsOf(t AccountTerms) (cur, point, dlrMode string) {
	cur, point, dlrMode = q.Cur, q.Point, q.DLRMode
	if cur == "" {
		cur = t.Currency
	}
	if point == "" {
		point = t.Point
	}
	if dlrMode == "" {
		dlrMode = t.DLRMode
	}
	return cur, NormalPoint(point), dlrMode
}

// --- at acceptance ----------------------------------------------------------

// Accepted is a message at the moment it is accepted.
type Accepted struct {
	MsgID   string
	Company int64
	Account int64
	Vendor  int64  // the vendor the route chose
	Model   string // prepaid | postpaid
	Parts   int    // the parts counted at acceptance
	Quote   Quote
}

// AcceptEvents are what is booked when a message is accepted: its sell side
// when the account is charged at acceptance, and its number lookup whatever
// the charge point (Saleem, 26 Oct: the lookup fee is taken at acceptance).
// No price in the quote = nothing, for the lookup too (D-130). The buy side
// is the vendor's to decide (VendorEvents).
func AcceptEvents(a Accepted) []Event {
	q := a.Quote
	if a.Company == 0 || q.MT == nil {
		return nil
	}
	parts := a.Parts
	if parts < 1 {
		parts = 1
	}
	var out []Event
	if NormalPoint(q.Point) == PointAcceptance {
		e := NewEvent(EventMT, a.MsgID, a.Company, a.Account, a.Vendor)
		e.Items, e.Parts = 1, int64(parts)
		e.Sell = SellSide(q.ChargeFor(parts), q.Cur, a.Model, RateIDOf(q.MT.Detail, "sell")).With(q.MT.Conv())
		if !e.Empty() {
			out = append(out, e)
		}
	}
	if q.HLR != nil {
		e := NewEvent(EventHLR, a.MsgID, a.Company, a.Account, 0)
		e.Items = 1
		e.Sell = SellSide(q.HLR.Charge, q.Cur, a.Model, RateIDOf(q.HLR.Detail, "sell")).With(q.HLR.Conv())
		if !e.Empty() {
			out = append(out, e)
		}
	}
	return out
}

// --- when a vendor answers --------------------------------------------------

// VendorAnswer is a message at the moment a vendor answered a submit of it
// (or the answer timed out).
type VendorAnswer struct {
	MsgID     string
	Company   int64
	Account   int64
	Vendor    int64
	Model     string
	Segments  int // the parts counted at acceptance
	SentParts int // the parts this vendor was sent (0 = Segments)
	Accepted  bool
	Quote     Quote
	Quoted    bool
	Terms     AccountTerms // for a quote written before the billing queue
	// VendorPoint is the vendor's charge point; Buy its price for the parts
	// sent (nil = no buying card priced it).
	VendorPoint string
	Buy         *Side
}

// Booking is events to book in one step. There are no once-only marks
// (Saleem, 29 Oct 2026): each moment happens once by itself — a message is
// queued once, taken by a vendor once (queues are taken, not leased), its
// receipt is handed once.
type Booking struct {
	Events []Event
}

// VendorEvents are what is booked when a vendor answers:
//   - the BUY side, at the vendor's charge point: every answer (submission)
//     or an accepted one (acceptance); delivery waits for the receipt;
//   - the SELL side of an account charged on submit, once per message, when
//     the vendor took it;
//   - for a message charged at acceptance, the difference when the vendor
//     was sent another number of parts than were counted then, once;
//   - for a message accepted before the billing queue (its quote not
//     booked), its acceptance events, once.
//
// When the sell and the buy side fall here together they are ONE event.
func VendorEvents(v VendorAnswer) []Booking {
	if v.Company == 0 {
		return nil
	}
	parts := v.SentParts
	if parts < 1 {
		parts = v.Segments
	}
	if parts < 1 {
		parts = 1
	}
	q := v.Quote
	var out []Booking
	cur, point, _ := q.TermsOf(v.Terms)
	if v.Quoted && !q.Booked && q.MT != nil {
		q.Cur, q.Point = cur, point
		if evs := AcceptEvents(Accepted{MsgID: v.MsgID, Company: v.Company, Account: v.Account, Vendor: v.Vendor,
			Model: v.Model, Parts: v.Segments, Quote: q}); len(evs) > 0 {
			out = append(out, Booking{Events: evs})
		}
	}
	mt := NewEvent(EventMT, v.MsgID, v.Company, v.Account, v.Vendor)
	mt.Items, mt.Parts = 1, int64(parts)
	switch NormalVendorPoint(v.VendorPoint) {
	case VendorSubmission:
		mt.Buy = v.Buy
	case VendorAcceptance:
		if v.Accepted {
			mt.Buy = v.Buy
		}
	}
	if v.Quoted && q.MT != nil && v.Accepted {
		switch point {
		case PointSubmit:
			mt.Sell = SellSide(q.ChargeFor(parts), cur, v.Model, RateIDOf(q.MT.Detail, "sell")).With(q.MT.Conv())
		case PointAcceptance:
			quoted := q.Parts
			if quoted < 1 {
				quoted = 1
			}
			if parts != quoted {
				d := NewEvent(EventMT, v.MsgID, v.Company, v.Account, v.Vendor)
				d.Parts = int64(parts - quoted)
				if diff := RoundLedger(q.ChargeFor(parts) - q.ChargeFor(quoted)); diff != 0 {
					d.Sell = &Side{Units: toUnits(diff), Currency: strings.ToUpper(cur), Model: v.Model, RateID: RateIDOf(q.MT.Detail, "sell"), Conv: q.MT.Conv()}
					out = append(out, Booking{Events: []Event{d}})
				}
			}
		}
	}
	if !mt.Empty() {
		out = append(out, Booking{Events: []Event{mt}})
	}
	return out
}

// --- at a message's final -----------------------------------------------------

// BillNote is what a message's receipt still needs to book, worked out when
// the vendor took it and filed with the message's receipt facts (or read back
// from its record): only the amounts a later moment can book.
type BillNote struct {
	Model   string `json:"o,omitempty"`
	Cur     string `json:"c,omitempty"`
	Point   string `json:"pt,omitempty"`
	DLRMode string `json:"dm,omitempty"`
	Parts   int    `json:"p,omitempty"`
	// MT is the message's sell side for the parts sent (a delivery charge
	// point books it on delivery); DLR the fee of ONE receipt.
	MT     int64 `json:"mt,omitempty"`
	MTRate int64 `json:"mr,omitempty"`
	DLR    int64 `json:"d,omitempty"`
	DLRRt  int64 `json:"dr,omitempty"`
	// MTConv and DLRConv are how those two convert, fixed at acceptance
	// with their prices (27 Oct 2026); nil in a note made before.
	MTConv  *Conv `json:"mc,omitempty"`
	DLRConv *Conv `json:"dc,omitempty"`
	// Buy is the vendor's price for the parts sent, when the vendor charges
	// on delivery; DLRBuy its price of one receipt.
	VendorPoint string `json:"vp,omitempty"`
	Buy         *Side  `json:"b,omitempty"`
	DLRBuy      *Side  `json:"db,omitempty"`
	// Took says the vendor accepted it (a message charged on submit was
	// charged, so its receipt may be).
	Took bool `json:"t,omitempty"`
}

// WithTerms fills what a note leaves out from the account's terms (a note
// made where the terms were not at hand).
func (n BillNote) WithTerms(t AccountTerms) BillNote {
	if n.Cur == "" {
		n.Cur = strings.ToUpper(t.Currency)
	}
	if n.Point == "" {
		n.Point = t.Point
	}
	n.Point = NormalPoint(n.Point)
	if n.DLRMode == "" {
		n.DLRMode = t.DLRMode
	}
	if n.Model == "" && t.Company != 0 {
		n.Model = t.ModelOf()
	}
	return n
}

// Empty reports a note with nothing left to book.
func (n BillNote) Empty() bool {
	return n.MT == 0 && n.DLR == 0 && n.Buy == nil && n.DLRBuy == nil
}

// String is the note as it is filed; "" for an empty one.
func (n BillNote) String() string {
	if n.Empty() {
		return ""
	}
	b, err := json.Marshal(n)
	if err != nil {
		return ""
	}
	return string(b)
}

// ParseBillNote reads a filed note.
func ParseBillNote(s string) (BillNote, bool) {
	if s == "" {
		return BillNote{}, false
	}
	var n BillNote
	if json.Unmarshal([]byte(s), &n) != nil {
		return BillNote{}, false
	}
	return n, true
}

// NoteFor is the note of a message a vendor has taken: what its receipt may
// still book. buy and dlrBuy are the vendor's prices (nil = none).
func NoteFor(model string, q Quote, quoted bool, t AccountTerms, parts int, vendorPoint string, buy, dlrBuy *Side) BillNote {
	cur, point, dlrMode := q.TermsOf(t)
	n := BillNote{Model: model, Cur: strings.ToUpper(cur), Point: point, DLRMode: dlrMode, Parts: parts,
		VendorPoint: NormalVendorPoint(vendorPoint)}
	if quoted && q.MT != nil {
		if point == PointDelivery {
			n.MT, n.MTRate, n.MTConv = toUnits(q.ChargeFor(parts)), RateIDOf(q.MT.Detail, "sell"), q.MT.Conv()
		}
		if q.DLR != nil {
			n.DLR, n.DLRRt, n.DLRConv = toUnits(q.DLR.Charge), RateIDOf(q.DLR.Detail, "sell"), q.DLR.Conv()
		}
	}
	if n.VendorPoint == VendorDelivery {
		n.Buy = buy
	}
	n.DLRBuy = dlrBuy
	return n
}

// Final is a message's outcome at one of its final moments.
type Final struct {
	MsgID   string
	Company int64
	Account int64
	Vendor  int64
	Note    BillNote
	// Status is the final status at this moment (delivered, failed, expired).
	Status string
	// ToClient: this is the first final the client is given and it is sent
	// to the client (what decides the sell side and the receipt fee).
	ToClient bool
	// FromVendor: this is the vendor's own first final (what decides a buy
	// side charged on delivery, and the vendor's receipt).
	FromVendor bool
	// VendorTook says a vendor took the message (it carries a vendor id): a
	// message charged on submit was charged.
	VendorTook bool
}

// FinalEvents are what a message's final books:
//   - its sell side, charged on delivery, when the client is told delivered;
//   - the receipt fee (today's rule, Saleem 26 Oct): the first final the
//     client is given, when its message was charged, and only a delivered
//     one when the account says delivered_only;
//   - the vendor's side charged on delivery, when the vendor says delivered,
//     and the vendor's price of its receipt (assumption A1).
//
// The message's two sides are one event when they fall here together, and
// so are the receipt's.
func FinalEvents(f Final) []Booking {
	n := f.Note
	if f.Company == 0 {
		return nil
	}
	delivered := f.Status == "delivered"
	var out []Booking
	mt := NewEvent(EventMT, f.MsgID, f.Company, f.Account, f.Vendor)
	mt.Items, mt.Parts = 1, int64(maxi(n.Parts, 1))
	if f.ToClient && delivered && n.Point == PointDelivery && n.MT != 0 {
		mt.Sell = &Side{Units: n.MT, Currency: n.Cur, Model: n.Model, RateID: n.MTRate, Conv: n.MTConv}
	}
	if f.FromVendor && delivered && n.VendorPoint == VendorDelivery && n.Buy != nil {
		b := *n.Buy
		mt.Buy = &b
	}
	if !mt.Empty() {
		out = append(out, Booking{Events: []Event{mt}})
	}
	rc := NewEvent(EventDLR, f.MsgID, f.Company, f.Account, f.Vendor)
	rc.Items = 1
	if f.ToClient && n.DLR != 0 && messageCharged(n.Point, f.Status, f.VendorTook || n.Took) &&
		!(n.DLRMode == "delivered_only" && !delivered) {
		rc.Sell = &Side{Units: n.DLR, Currency: n.Cur, Model: n.Model, RateID: n.DLRRt, Conv: n.DLRConv}
	}
	if f.FromVendor && n.DLRBuy != nil {
		b := *n.DLRBuy
		rc.Buy = &b
	}
	if !rc.Empty() {
		out = append(out, Booking{Events: []Event{rc}})
	}
	return out
}

// NoticeEvents is the fee of a progress receipt (the vendor's ENROUTE or
// ACCEPTD, or the gateway's own ACCEPTD) the client was sent: charged only
// when the account asked to be charged for every level (all_levels), for a
// message that was charged.
//
// A notice comes before the message's outcome is known: under a delivery
// charge point it is charged as the notice the client was sent; only a
// message charged on submit that no vendor took is known not to be charged.
func NoticeEvents(msgID string, company, account, vendor int64, n BillNote, vendorTook bool) []Booking {
	if company == 0 || n.DLRMode != "all_levels" || n.DLR == 0 || (n.Point == PointSubmit && !vendorTook) {
		return nil
	}
	e := NewEvent(EventDLR, msgID, company, account, vendor)
	e.Items = 1
	e.Sell = &Side{Units: n.DLR, Currency: n.Cur, Model: n.Model, RateID: n.DLRRt, Conv: n.DLRConv}
	return []Booking{{Events: []Event{e}}}
}

// messageCharged says whether a message with this outcome was charged under
// its account's charge point: a receipt of a message that was not is free
// (go-live test 18 Oct, P9 C2/C3).
func messageCharged(point, status string, vendorTook bool) bool {
	switch NormalPoint(point) {
	case PointSubmit:
		return vendorTook
	case PointDelivery:
		return status == "delivered"
	}
	return true
}

func maxi(a, b int) int {
	if a > b {
		return a
	}
	return b
}
