package dlr

import (
	"context"
	"encoding/json"

	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/model"
)

// Money a message's receipts decide (issues 3 and 18; Saleem, 26 Oct 2026).
// The billing role used to charge receipts minutes after the fact, reading
// the receipt rows back; the receipt path now books them at the moment it
// gives the client its receipt — the receipt fee (today's rule: the first
// final the client is given, a progress notice under all_levels, free when
// the message was not charged), the sell side of a message charged on
// delivery, and the vendor's sides charged on delivery — and writes what it
// booked on the receipt's own row (its `charge`).

// Biller books what the receipt path decides (billing.Moments).
type Biller interface {
	Book(ctx context.Context, company int64, bs []billing.Booking)
	// Terms are the accounts' terms, for a note that leaves some out.
	Terms() *billing.Terms
}

// NoteFinder is an optional Store capability: the note a message's record
// carries for its receipts (mysql.Store.BillNote: its `charge` column).
type NoteFinder interface {
	BillNote(ctx context.Context, ref model.RecordRef) (string, error)
}

// SetBiller gives the service the billing queue. nil = nothing is booked
// (a process with no Redis, a test).
func (s *Service) SetBiller(b Biller) {
	if s == nil {
		return
	}
	s.biller = b
}

// noteOf reads a filed note: the facts' own (BillNote JSON) or a record's
// charge mark that carries one.
func noteOf(s string) (billing.BillNote, bool) {
	if n, ok := billing.NoteOf(s); ok {
		return n, true
	}
	if n, ok := billing.ParseBillNote(s); ok && !n.Empty() {
		return n, true
	}
	return billing.BillNote{}, false
}

// recordNote is the note on a message's record ("" when the store cannot say).
func (s *Service) recordNote(ctx context.Context, ref model.RecordRef) string {
	if s.biller == nil {
		return ""
	}
	// The note rides the message's facts in Redis (Facts.Bill); the logs
	// keep no money (issue 16), so a store that is the record store has none.
	nf, ok := s.store.(NoteFinder)
	if !ok {
		return ""
	}
	n, err := nf.BillNote(ctx, ref)
	if err != nil {
		return ""
	}
	return n
}

// finalBill is what a message's final books, and the charge its receipt row
// carries. toClient: this is the first final the client is given and it is
// sent; fromVendor: it is the vendor's own first final.
func (s *Service) finalBill(note, msgID string, company, account, vendor int64, st model.Status, toClient, fromVendor bool) ([]billing.Booking, string) {
	if s.biller == nil || company == 0 {
		return nil, ""
	}
	n, ok := noteOf(note)
	if !ok {
		return nil, ""
	}
	n = n.WithTerms(s.biller.Terms().Account(account))
	bs := billing.FinalEvents(billing.Final{MsgID: msgID, Company: company, Account: account, Vendor: vendor, Note: n,
		Status: string(st), ToClient: toClient, FromVendor: fromVendor})
	return bs, chargeOf(bs, n.Model)
}

// noticeBill is the fee of a progress receipt the client is sent.
func (s *Service) noticeBill(note, msgID string, company, account, vendor int64) ([]billing.Booking, string) {
	if s.biller == nil || company == 0 {
		return nil, ""
	}
	n, ok := noteOf(note)
	if !ok {
		return nil, ""
	}
	n = n.WithTerms(s.biller.Terms().Account(account))
	bs := billing.NoticeEvents(msgID, company, account, vendor, n, n.Took)
	return bs, chargeOf(bs, n.Model)
}

// book books what was decided (kept and filed again by the biller when
// Redis is away).
func (s *Service) book(ctx context.Context, company int64, bs []billing.Booking) {
	if s.biller != nil && company != 0 && len(bs) > 0 {
		s.biller.Book(ctx, company, bs)
	}
}

// chargeOf is the charge a receipt row carries: the sell sides it booked.
func chargeOf(bs []billing.Booking, mdl string) string {
	m := billing.ChargeMark{Model: mdl}
	for _, b := range bs {
		for _, e := range b.Events {
			if e.Sell == nil {
				continue
			}
			switch e.Kind {
			case billing.EventMT:
				m.MT = billing.RoundLedger(m.MT + e.Sell.Amount())
			case billing.EventDLR:
				m.DLR = billing.RoundLedger(m.DLR + e.Sell.Amount())
			}
		}
	}
	return m.String()
}

// withCharge is a receipt entry with what it booked set: as rate version ids
// (issue 16), and the charge an older reader may still look for.
func withCharge(entryJSON, charge string, bs []billing.Booking) string {
	sell, buy := billing.BookedRates(bs)
	if charge == "" && sell == 0 && buy == 0 {
		return entryJSON
	}
	var e entry
	if json.Unmarshal([]byte(entryJSON), &e) != nil {
		return entryJSON
	}
	e.Charge, e.SellRate, e.BuyRate = charge, sell, buy
	b, err := json.Marshal(e)
	if err != nil {
		return entryJSON
	}
	return string(b)
}

// noteOfMessage is the note a message carries in hand: the one its vendor
// role worked out (Priced), else one from its quote and the account's terms
// (a message no vendor took: refused, written off).
func noteOfMessage(m model.Message) string {
	if m.Priced != nil && m.Priced.Charge != "" {
		return m.Priced.Charge
	}
	q, ok := billing.ParseQuote(m.Quote)
	if !ok {
		return ""
	}
	parts := m.SentParts
	if parts < 1 {
		parts = m.Segments
	}
	n := billing.NoteFor(m.BillingModel, q, true, billing.AccountTerms{}, parts, "", nil, nil)
	return n.String()
}

// BookNotice books the fee of the gateway's own ACCEPTD a caller pushed
// itself (PushMany): a progress receipt the client was sent.
func (s *Service) BookNotice(ctx context.Context, m model.Message) {
	if s == nil {
		return
	}
	bs, _ := s.noticeBill(noteOfMessage(m), m.ID, m.CompanyID, m.AccountID, m.VendorID)
	s.book(ctx, m.CompanyID, bs)
}

// BookFinal books what a final the gateway decided and a caller pushed itself
// (PushMany: a vendor's refusal) decides: the client's first final.
func (s *Service) BookFinal(ctx context.Context, m model.Message, st model.Status) {
	if s == nil {
		return
	}
	bs, _ := s.finalBill(noteOfMessage(m), m.ID, m.CompanyID, m.AccountID, m.VendorID, st, true, false)
	s.book(ctx, m.CompanyID, bs)
}

// BillOf is the note a sent message's receipt facts carry (Facts.Bill): what
// its vendor role worked out it may still book. "" = nothing.
func BillOf(m model.Message) string {
	if m.Priced == nil {
		return ""
	}
	if n, ok := billing.NoteOf(m.Priced.Charge); ok {
		return n.String()
	}
	return ""
}
