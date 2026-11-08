package dlr

import (
	"context"

	"github.com/yakugate/yakugate/internal/model"
)

// A receipt is matched to its message in this order (Saleem 2026-10-14):
//
//  1. the vendor's COMPANY + the vendor's message id + the handset number;
//  2. with no number on the receipt: company + vendor's message id;
//  3. with no vendor message id we know: our own message id.
//
// Why the company and not the vendor: a carrier whose send bind (TX) and
// receive bind (RX) are two vendor rows sends the message on one and its
// receipt on the other. Matched by the vendor row, as until this day, that
// receipt never found its message. The number tells two messages of one
// company apart when two of its vendors happen to give the same id.
//
// A vendor with no company (vendors.company_id NULL) is a company of its own:
// its receipts are matched by the vendor row, as before.
//
// With the receipt facts in Redis (29 Oct 2026) a message is ONE key, by the
// company and the vendor's id: rule 1's number is compared on its facts, and
// only the messages of a bulk call that share one id are told apart by it.
// Rule 3 needs no look of its own: a vendor role files a message the vendor
// gave no id under OUR id (smpp-vendor, http-vendor vendorRef), so a receipt
// that carries our id finds it by the same key.

// VendorGroups says which vendors are one carrier: the vendors of one company.
type VendorGroups interface {
	// Scope is the id a vendor's receipts are matched under: minus the
	// company id for a vendor of a company, the vendor's own id for one with
	// none (the same key as before for those).
	Scope(vendorID int64) int64
	// Members is every vendor of that scope: the company's vendors, or the
	// vendor alone.
	Members(vendorID int64) []int64
}

// SetGroups gives the service the vendors' companies. nil: each vendor is its
// own, as before.
func (s *Service) SetGroups(g VendorGroups) {
	if s != nil {
		s.groups = g
	}
}

// SetStatusMaps gives the service each vendor's own words for a receipt's
// status. A receipt read on a vendor row that has none (an RX row set up only
// to take receipts) is read with the map of the vendor that SENT its message.
func (s *Service) SetStatusMaps(of func(vendorID int64) map[string]string) {
	if s != nil {
		s.statusMaps = of
	}
}

func (s *Service) scope(vendorID int64) int64 {
	if s == nil || s.groups == nil {
		return vendorID
	}
	return s.groups.Scope(vendorID)
}

func (s *Service) members(vendorID int64) []int64 {
	if s == nil || s.groups == nil {
		return []int64{vendorID}
	}
	if m := s.groups.Members(vendorID); len(m) > 0 {
		return m
	}
	return []int64{vendorID}
}

// mapFor is the status map a receipt is read with: the one it came with, else
// the receiving vendor's own (a receipt applied later from the orphans comes
// with none, 2026-10-18), else that of the vendor that sent its message.
func (s *Service) mapFor(statusMap map[string]string, sender, receiver int64) map[string]string {
	if len(statusMap) > 0 || s.statusMaps == nil {
		return statusMap
	}
	if own := s.statusMaps(receiver); len(own) > 0 {
		return own
	}
	if sender == 0 || sender == receiver {
		return statusMap
	}
	return s.statusMaps(sender)
}

// receiptKey is what a receipt names: the vendor row it came in on, the
// vendor's id for the message (two, when it gave one in the field and another
// in the text), and the handset number when it said one.
type receiptKey struct {
	vendorID int64
	id, alt  string
	number   string
}

// ids is the receipt's ids to look up, the main one first.
func (k receiptKey) ids() []string {
	if k.alt == "" || k.alt == k.id {
		return []string{k.id}
	}
	return []string{k.id, k.alt}
}

// candidates is every form of every id the receipt carries, the main id's
// forms first.
func (k receiptKey) candidates() []string {
	var out []string
	seen := map[string]bool{}
	for _, id := range k.ids() {
		for _, c := range msgIDCandidates(id) {
			if !seen[c] {
				seen[c] = true
				out = append(out, c)
			}
		}
	}
	return out
}

// Match is a receipt's message as the record store found it.
type Match = model.ReceiptMatch

// MatchResolver is an optional Store capability (mysql.Store): the message a
// receipt names, looked for among all the vendors of the receipt's company,
// chosen by the number when the receipt carries one, and — when the id is
// one of ours — by our message id.
type MatchResolver interface {
	ResolveReceipt(ctx context.Context, vendorIDs []int64, vendorMsgID, number string) (Match, error)
}
