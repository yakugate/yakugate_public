package dlr

import (
	"context"
	"log"

	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/receiptpolicy"
)

// The account's receipt policy as a receipt reads it: its levels, whether the
// client wants intermediates, the word it is told, and what it was told already.

// AccountLevels is an optional capability of the Policies source: which
// intermediate receipts an account's clients get (config/0120). A source
// without it answers `registered` for everyone, which is the default anyway.
type AccountLevels interface {
	LevelsFor(accountID int64) string
}

// levelsFor is the account's dlr_levels setting.
func (s *Service) levelsFor(accountID int64) string {
	if s == nil || s.policies == nil {
		return LevelsRegistered
	}
	if l, ok := s.policies.(AccountLevels); ok {
		if v := l.LevelsFor(accountID); v != "" {
			return v
		}
	}
	return LevelsRegistered
}

// AccountPartReceipts is an optional capability of the Policies source:
// whether an account's clients get each part's final receipt of a long
// message as it comes (sms_service_config.part_receipts = 'each', config/0172,
// Saleem 4 Nov 2026). A source without it answers "after the final" for all.
type AccountPartReceipts interface {
	EachPartFor(accountID int64) bool
}

// eachPartFor is the account's part_receipts = 'each'.
func (s *Service) eachPartFor(accountID int64) bool {
	if s == nil || s.policies == nil {
		return false
	}
	if l, ok := s.policies.(AccountPartReceipts); ok {
		return l.EachPartFor(accountID)
	}
	return false
}

// partToClient says whether a part's own final receipt that does not close
// its long message goes to the client as it comes: the account asked for
// each part's (part_receipts = 'each'), the client wants a receipt of this
// outcome, it was not told already, and the account's receipt policy has no
// rule that rewrites this word (the client then hears only the message's
// answer, never the word the policy hides). Nothing is booked for it: the
// message's bill follows its vendor's rule, once, at the close.
func (s *Service) partToClient(accountID int64, told string, noReceipt, failureOnly bool, st model.Status, raw, code string) bool {
	if s.pusher == nil || told != "" || !s.eachPartFor(accountID) || !st.IsTerminal() ||
		!model.WantsTerminalReceipt(noReceipt, failureOnly, st) {
		return false
	}
	if pol := s.policyFor(accountID); pol != nil {
		if a, _ := pol.ForReceiptRule(st, raw, code); a != nil {
			return false
		}
	}
	return true
}

// IntermediateWanted is an optional Store capability: did the CLIENT ask to
// hear about progress (record_index.want_intermediate, hot/0045)?
type IntermediateWanted interface {
	WantsIntermediate(ctx context.Context, ref model.RecordRef) (bool, error)
}

// clientWantsIntermediate answers the client's own registered_delivery bit 4,
// as recorded at submit. A store that cannot say, or a message from before
// the column existed, answers YES: an HTTP client has always been sent the
// gateway's submit notice, and the reverse default would silently drop
// receipts an operator had just switched on.
func (s *Service) clientWantsIntermediate(ctx context.Context, ref model.RecordRef) bool {
	w, ok := s.store.(IntermediateWanted)
	if !ok {
		return true
	}
	want, err := w.WantsIntermediate(ctx, ref)
	if err != nil {
		log.Printf("[dlr] could not read whether %s wanted progress receipts (%v) — sending it", ref.MsgID, err)
		return true
	}
	return want
}

// statSent is the word a client receipt carries. Normally the vendor's own,
// but when something changed the outcome — a per-vendor code override, a
// multi-part group's aggregate — the word follows the status the client is
// actually being given, so the two can never disagree.
func statSent(stat Stat, st model.Status) Stat {
	if stat == "" || stat.Record() != st {
		return StatFor(st)
	}
	return stat
}

// policyFor is an account's receipt policy, or nil.
func (s *Service) policyFor(accountID int64) *receiptpolicy.Policy {
	if s == nil || s.policies == nil {
		return nil
	}
	return s.policies.For(accountID)
}

// toldAlready is what this client has already been told about a message, or
// "" — and "" without a lookup at all on a gateway where no account has a
// policy, which is the normal case and must stay free.
func (s *Service) toldAlready(ctx context.Context, ref model.RecordRef) string {
	if s == nil || s.policies == nil || !s.policies.Any() {
		return ""
	}
	teller, ok := s.teller()
	if !ok {
		return ""
	}
	told, err := teller.ToldStatus(ctx, ref)
	if err != nil {
		// Unknown is treated as "not told": the receipt goes out. A missed
		// read must not swallow a real vendor receipt.
		log.Printf("[dlr] could not read what %s was told (%v) — treating the vendor receipt as the client's first answer", ref.MsgID, err)
		return ""
	}
	return told
}
