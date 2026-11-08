package pipeline

import (
	"context"
	"time"

	"github.com/yakugate/yakugate/internal/hlr"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/netcode"
	"github.com/yakugate/yakugate/internal/refdata"
)

// --- stage 15/18: HLR — find the destination's network --- [SCOPE P1-14]
// MCC/MNC resolution ONLY, and only when the account is flagged
// (enforce_hlr_db_check). Per-number sender blocking is NOT here — stage 4
// (blocklist) owns all destination blocking (PO 2026-07-26).
//
// Each account lists the steps it wants, in order; the first step with an
// answer wins (2026-09-14, refdata.Account.HLROrder, gateway default
// client → data → range → live):
//
//	client   the operator code the client sent            hlr_source "operator"
//	data     our HLR data, unless older than the account's
//	         cache time limit (then it counts as unknown)  "db"
//	range    the operator the number range was issued to   "range"
//	live     a live lookup with the account's providers    "dip"
//	country  the country code only, no network              "country"
//
// Nothing found is "miss", or "dip_miss" when a provider was actually asked
// and could not answer — the difference billing needs for
// charge_failed_hlr. A client code the account's order does not list is
// ignored. The account's hlr_miss_policy then decides between sending with
// the network unknown and refusing.

// dipper is the single-provider live lookup the stage used before providers
// became per account — kept for callers that wire one hlr.Dipper (and the
// tests); it serves the live step when no liveLookup is set.
type dipper interface {
	Dip(ctx context.Context, msisdn string) (hlr.Entry, bool)
}

// liveLookup is the per-account live lookup (*hlr.Live).
type liveLookup interface {
	Try(ctx context.Context, providerIDs []int64, msisdn string) (e hlr.Entry, found, attempted bool)
}

// rangeIndex is the number-range and country index (*hlr.Ranges).
type rangeIndex interface {
	Range(msisdn string) (mcc, mnc string, ok bool)
	Country(msisdn string) (mcc string, ok bool)
}

type hlrStage struct {
	svc    *hlr.Service
	dip    dipper     // optional single live provider
	live   liveLookup // optional per-account providers; wins over dip
	ranges rangeIndex // optional operators-table index
	now    func() time.Time
}

// newHLRStage builds the stage from the deps. Each optional part is assigned
// only when set, so a nil pointer never becomes a non-nil interface holding a
// typed nil (which would make `s.dip != nil` wrongly true). withLive=false
// (a dry run that did not ask for a paid lookup) leaves the live step out.
func newHLRStage(d SubmitDeps, withLive bool) hlrStage {
	hs := hlrStage{svc: d.HLR}
	if withLive {
		if d.HLRLive != nil {
			hs.live = d.HLRLive
		} else if d.HLRDip != nil {
			hs.dip = d.HLRDip
		}
	}
	if d.HLRRanges != nil {
		hs.ranges = d.HLRRanges
	}
	return hs
}

func (hlrStage) Name() string { return "hlr" }
func (s hlrStage) Run(ctx context.Context, e *Envelope) *Reject {
	// GW-19 (2026-08-27): the resolution OUTCOME is stamped on the message
	// (mtlog hlr_source/hlr_ported) — the record IS the HLR log, and
	// summary_lookup recomputes from it like every other summary.
	clientMCC, clientMNC := e.Msg.MCC, e.Msg.MNC
	clientGave := clientMCC != "" && clientMNC != ""
	if !e.Account.HLRDBCheck {
		// HLR check off: nothing is looked up, and a client code is used as
		// it always was.
		if clientGave && e.Msg.HLRSource == "" {
			e.Msg.HLRSource = "operator"
		}
		return nil // HLRSource stays "" = not consulted
	}
	order := e.Account.HLROrder
	if order == nil {
		order = refdata.DefaultHLROrder // an account not loaded by the refdata loader
	}
	now := time.Now
	if s.now != nil {
		now = s.now
	}
	ttl := time.Duration(e.Account.HLRCacheTTLDays) * 24 * time.Hour
	e.Msg.MCC, e.Msg.MNC = "", ""
	e.Msg.HLROperator, e.Msg.HLRCCC = "", ""
	e.Msg.HLRCall = nil
	liveFailed := false
	for _, step := range order {
		switch step {
		case refdata.HLRStepClient:
			if clientGave {
				e.Msg.MCC, e.Msg.MNC = clientMCC, clientMNC
				e.Msg.HLRSource = "operator"
				return nil
			}
		case refdata.HLRStepData:
			if s.svc == nil {
				continue
			}
			if entry, ok := s.svc.Lookup(e.Msg.Destination); ok && !entry.Expired(ttl, now()) {
				e.Msg.MCC, e.Msg.MNC = entry.MCC, entry.MNC
				e.Msg.HLRSource = "db"
				e.Msg.HLRPorted = entry.Ported || s.otherNetworksRange(e)
				s.named(e, entry.OperatorName, entry.CCC)
				return nil
			}
		case refdata.HLRStepRange:
			if s.ranges == nil {
				continue
			}
			if mcc, mnc, ok := s.ranges.Range(e.Msg.Destination); ok {
				e.Msg.MCC, e.Msg.MNC = mcc, mnc
				e.Msg.HLRSource = "range"
				s.named(e, "", "")
				return nil
			}
		case refdata.HLRStepLive:
			// The Dipper is fully guarded (in-flight coalescing, per-dip
			// timeout, max-inflight ceiling with async fallback), so this
			// NEVER blocks the accept path past the latency budget.
			var (
				entry            hlr.Entry
				found, attempted bool
			)
			switch {
			case s.live != nil:
				entry, found, attempted = s.live.Try(ctx, e.Account.HLRProviderIDs, e.Msg.Destination)
			case s.dip != nil:
				entry, found = s.dip.Dip(ctx, e.Msg.Destination)
				attempted = true
			default:
				continue
			}
			// The call made to the provider, found or not, for the lookup's
			// log row (6 Nov 2026); the newest live step's.
			if entry.Call != nil {
				e.Msg.HLRCall = entry.Call
			}
			if found {
				e.Msg.MCC, e.Msg.MNC = entry.MCC, entry.MNC
				e.Msg.HLRSource = "dip"
				e.Msg.HLRPorted = entry.Ported || s.otherNetworksRange(e)
				s.named(e, entry.OperatorName, entry.CCC)
				return nil
			}
			liveFailed = liveFailed || attempted
		case refdata.HLRStepCountry:
			if s.ranges == nil {
				continue
			}
			if mcc, ok := s.ranges.Country(e.Msg.Destination); ok {
				// A country-only answer is an answer the account chose to
				// accept: routes and rates written for the whole country
				// (an MCC-only code) apply, network-specific ones do not.
				e.Msg.MCC, e.Msg.MNC = mcc, ""
				e.Msg.HLRSource = "country"
				return nil
			}
		}
	}
	e.Msg.HLRSource = "miss" // consulted, nothing found
	if liveFailed {
		e.Msg.HLRSource = "dip_miss"
	}
	// B-11 (PO 2026-09-03): an account may refuse rather than route blind.
	// Without MCC/MNC the route stage cannot pick an operator-specific vendor
	// and the rater cannot price against a per-operator card, so a customer
	// contracted per operator may prefer a clean refusal to a guess.
	//
	// The ruling is the STRICT reading: any unresolved number rejects. A
	// provider timeout, a provider outage and a full in-flight ceiling all
	// count as a miss here, exactly as a definitive not-found does — the three
	// are indistinguishable at this point by design (hlr.Dipper returns one
	// bool). Operational consequence, deliberately accepted: while an HLR
	// provider is down an account on this policy sends NOTHING. The dipper
	// logs its own failures, so an outage is diagnosable from the same run.
	//
	// Default is "proceed" — an account that has not opted in behaves exactly
	// as it did before this existed.
	if e.Account.HLRMissPolicy == "reject" {
		return reject(403, "HLR_UNRESOLVED: operator could not be identified for this destination")
	}
	return nil // routes undetected
}

// otherNetworksRange says the number's range in the operators table is a
// network other than the one the answer gave (6 Nov 2026, Saleem: "if mnc and
// mcc for a number is in the operators list, and the HLR provider says a
// different mnc/mcc, then it is ported").
func (s hlrStage) otherNetworksRange(e *Envelope) bool {
	if s.ranges == nil || e.Msg.MCC == "" {
		return false
	}
	mcc, mnc, ok := s.ranges.Range(e.Msg.Destination)
	return ok && (mcc != e.Msg.MCC || netcode.Norm(mnc) != netcode.Norm(e.Msg.MNC))
}

// namer is a range index that also knows a range's operator name and
// calling code (*hlr.Ranges); a test's index may not.
type namer interface {
	Named(msisdn string) (name, ccc string, ok bool)
}

// named sets the operator's name and the country calling code on the message
// (6 Nov 2026): the answer's own, else what the number range says, as long as
// the range is the same network the answer gave.
func (s hlrStage) named(e *Envelope, name, ccc string) {
	if (name == "" || ccc == "") && s.ranges != nil {
		if n, ok := s.ranges.(namer); ok {
			if mcc, mnc, inRange := s.ranges.Range(e.Msg.Destination); inRange {
				if rn, rc, has := n.Named(e.Msg.Destination); has {
					if rc != "" && ccc == "" {
						ccc = rc // the calling code is the country's, whatever the network
					}
					if name == "" && mcc == e.Msg.MCC && netcode.Norm(mnc) == netcode.Norm(e.Msg.MNC) {
						name = rn
					}
				}
			}
		}
	}
	e.Msg.HLROperator, e.Msg.HLRCCC = name, ccc
}

// ResolveNetwork runs an account's HLR order for one number outside a submit
// (GET /sms/v4/operator), so the lookup endpoint answers exactly as sending
// would. The caller asked for the lookup, so it runs even when the account's
// HLR check is off, and it never refuses.
func ResolveNetwork(ctx context.Context, d SubmitDeps, acct refdata.Account, dest string) model.Message {
	acct.HLRDBCheck = true
	acct.HLRMissPolicy = "proceed"
	m := model.Message{Destination: dest}
	newHLRStage(d, true).Run(ctx, &Envelope{Msg: &m, Account: &acct})
	return m
}
