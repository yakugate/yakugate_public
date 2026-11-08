// Loading routes, their vendors and levels, the stored regex columns, and
// the rate cards.

package refdata

import (
	"context"
	"encoding/json"
	"log"
	"regexp"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/sendplan"
	"github.com/yakugate/yakugate/lib/encoding"
)

func (l *MySQL) LoadRoutes(ctx context.Context) ([]Route, error) {
	vendors, err := l.loadRouteVendors(ctx)
	if err != nil {
		return nil, err
	}
	refs, readErr := l.ref.Routes(ctx)
	if readErr != nil && refs == nil {
		return nil, bare(readErr)
	}
	var out []Route
	for _, r := range refs {
		var (
			rt = Route{ID: r.ID, Name: r.Name, Seq: r.Priority, RouteType: r.RouteType, Weight: r.Weight,
				Direction: r.Direction, AccountID: r.AccountID, BuyRateCardID: r.BuyCard, SellRateCardID: r.SellCard}
			disabled, acceptUD = r.Disabled, r.AcceptUndetected
			mccmncsJSON        = r.MCCMNCs
			sre, rre, kre, pre = r.Sender, r.Receiver, r.Keyword, r.PriRule
			vendorID           = r.VendorID
			planJSON, backoff  = r.Plan, r.Backoff
			rounds, first, mx  = r.Rounds, r.First, r.MaxWait
		)
		rt.Disabled = b(disabled)
		rt.AcceptUndetectedNetwork = b(acceptUD)
		if mccmncsJSON.Valid && mccmncsJSON.String != "" {
			_ = json.Unmarshal([]byte(mccmncsJSON.String), &rt.MatchMCCMNCs)
		}
		// A regex that does not compile must NEVER leave the route in place
		// with that filter quietly missing (GW-32, PO 2026-08-28). Dropping
		// one filter WIDENS the route: a route meant for one network becomes
		// a catch-all and steals traffic another route was built to carry.
		// The route is skipped instead — a route that carries nothing is
		// safe and shows up as NO_ROUTE; a route that carries everything is
		// a silent mis-delivery. Same say-something rule as spam/templates.
		bad := false
		for _, f := range []struct {
			name string
			raw  dal.NullString
			dst  **Regex
		}{
			{"sender_regex", sre, &rt.SenderRegex},
			{"receiver_regex", rre, &rt.ReceiverRegex},
			{"keyword_regex", kre, &rt.KeywordRegex},
			{"priority_regex", pre, &rt.PriorityRegex},
		} {
			re, err := compileRegex(f.raw)
			if err != nil {
				log.Printf("[refdata] route %d (%s) %s does not compile (%v) — ROUTE SKIPPED", rt.ID, rt.Name, f.name, err)
				bad = true
				break
			}
			*f.dst = re
		}
		if bad {
			continue
		}
		// The rule's own vendor (vendor_id): an old rule's only vendor, and
		// Main's first vendor on a new one — kept for the older readers.
		if v, ok := vendors[vendorID]; ok {
			v.applyTo(&rt)
		}
		rt.Retry = sendplan.DefaultRetry
		if rounds.Valid || first.Valid || backoff.Valid || mx.Valid {
			r := sendplan.Retry{Rounds: int(rounds.Int64), FirstSec: int(first.Int64), Backoff: backoff.String, MaxWaitSec: int(mx.Int64)}
			if !rounds.Valid {
				r.Rounds = sendplan.DefaultRetry.Rounds
			}
			r = r.WithDefaults(true)
			if err := r.Validate(); err != nil {
				log.Printf("[refdata] route %d (%s) %v — using the gateway's default retries", rt.ID, rt.Name, err)
				r = sendplan.DefaultRetry
			}
			rt.Retry = r
		}
		if rt.Direction == "" || rt.Direction == "MT" {
			var plan *sendplan.Plan
			if planJSON.Valid && strings.TrimSpace(planJSON.String) != "" {
				plan, err = sendplan.Parse([]byte(planJSON.String))
				if err != nil {
					// Same rule as a bad pattern: a rule whose vendors cannot
					// be read sends nothing rather than something unintended.
					log.Printf("[refdata] route %d (%s) %v — ROUTE SKIPPED", rt.ID, rt.Name, err)
					continue
				}
			}
			if plan == nil {
				if vendorID == 0 || vendors[vendorID] == nil {
					continue // an old rule with no vendor carries nothing (was the INNER JOIN)
				}
				p := sendplan.FromLegacy(rt.RouteType, vendorID, rt.Weight)
				plan, rt.Legacy = &p, true
			}
			rt.Levels = resolveLevels(&rt, plan, plan.Levels, vendors, "")
			for code, lv := range plan.Exceptions {
				if res := resolveLevels(&rt, plan, lv, vendors, code); len(res) > 0 {
					if rt.Exceptions == nil {
						rt.Exceptions = map[string][]Level{}
					}
					rt.Exceptions[code] = res
				}
			}
			// A network-only plan (no levels of its own) is fine as long as one
			// of its networks still has a vendor.
			if len(rt.Levels) == 0 && len(rt.Exceptions) == 0 {
				log.Printf("[refdata] route %d (%s) names no vendor that exists — ROUTE SKIPPED", rt.ID, rt.Name)
				continue
			}
			if rt.VendorCID == "" { // no usable vendor_id: take the plan's first vendor
				if v, ok := vendors[plan.FirstVendor()]; ok {
					v.applyTo(&rt)
				}
			}
		} else if vendorID != 0 && vendors[vendorID] == nil {
			continue // MO rule naming a vendor that is gone (was the INNER JOIN)
		} else if vendorID == 0 {
			continue // MO rules are the mo role's (internal/mo); nothing here reads them without a vendor
		}
		out = append(out, rt)
	}
	return out, bare(readErr)
}

// routeVendor is what a rule needs to know about a vendor it names.
type routeVendor struct {
	id                  int64
	cid, status, transp string
	wire                encoding.WireRules
	multipart           string
	defaultBuy          int64           // vendors.default_mt_buying_rate_card_id (config/0111)
	deployments         deployment.List // where it is placed (config/0122); empty = everywhere
	shortCodes          []string        // vendors.short_codes (config/0161): senders with a queue of their own
}

func (v *routeVendor) applyTo(r *Route) {
	r.VendorCID, r.VendorID, r.VendorStatus, r.Transport = v.cid, v.id, v.status, v.transp
	r.VendorWire, r.VendorMultipart = v.wire, v.multipart
	r.VendorDeployments = v.deployments
	r.VendorShortCodes = v.shortCodes
}

func (l *MySQL) loadRouteVendors(ctx context.Context) (map[int64]*routeVendor, error) {
	// The short codes (config/0161): a store without the column has none.
	refs, err := l.ref.RouteVendors(ctx, l.hasVendorShortCodes(ctx))
	if err != nil && refs == nil {
		return nil, bare(err)
	}
	out := map[int64]*routeVendor{}
	for _, r := range refs {
		v := routeVendor{id: r.ID, cid: r.CID, status: r.Status, transp: r.Transport, defaultBuy: r.DefaultBuy}
		wireJSON, mpMode, acceptedJSON, deps, scJSON := r.WireJSON, r.MultipartMode, r.Accepted, r.Deployments, r.ShortCodes
		v.shortCodes = parseStringList(dal.NullString{String: scJSON, Valid: scJSON != ""})
		// Forgiving, as everywhere the engine reads placement: an unreadable
		// value means everywhere rather than nowhere.
		v.deployments, _ = deployment.Parse(deps)
		// Problems are the vendor role's to report (it logs them on load);
		// here the same defaults apply, so the count matches what it sends.
		v.wire, _ = encoding.ParseVendorEncodings(json.RawMessage(wireJSON), json.RawMessage(acceptedJSON))
		v.multipart = encoding.VendorMultipartMode(mpMode)
		out[v.id] = &v
	}
	return out, bare(err)
}

// resolveLevels turns a plan's levels into vendor entries: copies of the
// rule, one per vendor, carrying that vendor and its prices for this rule.
// Buy: the vendor's own card for this rule, else the rule's buying card (an
// old rule's), else the vendor's default buy card — the same ladder billing
// charges by, so LCR and the loss check compare what will be paid. Sell: the
// vendor's own card for this rule, else the rule's. A vendor that no longer
// exists is left out; a level left empty is dropped. Every level chooses by
// the rule's way (plan.MethodFor: the plan's, else — a rule stored before
// 2026-09-30 — the level's own).
func resolveLevels(rule *Route, plan *sendplan.Plan, levels []sendplan.Level, vendors map[int64]*routeVendor, where string) []Level {
	var out []Level
	for i, lv := range levels {
		res := Level{Method: plan.MethodFor(lv), Share: lv.Share, Legacy: rule.Legacy}
		for _, pv := range lv.Vendors {
			v, ok := vendors[pv.VendorID]
			if !ok {
				log.Printf("[refdata] route %d (%s) %s %s: vendor %d does not exist — left out", rule.ID, rule.Name, where, sendplan.LevelName(i), pv.VendorID)
				continue
			}
			e := *rule
			e.Levels, e.Exceptions = nil, nil
			v.applyTo(&e)
			e.Level = len(out)
			if lv.Share {
				e.Pct = pv.Pct
				e.Weight = pv.Pct
			}
			switch {
			case pv.BuyRateCardID > 0:
				e.BuyRateCardID = pv.BuyRateCardID
			case rule.BuyRateCardID > 0:
				e.BuyRateCardID = rule.BuyRateCardID
			default:
				e.BuyRateCardID = v.defaultBuy
			}
			if pv.SellRateCardID > 0 {
				e.SellRateCardID = pv.SellRateCardID
			}
			// The whole ladder too: billing steps past a disabled or empty
			// card to the next one, so the route stage must as well.
			e.BuyCards = cardLadder(pv.BuyRateCardID, rule.BuyRateCardID, v.defaultBuy)
			e.SellCards = cardLadder(pv.SellRateCardID, rule.SellRateCardID)
			res.Vendors = append(res.Vendors, &e)
		}
		if len(res.Vendors) > 0 {
			out = append(out, res)
		}
	}
	return out
}

// cardLadder keeps the ids that name a card, in order (Route.BuyCards).
func cardLadder(ids ...int64) []int64 {
	var out []int64
	for _, id := range ids {
		if id > 0 {
			out = append(out, id)
		}
	}
	return out
}

// LoadRates reads buying rate-card rows (kind='buy') for LCR cost comparison,
// grouped by rate_card_id. [SCOPE P1-15 LCR]
func (l *MySQL) LoadRates(ctx context.Context) (map[int64][]RateRow, error) {
	m := map[int64][]RateRow{}
	// Was WHERE rc.kind='buy' — LCR (RateFor) was its only consumer. Widened
	// for loss detection (S-03, PO 2026-09-03), which needs the sell side
	// too; RateFor now filters Kind=="buy" itself so its own behaviour is
	// byte-for-byte unchanged.
	//
	// The same rows billing.Rater loads (2026-10-01): a scheduled row is
	// loaded too and skipped at match time until its effective_from, as
	// billing does — filtering it here left the cache on the old price until
	// some unrelated config change made it reload.
	refs, err := l.ref.Rates(ctx)
	if err != nil && !rowsErr(err) {
		return m, nil // rate cards optional; empty is fine
	}
	for _, x := range refs {
		id := x.CardID
		r := RateRow{Kind: x.Kind, Currency: x.Currency, MCC: x.MCC, MNC: x.MNC, Country: x.Country, Price: x.Price,
			ConvRate: x.ConvRate, CRPRef: x.CRPRef, ID: x.ID, TaxPct: x.TaxPct, ConvTo: x.ConvTo,
			OwnerBaseRate: x.OwnerBaseRate, OwnerBaseCRP: x.OwnerBaseCRP}
		eff := x.EffectiveFrom
		if eff.Valid {
			r.EffectiveFrom = eff.Time
		}
		m[id] = append(m[id], r)
	}
	return m, bare(err)
}

// compileRegex parses a stored {match,replace} JSON column. It reports a
// compile failure to the caller (GW-32) instead of quietly returning a Regex
// whose Match is nil — the stages skip a nil Match, so swallowing the error
// removes a FILTER rather than the rule, which widens whatever it guards.
// Malformed JSON is a different thing and stays "no rule configured": the
// column is optional, and an unreadable blob names no pattern to enforce.
func compileRegex(s dal.NullString) (*Regex, error) {
	if !s.Valid || s.String == "" {
		return nil, nil
	}
	var raw RawRegexJSON
	if err := json.Unmarshal([]byte(s.String), &raw); err != nil {
		return nil, nil
	}
	r := &Regex{}
	if raw.Match != "" {
		re, err := regexp.Compile(raw.Match)
		if err != nil {
			return nil, err
		}
		r.Match = re
	}
	if raw.Replace != "" {
		rep := raw.Replace
		r.Replace = &rep
	}
	if r.Match == nil && r.Replace == nil {
		return nil, nil
	}
	return r, nil
}

// RawRegexJSON is the on-disk {match,replace} JSON.
type RawRegexJSON struct {
	Match   string `json:"match"`
	Replace string `json:"replace"`
}

// hasVendorShortCodes reports whether the config store has vendors.short_codes
// (config/0161). Asked once a load; a store not upgraded yet has none.
func (l *MySQL) hasVendorShortCodes(ctx context.Context) bool {
	return l.ref.HasVendorShortCodes(ctx)
}
