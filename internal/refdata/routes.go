// Routes: the Route record, its levels and regex conditions, and the cache's
// route lookups.

package refdata

import (
	"encoding/json"
	"regexp"
	"sort"

	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/netcode"
	"github.com/yakugate/yakugate/internal/sendplan"
	"github.com/yakugate/yakugate/lib/encoding"
)

// Route maps network (combined MCC+MNC) + regex conditions -> vendor, ordered
// by seq.
type Route struct {
	ID int64 // routes.id — stamped onto the message as RouteID (audit)
	// Name is the operator's own label for the rule ("jazz", "falcon MT").
	// Carried so a flow capture can say WHICH rule matched in the words the
	// operator wrote, not just an id they would have to look up.
	Name      string
	Seq       int    // priority (lower = better); routes sharing a Seq form one tier
	RouteType string // direct | failover | loadbalance | lcr | capacity | quality
	Weight    int    // loadbalance share within a tier (round-robin proportional)
	Direction string // MT (submit path) | MO (inbound; ignored by the submit route stage)
	AccountID int64
	// MatchMCCMNCs is the combined "mccmnc" strings this route serves (e.g.
	// "41001" = MCC 410 MNC 01, same convention as operators.mccmnc and the
	// request's `operator` field) — nil/empty matches ANY network, same
	// wildcard semantics the old blank match_mcc/match_mnc had. Replaces the
	// old separate match_mcc/match_mnc/match_mccs/match_mncs (2026-07-28
	// routes review — one combined array avoids the ambiguity of separate
	// MCC/MNC arrays: does mccs=[410,420]+mncs=[01,03] mean any combination
	// or specific pairs?). [SCOPE P1-15]
	MatchMCCMNCs []string
	// AcceptUndetectedNetwork: this route also matches when HLR couldn't
	// determine the destination's network at all (e.g.Msg.MCC/MNC empty) —
	// distinct from MatchMCCMNCs being empty (which means "any KNOWN
	// network"), since a route scoped to specific networks may still want to
	// opt in to undetected traffic too. Column config/0033; read by the
	// route stage's routeMismatch (a listed-network rule refuses an unknown
	// network unless it opted in).
	AcceptUndetectedNetwork bool
	SenderRegex             *Regex
	ReceiverRegex           *Regex
	KeywordRegex            *Regex
	// PriorityRegex matches the message's priority (as a string) — a route
	// match FILTER like the three regexes above, not a priority-setting
	// mechanism (2026-07-28 routes review: forced_priority was dropped —
	// template-level priority forcing, S-11, already covers that need —
	// priority_regex was repurposed as a plain condition instead of removed
	// alongside it).
	PriorityRegex *Regex
	VendorCID     string
	VendorID      int64
	VendorStatus  string // vendors.service_status: active|paused|disabled (failover health)
	// VendorDeployments is where the vendor is placed (vendors.deployments,
	// config/0122); empty = everywhere. With separate queues (D-121) a
	// deployment may only pick a vendor placed on it — it sends its own
	// messages, so one put on a vendor that does not run here would wait for
	// ever.
	VendorDeployments deployment.List
	// VendorShortCodes are the vendor's senders that have a queue of their
	// own (vendors.short_codes, config/0161, issue 22): a message from one of
	// them waits there, every other one in the vendor's common queue.
	VendorShortCodes []string `json:",omitempty"`
	Transport        string
	// VendorWire and VendorMultipart are how the vendor is SENT a message
	// (protocol_config.encodings / .multipart_mode). The route stage counts
	// the parts the vendor will receive with them, and the client is charged
	// that count (PO 2026-09-21: billed by the vendor's encoding).
	VendorWire      encoding.WireRules `json:",omitempty"`
	VendorMultipart string             `json:",omitempty"`
	BuyRateCardID   int64              // buying_rate_card_id (LCR cost comparison)
	// SellRateCardID is routes.selling_rate_card_id — the route's OWN sell
	// card (on a vendor entry: the vendor's card for this rule, else the
	// rule's), for loss detection (S-03). Since routing plans (D-120,
	// routing-plans.md §7) the loss check reads billing's whole ladder, the
	// account's default selling card included (pipeline sellCard); a message
	// no card prices is still "unpriced", never a loss.
	SellRateCardID int64
	// BuyCards and SellCards are the card ladders billing walks for this
	// vendor entry (billing.Rater.RateFull): the first card that is enabled
	// and holds a rate prices the side (PricedCard) — a disabled or empty card
	// is stepped past, never read as "unpriced" (QA BILL-004). Buy: the
	// vendor's own card for this rule, the rule's buying card, the vendor's
	// default; sell: the vendor's own card for this rule, the rule's selling
	// card (the route stage adds the account's default after them). Nil on a
	// rule built without them: BuyRateCardID/SellRateCardID alone.
	BuyCards  []int64 `json:",omitempty"`
	SellCards []int64 `json:",omitempty"`
	Disabled  bool

	// --- "Where to send" (config/0131, Saleem 2026-09-30) ---
	// Levels is Main, then Backup 1, Backup 2…: each a way of choosing plus
	// its vendors. Every MT rule that loaded has at least Main. An old rule
	// (send_plan NULL, Legacy) is one Main level with its own vendor.
	Levels []Level `json:",omitempty"`
	// Exceptions gives one network (MCC+MNC code) its own levels.
	Exceptions map[string][]Level `json:",omitempty"`
	// Retry is the rule's retries in rounds (defaults when the rule has none).
	Retry  sendplan.Retry `json:",omitempty"`
	Legacy bool           `json:",omitempty"`
	// On a VENDOR ENTRY (a Route inside a Level): which level it is in
	// (0 = Main) and its share when that level shares by %.
	Level int `json:",omitempty"`
	Pct   int `json:",omitempty"`
}

// Level is one level of a rule: Main or a backup. Vendors are vendor entries
// — copies of the rule carrying one vendor's details and its prices for this
// rule — so every vendor-level decision reads one *Route, as it always has.
type Level struct {
	Method  string
	Share   bool
	Vendors []*Route
	// Legacy marks an old rule's level (or an old shared tier): a down
	// vendor's share is dropped and the rest split by their own weights, as
	// old load-balanced tiers always did.
	Legacy bool `json:",omitempty"`
}

// LevelsFor returns the levels a message to mcc/mnc uses: the exception for
// its network when the rule has one, else the rule's own. key is the
// exception's code, "" for the rule's own. On a network-only rule (no levels
// of its own) a network without an exception gets nil: the rule does not
// carry it.
//
// Codes are tried in sorted order, so two codes naming the same network
// ("41004" and "410004") always resolve the same way — never by map order.
func (r *Route) LevelsFor(mcc, mnc string) (levels []Level, key string) {
	if mcc != "" && mnc != "" && len(r.Exceptions) > 0 {
		codes := make([]string, 0, len(r.Exceptions))
		for code := range r.Exceptions {
			codes = append(codes, code)
		}
		sort.Strings(codes)
		for _, code := range codes {
			if netcode.Matches(code, mcc, mnc) {
				return r.Exceptions[code], code
			}
		}
	}
	return r.Levels, ""
}

// NetworksOnly reports whether the rule sends only for the networks it has
// its own vendors for.
func (r *Route) NetworksOnly() bool { return len(r.Levels) == 0 && len(r.Exceptions) > 0 }

// ExceptionCodes lists the networks that have their own vendors, sorted.
func (r *Route) ExceptionCodes() []string {
	out := make([]string, 0, len(r.Exceptions))
	for c := range r.Exceptions {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// Regex is the standing {match, replace} JSON shape; either part may be nil.
type Regex struct {
	Match   *regexp.Regexp
	Replace *string
}

// regexJSON is Regex's wire shape for the Redis reference snapshot
// (2026-07-24): the PATTERN travels, the compiled program is rebuilt on
// unmarshal — *regexp.Regexp itself is not serializable.
type regexJSON struct {
	Pattern string  `json:"pattern"`
	Replace *string `json:"replace,omitempty"`
}

// MarshalJSON serializes the raw pattern (compiled form is process-local).
func (r Regex) MarshalJSON() ([]byte, error) {
	doc := regexJSON{Replace: r.Replace}
	if r.Match != nil {
		doc.Pattern = r.Match.String()
	}
	return json.Marshal(doc)
}

// UnmarshalJSON recompiles the pattern. A pattern that no longer compiles
// yields a nil Match (rule inert), never an error — one bad row must not
// take the whole snapshot down, matching loader_mysql's compileRegex policy.
func (r *Regex) UnmarshalJSON(b []byte) error {
	var doc regexJSON
	if err := json.Unmarshal(b, &doc); err != nil {
		return err
	}
	r.Replace = doc.Replace
	if doc.Pattern != "" {
		if re, err := regexp.Compile(doc.Pattern); err == nil {
			r.Match = re
		}
	}
	return nil
}

func (m *Mem) Routes() []Route {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.routes
}

// VendorSwitchedOn reports what the cached routes say about a vendor's
// operator switch: on=true when a route names it with status active. known is
// false when no cached route names the vendor at all.
func (m *Mem) VendorSwitchedOn(cid string) (on, known bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for i := range m.routes {
		if r := &m.routes[i]; r.VendorCID == cid {
			return r.VendorStatus == "" || r.VendorStatus == "active", true
		}
	}
	return false, false
}

// RoutesFor returns the account's own routes and the shared global
// (account_id NULL -> 0) routes as two pre-indexed slices (chunk-7 ruling
// 7.2). Snapshots into the current refresh's route set; callers must not
// mutate them.
func (m *Mem) RoutesFor(accountID int64) (own, global []*Route) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.routesByAcct[accountID], m.routesGlobal
}

// RouteByID finds a routing rule by id (nil when it no longer exists).
func (m *Mem) RouteByID(id int64) *Route {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for i := range m.routes {
		if m.routes[i].ID == id {
			return &m.routes[i]
		}
	}
	return nil
}
