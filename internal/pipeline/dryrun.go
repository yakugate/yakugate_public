package pipeline

// The "Will this send?" check (Tools, PO 2026-09-12).
//
// Support's most common question is "why was my message refused?", and the
// honest answer used to need an engineer: read the reject log, then the
// account's settings, then the routing rules. This file lets the engine answer
// it directly, before anything is sent — by running the SAME stages, in the
// SAME order, over the SAME cached configuration, and reporting what each one
// decided.
//
// What makes it safe to run on a live gateway is what it leaves out. Three
// stages have side effects and are replaced by placeholders in place (see
// submitStages): rate-limit spends tokens, dedup claims a key, enqueue writes
// to a vendor queue and holds balance. Everything else only reads. The route
// stage runs in dry-run mode (no monitoring count, no capture record), no
// security events are raised, and a live HLR dip — which costs money — runs
// only when the caller asks for it.

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/refdata"
	"github.com/yakugate/yakugate/internal/sendplan"
)

// DryRunOptions tunes a check.
type DryRunOptions struct {
	// LiveHLR allows a live HLR dip on a cache miss. Off by default: a dip
	// is a paid network call, and a check should not cost money unless the
	// operator chose that.
	LiveHLR bool
	// SkipIPAllow reports the IP allow list as not checked. Set when the
	// caller gave no address to test: a check run from the portal has no
	// client address of its own worth judging.
	SkipIPAllow bool
}

// Why each skipped stage is not judged. Exported so the caller can describe
// what WOULD apply (the configured caps) next to the reason.
const (
	NotCheckedRateLimit = "depends on how much the account is sending at that second"
	NotCheckedDedup     = "an identical message (same sender, number and text) sent recently would be answered as a duplicate with the first message's id"
	NotCheckedEnqueue   = "nothing is written to the vendor queue and no balance is held"
	NotCheckedIPAllow   = "no caller address was given, so the account's IP allow list was not tested"
)

// notChecked stands in for a stage a check must not run.
type notChecked struct{ name, why string }

func (s notChecked) Name() string                         { return s.name }
func (notChecked) Run(context.Context, *Envelope) *Reject { return nil }

// NewDryRun builds the submit pipeline for a check. Build one per check: it is
// a slice of small values, and a fresh one keeps the load-balance counter of a
// check apart from real traffic.
func NewDryRun(d SubmitDeps, opts DryRunOptions) *Pipeline {
	return New(submitStages(d, &opts)...)
}

// NewPriceCheck builds the two stages that decide a price: HLR (which
// network the number is on) and route (which rule, so which rate cards). The
// content gates are left out on purpose — "what would this number cost?"
// should be answerable before the message is written.
func NewPriceCheck(d SubmitDeps, opts DryRunOptions) *Pipeline {
	return New(newHLRStage(d, opts.LiveHLR), routeStage{ref: d.Ref, rr: newRR(), fx: d.FX, qualityMinSample: d.QualityMinSample, health: d.VendorHealth, dryRun: true,
		lossAcceptNoRate: lossAcceptsNoRate(), baseCur: BaseCurrency()})
}

// Step is what one stage did in a check.
type Step struct {
	Stage string `json:"stage"`
	// Outcome is "pass", "refused" or "not_checked". A stage after the one
	// that refused does not appear at all: real traffic never reaches it.
	Outcome string `json:"outcome"`
	// Note says why a stage was not checked.
	Note string `json:"note,omitempty"`
	// Changed lists what the stage altered, as "before -> after" — the same
	// wording a flow capture uses.
	Changed map[string]any `json:"changed,omitempty"`
	Micros  int64          `json:"micros"`
}

// TraceResult is a whole check.
type TraceResult struct {
	Steps  []Step
	Reject *Reject
	// RefusedAt names the stage that refused ("" when none did).
	RefusedAt string
	// BeforeRoute is the message as the route stage saw it — after HLR and
	// auto-replace, before the winning rule's rewrites. nil when a stage
	// before route refused. ExplainRoutes judges every rule against this.
	BeforeRoute *model.Message
}

// Trace runs the stages like Run, but records each one's outcome and writes
// nothing to a flow capture — a check is not traffic, and must not appear in
// a capture an operator is reading as the account's real submits.
func (p *Pipeline) Trace(ctx context.Context, e *Envelope) TraceResult {
	var out TraceResult
	for _, s := range p.stages {
		if s.Name() == "route" {
			cp := *e.Msg
			out.BeforeRoute = &cp
		}
		if nc, ok := s.(notChecked); ok {
			out.Steps = append(out.Steps, Step{Stage: nc.name, Outcome: "not_checked", Note: nc.why})
			continue
		}
		before := captureSnapshot(e.Msg)
		noted := len(e.Notes)
		start := time.Now()
		r := s.Run(ctx, e)
		step := Step{Stage: s.Name(), Outcome: "pass", Micros: time.Since(start).Microseconds(),
			Changed: captureChanges(before, captureSnapshot(e.Msg))}
		// What the stage noticed and let pass (a check it could not make).
		if len(e.Notes) > noted {
			step.Note = strings.Join(e.Notes[noted:], "; ")
		}
		if r != nil {
			step.Outcome = "refused"
			out.Steps = append(out.Steps, step)
			out.Reject, out.RefusedAt = r, s.Name()
			return out
		}
		out.Steps = append(out.Steps, step)
	}
	return out
}

// RouteCheck is one routing rule, judged against one message.
type RouteCheck struct {
	ID       int64  `json:"id"`
	Name     string `json:"name"`
	Scope    string `json:"scope"` // account (rules for every account are retired)
	Priority int    `json:"priority"`
	Type     string `json:"type"`
	Vendor   string `json:"vendor"`
	// VendorUp is false when no vendor of the rule can send now (paused,
	// disabled, or no working connection for its "down after" time): a
	// matching rule with no vendor up hands the message to the next rule.
	VendorUp bool `json:"vendorUp"`
	// VendorDown says why the rule's first vendor is down; "" when it is up.
	VendorDown string `json:"vendorDown,omitempty"`
	Matched    bool   `json:"matched"`
	// Why says what stopped the rule taking the message; "" when it matched.
	Why string `json:"why,omitempty"`
	// Levels are Main and the backups the message would use (the exception
	// for its network when the rule has one), each vendor with its state.
	Levels    []LevelCheck `json:"levels,omitempty"`
	Exception string       `json:"exception,omitempty"`
}

// LevelCheck is one level of a rule as a message would meet it.
type LevelCheck struct {
	Name    string        `json:"name"` // Main, Backup 1, …
	Method  string        `json:"method"`
	Share   bool          `json:"share,omitempty"`
	Vendors []VendorCheck `json:"vendors"`
}

// VendorCheck is one vendor of a level.
type VendorCheck struct {
	Vendor string `json:"vendor"`
	Up     bool   `json:"up"`
	Down   string `json:"down,omitempty"`
	Pct    int    `json:"pct,omitempty"`
}

// ExplainRoutes judges every routing rule of the account against msg, in the
// order the route stage considers them. It asks routeMismatch, the function
// the route stage itself asks. health may be nil (only the operator's switch
// then counts).
func ExplainRoutes(ref *refdata.Mem, health VendorHealth, acct *refdata.Account, msg *model.Message) []RouteCheck {
	if ref == nil || acct == nil || msg == nil {
		return nil
	}
	e := &Envelope{Msg: msg, Account: acct}
	own, _ := ref.RoutesFor(acct.ID)
	state := func(r *refdata.Route) (bool, string) {
		switch {
		case r.VendorStatus == "paused":
			return false, "paused by an operator"
		case !vendorSwitchedOn(r):
			return false, "disabled"
		case health != nil:
			if down, reason := health.Down(r.VendorID); down {
				return false, reason
			}
		}
		return true, ""
	}
	var out []RouteCheck
	for _, r := range sortedRoutes(own) {
		why := routeMismatch(r, e)
		rc := RouteCheck{
			ID: r.ID, Name: r.Name, Scope: routeScope(r), Priority: r.Seq, Type: r.RouteType,
			Vendor: r.VendorCID, Matched: why == "", Why: why,
		}
		if len(r.Levels) > 0 || len(r.Exceptions) > 0 {
			_, rc.Exception = r.LevelsFor(msg.MCC, msg.MNC)
		}
		first := true
		for i, lv := range ruleLevels(r, msg) {
			lc := LevelCheck{Name: sendplan.LevelName(i), Method: lv.Method, Share: lv.Share}
			for _, v := range lv.Vendors {
				up, down := state(v)
				if first {
					rc.VendorDown, first = down, false
				}
				rc.VendorUp = rc.VendorUp || up
				vc := VendorCheck{Vendor: v.VendorCID, Up: up, Down: down}
				if lv.Share {
					vc.Pct = v.Pct
				}
				lc.Vendors = append(lc.Vendors, vc)
			}
			rc.Levels = append(rc.Levels, lc)
		}
		if rc.VendorUp {
			rc.VendorDown = ""
		}
		out = append(out, rc)
	}
	return out
}

// sortedRoutes is a copy in the route stage's order: priority, then id. A
// copy, because the slices RoutesFor returns belong to the cache.
func sortedRoutes(in []*refdata.Route) []*refdata.Route {
	out := append([]*refdata.Route(nil), in...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Seq != out[j].Seq {
			return out[i].Seq < out[j].Seq
		}
		return out[i].ID < out[j].ID
	})
	return out
}
