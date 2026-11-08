package abusemonitor

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/yakugate/yakugate/internal/billingstore"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/abuse"
	"github.com/yakugate/yakugate/internal/dal"
)

// The daily reputation score (config/0107, abuse.Reputation). Shown only:
// nothing here or anywhere else acts on it (ruling 2026-09-09).
//
// Evidence comes from what is already kept, never from a scan of message
// tables: the summaries the billing role maintains (delivery from
// summary_mt, lookups from summary_lookup), the month's rejects, the
// account's STOP opt-outs, and the abuse monitor's own audit rows.

// reputationEvery is how often the scores are rewritten.
const reputationEvery = 24 * time.Hour

// A RESET (config/0143, Saleem 2026-10-07: "also need reputation reset
// button"). The score is rebuilt from the last 90 / 30 days every day, so a
// number put back by hand would be undone the next morning; a reset is a
// MOMENT instead — account_reputation.reset_at, written by the Admin API —
// and an account that has one is scored only on what happened after it. Its
// age still counts: that is a fact, not evidence. A reset newer than the
// row's own computed_at makes the role score THAT account again within
// resetLookEvery (rescoreResets), so the operator sees the new number in
// seconds, not tomorrow.

// repReset is one account's reset: the moment, and the same moment as text on
// each clock it is compared on.
type repReset struct {
	at     time.Time // UTC
	cutCfg string    // on the config store's session clock (audit_log, blocklist rows)
	cutUTC string    // in UTC (the hot store's summaries and rejects)
}

const repStamp = "2006-01-02 15:04:05"

// resetLookEvery is how often the role looks for a reset its scores do not
// show yet — its own pace, not the sweep's: the operator is waiting.
const resetLookEvery = 10 * time.Second

// loadResets reads the resets that still cut a window short (the longest
// window is 90 days). A store without the columns yet has none.
func (p *Process) loadResets(ctx context.Context) map[int64]repReset {
	out := map[int64]repReset{}
	rows, err := p.data.Reputation.Resets(ctx)
	if err != nil {
		if !strings.Contains(err.Error(), "1054") && !strings.Contains(err.Error(), "1146") {
			p.log.Printf("reputation resets: %v", err)
		}
		return out
	}
	for _, r := range rows {
		id, utc, cfg := r.Account, r.UTC, r.Config
		at, err := time.Parse(repStamp, utc)
		if err != nil {
			continue
		}
		out[id] = repReset{at: at, cutCfg: cfg, cutUTC: utc}
	}
	return out
}

// dbStamp is the config store's own UTC clock, as text. reset_at is written
// on that clock (the Admin API's UTC_TIMESTAMP()), so computed_at is too:
// "is the reset newer than the score?" is then never a question of two
// machines' clocks. It is read BEFORE the resets are, so a reset that lands
// while a pass is running stays newer than the score and is scored next.
func (p *Process) dbStamp(ctx context.Context, now time.Time) string {
	s, err := p.data.Reputation.Clock(ctx)
	if err != nil || !s.Valid {
		return now.UTC().Format(repStamp)
	}
	return s.String
}

// rescoreResets scores again each account whose reset its score does not
// show yet (reset_at at or after computed_at) — those accounts ONLY, each
// read by its own indexed queries. The day's full pass reads every account's
// evidence in grouped scans, which on a busy store takes over a minute (one
// month's rejects alone); a person who pressed Reset is not kept waiting for
// that.
func (p *Process) rescoreResets(ctx context.Context) (int, error) {
	now := time.Now()
	stamp := p.dbStamp(ctx, now)
	rows, err := p.data.Reputation.Pending(ctx)
	if err != nil {
		if strings.Contains(err.Error(), "1054") || strings.Contains(err.Error(), "1146") {
			return 0, nil // a store older than config/0143: nothing to do
		}
		return 0, err
	}
	in := map[int64]*abuse.ReputationInputs{}
	for _, r := range rows {
		in[r.Account] = &abuse.ReputationInputs{AgeDays: int(r.AgeDays.Int64)}
	}
	if len(in) == 0 {
		return 0, nil
	}
	resets := p.loadResets(ctx)
	p.applyResets(ctx, now, in, resets)
	written := 0
	for id, r := range in {
		if _, ok := resets[id]; !ok {
			continue // not read as a reset (older than every window): the day's pass scores it
		}
		if err := p.writeScore(ctx, id, r, stamp); err != nil {
			return written, err
		}
		written++
	}
	return written, nil
}

// writeScore stores one account's score. The reset columns are not named:
// a rewrite leaves an operator's reset where it is.
func (p *Process) writeScore(ctx context.Context, id int64, r *abuse.ReputationInputs, stamp string) error {
	score, reasons := abuse.Reputation(*r)
	why, _ := json.Marshal(reasons)
	inputs, _ := json.Marshal(r)
	if err := p.data.Reputation.WriteScore(ctx, id, score, string(why), string(inputs), stamp); err != nil {
		return fmt.Errorf("account %d: %w", id, err)
	}
	return nil
}

// watchResets scores again within resetLookEvery of an operator's reset. It
// runs beside Step for the life of the process — a sweep over many day tables
// takes minutes, and the person who pressed Reset is watching the number —
// and only while this copy holds the role (Step keeps leaseOK). repMu keeps
// it and the daily run from scoring at the same time.
func (p *Process) watchResets(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			p.log.Printf("reputation reset watch stopped: %v", r)
		}
	}()
	t := time.NewTicker(resetLookEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if !p.leaseOK.Load() {
			continue
		}
		p.repMu.Lock()
		if n, err := p.rescoreResets(ctx); err != nil {
			p.log.Printf("reputation after a reset: %v (%d written)", err, n)
		} else if n > 0 {
			p.log.Printf("reputation after a reset: %d account score(s) written", n)
		}
		p.repMu.Unlock()
	}
}

// applyResets replaces a reset account's evidence with what happened SINCE
// its reset. Resets are rare (a person does one), so each is read on its own
// rather than bending the grouped queries that serve every other account.
//
// The summaries are kept in buckets: one that began before the reset is left
// out whole, so the traffic of the reset's own bucket is not counted either —
// an error in the customer's favour, and gone by the next bucket.
func (p *Process) applyResets(ctx context.Context, now time.Time, in map[int64]*abuse.ReputationInputs, resets map[int64]repReset) {
	// said logs a failed read of one input; the input then counts nothing.
	said := func(err error) bool {
		if err != nil && err != dal.ErrNoRows && !strings.Contains(err.Error(), "1146") {
			p.log.Printf("reputation input after a reset: %v", err)
		}
		return err == nil
	}
	for id, rs := range resets {
		r := in[id]
		if r == nil {
			continue // the account is gone
		}
		ago := int(now.UTC().Sub(rs.at).Hours() / 24)
		if ago < 0 {
			ago = 0
		}
		*r = abuse.ReputationInputs{AgeDays: r.AgeDays, Reset: true, ResetDaysAgo: ago}

		if rows, err := p.data.Reputation.MonitorActionsSince(ctx, id, rs.cutCfg); err == nil {
			for _, a := range rows {
				action, n := a.Action, a.N
				switch action {
				case "abuse.warn":
					r.Warns += n
				case "abuse.throttle":
					r.Throttles += n
				case "abuse.suspend":
					r.Suspends += n
				}
			}
		} else {
			p.log.Printf("reputation input after a reset (abuse actions): %v", err)
		}

		var sent int
		if d, f, s, err := p.data.Reputation.DeliverySince(ctx, id, rs.cutUTC); said(err) {
			r.Delivered, r.Final, sent = d, f, s
		}
		r.Sent, r.Submitted = sent, sent
		if l, u, err := p.data.Reputation.LookupsSince(ctx, id, rs.cutUTC); said(err) {
			r.LookedUp, r.Unknown = l, u
		}
		if n, err := p.data.Reputation.OptOutsSince(ctx, id, rs.cutCfg); said(err) {
			r.OptOuts = n
		}
		for _, t := range p.rejectsTables(ctx, now) {
			var refused int
			if n, err := p.data.Reputation.RefusalsSince(ctx, t, id, rs.cutUTC); said(err) {
				refused = n
			}
			r.Refused += refused
			r.Submitted += refused
		}
	}
}

// reputationDue reports whether a day has passed since the last run, looking
// at the table itself so a restart does not recompute (or skip) a day.
func (p *Process) reputationDue(ctx context.Context, now time.Time) bool {
	if !p.lastReputation.IsZero() {
		return now.Sub(p.lastReputation) >= reputationEvery
	}
	last, err := p.data.Reputation.LastComputed(ctx)
	if err != nil {
		return false // table missing (not yet migrated) or unreadable: try next sweep
	}
	if last.Valid && now.UTC().Sub(last.Time) < reputationEvery {
		p.lastReputation = now.Add(-now.UTC().Sub(last.Time))
		return false
	}
	return true
}

// updateReputation recomputes every account's score and stores it.
func (p *Process) updateReputation(ctx context.Context, now time.Time) (int, error) {
	in := map[int64]*abuse.ReputationInputs{}
	get := func(id int64) *abuse.ReputationInputs {
		if in[id] == nil {
			in[id] = &abuse.ReputationInputs{}
		}
		return in[id]
	}
	// Accounts and their age.
	accts, err := p.data.Reputation.Accounts(ctx)
	if err != nil {
		return 0, fmt.Errorf("accounts: %w", err)
	}
	for _, a := range accts {
		r := get(a.Account)
		if a.Created.Valid {
			r.AgeDays = int(now.Sub(a.Created.Time).Hours() / 24)
		}
	}
	if len(in) == 0 {
		return 0, nil
	}
	// The abuse monitor's own actions, last 90 days.
	if rows, err := p.data.Reputation.MonitorActions(ctx); err == nil {
		for _, a := range rows {
			id, action, n := a.Account, a.Action, a.N
			if in[id] == nil {
				continue
			}
			switch action {
			case "abuse.warn":
				in[id].Warns += n
			case "abuse.throttle":
				in[id].Throttles += n
			case "abuse.suspend":
				in[id].Suspends += n
			}
		}
	} else {
		p.log.Printf("reputation input (abuse actions): %v", err)
	}
	// Delivery and volume, last 30 days (hot store, UTC buckets).
	for id, v := range p.sums(p.data.Reputation.Delivery(ctx)) {
		if r := in[id]; r != nil {
			r.Delivered, r.Final, r.Sent = v[0], v[1], v[2]
			r.Submitted += v[2]
		}
	}
	// Number lookups, last 30 days.
	for id, v := range p.sums(p.data.Reputation.Lookups(ctx)) {
		if r := in[id]; r != nil {
			r.LookedUp, r.Unknown = v[0], v[1]
		}
	}
	// STOP opt-outs, last 30 days.
	for id, v := range p.sums(p.data.Reputation.OptOuts(ctx)) {
		if r := in[id]; r != nil {
			r.OptOuts = v[0]
		}
	}
	// Submits refused for spam or a blocked number (trap numbers answer the
	// same code by design), last 30 days: this month's and last month's
	// rejects tables. A refused submit never reaches summary_mt, so it is
	// added to the submitted count too.
	//
	// From the accept path's daily counts when the monitor reads counters and
	// they go back the 30 days (counters.go); until then, and on the records
	// source, from the two tables — once a day.
	if counted, ok := p.refusalsFromCounters(ctx, now); ok {
		for id, n := range counted {
			if r := in[id]; r != nil {
				r.Refused += int(n)
				r.Submitted += int(n)
			}
		}
	} else {
		for _, t := range p.rejectsTables(ctx, now) {
			for id, v := range p.sums(p.data.Reputation.Refusals(ctx, t)) {
				if r := in[id]; r != nil {
					r.Refused += v[0]
					r.Submitted += v[0]
				}
			}
		}
	}

	// Accounts an operator reset: only what happened since (config/0143).
	// The stamp is read first, so a reset that lands from here on stays
	// newer than the score it missed.
	stamp := p.dbStamp(ctx, now)
	p.applyResets(ctx, now, in, p.loadResets(ctx))

	written := 0
	for id, r := range in {
		if err := p.writeScore(ctx, id, r, stamp); err != nil {
			return written, err
		}
		written++
	}
	p.lastReputation = now
	return written, nil
}

// sums takes a grouped read of the reputation's inputs: each account's
// counts. A failed read is logged and yields nothing: one missing input (a
// month with no rejects table yet) must not stop the day's scores.
func (p *Process) sums(out map[int64][]int, err error) map[int64][]int {
	if err != nil {
		if !strings.Contains(err.Error(), "1146") {
			p.log.Printf("reputation input: %v", err)
		}
		return map[int64][]int{}
	}
	return out
}

// rejectsTables are the refusals of the last 31 days, of every company
// (rejects_<company>_<YYYYMMDD>).
func (p *Process) rejectsTables(ctx context.Context, now time.Time) []string {
	if p.hot == nil {
		return nil
	}
	all, err := p.hot.ListRecordTables(ctx)
	if err != nil {
		p.log.Printf("list refusal tables: %v", err)
		return nil
	}
	oldest := billingstore.DayStart(now, -31)
	var out []string
	for _, t := range all {
		if t.Kind == billingstore.KindRejects && !t.Date.Before(oldest) {
			out = append(out, t.Name)
		}
	}
	return out
}
