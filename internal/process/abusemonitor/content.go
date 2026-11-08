package abusemonitor

import (
	"context"
	"encoding/json"
	"math"
	"strings"

	"github.com/yakugate/yakugate/internal/abuse"
)

// The content signals and the weighted score (P10 register, rulings of
// 2026-09-09; wired 2026-09-14).
//
// Formatting anomaly, opt-out rate and brand impersonation were each ruled
// "score input only, never its own rule": alone each is weak evidence (a real
// sale looks loud, a poor offer collects STOPs, a payments partner names a
// bank). So they never act here. They hand a 0..1 strength to the weighted
// score, together with the rung every ladder signal reached, and only the
// score acts — and it is capped at throttle by ruling: a sum of weak signals
// may slow an account, never suspend it.

// contributions is each account's input to the score in one sweep, plus a
// few facts per signal for the audit line when the score acts.
type contributions struct {
	by    map[int64][]abuse.Contribution
	notes map[int64]map[string]any
}

func newContributions() *contributions {
	return &contributions{by: map[int64][]abuse.Contribution{}, notes: map[int64]map[string]any{}}
}

func (c *contributions) add(acct int64, signal string, strength float64) {
	if strength <= 0 {
		return
	}
	c.by[acct] = append(c.by[acct], abuse.Contribution{Signal: signal, Strength: strength})
}

func (c *contributions) note(acct int64, key string, v any) {
	if c.notes[acct] == nil {
		c.notes[acct] = map[string]any{}
	}
	c.notes[acct][key] = v
}

// severityStrength is how a ladder signal enters the score: at the rung it
// reached. Its own rule has already acted on it; here it only corroborates.
func severityStrength(s abuse.Severity) float64 {
	switch s {
	case abuse.Warn:
		return 1.0 / 3
	case abuse.Throttle:
		return 2.0 / 3
	case abuse.Suspend:
		return 1
	}
	return 0
}

// msg is one sampled MT message: what the content signals read of it.
type msg struct {
	body   string // decrypted
	dest   string
	sender string
}

// defaultSampleMsgs is one account's sample when the rule gives none.
const defaultSampleMsgs = 200

// sampleMsgs reads MT messages accepted between fromMin and toMin minutes
// ago (toMin 0 = now), newest first, bodies decrypted: every content signal
// judges the text itself. only, when set, restricts the read to those
// accounts. reader names the caller, which keeps its own place in the round.
//
// Two budgets. perAcct is ONE account's sample (0 = defaultSampleMsgs): a
// table is one account's day, so it is the most that is read from a table
// and the most that is kept for an account. limit is the ceiling for the
// whole read (0 = 2000). With the ceiling alone, read in table-name order,
// one account sending 2,000 clean messages used all of it, and ten disguised
// messages from an account whose table sorts later were never read (QA
// 2026-10-08, B-09).
//
// When the ceiling still cuts a read short, the next sweep starts at the
// table after the last one read, so no account is always the one left out.
func (p *Process) sampleMsgs(ctx context.Context, reader string, tables []string, fromMin, toMin, perAcct, limit int, only map[int64]bool) map[int64][]msg {
	if p.view != nil && toMin == 0 {
		// From the texts the accept path kept (counters.go).
		return p.sampleCounters(ctx, reader, fromMin, perAcct, limit, only)
	}
	out := map[int64][]msg{}
	if len(tables) == 0 {
		return out
	}
	if limit <= 0 {
		limit = 2000
	}
	if perAcct <= 0 {
		perAcct = defaultSampleMsgs
	}
	var ids []int64 // the accounts read, when only some are
	if len(only) > 0 {
		ids = make([]int64, 0, len(only))
		for id := range only {
			ids = append(ids, id)
		}
	}
	n := len(tables)
	start := p.cursor[reader] % n
	read, next := 0, -1
	for i := 0; i < n; i++ {
		at := (start + i) % n
		if read >= limit {
			next = at
			break
		}
		take := perAcct
		if limit-read < take {
			take = limit - read
		}
		t := tables[at]
		// The newest of EACH account (dal.MessageWindows.Sample), so one
		// busy account does not take every place of the sample.
		rows, err := p.data.Windows.Sample(ctx, t, fromMin, toMin, ids, perAcct, limit-read)
		if err != nil {
			continue
		}
		for _, r := range rows {
			id := r.Account
			m := msg{body: r.Body, dest: r.Destination, sender: r.Sender}
			read++
			if len(out[id]) >= perAcct {
				continue
			}
			m.body = p.plainBody(ctx, m.body)
			out[id] = append(out[id], m)
		}
	}
	if p.cursor == nil {
		p.cursor = map[string]int{}
	}
	if next >= 0 {
		p.cursor[reader] = next
	} else {
		delete(p.cursor, reader)
	}
	return out
}

// bodiesOf is the text of each message, for the signals that read nothing else.
func bodiesOf(msgs []msg) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = m.body
	}
	return out
}

// countSent is each listed account's MT messages in the last windowMin.
func (p *Process) countSent(ctx context.Context, tables []string, windowMin int, only map[int64]bool) map[int64]int {
	out := map[int64]int{}
	if len(only) == 0 {
		return out
	}
	ids := make([]int64, 0, len(only))
	for id := range only {
		ids = append(ids, id)
	}
	for _, t := range tables {
		rows, err := p.data.Windows.Sent(ctx, t, windowMin, ids)
		if err != nil {
			continue
		}
		for _, r := range rows {
			out[r.Account] += r.N
		}
	}
	return out
}

// --- formatting: the account's text got louder than its own norm ---

func (p *Process) evalFormatting(ctx context.Context, tables []string, raw json.RawMessage, c *contributions) {
	var prm abuse.FormattingParams
	if !p.decodeParams("formatting", raw, &prm) {
		return
	}
	if p.view != nil {
		p.evalFormattingCounters(ctx, prm, c) // the shape from sums, no text read
		return
	}
	window := p.sampleMsgs(ctx, "formatting", tables, prm.WindowMin, 0, prm.SampleMsgs, prm.ScanLimit, nil)
	if len(window) == 0 {
		return
	}
	accts := make(map[int64]bool, len(window))
	for id := range window {
		accts[id] = true
	}
	baseline := p.sampleMsgs(ctx, "formatting baseline", tables, prm.BaselineHours*60+prm.WindowMin, prm.WindowMin, prm.SampleMsgs, prm.ScanLimit, accts)
	for id, msgs := range window {
		pp := prm
		if !p.over.apply("formatting", id, &pp) {
			continue // this account's own rules (config/0106)
		}
		ws, wn := abuse.AverageShape(bodiesOf(msgs))
		bs, bn := abuse.AverageShape(bodiesOf(baseline[id]))
		if s := abuse.EvalFormatting(ws, bs, wn, bn, pp); s > 0 {
			c.add(id, "formatting", s)
			c.note(id, "formatting", map[string]any{"strength": round3(s),
				"window": shapeNote(ws), "baseline": shapeNote(bs), "window_msgs": wn, "baseline_msgs": bn})
		}
	}
}

func shapeNote(s abuse.Shape) map[string]any {
	m := map[string]any{"punct": round3(s.Punct), "emoji": round3(s.Emoji), "padding": round3(s.Padding)}
	if s.Cased {
		m["caps"] = round3(s.Caps)
	}
	return m
}

// --- opt-out rate: recipients replying STOP to this account ---

// evalOptOut counts the account's opt-outs in the window — its blocklist rows
// with source 'unsub', written by the MO path when a subscriber sends STOP
// (internal/mo/optout.go) — per thousand messages it sent in the same window.
func (p *Process) evalOptOut(ctx context.Context, tables []string, raw json.RawMessage, c *contributions) {
	var prm abuse.OptOutParams
	if !p.decodeParams("optout", raw, &prm) {
		return
	}
	// A repeat STOP after a START re-enables the row, which moves updated_at;
	// a first STOP is its created_at. Both are this store's own clock, so
	// NOW() on the same connection compares like with like.
	rows, err := p.data.Accounts.OptOuts(ctx, prm.WindowMin)
	if err != nil {
		p.log.Printf("opt-out counts: %v", err)
		return
	}
	optOuts := map[int64]int{}
	accts := map[int64]bool{}
	for _, r := range rows {
		if r.N > 0 {
			optOuts[r.Account], accts[r.Account] = r.N, true
		}
	}
	if len(optOuts) == 0 {
		return
	}
	sent := map[int64]int{}
	if p.view != nil {
		var ok bool
		if sent, ok = p.sentCounters(ctx, "optout", prm.WindowMin, accts); !ok {
			return
		}
	} else {
		sent = p.countSent(ctx, tables, prm.WindowMin, accts)
	}
	for id, n := range optOuts {
		pp := prm
		if !p.over.apply("optout", id, &pp) {
			continue // this account's own rules (config/0106)
		}
		if s := abuse.EvalOptOutRate(n, sent[id], pp); s > 0 {
			c.add(id, "optout", s)
			c.note(id, "optout", map[string]any{"strength": round3(s), "opt_outs": n, "sent": sent[id],
				"per_thousand": round3(float64(n) * 1000 / float64(sent[id]))})
		}
	}
}

// --- brand impersonation: a protected name in someone else's text ---

func (p *Process) evalBrand(ctx context.Context, tables []string, raw json.RawMessage, c *contributions) {
	var prm abuse.BrandParams
	if !p.decodeParams("brand", raw, &prm) {
		return
	}
	brands := p.loadBrands(ctx)
	if len(brands) == 0 {
		return
	}
	for id, msgs := range p.sampleMsgs(ctx, "brand", tables, prm.WindowMin, 0, prm.SampleMsgs, prm.ScanLimit, nil) {
		bodies := bodiesOf(msgs)
		pp := prm
		if !p.over.apply("brand", id, &pp) {
			continue // this account's own rules (config/0106)
		}
		plain, disguised := 0, 0
		named := map[string]bool{}
		for _, b := range bodies {
			hits := abuse.FindBrandImpersonation(b, id, brands)
			if len(hits) == 0 {
				continue
			}
			d := false
			for _, h := range hits {
				named[h.Brand] = true
				d = d || h.Disguised
			}
			if d {
				disguised++
			} else {
				plain++
			}
		}
		if s := abuse.EvalBrand(plain, disguised, len(bodies), pp); s > 0 {
			names := make([]string, 0, len(named))
			for n := range named {
				names = append(names, n)
			}
			c.add(id, "brand", s)
			c.note(id, "brand", map[string]any{"strength": round3(s), "brands": names,
				"plain_msgs": plain, "disguised_msgs": disguised, "scanned": len(bodies)})
		}
	}
}

// loadBrands is the protected list: sender ids an operator marked "protected
// in message text" (sender_ids.body_protected, config/0104), each with the
// accounts entitled to it — those holding it as an APPROVED sender id, or
// naming it in their own allowed sender list. The '*' wildcard entitles
// nobody to a protected name: it means "may send as anything", which is not
// "may claim to be the bank".
func (p *Process) loadBrands(ctx context.Context) []abuse.Brand {
	names, err := p.data.Accounts.ProtectedNames(ctx)
	if err != nil {
		p.log.Printf("load protected names: %v", err)
		return nil
	}
	byName := map[string]*abuse.Brand{}
	for _, n := range names {
		if n != "" {
			byName[n] = &abuse.Brand{Name: n, AllowedAccounts: map[int64]bool{}}
		}
	}
	if len(byName) == 0 {
		return nil
	}
	if held, err := p.data.Accounts.ApprovedSenders(ctx); err == nil {
		for _, h := range held {
			if byName[h.Sender] != nil {
				byName[h.Sender].AllowedAccounts[h.Account] = true
			}
		}
	}
	if lists, err := p.data.Accounts.AllowedSenderLists(ctx); err == nil {
		for _, l := range lists {
			id := l.Account
			var list []string
			if json.Unmarshal(l.List, &list) != nil {
				continue
			}
			for _, s := range list {
				if b := byName[strings.ToUpper(strings.TrimSpace(s))]; b != nil {
					b.AllowedAccounts[id] = true
				}
			}
		}
	}
	out := make([]abuse.Brand, 0, len(byName))
	for _, b := range byName {
		out = append(out, *b)
	}
	return abuse.PrepBrands(out)
}

// --- the weighted score ---

// evalScore sums each account's weighted contributions and acts on the total:
// warn or throttle, never suspend (PO 2026-09-09), and only when at least two
// different signals agree (abuse.EvalScore).
func (p *Process) evalScore(raw json.RawMessage, c *contributions, merge func(int64, finding)) {
	var prm abuse.ScoreParams
	if !p.decodeParams("score", raw, &prm) {
		return
	}
	for id, list := range c.by {
		pp := prm
		pp.Weights = make(map[string]float64, len(prm.Weights))
		for k, w := range prm.Weights {
			pp.Weights[k] = w
		}
		if !p.over.apply("score", id, &pp) {
			continue // this account's own rules (config/0106)
		}
		s := abuse.EvalScore(list, pp)
		if s.Severity <= abuse.None {
			continue
		}
		if s.Severity > abuse.Throttle {
			s.Severity = abuse.Throttle // the cap is the ruling; never trust arithmetic to hold it
		}
		strengths := map[string]float64{}
		for _, ct := range list {
			if ct.Strength > strengths[ct.Signal] {
				strengths[ct.Signal] = round3(ct.Strength)
			}
		}
		det := map[string]any{"points": round3(s.Points), "signals": s.Signals, "strengths": strengths,
			"points_warn": pp.PointsWarn, "points_throttle": pp.PointsThrottle}
		for k, v := range c.notes[id] {
			det[k] = v
		}
		merge(id, ladder("score", s.Severity, pp.Common, det))
	}
}

func round3(v float64) float64 { return math.Round(v*1000) / 1000 }
