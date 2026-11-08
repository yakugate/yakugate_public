// Package reloadgate says when a role should read a set of settings again.
//
// A role that keeps settings in memory (the dlr and mo roles' webhook
// targets) used to read them in full every second — a join over three
// tables, sixty times a minute, for rows that change a few times a day
// (2026-10-13). With a gate it asks one small question every few seconds —
// "how many rows, and when did the newest change?" — and reads in full only
// when the answer moved, and at least every Backstop whatever the answer.
package reloadgate

import (
	"context"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// Gate decides when to load. The zero value loads on every call; set Sig.
type Gate struct {
	// Look is how often the fingerprint is read (at least a second).
	Look time.Duration
	// Backstop is the longest time between two full loads.
	Backstop time.Duration
	// Sig reads the fingerprint. fresh says a row changed within the last
	// second of the store's clock: another change may still come under the
	// same stamp, so the fingerprint is not kept and the next look loads
	// again.
	Sig func(ctx context.Context) (sig string, fresh bool, err error)

	lastSig  string
	lastLook time.Time
	lastLoad time.Time
}

// Due reports whether to load now; call it on every turn of the role's loop.
// When it says yes and the load then fails, call Failed so the next call
// tries again.
//
// The fingerprint is always read BEFORE the load it leads to, so a change
// made while the load runs, or right after it, shows at the next look.
func (g *Gate) Due(ctx context.Context, now time.Time) bool {
	if g.Sig == nil {
		return true
	}
	if g.lastLoad.IsZero() || (g.Backstop > 0 && now.Sub(g.lastLoad) >= g.Backstop) {
		g.lastLoad, g.lastLook = now, now
		g.lastSig = ""
		if sig, fresh, err := g.Sig(ctx); err == nil && !fresh {
			g.lastSig = sig
		}
		return true
	}
	if now.Sub(g.lastLook) < g.Look {
		return false
	}
	g.lastLook = now
	sig, fresh, err := g.Sig(ctx)
	if err != nil {
		return false // the store did not answer: the Backstop load will say so
	}
	if fresh {
		// Not kept: the next look loads once more, for a change that came
		// under the same stamp after this one.
		g.lastSig, g.lastLoad = "", now
		return true
	}
	if sig == g.lastSig {
		return false
	}
	g.lastSig, g.lastLoad = sig, now
	return true
}

// Failed tells the gate the load it asked for did not happen.
func (g *Gate) Failed() {
	g.lastLoad = time.Time{}
}

// Tables is a fingerprint over whole tables or parts of them: each entry is
// a table name, optionally followed by a WHERE clause, written in code (never
// from input) — "http_endpoints WHERE type='dlr'". Every table must have an
// updated_at that moves on a change. The one question over all of them is
// the DAL's (dal.ChangeMarks); nil marks give no Sig, so the gate loads on
// every call.
func Tables(marks dal.ChangeMarks, tables ...string) func(ctx context.Context) (string, bool, error) {
	if marks == nil {
		return nil
	}
	return marks.Fingerprint(tables...)
}
