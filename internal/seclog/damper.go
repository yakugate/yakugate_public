package seclog

// A blocked address keeps sending. Every one of those requests is refused
// pre-auth and would otherwise write its own line, so a fast attacker turns
// the security log into a flood — filling the disk with the same fact
// repeated, and burying the events that matter. Logging must never become the
// damage.
//
// The damper writes the FIRST occurrence of a repeated event immediately,
// then holds identical ones for a window and folds them into the next line as
// a `repeated` count. Nothing is lost that an operator needs: they learn it
// happened, and how often.

import (
	"strconv"
	"sync"
	"time"
)

// damperWindow is how long identical events are folded together.
const damperWindow = time.Minute

// damperMax bounds the tracking map. An attacker rotating source addresses
// would otherwise grow it without limit — the flood defence must not itself
// be a memory leak. Past this size the map is cleared and everything logs
// again, which is the safe direction: too many lines beats silence.
const damperMax = 10000

type damper struct {
	mu   sync.Mutex
	seen map[string]*damperEntry
}

type damperEntry struct {
	last     time.Time
	held     int
	written  int // lines written in this window
	inWindow bool
}

// authFailBurst is how many failed logins of ONE login from ONE address are
// each written in a window before the rest are folded into a count.
//
// A failed login is not a flood: the lockout stops a login after five in a
// minute, and what follows is "locked", a different event. But it was damped
// like one — three wrong passwords for a username gave ONE line, and the
// other two appeared as a count only if that login failed again a minute
// later (go-live test P1 G3, 2026-10-12: "every failed login is in the
// role's system log with its address"). Ten is above the lockout's five, so
// every try up to the lock has its own line, and a caller that is never
// locked (no credentials at all, a hundred times a second) is still folded.
const authFailBurst = 10

// burstOf is how many identical events are written in full in one window.
func burstOf(e Event) int {
	if e.Type == EventAuthFail {
		return authFailBurst
	}
	return 1
}

func newDamper() *damper { return &damper{seen: map[string]*damperEntry{}} }

// allow reports whether this event should be written now, and how many
// identical ones were held back since the last write.
func (d *damper) allow(key string, now time.Time) (write bool, repeated int) {
	return d.allowBurst(key, now, 1)
}

// allowBurst is allow for an event of which the first `burst` in a window are
// each written, and only the ones after that folded.
func (d *damper) allowBurst(key string, now time.Time, burst int) (write bool, repeated int) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.seen) >= damperMax {
		d.seen = map[string]*damperEntry{}
	}
	e, ok := d.seen[key]
	if !ok {
		d.seen[key] = &damperEntry{last: now, written: 1}
		return true, 0
	}
	if now.Sub(e.last) < damperWindow {
		if e.written < burst {
			e.written++
			return true, 0
		}
		e.held++
		return false, 0
	}
	held := e.held
	e.last, e.held, e.written = now, 0, 1
	return true, held
}

// damperKey identifies "the same thing happening again": the kind of event,
// who it was about, and where it came from. A different account or a
// different address is a different fact and is never folded in.
func damperKey(e Event) string {
	return e.Type + "|" + e.Account + "|" + e.Identity + "|" + e.IP
}

// repeatedNote renders the held count for the log message.
func repeatedNote(n int) string {
	return " (" + strconv.Itoa(n) + " more since the last line)"
}
