package httpvendor

// A session vendor's session is got when the vendor is LOADED (Saleem,
// 2026-10-12: "if http endpoint has session, then session must be got on
// startup of vendor connector").
//
// Until now the login ran inside the first send: the first message of a
// vendor waited on it, and a login that could not work — a wrong password, a
// login URL that is not there — was found out by a customer's message. The
// vendor meanwhile showed as working, because nothing had been asked of it.
//
// Now every load (the role's start, the reload timer, a changed vendor or
// call) makes sure each vendor that sends under a session HAS one:
//
//   - a valid session in the shared store is adopted — no login is made, so
//     the 30 s reload costs one small read a vendor, and N processes still
//     share ONE session (the adapter's own lock serialises a real login);
//   - none, or an expired one: the login chain runs, off the Step goroutine,
//     so a login that hangs holds up no other vendor and no status poll;
//   - a login that fails marks the vendor NOT READY (never "down": it was
//     not asked to send anything) and is tried again later — after 30 s,
//     then twice as long each time up to 10 minutes, so a wrong password
//     does not hammer the vendor's login. A changed vendor or call is tried
//     at once.
//
// A session that runs out with no traffic is renewed the same way at the
// next load, so the vendor stays ready.
//
// Only with the shared session store. Without it each reload builds adapters
// with an empty memory, and logging in ahead would mean a login per vendor
// every reload; such an install keeps the old way (login at the first send).

import (
	"context"
	"time"
)

const (
	sessionLoginTimeout = 45 * time.Second
	sessionRetryFirst   = 30 * time.Second
	sessionRetryMax     = 10 * time.Minute
)

// sessionTry is where one vendor stands with the login made at load.
type sessionTry struct {
	running bool      // a try is in flight: never two logins of one vendor from here
	fails   int       // failed tries in a row
	next    time.Time // after a failure: no try before this
	ready   bool      // the last try ended with a session
}

// sessionWait is how long to wait after the n-th failure in a row.
func sessionWait(fails int) time.Duration {
	d := sessionRetryFirst
	for i := 1; i < fails && d < sessionRetryMax; i++ {
		d *= 2
	}
	if d > sessionRetryMax {
		d = sessionRetryMax
	}
	return d
}

// keepSessions starts a login for every loaded vendor that needs a session
// and has no try in flight or waiting, and forgets the vendors that left.
func (p *Process) keepSessions(ctx context.Context, vendors []vendorAdapter) {
	if p.sessions == nil {
		return
	}
	now := time.Now()
	p.sessMu.Lock()
	if p.sessTries == nil {
		p.sessTries = map[int64]*sessionTry{}
	}
	served := make(map[int64]bool, len(vendors))
	var due []vendorAdapter
	for _, v := range vendors {
		if v.ad == nil || !v.ad.NeedsSession() {
			continue
		}
		served[v.id] = true
		st := p.sessTries[v.id]
		if st == nil {
			st = &sessionTry{}
			p.sessTries[v.id] = st
		}
		if st.running || now.Before(st.next) {
			continue
		}
		st.running = true
		due = append(due, v)
	}
	for id := range p.sessTries {
		if !served[id] {
			delete(p.sessTries, id)
		}
	}
	p.sessMu.Unlock()
	for _, v := range due {
		go p.getSession(ctx, v)
	}
}

// getSession makes sure one vendor has its session and records the outcome.
func (p *Process) getSession(ctx context.Context, v vendorAdapter) {
	c, cancel := context.WithTimeout(ctx, sessionLoginTimeout)
	err := v.ad.EnsureSession(c)
	cancel()

	p.sessMu.Lock()
	st := p.sessTries[v.id]
	if st == nil { // the vendor left while the login ran
		p.sessMu.Unlock()
		return
	}
	st.running = false
	wasReady := st.ready
	var wait time.Duration
	if err != nil {
		st.fails++
		st.ready = false
		wait = sessionWait(st.fails)
		st.next = time.Now().Add(wait)
	} else {
		st.fails, st.ready, st.next = 0, true, time.Time{}
	}
	p.sessMu.Unlock()

	if ctx.Err() != nil {
		return // the role is stopping: a cut login says nothing about the vendor
	}
	if err != nil {
		p.health.NotReady(v.id, err.Error())
		p.log.ForVendor(v.cid).Warnf("session", "no session yet, so nothing can be sent through this vendor: %v (tried again in %s, or at once when the vendor or its login call is changed)",
			err, wait)
		return
	}
	// Ends "not ready" and nothing else: holding a session says nothing
	// about whether the vendor's send call is being answered.
	p.health.Ready(v.id)
	if !wasReady {
		p.log.ForVendor(v.cid).Infof("session", "session ready")
	}
}

// forgetSessionWaits lets every waiting login be tried at the next load.
func (p *Process) forgetSessionWaits() {
	p.sessMu.Lock()
	for _, st := range p.sessTries {
		st.next = time.Time{}
	}
	p.sessMu.Unlock()
}
