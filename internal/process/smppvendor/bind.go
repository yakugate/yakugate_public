// Binds to the vendor: dialling, waiting after a failed or dropped bind,
// and going back to the home host after a failover.

package smppvendor

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/lib/smpp/session"
)

// runVendor keeps one session bound, reconnecting with backoff on failure. A
// vendor with max_reconnect_attempts>0 gives up after that many consecutive
// failures and waits for a manual reconnect command instead of retrying
// forever (item 1) — the unconfigured default (0) keeps the original
// unbounded-backoff behavior.
//
// A bind the vendor DROPS is waited out like a bind it refuses (2026-10-08).
// The loop used to dial again the instant a bound session ended, and a bind
// that had succeeded put the wait back to its shortest — so against a vendor
// that allows fewer sessions than are configured, and makes room for a new
// bind by closing an old one, the sessions took turns throwing each other
// off for ever, with no pause at all: 640 binds a second on the dev box with
// two sessions against a vendor that allows one. That is what the vendor in
// production called "too many sessions", and why monitoring went on showing
// one healthy bind per gateway. Now a bind that ended before it had lasted
// stableBind is one more failed attempt: the wait doubles to 30 s and
// max_reconnect_attempts applies; one that had lasted waits the retry
// interval once and starts over. h is the supervisor's handle for this
// session (nil in tests): it is told which connection the session holds.
func (p *Process) runVendor(ctx context.Context, v vendorConn, key sessionKey, host string, h *supHandle) {
	v = p.withLive(v)
	baseBackoff := retryBase(v)
	backoff := baseBackoff
	attempts := 0
	// A vendor with more than one host: this session starts on its own (the
	// hosts are shared out across the sessions), and after an attempt that
	// failed it tries the NEXT one (2026-10-08). It used to try its own host
	// for ever, so a vendor with one session and a second host for when the
	// first is down never reached the second.
	hostAt := 0
	for i, h := range v.hosts {
		if h == host {
			hostAt = i
		}
	}
	// homeAt is this session's OWN host. A session that had to bind to another
	// one goes back when its own answers again (2026-10-12, go-live test P3
	// E8, Saleem: "when connected with second it does not go back to first").
	// homeWait is how long between looks at it; it doubles (to ten minutes)
	// each time a way back is tried and the bind there is refused, so a host
	// that takes connections and refuses binds is not tried every minute.
	homeAt := hostAt
	homeWait := time.Duration(0)
	triedHome := false
	for ctx.Err() == nil {
		// The retry interval and the reconnect limit are live settings too
		// (QA SMPP-002, 2026-10-01): an edit reaches the next attempt
		// without waiting for a restart.
		v = p.withLive(v)
		if b := retryBase(v); b != baseBackoff {
			baseBackoff, backoff = b, b
		}
		if v.maxReconnect > 0 && attempts >= v.maxReconnect {
			p.setUnbound(key, fmt.Sprintf("max_reconnect_attempts (%d) reached; awaiting manual reconnect", v.maxReconnect))
			p.log.ForVendor(v.cid).Errorf("bind_giveup", "session %d: giving up after %d attempts — use the reconnect command to resume", key.Idx, attempts)
			if !p.awaitReconnectSignal(ctx, key.VendorID) {
				return
			}
			attempts = 0
			continue
		}
		if len(v.hosts) > 0 {
			host = v.hosts[hostAt%len(v.hosts)]
			p.setAddr(key, host)
		}
		cl, err := session.DialContext(ctx, host, v.systemID, v.password, v.mode, v.dialTimeout, v.tlsConfig, v.bindOpt)
		if err != nil {
			if ctx.Err() != nil {
				return // stopped while it was connecting
			}
			attempts++
			hostAt++ // the next attempt goes to the vendor's next host, if it has one
			if triedHome {
				// The way back did not work: its own host answered the look
				// and then would not bind. Look less often.
				triedHome = false
				if homeWait *= 2; homeWait > maxHomeWait {
					homeWait = maxHomeWait
				}
			}
			p.log.ForVendor(v.cid).Warnf("bind_fail", "session %d dial/bind %s: %v (attempt %d)", key.Idx, host, err, attempts)
			p.setUnbound(key, err.Error()) // monitor: record the dial failure (item 7)
			wait := backoff
			// While routing counts the vendor as down, try again every
			// recheck_after_sec instead (config/0103): its traffic is going
			// elsewhere, and a fixed, configured rhythm is what tells the
			// operator when it will next be tested.
			if t := p.timing(v.id); p.health.Down(v.id, t.downAfter) && t.recheckAfter > 0 {
				wait = t.recheckAfter
			}
			if !sleep(ctx, wait) {
				return
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		p.log.ForVendor(v.cid).Infof("bind_ok", "session %d bound to %s (window %d)", key.Idx, host, v.window)
		if v.submitTPS > 0 {
			// Said out loud because the UNIT changed (config/0124): it used
			// to be per process, so two smpp-vendor processes sent a carrier
			// twice its contracted rate. Better read here than inferred from
			// a carrier complaint, in either direction.
			p.log.ForVendor(v.cid).Infof("rate_limit",
				"submit rate capped at %d/s for this deployment (%s), %s",
				v.submitTPS, deployment.ID(), rateUnit(p.rateShared, "smpp-vendor"))
		}
		h.holds(cl)
		boundAt := time.Now()
		// Bound to a host that is not this session's own: look at its own
		// every homeWait, and leave this bind for it when it answers.
		sctx, goHome, stopWatch := ctx, (*atomic.Bool)(nil), func() {}
		if len(v.hosts) > 1 && hostAt%len(v.hosts) != homeAt {
			if homeWait <= 0 {
				homeWait = p.homeRecheck(v.id)
			}
			var cancel context.CancelFunc
			sctx, cancel = context.WithCancel(ctx)
			goHome, stopWatch = &atomic.Bool{}, cancel
			go watchHost(sctx, v.hosts[homeAt], homeWait, v.dialTimeout, func() { goHome.Store(true); cancel() })
		} else {
			homeWait, triedHome = 0, false // on its own host: the next time away starts afresh
		}
		ended := p.serveVendor(sctx, v, key, cl)
		stopWatch()
		cl.Close()
		h.holds(nil)
		if ctx.Err() != nil {
			return // we ended it
		}
		if goHome != nil && goHome.Load() {
			// Its own host answers again. The bind was ended in order (what
			// was in flight has been answered), and the next attempt is there.
			p.log.ForVendor(v.cid).Infof("bind_home", "session %d: its own host %s answers again; leaving %s to bind there",
				key.Idx, v.hosts[homeAt], host)
			p.setUnbound(key, "going back to its own host "+v.hosts[homeAt])
			hostAt, triedHome = homeAt, true
			backoff, attempts = baseBackoff, 0
			continue
		}
		// The bind ended and we did not end it: the vendor closed it, or the
		// link died. Never dial again at once.
		lasted := time.Since(boundAt)
		if lasted >= stableBind {
			backoff, attempts = baseBackoff, 0 // it was a working bind: start over
		} else {
			attempts++
		}
		wait := backoff
		why := fmt.Sprintf("the vendor closed this bind %s after it was made", lasted.Round(time.Second))
		switch {
		case ended == endSilent:
			why = fmt.Sprintf("nothing was heard from the vendor (enquire_link unanswered); the bind was closed %s after it was made", lasted.Round(time.Second))
			hostAt++ // a host that has gone quiet: the next one, if there is one
		case lasted < stableBind:
			why += " — it may allow fewer sessions than are set (sessions are per gateway: each deployment opens that many)"
		}
		p.setUnbound(key, why)
		if lasted < stableBind {
			p.log.ForVendor(v.cid).Warnf("bind_dropped", "session %d: bind to %s ended after %s (attempt %d); binding again in %s",
				key.Idx, host, lasted.Round(time.Millisecond), attempts, wait)
		} else {
			p.log.ForVendor(v.cid).Infof("bind_lost", "session %d: bind to %s ended after %s; binding again in %s",
				key.Idx, host, lasted.Round(time.Second), wait)
		}
		if !sleep(ctx, wait) {
			return
		}
		if lasted < stableBind && backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

// stableBind is how long a bind must last to count as a working one. A bind
// that ends sooner is treated as a failed attempt (see runVendor).
var stableBind = 30 * time.Second

// retryBase is the first wait before binding again: the vendor's retry
// interval, else one second. It doubles per failure up to 30 s.
func retryBase(v vendorConn) time.Duration {
	if v.retryInterval > 0 {
		return v.retryInterval
	}
	return time.Second
}

// awaitReconnectSignal blocks a gave-up session until an admin reconnect
// command arrives for its vendor (or the context is cancelled).
func (p *Process) awaitReconnectSignal(ctx context.Context, vendorID int64) bool {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
			if p.consumeReconnect(vendorID) {
				return true
			}
		}
	}
}

// Why a bound session ended (serveVendor's answer).
// maxHomeWait is the longest a session bound to another host waits between
// looks at its own.
const maxHomeWait = 10 * time.Minute

// homeRecheck is how often a session bound away from its own host looks at
// it: the vendor's "recheck after" (a minute unless changed).
func (p *Process) homeRecheck(vendorID int64) time.Duration {
	if t := p.timing(vendorID); t.recheckAfter > 0 {
		return t.recheckAfter
	}
	return time.Minute
}

// watchHost looks at addr every `every` and calls up — once — when it takes a
// connection. Only a connection is tried, not a bind: a second bind while the
// session holds one could be refused by a vendor that counts sessions, and
// would tell us nothing. Stops when ctx ends.
func watchHost(ctx context.Context, addr string, every, dialTimeout time.Duration, up func()) {
	if dialTimeout <= 0 || dialTimeout > 5*time.Second {
		dialTimeout = 5 * time.Second
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		d := net.Dialer{Timeout: dialTimeout}
		c, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			continue
		}
		c.Close()
		if ctx.Err() == nil {
			up()
		}
		return
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
