package smppserver

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/platform/bootstrap"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/refdata"
	"github.com/yakugate/yakugate/internal/tlsconf"
	"github.com/yakugate/yakugate/lib/smpp/pdu"
)

// Run starts the SMPP listener(s) and serves until the context is cancelled.
// [SCOPE P2-RUN] Runnable — makes Process a server-style process.
func (p *Process) Run(ctx context.Context) error {
	log := logging.New(p.Name())

	if err := bootstrap.Ensure(ctx, p.cfg); err != nil {
		log.Error(logging.SystemScope, "bootstrap", "bootstrap failed", logging.Fields{"error": err.Error()})
	}
	// Redis gate (2026-07-24): mirror httpserver — snapshot sync + version
	// watch when gated, else the original 5-minute in-memory refresh. The
	// syncer is fleet-leased, so running it in every gated role is safe (only
	// one instance actually syncs at a time).
	if p.sync != nil {
		if err := p.sync.SyncRef(ctx); err != nil {
			log.Error(logging.SystemScope, "refdata_sync", "initial redis reference sync failed", logging.Fields{"error": err.Error()})
		}
		if err := p.sync.SyncHLR(ctx); err != nil {
			log.Error(logging.SystemScope, "hlr_sync", "initial redis HLR sync failed", logging.Fields{"error": err.Error()})
		}
		go p.sync.Start(ctx, envSeconds("YAKU_REDIS_SYNC_SEC", 30))
		go p.ref.StartRedisWatch(ctx, p.redisLoader, envSeconds("YAKU_REDIS_WATCH_SEC", 3), log.Printf)
	}
	if err := p.ref.RefreshForce(ctx); err != nil {
		log.Error(logging.SystemScope, "refdata_refresh", "reference cache refresh failed", logging.Fields{"error": err.Error()})
	}
	ac, _, _ := p.ref.Counts()
	log.Info(logging.SystemScope, "refdata_loaded", "reference cache loaded", logging.Fields{"accounts": ac})
	if p.rater != nil {
		// Prices for the exact reservation at accept, kept as fresh as the cache.
		if err := p.rater.Reload(ctx); err != nil {
			log.Error(logging.SystemScope, "rater_reload", "rate cards unreadable", logging.Fields{"error": err.Error()})
		}
		p.ref.OnReload(p.rater.FollowReferenceData(log.Printf)) // with the reference data (QA BILL-001)
		go p.rater.KeepTrying(ctx, 2*time.Second, log.Printf)   // not read yet: every two seconds until it is
		go func() {
			// Every 30 s only when the cards changed (one small statement
			// says whether); in full every fifth minute whatever it says, as
			// the http-server does. It was four whole tables every 30 s.
			t := time.NewTicker(30 * time.Second)
			defer t.Stop()
			for n := 1; ; n++ {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					if n%10 == 0 {
						_ = p.rater.Reload(ctx)
					} else {
						_, _ = p.rater.ReloadIfChanged(ctx)
					}
				}
			}
		}()
	}
	if p.sync == nil {
		go p.ref.StartRefresh(ctx, refdata.RefreshEvery(), log.Printf) // 30 s, as the http-server (QA SMPP-004)
	}
	go p.health.Run(ctx, log.Printf)                              // vendor health for routing (PO 2026-09-14)
	go p.queueOK.Run(ctx)                                         // is the queue store answering, for readiness
	go p.ref.StartQualityRefresh(ctx, 30*time.Second, log.Printf) // quality counts per vendor and network
	p.pumpCtx = ctx
	p.legs.Add(2)
	go func() { defer p.legs.Done(); p.deliverClientDLRs(ctx, log) }() // smsc leg: push receipts to bound clients
	go func() { defer p.legs.Done(); p.deliverClientMO(ctx, log) }()   // P6: push MO messages to bound clients
	go p.assemblyReaper(ctx, log)                                      // D-096: release incomplete concat groups past the timeout
	p.importHandedOff(ctx)                                             // issue 5: what another gateway parked for this one's clients

	lns, err := p.listen(log)
	if err != nil {
		return err
	}

	// Graceful shutdown [SCOPE P2-SHUTDOWN] (2026-07-26): stop accepting, then
	// send each live session an unbind and give in-flight exchanges a moment
	// to drain before the deferred conn.Close in serve() tears them down.
	// Run returns only when that is done (stopped): it used to return the
	// moment the listeners closed, the process went with it, and the drain
	// never ran (go-live test 18 Oct 2026, P2 E1: receipts given twice).
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		<-ctx.Done()
		for _, l := range lns {
			_ = l.Close()
		}
		p.stopDrain(log)
	}()

	// Binds are checked against their accounts for as long as this role runs.
	go p.watchBinds(ctx)

	var wg sync.WaitGroup
	for _, l := range lns {
		wg.Add(1)
		go func(l net.Listener) {
			defer wg.Done()
			for {
				conn, err := l.Accept()
				if err != nil {
					return // listener closed
				}
				// Pre-auth connection cap [SCOPE P2-CONNCAP] (2026-07-26):
				// refuse cheaply BEFORE spawning a session so a TCP flood
				// cannot exhaust file descriptors ahead of any bind auth.
				if n := atomic.AddInt64(&p.connCount, 1); p.maxConns > 0 && n > int64(p.maxConns) {
					atomic.AddInt64(&p.connCount, -1)
					_ = conn.Close()
					continue
				}
				go p.serve(ctx, conn)
			}
		}(l)
	}
	wg.Wait()
	<-stopped // the receipts out are acked and every bound client unbound
	return nil
}

// plainOff is the value of YAKU_SMPP_ADDR that opens no plain SMPP port, so
// clients bind over TLS only (full go-live test 18 Oct 2026, P2 A5: an empty
// setting means :2775 on every interface, and there was no way to close it).
const plainOff = "off"

// listen opens the client ports: the plain SMPP port (YAKU_SMPP_ADDR, unless
// it is "off") and the SMPP over TLS port when its address, certificate and
// key are all set. It fails when no port could be opened — a smpp-server no
// client can reach is a setting to fix, not a role to keep running — and
// when the plain port is set and cannot be opened, as it always has.
func (p *Process) listen(log *logging.Logger) ([]net.Listener, error) {
	var lns []net.Listener
	plainOn := !strings.EqualFold(strings.TrimSpace(p.addr), plainOff)
	if plainOn {
		pl, err := net.Listen("tcp", p.addr)
		if err != nil {
			return nil, err
		}
		lns = append(lns, pl)
		log.Info(logging.SystemScope, "listener", "listening for SMPP", logging.Fields{"addr": p.addr, "tls": false})
	} else {
		log.Info(logging.SystemScope, "listener", "no plain SMPP port: YAKU_SMPP_ADDR=off, clients bind over TLS only", nil)
	}

	// SMPPS listener. TLS is opt-in by cert+key path, and YAKU_SMPP_TLS=off
	// force-disables it even when paths are set (PO 2026-09-08) — the same
	// switch shape the HTTP edge got in the same session.
	tlsOff := strings.EqualFold(strings.TrimSpace(env("YAKU_SMPP_TLS", "")), "off")
	tlsWanted := !tlsOff && p.tlsAddr != "" && p.tlsCert != "" && p.tlsKey != ""
	if tlsWanted {
		cert, err := tls.LoadX509KeyPair(p.tlsCert, p.tlsKey)
		if err != nil {
			log.Error(logging.SystemScope, "listener", "TLS cert load failed; TLS listener disabled", logging.Fields{"error": err.Error(), "cert": p.tlsCert})
		} else {
			// MinVersion set EXPLICITLY (PO 2026-09-08). It was absent: the
			// listener inherited whatever crypto/tls defaulted to — TLS 1.2
			// today, but a language default is not a transport-security
			// policy, and it can move on a Go upgrade.
			cfg := &tls.Config{
				Certificates: []tls.Certificate{cert},
				MinVersion:   tls.VersionTLS12,
			}
			// Mutual TLS (row 2.2.8), opt-in by CA bundle path, PO 2026-09-08:
			// set YAKU_SMPP_TLS_CLIENT_CA and the SMPPS port requires a client
			// certificate signed by that bundle. ANY certificate from it is
			// accepted — it proves the connection came from someone we issued
			// to, while system_id and password still prove which customer.
			// The plaintext port is untouched: it has no TLS to carry one.
			//
			// A bundle that cannot be read fails the LISTENER rather than
			// quietly serving without client certificates — silently dropping
			// back to anonymous clients is the one outcome nobody would spot.
			clientCA := env("YAKU_SMPP_TLS_CLIENT_CA", "")
			caErr := false
			if clientCA != "" {
				pool, perr := tlsconf.ClientCAPool(clientCA)
				if perr != nil {
					log.Error(logging.SystemScope, "listener", "client CA bundle unusable; TLS listener NOT started",
						logging.Fields{"error": perr.Error(), "clientCA": clientCA})
					caErr = true
				} else {
					cfg.ClientCAs = pool
					cfg.ClientAuth = tls.RequireAndVerifyClientCert
				}
			}
			var tl net.Listener
			if !caErr {
				tl, err = tls.Listen("tcp", p.tlsAddr, cfg)
			} else {
				err = errCAUnusable
			}
			if err != nil {
				log.Error(logging.SystemScope, "listener", "TLS listen failed", logging.Fields{"error": err.Error(), "addr": p.tlsAddr})
			} else {
				lns = append(lns, tl)
				f := logging.Fields{"addr": p.tlsAddr, "tls": true, "minVersion": "TLS1.2", "cert": p.tlsCert,
					"clientCertRequired": clientCA != ""}
				if clientCA != "" {
					f["clientCA"] = clientCA
				}
				if cert.Leaf != nil {
					// Renewal means a restart, and restarting this edge drops
					// every bound client — so say when that is due.
					f["certExpires"] = cert.Leaf.NotAfter.UTC().Format(time.RFC3339)
					f["certExpiresInDays"] = int(time.Until(cert.Leaf.NotAfter).Hours() / 24)
				}
				log.Info(logging.SystemScope, "listener", "listening for SMPP over TLS", f)
			}
		}
	}
	if !tlsWanted && plainOn {
		reason := "no certificate configured (YAKU_SMPP_TLS_CERT/KEY)"
		switch {
		case tlsOff:
			reason = "disabled by YAKU_SMPP_TLS=off"
		case p.tlsAddr == "":
			reason = "no listen address configured (YAKU_SMPP_TLS_ADDR)"
		}
		log.Warn(logging.SystemScope, "listener",
			"SMPPS is NOT enabled — bind passwords and message bodies cross this port in the clear",
			logging.Fields{"reason": reason})
	}
	if len(lns) == 0 {
		return nil, errNoPort
	}
	return lns, nil
}

// errNoPort: the plain port is switched off and the TLS port did not open.
var errNoPort = errors.New("no SMPP port is open: YAKU_SMPP_ADDR=off switches the plain port off, and the SMPP over TLS port did not open " +
	"(it needs YAKU_SMPP_TLS_ADDR, YAKU_SMPP_TLS_CERT and YAKU_SMPP_TLS_KEY, a certificate that loads, and no YAKU_SMPP_TLS=off) — see the lines above")

// How long a stopping smpp-server waits: for the deliver_sm it has out to be
// answered (legsFor — a client's answer has respTimeout), then for the
// clients to answer the unbind (unbindFor). Under the supervisor's 20 s.
const (
	legsFor   = respTimeout + time.Second
	unbindFor = 2 * time.Second
)

// stopDrain is a stop's last work, in order: the delivery legs claim nothing
// more and finish the round they have out — answered by the client and taken
// off the queue — so a receipt a client has is never handed over again;
// then every bound client is sent an unbind, and has unbindFor to answer it
// before its connection is closed.
//
// Go-live test 18 Oct 2026 (P2 E1, E6): the process exited a few
// milliseconds after its first unbind: of 48 binds 4 got it, and the final
// receipts pushed in that last moment were answered by the client but never
// acked, so the next process gave them again when their lease ran out (10
// and 12 in two runs).
func (p *Process) stopDrain(log *logging.Logger) {
	done := make(chan struct{})
	go func() { p.legs.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(legsFor):
		log.Warn(logging.SystemScope, "shutdown", "deliveries still waiting for their answers at the stop: they are handed over again when their lease ends", nil)
	}
	// Acks the queue store refused are made once more: what is still kept
	// after that goes with the process, and is handed over again.
	actx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	if left := p.receiptAcks(log).Flush(actx); left > 0 {
		log.Warn(logging.SystemScope, "shutdown", "delivered receipts the queue store would not take off at the stop: they are handed over again when their lease ends",
			logging.Fields{"receipts": left})
	}
	cancel()
	// The tries noted meanwhile are written before the process goes.
	p.tryLog().Close()
	// Its own band, so a shutdown broadcast can never reuse a number a DLR
	// push still has outstanding. [SCOPE P2-SEQ]
	for _, s := range p.conns.all() {
		p.unbindMu.Lock()
		seq := wrapSeq(&p.unbindSeq, seqUnbind)
		p.unbindMu.Unlock()
		s.send(pdu.UnbindPDU(seq))
	}
	// A client that answers the unbind ends its own session (serve returns on
	// unbind_resp); the ones that do not are closed when the time is up.
	deadline := time.Now().Add(unbindFor)
	for len(p.conns.all()) > 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	for _, s := range p.conns.all() {
		_ = s.conn.Close()
	}
}

// errCAUnusable stands in when the client CA bundle could not be read: the TLS
// listener is deliberately not started, rather than started without client
// certificates, so a mistyped path fails loudly instead of quietly serving
// anonymous clients.
var errCAUnusable = errors.New("client CA bundle unusable")
