package mo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/yakugate/yakugate/internal/callbackmap"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/dialect"
	"github.com/yakugate/yakugate/internal/reloadgate"
	"github.com/yakugate/yakugate/internal/webhookalert"
	"os"
	"strconv"

	"github.com/yakugate/yakugate/internal/crypto"
	"github.com/yakugate/yakugate/internal/inboundauth"
	"github.com/yakugate/yakugate/internal/ratelimit"
	"github.com/yakugate/yakugate/lib/encoding"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	mosvc "github.com/yakugate/yakugate/internal/mo"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/pushcall"
	"github.com/yakugate/yakugate/internal/pushpool"
	"github.com/yakugate/yakugate/internal/pushretry"
	"github.com/yakugate/yakugate/internal/queuecheck"
	"github.com/yakugate/yakugate/internal/vendors/httpadapter"
	"github.com/yakugate/yakugate/internal/webhookauth"
)

// Run serves the MO webhook endpoint and drains MO to mo_url clients. [SCOPE P6]
//
//	INPUT   — POST/GET /mo/<vendor_cid> and each MO door's own path (issue 20)
//	          callbacks (HTTP vendor MO) + staged mo_queue
//	LOGIC   — webhook: parse subscriber/shortcode/text -> mo.Service.Ingest (route +
//	          stage). poster: drain stale MO for accounts with mo_url, POST it, then
//	          finalize (hot record direction=MO + drop staging).
//	OUTPUT  — MO staged; MO delivered to clients; inbound records persisted.
func (p *Process) Run(ctx context.Context) error {
	if p.svc == nil || p.hot == nil || p.traffic == nil {
		p.log.Printf("stores unavailable; mo idle")
		<-ctx.Done()
		return ctx.Err()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/mo/", p.handleWebhook)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	// Any other path is an MO door's own path, or nothing (issue 20, as the
	// http-server's catch-all serves an account's own submit path). A path
	// changed in the portal is picked up with the doors' next load.
	mux.HandleFunc("/", p.handleDoorPath)
	srv := &http.Server{Addr: p.addr, Handler: mux}
	// Organized shutdown (PO 2026-07-30, D-086): drain in-flight vendor MO
	// webhooks instead of cutting their connections mid-request. Same
	// pattern/timeout as P1's submit server. Run returns as soon as Shutdown
	// is CALLED; Stop waits for it (stop.go).
	p.drain.Add(1)
	go func() {
		defer p.drain.Done()
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	// MO that came in at another gateway for this one's clients are moved
	// into its own queue (issue 5): one process of the deployment at a
	// time, beside smpp-server's.
	if p.handoff != nil && p.queue != nil {
		p.drain.Add(1)
		go func() { defer p.drain.Done(); p.handoff.ImportMO(ctx, p.queue, p.strays) }()
	}
	// periodic MO-route refresh
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				p.svc.Reload(ctx)
				if err := p.doors.Reload(ctx); err != nil {
					p.log.Printf("%v", err)
				}
				p.warnUnguarded()
				p.sayDialectProblems()
			}
		}
	}()
	// The inbound doors (config/0121): who may POST MO for each vendor, and
	// with what. Loaded before the listener answers anything. The old shared
	// settings are retired first (PO 2026-09-24): whatever a vendor still has
	// there moves onto its two doors (the dlr role tries too; a lock makes
	// one of them do it).
	if err := inboundauth.MoveLegacy(ctx, p.engine().Doors, env("YAKU_TOKEN_PEPPER", "yaku-dev-token-pepper"), p.log.Printf); err != nil {
		p.log.Printf("inbound: moving the old shared settings: %v", err)
	}
	if err := p.doors.Reload(ctx); err != nil {
		p.log.Printf("%v", err)
	}
	p.warnUnguarded()
	p.sayDialectProblems()
	p.drain.Add(1)
	go func() { defer p.drain.Done(); p.postClientMO(ctx) }() // mo_url delivery for unbound/HTTP clients
	go p.sweepGiveUp(ctx)                                     // and the backstop for MO nobody is claiming
	p.log.Printf("MO webhook listening on %s", p.addr)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return ctx.Err()
}

// Step lets the process satisfy worker.Worker; Run takes precedence.
func (p *Process) Step(ctx context.Context) error { return nil }

// handleWebhook takes an MO callback on a door's default path,
// /mo/<cid>[/<token>]. Both forms name the vendor; the token is checked by
// the door rather than being part of the lookup, so a wrong secret reads as
// "forbidden" in the log and not as "unknown vendor".
func (p *Process) handleWebhook(w http.ResponseWriter, r *http.Request) {
	cid, urlToken := webhookauth.SplitPath(strings.TrimPrefix(r.URL.Path, "/mo/"))
	if cid == "" {
		http.Error(w, "missing vendor", http.StatusNotFound)
		return
	}
	p.serveDoor(w, r, cid, urlToken)
}

// handleDoorPath takes an MO callback on any other path:
//
//	a door's own path       that door (issue 20, inbound_endpoints.custom_path)
//	an MO dialect's own URL the door of that dialect the caller's credentials
//	                        name (27 Oct, sms_service_dialects.url; serveDialect)
//	anything else           404
//
// Each is the whole path, or the path and then the token for a door that
// reads its token from the path. A path nothing names is 404, all of them in
// ONE bucket — the path is the caller's, and a count for each would let
// anyone grow this role's memory without limit.
func (p *Process) handleDoorPath(w http.ResponseWriter, r *http.Request) {
	if door, urlToken, ok := p.doors.ForPath(r.URL.Path, inboundauth.KindMO); ok {
		p.serveDoor(w, r, door.VendorCID, urlToken)
		return
	}
	if doors, urlToken, ok := p.doors.ForDialectPath(r.URL.Path, inboundauth.KindMO); ok {
		p.serveDialect(w, r, doors, urlToken)
		return
	}
	if !p.doors.Loaded() {
		http.Error(w, "inbound endpoints not loaded yet", http.StatusServiceUnavailable)
		return
	}
	p.hooks.Note(unknownVendor, "unknown_path")
	http.NotFound(w, r)
}

// serveDialect takes an MO callback to an MO dialect's own URL (27 Oct,
// Saleem: "dialects for mo and dlr will also have custom urls, similar to MT
// one"). Every door reading with the dialect shares the URL, so the caller's
// credentials say which door it is (inboundauth.Guard.Identify); the MO is
// then judged and read at that door exactly as on its own path. Credentials
// that name none of them are a bare 403, counted once.
func (p *Process) serveDialect(w http.ResponseWriter, r *http.Request, doors []inboundauth.Endpoint, urlToken string) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	door, res := p.guard.Identify(r, body, doors, urlToken)
	if !res.OK {
		p.log.Printf("REFUSED MO at the dialect URL %s from %s: %s", doors[0].DialectURL, r.RemoteAddr, res.Reason)
		// ONE bucket: no door was named.
		p.hooks.Note(unknownVendor, "refused")
		if res.RetryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(res.RetryAfter))
		}
		http.Error(w, http.StatusText(res.Status), res.Status)
		return
	}
	p.serveFound(w, r, door.VendorCID, door, body, urlToken)
}

// serveDoor parses an inbound HTTP vendor MO at its door and ingests it. The
// subscriber, shortcode, and text are read by the door's dialect, else from
// form/query/JSON under common aliases.
func (p *Process) serveDoor(w http.ResponseWriter, r *http.Request, cid, urlToken string) {
	// The body is read first because a signed-body scheme must see the exact
	// bytes the vendor signed. It is bounded, and nothing is done with it
	// until the caller has proved itself.
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	// Authenticate BEFORE ingesting: a forged MO would be staged, delivered to
	// the client, billed, and recorded as genuine. Since 2026-09-23 this is
	// the account edge's own machinery — JWT, token, password, signature, IP
	// allow-list, throughput, ladders, spray guard (config/0121).
	door, known := p.doors.For(cid, inboundauth.KindMO)
	if known {
		p.serveFound(w, r, cid, door, body, urlToken)
		return
	}
	res := inboundauth.Result{Status: http.StatusServiceUnavailable, Reason: "inbound endpoints not loaded yet"}
	if p.doors.Loaded() {
		// No such vendor. Answered with the SAME 403 a wrong secret gets, so
		// a caller cannot map our vendors by watching the status code — the
		// rule the URL-token split at the top of this handler already
		// follows. (Before config/0121 this fell out of an empty config
		// failing its own check; the refactor turned it into a 503, which
		// both told a stranger the cid was unknown and broke the contract
		// the MO matrix pins.) Not read yet (the 503 above): refuse rather
		// than let a callback through unauthenticated, and say so plainly
		// for the operator.
		res = inboundauth.Result{Status: http.StatusForbidden, Reason: "unknown vendor"}
	}
	p.log.Printf("REFUSED MO for vendor %s from %s: %s", cid, r.RemoteAddr, res.Reason)
	// An unknown cid goes in ONE bucket: the path is caller-controlled, and a
	// per-cid entry would let anyone grow this map without limit.
	p.hooks.Note(unknownVendor, "unknown_vendor")
	http.Error(w, http.StatusText(res.Status), res.Status)
}

// serveFound judges an MO callback at its door and, once the caller has
// proved itself, reads and ingests it. The door's default path, its own path
// and its dialect's URL all end here.
func (p *Process) serveFound(w http.ResponseWriter, r *http.Request, cid string, door inboundauth.Endpoint, body []byte, urlToken string) {
	if res := p.guard.Check(r, body, door, urlToken); !res.OK {
		p.log.Printf("REFUSED MO for vendor %s from %s: %s", cid, r.RemoteAddr, res.Reason)
		p.hooks.Note(cid, "refused")
		if res.RetryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(res.RetryAfter))
		}
		http.Error(w, http.StatusText(res.Status), res.Status)
		return
	}
	// The door's own answer, when it has one (config/0125); nil answers as
	// always — 200 when kept, 503 when it must be resent.
	reply := door.Reply
	if door.Request != nil {
		p.ingestMapped(w, r, cid, door, body)
		return
	}
	fields := parseFields(r, body)
	src := firstNonEmpty(fields, "source", "from", "msisdn", "sender")
	dst := firstNonEmpty(fields, "destination", "to", "shortcode", "receiver")
	text := firstNonEmpty(fields, "text", "message", "content", "body")
	if src == "" || dst == "" {
		p.hooks.Note(cid, "unreadable")
		// Nothing usable to route: answered as taken unless the dialect has
		// a rule for an unreadable call.
		reply.Write(w, callbackmap.Outcome{Reason: callbackmap.ReasonUnreadable})
		return
	}
	monitor.Flow.NoteMO(1)
	// The door knows its vendor (loaded with the doors). It was looked up in
	// MySQL on every call, and with MySQL away the lookup gave 0: an MO for a
	// rule pinned to the vendor matched nothing, its orphan could not be
	// filed either, and it was answered 200 and lost (go-live test of 18 Oct
	// 2026, P6 E11: 10 of 10).
	vid := door.VendorID
	if vid == 0 {
		var err error
		if vid, err = p.vendorID(r.Context(), cid); err != nil {
			p.hooks.Note(cid, "error")
			p.log.Printf("mo vendor=%s: the vendor could not be read (%v) — not kept; the vendor is told to send it again", cid, err)
			reply.Write(w, callbackmap.Outcome{Reason: callbackmap.ReasonStore, Count: 1})
			return
		}
	}
	// Detect, do not assume (P6-B5). This hardcoded "gsm7" was the other half
	// of the UCS-2 bug: an Urdu or Arabic MO arriving over HTTP was labelled
	// GSM-7 the moment it landed, so every downstream leg — the SMPP push to a
	// bound client, the record, the segment count — carried the wrong encoding.
	// The SMPP ingress edge already derives it from the PDU's data_coding.
	enc, _, err := encoding.Plan(text, "")
	if err != nil || enc == "" {
		enc = "gsm7"
	}
	id, staged, err := p.svc.Ingest(r.Context(), mosvc.Inbound{
		VendorID: vid, Source: src, Dest: dst, Text: text, Encoding: enc, Transport: model.TransportHTTP})
	if err != nil {
		p.hooks.Note(cid, "error")
		p.log.Printf("mo ingest vendor=%s: %v", cid, err)
		// Not kept, so say so: the vendor retries later, where a 200 lost the
		// message. (Since 2026-09-13 this includes an encrypted company's MO
		// that could not be encrypted — it is never staged in plain text.)
		reply.Write(w, callbackmap.Outcome{Reason: callbackmap.ReasonStore, Count: 1})
		return
	} else if staged {
		p.hooks.Note(cid, "staged")
		p.log.Printf("mo staged %s vendor=%s %s->%s", id, cid, src, dst)
	} else {
		// No MO route owns this shortcode — a customer whose number is not
		// set up, or a vendor sending to the wrong one.
		p.hooks.Note(cid, "unrouted")
		p.log.Printf("mo unrouted vendor=%s dst=%s", cid, dst)
	}
	reply.Write(w, callbackmap.Outcome{MessageID: id, Count: 1})
}

// ingestMapped reads an incoming-message callback through its door's dialect
// (config/0125, one model with a client's dialect since 2026-10-08): the
// vendor's field names, several messages in one call, text written as hex or
// UCS-2 hex, the network it came from, and the answer the vendor expects.
//
// A message with no sender or receiver is acknowledged and dropped, as on
// the old path (a call with none readable is UNREADABLE, answered as taken
// unless the dialect has a rule for it). One we could not keep is answered as
// STORE_UNAVAILABLE so the vendor
// sends the call again — any message of the batch already kept then arrives
// twice, which is why a vendor that batches should be answered per message
// where it can be (noted, not yet deduplicated by the vendor's message id).
func (p *Process) ingestMapped(w http.ResponseWriter, r *http.Request, cid string, door inboundauth.Endpoint, body []byte) {
	items, err := door.Request.Read(r, body)
	if err != nil || len(items) == 0 {
		p.hooks.Note(cid, "unreadable")
		if err != nil {
			p.log.Printf("mo vendor=%s: %v", cid, err)
		}
		door.Reply.Write(w, callbackmap.Outcome{Reason: callbackmap.ReasonUnreadable})
		return
	}
	vid := door.VendorID
	if vid == 0 {
		var err error
		if vid, err = p.vendorID(r.Context(), cid); err != nil {
			p.hooks.Note(cid, "error")
			p.log.Printf("mo vendor=%s: the vendor could not be read (%v) — not kept; the vendor is told to send it again", cid, err)
			door.Reply.Write(w, callbackmap.Outcome{Reason: callbackmap.ReasonStore, Count: len(items)})
			return
		}
	}
	failed := false
	readable := 0
	var lastID, lastVendorID string
	for _, it := range items {
		src, dst, text := it.Values["from"], it.Values["to"], it.Values["text"]
		if src == "" || dst == "" {
			p.hooks.Note(cid, "unreadable")
			continue
		}
		readable++
		monitor.Flow.NoteMO(1)
		enc, _, err := encoding.Plan(text, "")
		if err != nil || enc == "" {
			enc = "gsm7"
		}
		in := mosvc.Inbound{VendorID: vid, Source: src, Dest: dst, Text: text, Encoding: enc, Transport: model.TransportHTTP}
		// The network the handset is on, when the vendor says: "41001" is
		// MCC 410 + MNC 01, and routes can then match on it.
		if op := strings.TrimSpace(it.Values["operator"]); len(op) >= 5 && len(op) <= 6 {
			in.MCC, in.MNC = op[:3], op[3:]
		}
		id, staged, err := p.svc.Ingest(r.Context(), in)
		vendorRef := it.Values["message_id"]
		switch {
		case err != nil:
			failed = true
			p.hooks.Note(cid, "error")
			p.log.Printf("mo ingest vendor=%s ref=%s: %v", cid, vendorRef, err)
			continue
		case staged:
			p.hooks.Note(cid, "staged")
			p.log.Printf("mo staged %s vendor=%s ref=%s %s->%s", id, cid, vendorRef, src, dst)
		default:
			p.hooks.Note(cid, "unrouted")
			p.log.Printf("mo unrouted vendor=%s ref=%s dst=%s", cid, vendorRef, dst)
		}
		lastID, lastVendorID = id, vendorRef
	}
	out := callbackmap.Outcome{MessageID: lastID, VendorMessageID: lastVendorID, Count: len(items)}
	switch {
	case failed:
		out.Reason = callbackmap.ReasonStore
	case readable == 0:
		// Not one message of the call had a sender and a receiver.
		out.Reason = callbackmap.ReasonUnreadable
	}
	door.Reply.Write(w, out)
}

// warnUnguarded says which vendors' MO doors nothing guards: such a door
// refuses every callback, and the vendor sees only "forbidden".
//
// It used to read the vendors' old shared setting (vendors.inbound_config),
// which config/0126 empties onto the doors at start — so every vendor was
// said to have NO inbound security, a vendor whose door had a token among
// them (go-live test of 18 Oct 2026, P4-6, P6 G4). It reads the doors now,
// as the dlr role does. Once each, not every 30 s refresh — a line repeated
// for ever stops being read, and this one is the only signal an operator gets.
func (p *Process) warnUnguarded() {
	var cids []string
	for _, k := range p.doors.Unconfigured() {
		if cid, ok := strings.CutSuffix(k, "|"+inboundauth.KindMO); ok {
			cids = append(cids, cid)
		}
	}
	for _, cid := range p.warner.Warn(cids) {
		p.log.Printf("vendor %s has NO inbound security configured on its MO door — its MO will be REFUSED (set a credential on its MO endpoint)", cid)
	}
}

// sayDialectProblems logs, once per change, each MO door whose dialect could
// not be loaded. Such a door reads incoming messages the old way (common field
// names, an empty 200), and this line is the only thing that says so.
func (p *Process) sayDialectProblems() {
	for _, line := range p.dialectWarner.Warn(p.doors.MappingProblems(inboundauth.KindMO)) {
		p.log.Printf("MO door of vendor %s — its dialect is NOT in use, callbacks are read as if it had none", line)
	}
	// A path not used, or a token two doors on one dialect URL share (27
	// Oct): callbacks there are refused, and only this line says why.
	for _, line := range p.pathWarner.Warn(p.doors.PathProblems(inboundauth.KindMO)) {
		p.log.Printf("MO door of vendor %s", line)
	}
}

// vendorID resolves a vendor cid to its id (0 if unknown — MO still routes by
// dest). An error is a config store that did not answer, not an unknown
// vendor. Only for a door loaded without its vendor's id.
func (p *Process) vendorID(ctx context.Context, cid string) (int64, error) {
	id, err := p.traffic.VendorRows().VendorIDByCID(ctx, cid)
	if errors.Is(err, dal.ErrNoRows) {
		return 0, nil
	}
	return id, err
}

// sweepGiveUp abandons staged MO nobody is attempting — an account with no
// mo_url and no live bind is claimed by neither drainer, so without this its
// messages would age for ever and the policy's time limit would never fire.
// This is also where `valid_until` is finally enforced: the column has been on
// mo_queue since the table was created and nothing ever read it (P6-B1).
func (p *Process) sweepGiveUp(ctx context.Context) {
	t := time.NewTicker(5 * time.Minute)
	defer t.Stop()
	if p.retry.GiveUpAfter <= 0 {
		p.log.Printf("no client-push time limit configured (YAKU_PUSH_GIVE_UP_AFTER_SEC=0) — undelivered MO is kept indefinitely except where valid_until says otherwise")
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n, err := p.queue.SweepGiveUpMO(ctx, p.retry.GiveUpAfter)
		if err != nil {
			p.log.Printf("sweep undeliverable MO: %v", err)
			continue
		}
		if n > 0 {
			p.push.NoteGaveUp(n)
			p.log.Printf("gave up on %d staged MO (past the time limit or its own valid_until)", n)
		}
	}
}

// postClientMO delivers staged MO to the accounts that have an HTTP mo_url, by
// POST (or GET). Under mo_channel 'both' only *stale* jobs (older than
// staleSec) are claimed so a live SMPP bind on smpp-server gets first crack,
// and only within the attempt window the bind has left (config/0146); under
// 'url' the bind never claims, so nothing waits. The 60s lease avoids double
// send.
//
// One lane per account, paced by that account's endpoint (internal/pushpool,
// PO 2026-09-11) — the same runner P5 uses for receipts.
func (p *Process) postClientMO(ctx context.Context) {
	var targets atomic.Pointer[map[int64]moTarget]
	targets.Store(&map[int64]moTarget{})
	pool := pushpool.New(
		func(ctx context.Context, acct int64, n int) ([]model.Message, error) {
			// The account's MO throughput (2026-09-14), shared with its SMPP
			// binds: take only what its bucket grants this second, so the
			// rest waits in the queue without an attempt being counted.
			// Nothing new while the DB worker's record queue is full: an MO
			// pushed now could not be recorded (P14 A4).
			if p.recQ.Full() {
				return nil, nil
			}
			// The bucket is this gateway's (4 Nov 2026), shared with this
			// gateway's binds only, as the limit is this gateway's share.
			key := ratelimit.MOKey(acct, deployment.ID())
			tgt := (*targets.Load())[acct]
			limit := tgt.moThroughput
			if p.grant != nil && limit > 0 {
				n = p.grant.Grant(key, limit, n)
				if n == 0 {
					if p.rateHeld.Note(acct) {
						p.log.Info(logging.SystemScope, "mo_rate_held", "held by its MO rate on this gateway: waits in its queue for the next second",
							logging.Fields{"account": acct, "gateway": deployment.ID(), "limit": limit})
					}
					return nil, nil
				}
			}
			claim, stale := tgt.claim(acct)
			jobs, err := p.queue.ClaimMOStale(ctx, []model.MOClaim{claim}, stale, n)
			if err != nil {
				p.log.Printf("claim mo acct=%d: %v", acct, err)
			}
			if p.grant != nil && limit > 0 {
				p.grant.Return(key, n-len(jobs))
			}
			return jobs, err
		},
		func(ctx context.Context, client *http.Client, m model.Message) {
			if tgt, ok := (*targets.Load())[m.AccountID]; ok {
				p.pushMO(ctx, client, tgt, m)
			}
		},
		time.Second)
	// Each account's lane checks its MO every YAKU_QUEUE_MO_EVERY_MS, busy or
	// idle, and takes at most YAKU_QUEUE_MO_PER_INTERVAL (issue 7).
	chk := queuecheck.For(queuecheck.MO)
	pool.SetCheck(chk.Every, chk.PerInterval)
	// A stop: the pushes already out are answered and recorded (up to
	// pushStopFor), and the last second's failure counts are written.
	defer func() {
		if !pool.StopWithin(pushStopFor) {
			p.log.Printf("mo_url pushes still out after %s: they are sent again when their lease ends", pushStopFor)
		}
		p.alerts().Flush(context.WithoutCancel(ctx))
	}()
	// The targets are read again when their tables changed (asked every few
	// seconds) and every 30 s whatever they say — not every second.
	load := p.loadMOUrls
	gate := webhookGate(p.engine().Marks, "mo")
	if p.moTargets != nil {
		load, gate = p.moTargets, &reloadgate.Gate{} // a test's own targets: read every time
	}
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()
	for {
		// What the last second's failed pushes counted, written in one go.
		p.alerts().Flush(ctx)
		if gate.Due(ctx, time.Now()) {
			// nil means the config store did not answer: keep the lanes running.
			if urls := load(ctx); urls != nil {
				targets.Store(&urls)
				limits := make(map[int64]pushpool.Limits, len(urls))
				for acct, tgt := range urls {
					limits[acct] = tgt.limits
				}
				pool.Sync(ctx, limits)
			} else {
				gate.Failed()
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// pushMO delivers one MO and settles it: finalized on a 2xx, otherwise backed
// off or given up by the endpoint's retry policy.
func (p *Process) pushMO(ctx context.Context, client *http.Client, tgt moTarget, m model.Message) {
	m.Transport = model.TransportHTTP // client delivery leg (record's protocol/transport)
	// Already pushed by this process, its record not yet written (the record
	// store was away, or the record queue full): it is recorded, not pushed
	// a second time (delivered.go).
	if p.delivered.Has(m.ID) {
		try, ok := p.delivered.Try(m.ID)
		if !ok {
			try = dal.ClientTry{TryID: dal.NewTryID(), Channel: dal.TryHTTP, Result: dal.TryAccepted, EndpointID: tgt.endpointID}
		}
		p.finishDelivered(ctx, tgt, m, try)
		return
	}
	// Past the account's limit already (its bind used the tries, or the
	// limit was lowered): given up without one more try (config/0173).
	if tgt.maxAttempts > 0 && m.Attempts > tgt.maxAttempts {
		p.retryOrGiveUp(ctx, m, tgt, "no tries left", false)
		return
	}
	// An encrypted company's MO is staged encrypted and opened only for the
	// push (PO 2026-09-13); the record below keeps it encrypted. If it cannot
	// be opened nothing is sent, and the lease brings it back.
	wire := m
	body, err := p.moText(ctx, m.Body)
	if err != nil {
		p.log.Printf("mo %s acct=%d not delivered: cannot decrypt (%v) — will retry", m.ID, m.AccountID, err)
		return
	}
	wire.Body = body
	sentAt := time.Now()
	// Every try is a row (mo_sub_, D-140), the call on it; its text was
	// sealed: the call's bodies are sealed too.
	private := crypto.IsCiphertext(m.Body)
	ok, failure, final, try := p.deliver(ctx, client, tgt, wire)
	try.Private = private
	if took := time.Since(sentAt); ok {
		p.push.NoteOK(took)
		p.byAcct.NoteOK(m.AccountID)
	} else {
		p.push.NoteFailed(failure, took)
		p.byAcct.NoteFailed(m.AccountID, failure)
	}
	if !ok {
		p.tries.Note(mosvc.TryOf(m, try))
		// P6-B1: back off by the policy, then abandon. Previously a
		// failed delivery simply waited for the 60s lease to lapse and
		// tried again, for ever.
		p.retryOrGiveUp(ctx, m, tgt, failure, final)
		return
	}
	// It answered: the endpoint is not failing, whatever it was doing before.
	p.clearWebhookFailure(ctx, tgt.endpointID)
	// The try it took is the MO's final row, written before it leaves the
	// queue: sealed here (the log seals the others as it writes them).
	try.TryID = dal.NewTryID()
	if private {
		try = pushcall.Seal(ctx, p.sealer(), try)
	}
	p.delivered.NoteTry(m.ID, try)
	p.finishDelivered(ctx, tgt, m, try)
}

// finishDelivered records an MO its customer took — the try it took, as its
// final row — and takes it off mo_queue. When that fails it stays on the
// queue and in p.delivered: its next claim writes the same try and does not
// push it again.
func (p *Process) finishDelivered(ctx context.Context, tgt moTarget, m model.Message, try dal.ClientTry) {
	if err := mosvc.Finalize(ctx, p.moRec(), p.queue, mosvc.Via(m, model.TransportHTTP), try); err != nil {
		p.log.Printf("finalize mo %s: %v — delivered; its record is written again when it is next claimed, and it is not pushed again", m.ID, err)
		return
	}
	p.delivered.Forget(m.ID)
	p.log.Printf("mo delivered %s acct=%d %s->%s -> %s",
		m.ID, m.AccountID, m.Sender, m.Destination, tgt.url)
}

// deliver sends one MO to the account's webhook. ok on success; final when
// the row's own rules call the failure one that will never improve.
//
// The webhook row is an ordinary HTTP endpoint (pushcall, PO 2026-09-14): its
// headers, auth and TLS always apply, and a row that shapes its own request is
// sent exactly as configured, with {message_id} {from} {to} {text}
// {encoding} {accepted_at}. A row
// with none gets the gateway's own GET/POST, as before.
func (p *Process) deliver(ctx context.Context, client *http.Client, tgt moTarget, m model.Message) (ok bool, failure string, final bool, try dal.ClientTry) {
	// The try as a row (D-140): a call that could not even be made is one
	// too, with no answer and why.
	try = dal.ClientTry{Channel: dal.TryHTTP, EndpointID: tgt.endpointID, Attempt: webhookTries(m.Attempts, tgt.minAttempts),
		At: time.Now()}
	unmade := func(why string) (bool, string, bool, dal.ClientTry) {
		try.Result, try.Error = dal.TryNoAnswer, why
		return false, why, false, try
	}
	if tgt.epErr != nil {
		return unmade("webhook settings: " + tgt.epErr.Error())
	}
	moURL, method := tgt.url, strings.ToUpper(tgt.method)
	facts := p.pushFacts(ctx, m)
	fields := map[string]string{
		"msgid": m.ID, "from": m.Sender, "to": m.Destination, "text": m.Body, "encoding": m.Encoding,
		// When the gateway took it in, RFC 3339 UTC (4 Nov 2026): {accepted_at}.
		"accepted_at": mosvc.AcceptedText(m),
	}
	// The subscriber's network and the vendor (4 Nov 2026): {mcc}, {mnc},
	// {operator}, {ported}, {vendor}, {vendor_name}.
	for k, v := range facts.Sources() {
		fields[k] = v
	}
	req, err := tgt.ep.Request(ctx, fields, func(ctx context.Context) (*http.Request, error) {
		return mosvc.NewWebhookRequestWith(ctx, moURL, method, m, tgt.names, facts)
	})
	if err != nil {
		return unmade(err.Error())
	}
	// [SCOPE P13-CAPTURE] The push to the customer's own endpoint. The other
	// half of the MO story: mo_received says we took the message in, this says
	// whether it actually reached them — which is the question behind almost
	// every "we never got that MO" report.
	cap, capturing := logging.ActiveForAccountID(m.AccountID)
	if capturing {
		logging.CaptureTo(cap, p.log, "mo_push", "delivering to client", logging.Fields{
			"messageId": m.ID, "url": req.URL.String(), "method": req.Method,
			"from": m.Sender, "to": m.Destination, "attempts": m.Attempts,
		})
	}
	code, call, err := tgt.ep.SendCaptured(tgt.ep.Client(client), req)
	// The call as it went, request and answer, on its try (D-140).
	pushcall.Fill(&try, call, p.callMode)
	if capturing {
		f := logging.Fields{"messageId": m.ID, "url": moURL}
		msg := "client accepted"
		switch {
		case err != nil && code == 0:
			msg, f["error"] = "client endpoint unreachable", err.Error()
		case err != nil:
			msg, f["status"] = "client refused", code
		default:
			f["status"] = code
		}
		logging.CaptureTo(cap, p.log, "mo_push_result", msg, f)
	}
	if err != nil {
		return false, err.Error(), errors.Is(err, httpadapter.ErrPermanent), try
	}
	return true, "", false, try
}

// retryOrGiveUp applies the endpoint's retry policy to one failed MO delivery:
// delay the next attempt, or abandon the message and record the failing
// endpoint for the Admin API to tell the company about (the engine has no
// mailer, and the Admin API may not read the queue store).
func (p *Process) retryOrGiveUp(ctx context.Context, m model.Message, tgt moTarget, failure string, final bool) {
	// The webhook's own tries, not the bind's (webhookTries).
	tries := webhookTries(m.Attempts, tgt.minAttempts)
	up, why := tgt.policy.GiveUp(tries, m.CreatedAt, time.Now())
	code := "WEBHOOK_UNREACHABLE"
	if final {
		up, why = true, "the endpoint's answer is final (not one of its retry codes)"
	}
	// The account's own limit (config/0173): every try counts, bind and
	// webhook (m.Attempts); whichever limit comes first.
	if tgt.maxAttempts > 0 && m.Attempts >= tgt.maxAttempts {
		up, why, code = true, fmt.Sprintf("reached the account's limit of %d tries", tgt.maxAttempts), "MAX_ATTEMPTS_REACHED"
	}
	if up {
		if err := p.queue.GiveUpMO(ctx, m.ID); err != nil {
			p.log.Printf("give up mo %s: %v", m.ID, err)
			return
		}
		p.push.NoteGaveUp(1)
		p.byAcct.NoteGaveUp(m.AccountID)
		p.log.Printf("GIVING UP on MO %s for acct=%d after %d attempt(s) — %s (%s) -> %s",
			m.ID, m.AccountID, tries, why, failure, tgt.url)
		// The message stays on the incoming log, stamped with why it never
		// reached the customer (2026-09-16). Before this an abandoned MO was
		// recorded NOWHERE: the staging row was marked expired and that was
		// the end of it, so a message we had taken from the carrier simply
		// disappeared.
		m.ErrorCode = code
		if err := mosvc.RecordOutcome(ctx, p.moRec(), m, model.StatusExpired); err != nil {
			p.log.Printf("record abandoned mo %s: %v", m.ID, err)
		}
		p.recordWebhookFailure(ctx, tgt, m.AccountID, failure, true)
		return
	}
	// Not abandoned, but the endpoint did not answer — that is the fact the
	// customer needs early, so it is recorded on every attempt.
	p.recordWebhookFailure(ctx, tgt, m.AccountID, failure, false)
	delay := tgt.policy.Delay(tries)
	if err := p.queue.RescheduleMO(ctx, m.ID, delay); err != nil {
		p.log.Printf("reschedule mo %s: %v", m.ID, err)
		return
	}
	p.log.Printf("mo post %s acct=%d: %s (attempt %d, retrying in %s)",
		m.ID, m.AccountID, failure, tries, delay)
}

// webhookTries is how many times the customer's webhook has had this MO.
//
// Under mo_channel 'both' the queue counts the bind's budget too: the
// webhook's first claim raises the count to mo_bind_attempts (3) whether the
// bind tried or not, so the webhook is on its first try at 3. The give-up and
// the wait compared that count with the webhook's own Retry: a webhook set to
// 2 tries got one, 5 got three, and the first retry waited 4 minutes, the
// fourth step of the backoff (go-live test of 18 Oct 2026, P6 E2). One less
// than the budget is taken off: the webhook's own tries, never under one.
// (A bind that spent its whole budget leaves the count one higher, at
// budget + 1 on the webhook's first try; that case is counted one try long.)
func webhookTries(attempts, minAttempts int) int {
	if minAttempts <= 0 {
		return attempts
	}
	return max(attempts-minAttempts+1, 1)
}

// recordWebhookFailure notes a push the customer's endpoint did not take, for
// the one row per endpoint the Admin API's alert poller reads — one per
// endpoint, never one per message.
//
// Noted on EVERY failed attempt since 2026-09-16 (PO), not only when a
// message is abandoned: the customer wants to hear that their endpoint stopped
// answering while there is still time to fix it, and abandoning the first
// message takes a day. gaveUp says whether this particular failure also
// discarded the message, which is the harsher number the mail leads with.
//
// The row itself is written by internal/webhookalert, at most about once a
// second per endpoint (it was one config-store write per failed push).
func (p *Process) recordWebhookFailure(ctx context.Context, tgt moTarget, accountID int64, failure string, gaveUp bool) {
	p.alerts().Failure(ctx, tgt.endpointID, accountID, tgt.url, failure, gaveUp)
}

// clearWebhookFailure forgets an endpoint's outage the moment anything reaches
// it (PO 2026-09-16: "once any message succeeds, reset the counters").
//
// Deleting the row IS the reset: the counters, and with them the number of
// mails already sent about that outage, go with it — so a later outage is
// treated as new and the customer is told about it again. No row means the
// endpoint is not failing. internal/webhookalert does it at once after a
// failure this process saw, else at most every few seconds (it was one
// DELETE per delivered message).
func (p *Process) clearWebhookFailure(ctx context.Context, endpointID int64) {
	p.alerts().OK(ctx, endpointID)
}

// alerts is this role's webhook alert writer, made on first use.
func (p *Process) alerts() *webhookalert.Tracker {
	p.alertOnce.Do(func() { p.alertT = webhookalert.New(p.traffic.WebhookAlerts(), "mo", p.log.Printf) })
	return p.alertT
}

// webhookGate is the gate for the customers' MO webhook targets: the endpoint
// rows of that kind, their accounts and the accounts' dialects. Looked at
// every YAKU_WEBHOOK_RELOAD_SEC (3), read in full at least every 30 s (the
// dlr role's twin).
func webhookGate(marks dal.ChangeMarks, kind string) *reloadgate.Gate {
	look := 3
	if n, err := strconv.Atoi(strings.TrimSpace(os.Getenv("YAKU_WEBHOOK_RELOAD_SEC"))); err == nil && n >= 1 {
		look = n
	}
	return &reloadgate.Gate{Look: time.Duration(look) * time.Second, Backstop: 30 * time.Second,
		Sig: reloadgate.Tables(marks, "http_endpoints WHERE type = '"+kind+"'", "sms_service_config", "sms_service_dialects")}
}

// loadMOUrls returns account_id -> mo_url and -> mo_method for accounts with one
// set. Client MO webhooks are http_endpoints rows since D-087 (migration
// config/0052 moved them out of sms_service_config.mo_config and DROPPED that
// column — this query read the dropped column until 2026-08-10, so the mo_url
// poster silently delivered NOTHING; found by the P5 matrix campaign, same
// drift as the dlr_url poster).
func (p *Process) loadMOUrls(ctx context.Context) map[int64]moTarget {
	out := map[int64]moTarget{}
	// The account's MO channel comes along (config/0146): it decides whether
	// this leg may claim at all, and from which attempt.
	// The rows are read by the DAL (dal.CustomerWebhooks); a row that cannot
	// be read is left out, as before.
	err := p.traffic.CustomerWebhooks().MOWebhooks(ctx, func(w dal.WebhookRow, err error) {
		epID, acct := w.ID, w.AccountID
		u, meth, retryJSON, respMap, channel := w.URL, w.Method, w.Retry, w.ResponseMap, w.Channel
		ratePerSec, concurrency, timeoutMS, bindAttempts := w.RateLimit, w.Concurrency, w.TimeoutMS, w.BindAttempts
		deployJSON := w.Deployments
		raw := pushcall.Raw{Name: w.Name, Headers: w.Headers, Auth: w.Auth, BodyTemplate: w.BodyTemplate, TLS: w.TLS, Config: w.Config}
		if err == nil {
			if channel == "bind" {
				return // the SMPP bind owns this account's MO outright
			}
			raw.ID, raw.URL, raw.Method = epID, u, meth
			var ep *pushcall.Endpoint
			var epErr error
			if p.webhooks != nil {
				ep, epErr = p.webhooks.Get(ctx, raw)
			}
			// Per-endpoint retry policy over the global default; the `retry`
			// column has existed unread since D-087 (P6-B15).
			// This gateway's share of the account's MO rate (config/0123).
			// An account on the All row carries none and is uncapped here.
			deps, _ := deployment.Parse(deployJSON)
			// `both` hands over only after the bind has spent its budget;
			// `url` (and both with no budget) claims from the first attempt.
			minAttempts := 0
			if channel == "both" && bindAttempts > 0 {
				minAttempts = bindAttempts
			}
			out[acct] = moTarget{endpointID: epID, url: u, method: meth, minAttempts: minAttempts, maxAttempts: w.MaxAttempts,
				moThroughput: deps.Rate(deployment.ID(), deployment.MO), ep: ep, epErr: epErr,
				names:  dialect.PushNamesJSON(respMap),
				policy: pushretry.Resolve(p.retry, retryJSON),
				limits: pushpool.Limits{RatePerSec: ratePerSec, Concurrency: concurrency,
					Timeout: time.Duration(timeoutMS) * time.Millisecond}}
		}
	})
	if err != nil {
		p.log.Printf("load mo_urls: %v", err)
		return nil // "could not read", distinct from "no endpoints": the caller keeps its lanes
	}
	return out
}

// moTarget is one account's MO webhook and the policy governing pushes to it.
type moTarget struct {
	endpointID int64
	url        string
	method     string
	// minAttempts is the bind's budget under mo_channel='both' (config/0146):
	// this leg claims an MO only once the bind has failed that many times, or
	// when nobody has tried it. 0 = 'url': the webhook's from the start.
	minAttempts int
	// maxAttempts is the account's mo_max_attempts (config/0173): the most
	// tries, bind and webhook together; 0 = the policy's alone.
	maxAttempts int
	policy      pushretry.Policy
	limits      pushpool.Limits // rate_limit per second, submit_concurrency in flight, timeout_ms
	// moThroughput is THIS DEPLOYMENT's share of the account's MO cap per
	// second (config/0123 — the account-level mo_throughput is retired), on
	// top of the endpoint's own rate_limit; 0 = none.
	moThroughput int
	// ep is the row built into its push; epErr why it could not be. nil ep =
	// the gateway's own request with nothing added.
	ep    *pushcall.Endpoint
	epErr error
	// names is the account's dialect applied to the push (PO 2026-09-17):
	// our field names -> the customer's; nil = native shape.
	names map[string]string
}

// bindFirstSec is how old an untried MO must be before the webhook takes it
// under mo_channel 'both': a live bind gets that long to claim it first.
const bindFirstSec = 5

// claim is what the webhook poster asks the queue for, for this account: the
// attempt window its mo_channel leaves the webhook, and how old an untried
// message must be. Under 'url' no bind ever claims, so nothing waits.
func (t moTarget) claim(acct int64) (model.MOClaim, int) {
	if t.minAttempts == 0 {
		return model.MOClaim{AccountID: acct}, 0
	}
	return model.MOClaim{AccountID: acct, MinAttempts: t.minAttempts}, bindFirstSec
}

// parseFields collects request params from the query string, then the body:
// a JSON object when the body is one, else a form.
//
// A JSON body was read as a form FIRST, and form keys won: a text holding
// "&to=9" made `9"}` the destination, and the MO went unrouted and was lost
// (encoding test of 19 Oct 2026, enc-106; also "&text=", "&from="). A body
// that is a JSON object is now read only as JSON.
func parseFields(r *http.Request, body []byte) map[string]string {
	out := map[string]string{}
	for k, v := range r.URL.Query() {
		if len(v) > 0 {
			out[strings.ToLower(k)] = v[0]
		}
	}
	var j map[string]any
	if json.Unmarshal(body, &j) == nil {
		for k, v := range j {
			if s, ok := v.(string); ok && out[strings.ToLower(k)] == "" {
				out[strings.ToLower(k)] = s
			}
		}
		return out
	}
	if vals, err := url.ParseQuery(string(body)); err == nil {
		for k, v := range vals {
			if len(v) > 0 && out[strings.ToLower(k)] == "" {
				out[strings.ToLower(k)] = v[0]
			}
		}
	}
	return out
}

func firstNonEmpty(fields map[string]string, keys ...string) string {
	for _, k := range keys {
		if v := fields[k]; v != "" {
			return v
		}
	}
	return ""
}

// moText is an MO's text as the customer must receive it: staged MO of an
// encrypted company is ciphertext, opened here. Plain text passes through.
func (p *Process) moText(ctx context.Context, body string) (string, error) {
	if !crypto.IsCiphertext(body) {
		return body, nil
	}
	if p.cry == nil {
		return "", errors.New("the message is encrypted and this process has no encryption")
	}
	return p.cry.Decrypt(ctx, body)
}
