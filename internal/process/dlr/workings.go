package dlr

import (
	"context"
	"fmt"
	"github.com/yakugate/yakugate/internal/callbackmap"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/dialect"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/reloadgate"
	"github.com/yakugate/yakugate/internal/webhookalert"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"errors"
	dlrsvc "github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/inboundauth"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/internal/pushcall"
	"github.com/yakugate/yakugate/internal/pushpool"
	"github.com/yakugate/yakugate/internal/pushretry"
	"github.com/yakugate/yakugate/internal/queuecheck"
	"github.com/yakugate/yakugate/internal/ratelimit"
	"github.com/yakugate/yakugate/internal/receiptpolicy"
	"github.com/yakugate/yakugate/internal/vendorquality"
	"github.com/yakugate/yakugate/internal/vendors/httpadapter"
	"github.com/yakugate/yakugate/internal/webhookauth"
)

func lookupEnv(k string) (string, bool) { return os.LookupEnv(k) }

// Run serves the vendors' receipt doors until the context is cancelled.
//
//	INPUT   — POST/GET /dlr/<vendor_cid>[/<token>] and each door's own path
//	          (issue 20): callbacks from vendors
//	LOGIC   — the door checks the caller, its dialect (or the default
//	          reading, issue 19) reads the receipts, then the shared
//	          dlr.Service correlates + records + pushes
//	OUTPUT  — record raw_dlr + status updated; the door's answer to the vendor
func (p *Process) Run(ctx context.Context) error {
	if p.hot == nil || p.traffic == nil {
		p.log.Printf("stores unavailable; dlr idle")
		<-ctx.Done()
		return ctx.Err()
	}
	p.loadVendors(ctx)
	// Receipt-code overrides, refreshed on their own ticker: this process is a
	// server loop, so there is no Step cadence to hang it off.
	p.vstatus.Start(ctx, 30*time.Second, p.log.Printf)
	// The accounts' receipt policies, on the same cadence and for the same
	// reason: a server loop has no Step to hang a refresh off.
	p.policies.Start(ctx, 30*time.Second, p.log.Printf)
	// The inbound doors, on the same cadence: a credential set in the portal
	// starts working within one refresh.
	// The old shared inbound settings are retired (PO 2026-09-24): whatever
	// a vendor still has there moves onto its two doors before the first
	// load, so nothing it guarded is left unguarded.
	if err := inboundauth.MoveLegacy(ctx, p.engine().Doors, env("YAKU_TOKEN_PEPPER", "yaku-dev-token-pepper"), p.log.Printf); err != nil {
		p.log.Printf("inbound: moving the old shared settings: %v", err)
	}
	p.doors.Start(ctx, 30*time.Second, p.log.Printf)
	go p.warnUnconfigured(ctx)
	// Receipts that arrive here give messages their verdicts; count them per
	// vendor and network for quality routing (PO 2026-09-14).
	vendorquality.StartCounting(ctx, p.cfg, p.log.Printf)
	// Receipts filed as orphans get more tries at finding their message
	// (2026-09-16). This role runs the sweep for the whole gateway — on a
	// gateway WITHOUT the receipt facts in Redis only: with them a receipt is
	// looked for three times, 5 s apart, before it is an orphan, and an
	// orphan is evidence (29 Oct 2026).
	rematched := p.svc.StartRematch(ctx, p.log.Printf)
	p.drain.Add(1)
	go func() { defer p.drain.Done(); <-rematched }() // a stop waits for the orphan it is applying
	// With the records written directly, the receipts finished in Redis
	// leave their rows on the receipts' own list: written out here, one
	// process of the deployment at a time (the SMPP vendor role and the DB
	// worker write the same list when they run).
	if p.rcptQueue != nil && p.rcptSink != nil {
		p.drain.Add(1)
		go func() { defer p.drain.Done(); p.rcptQueue.Drain(ctx, p.rcptSink, p.log.Printf) }()
	}
	// What the receipt facts kept before 29 Oct 2026 and nothing reads any
	// more (the facts hashes, the waiting sets, the told marks, the groups'
	// sets, the pointers by number) goes once, by the first dlr role of the
	// gateway to start on this build.
	if p.facts != nil {
		p.drain.Add(1)
		go func() {
			defer p.drain.Done()
			if n, err := p.facts.DropOldKeys(ctx); err != nil {
				p.log.Printf("letting go of the receipt facts' old keys: %v", err)
			} else if n > 0 {
				p.log.Printf("let go of %d key(s) the receipt facts kept before 29 Oct 2026 (facts hashes, waiting sets, told marks)", n)
			}
		}()
	}
	// Receipts that came in at another gateway for this one's clients are
	// moved into its own client queue (issue 5): one process of the
	// deployment at a time, beside smpp-server's.
	if p.handoff != nil && p.handoffTo != nil {
		p.drain.Add(1)
		go func() { defer p.drain.Done(); p.handoff.ImportReceipts(ctx, p.handoffTo, p.strays) }()
	}
	if p.live != nil {
		if n, err := monitor.DropAwaitSets(ctx, p.live); err != nil {
			p.log.Printf("dropping the old waiting sets of monitoring: %v", err)
		} else if n > 0 {
			p.log.Printf("dropped %d old waiting set(s) of monitoring (await:v): the receipt facts' own sets are read instead", n)
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/dlr/", p.handleWebhook)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	// Any other path is a receipt door's own path, or nothing (issue 20, as
	// the http-server's catch-all serves an account's own submit path). A
	// path changed in the portal is picked up with the doors' next load.
	mux.HandleFunc("/", p.handleDoorPath)
	srv := &http.Server{Addr: p.addr, Handler: mux}
	// Organized shutdown (PO 2026-07-30, D-086): drain in-flight vendor
	// webhooks instead of cutting their connections — a vendor mid-POST gets
	// its 200, not a reset. Same pattern/timeout as P1's submit server. Run
	// returns as soon as Shutdown is CALLED; Stop waits for it (stop.go).
	p.drain.Add(1)
	go func() {
		defer p.drain.Done()
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	// the vendors' companies, refreshed for the receipt matching
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				p.loadVendors(ctx)
			}
		}
	}()
	// smsc leg for HTTP/unbound clients: drain client_dlr → POST to dlr_url (D-022)
	if p.queue != nil {
		p.drain.Add(2)
		go func() { defer p.drain.Done(); p.postClientDLRs(ctx) }()
		go p.sweepGiveUp(ctx)
		go func() { defer p.drain.Done(); p.sweepNoReceipt(ctx) }()
	}
	p.log.Printf("DLR webhook listening on %s", p.addr)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return ctx.Err()
}

// sweepGiveUp abandons receipts nobody is even attempting. The poster only
// claims accounts that HAVE a dlr_url, and smpp-server only claims accounts
// that are bound right now — so an account with neither would keep its receipts
// for ever: never claimed, attempts never incremented, the policy's time limit
// never evaluated. This is the backstop that closes that hole.
//
// It can only use the GLOBAL policy window: there is no endpoint in play whose
// override could apply. A per-endpoint limit still governs everything that is
// actually being delivered.
func (p *Process) sweepGiveUp(ctx context.Context) {
	if p.retry.GiveUpAfter <= 0 {
		p.log.Printf("no client-push time limit configured (YAKU_PUSH_GIVE_UP_AFTER_SEC=0) — undeliverable receipts are kept indefinitely")
		return
	}
	t := time.NewTicker(5 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n, err := p.queue.SweepGiveUpClientDLR(ctx, p.retry.GiveUpAfter)
		if err != nil {
			p.log.Printf("sweep undeliverable receipts: %v", err)
			continue
		}
		if n > 0 {
			p.push.NoteGaveUp(n)
			p.log.Printf("gave up on %d receipt(s) undelivered for more than %s", n, p.retry.GiveUpAfter)
		}
	}
}

// sweepNoReceipt answers for a vendor that never sent one.
//
// An account's no_receipt rule (config/0119) says: if my message has been
// sitting at `sent` for longer than N minutes, stop waiting and tell my
// client something. This is what customers integrating over HTTP ask for
// most — their system has no other way to close a message out.
//
// The record is NOT changed: the message really is still `sent`, nobody knows
// what became of it, and a receipt arriving next week is still recorded and
// still moves the status. Only the client's side of it is settled, once,
// through told_status — which is also what takes the message out of this
// sweep's own query, so no message is answered twice.
//
// This role runs it for the whole gateway, like the orphan re-match sweep:
// it is the one role that is always present when receipts matter, and a
// per-account index walk a minute costs nothing beside the webhook traffic.
// awaiting is one account's messages that have waited for a receipt longer
// than olderThan: from open_msgs on a gateway without Redis, else from the
// day tables — a message a vendor took (sub_) with no closing receipt and
// nothing told (dlr_). The receipt facts kept waiting sets in the main Redis
// for this until 29 Oct 2026 (four commands a message for a rule few
// accounts have). Only this deployment's and the shared ones (D-121).
//
// Either way it reads on from where the last pass of this account stopped
// (after; next is where this one did), so the old end of the list is never
// read twice.
func (p *Process) awaiting(ctx context.Context, accountID int64, olderThan time.Duration, after dal.AwaitCursor, limit int) (rows []dal.AwaitingReceipt, next dal.AwaitCursor, err error) {
	if dal.OpenMessagesInSQL() {
		return p.hot.AwaitingReceiptOlderAfter(ctx, accountID, olderThan, after, limit)
	}
	company, err := p.companyOf(ctx, accountID)
	if err != nil {
		return nil, after, err
	}
	return p.hot.AwaitingReceiptInDays(ctx, company, accountID, olderThan, after, limit, dal.AwaitDays(olderThan))
}

// companyOf is an account's company (its log tables are its company's, D-136).
func (p *Process) companyOf(ctx context.Context, accountID int64) (int64, error) {
	if p.traffic == nil {
		return 0, nil
	}
	c, err := p.traffic.AccountLookups().AccountCompany(ctx, accountID)
	if err == dal.ErrNoRows {
		return 0, nil
	}
	return c, err
}

// noReceiptBatch is how many waiting messages of one account a pass reads:
// a backlog drains over the following minutes instead of one sweep holding
// the store for minutes.
const noReceiptBatch = 500

func (p *Process) sweepNoReceipt(ctx context.Context) {
	if p.hot == nil || p.svc == nil {
		return
	}
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	cursors := map[int64]dal.AwaitCursor{} // open_msgs: where each account's last pass stopped
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		for _, acct := range p.policies.TimeoutAccounts() {
			action, after := p.policies.For(acct).Timeout()
			if action == nil {
				continue // the policy changed under us between the two calls
			}
			cursors[acct] = p.answerNoReceipt(ctx, acct, action, after, cursors[acct])
		}
	}
}

// answerNoReceipt is one pass of the no-receipt rule over one account: each
// message that has waited longer than `after` is told the rule's answer, once
// (TellOnce: not a message told already, nor one that is final). It returns
// where the next pass reads on from in open_msgs: past every message read,
// unless one could not be answered — then from where this pass began, so it
// is tried again.
func (p *Process) answerNoReceipt(ctx context.Context, acct int64, action *receiptpolicy.Action, after time.Duration, from dal.AwaitCursor) dal.AwaitCursor {
	rows, next, err := p.awaiting(ctx, acct, after, from, noReceiptBatch)
	if err != nil {
		p.log.Printf("no-receipt sweep for account %d: %v", acct, err)
		return from
	}
	// The account's own rule, or the gateway default's (issue 23): its name
	// goes to told_by.
	rule := receiptpolicy.RuleNoReceipt
	if _, _, r := p.policies.For(acct).TimeoutRule(); r != "" {
		rule = r
	}
	told, failed := 0, false
	for _, r := range rows {
		// A stop ends the pass between two messages, never inside one:
		// TellOnce marks the message told and then pushes, and a cut
		// between the two would leave the client never told.
		if ctx.Err() != nil {
			failed = true
			break
		}
		// Decided per message, so a split really splits instead of
		// giving the whole batch one answer.
		o := receiptpolicy.Decide(action, rule, nil)
		ok, err := p.svc.TellOnce(context.WithoutCancel(ctx), dlrsvc.Told{
			MsgID: r.MsgID, AccountID: acct, CompanyID: r.CompanyID, Src: r.Sender, Dst: r.Destination,
			BindSlot: r.BindSlot, NoReceipt: r.NoReceipt, FailureReceiptOnly: r.FailureReceiptOnly,
			Owner: r.Owner, VendorID: r.VendorID, VendorMsgID: r.VendorMsgID,
		}, o)
		if err != nil {
			p.log.Printf("no-receipt answer for %s: %v", r.MsgID, err)
			failed = true
			continue
		}
		// Told now, or told already, or final: nothing more to answer for.
		if ok {
			told++
		}
	}
	if told > 0 {
		p.log.Printf("account %d: told the client about %d message(s) whose vendor sent no receipt within %s",
			acct, told, after)
	}
	if failed {
		return from
	}
	return next
}

// postClientDLRs pushes client_dlr receipts to the accounts that have an HTTP
// dlr_url. Under dlr_channel 'both' it only claims *stale* jobs (older than
// bindFirstSec) so a live SMPP bind on smpp-server gets first crack; under
// 'url' the bind never claims, so nothing waits (2026-10-15 — it waited 5 s
// for a bind that would never come). The 60s claim lease prevents a double
// send if both drain at once. [SCOPE P5]
//
// One lane per account, paced by that account's endpoint (internal/pushpool,
// PO 2026-09-11): its rate_limit per second, submit_concurrency in flight,
// timeout_ms each. This loop only keeps the lanes matched to the endpoints.
func (p *Process) postClientDLRs(ctx context.Context) {
	var targets atomic.Pointer[map[int64]dlrTarget]
	targets.Store(&map[int64]dlrTarget{})
	pool := pushpool.New(
		func(ctx context.Context, acct int64, n int) ([]dlrJob, error) {
			tgt, ok := (*targets.Load())[acct]
			if !ok {
				return nil, nil
			}
			// The account's DLR throughput (2026-09-14), shared with its SMPP
			// binds: take only what its bucket grants this second. A held
			// receipt stays queued with its attempt count untouched, which
			// matters here — under 'both' that count decides which leg owns it.
			// The bucket is this gateway's (4 Nov 2026), shared with this
			// gateway's binds only, as the limit is this gateway's share.
			key := ratelimit.DLRKey(acct, deployment.ID())
			paced := p.grant != nil && tgt.dlrThroughput > 0
			if paced {
				n = p.grant.Grant(key, tgt.dlrThroughput, n)
				if n == 0 {
					if p.rateHeld.Note(acct) {
						p.log.Info(logging.SystemScope, "dlr_rate_held", "held by its receipt rate on this gateway: waits in its queue for the next second",
							logging.Fields{"account": acct, "gateway": deployment.ID(), "limit": tgt.dlrThroughput})
					}
					return nil, nil
				}
			}
			claim, stale := tgt.claim(acct)
			jobs, err := p.queue.ClaimClientDLRStale(ctx, []model.DLRClaim{claim}, stale, n)
			if paced {
				p.grant.Return(key, n-len(jobs))
			}
			if err != nil {
				p.log.Printf("claim client_dlr acct=%d: %v", acct, err)
				return nil, err
			}
			// One lookup for the whole claimed batch, not one per receipt: by
			// the time a receipt arrives the message itself is long gone, and
			// the reference lives on its record. Costs nothing for accounts
			// that never send references (the map simply comes back empty).
			refs := p.clientRefs(ctx, tgt.companyID, jobs)
			out := make([]dlrJob, len(jobs))
			for i, j := range jobs {
				out[i] = dlrJob{ClientDLR: j, ref: refs[j.ClientMsgID]}
			}
			return out, nil
		},
		func(ctx context.Context, client *http.Client, j dlrJob) {
			// The endpoint as it is NOW: a URL edited mid-batch takes effect on
			// the next push, not the next claim.
			if tgt, ok := (*targets.Load())[j.AccountID]; ok {
				p.pushDLR(ctx, client, tgt, j)
			}
		},
		time.Second)
	// Each account's lane checks its receipts every YAKU_QUEUE_DLR_EVERY_MS,
	// busy or idle, and takes at most YAKU_QUEUE_DLR_PER_INTERVAL (issue 7).
	chk := queuecheck.For(queuecheck.DLR)
	pool.SetCheck(chk.Every, chk.PerInterval)
	// One message's receipts reach its customer in order: one after another,
	// a progress word before the final one (go-live test 18 Oct 2026: 'sent'
	// came after 'delivered'); other messages' side by side, as before.
	pool.Order(receiptOrder)
	// A stop: the pushes already out are answered and acked (up to
	// pushStopFor), and the last second's failure counts are written.
	defer func() {
		if !pool.StopWithin(pushStopFor) {
			p.log.Printf("dlr_url pushes still out after %s: they are sent again when their lease ends", pushStopFor)
		}
		p.alerts().Flush(context.WithoutCancel(ctx))
	}()
	load := p.loadDLRUrls
	// The targets are read again when their tables changed (asked every few
	// seconds) and every 30 s whatever they say — not every second.
	gate := webhookGate(p.engine().Marks, "dlr")
	if p.dlrTargets != nil {
		load, gate = p.dlrTargets, &reloadgate.Gate{} // a test's own targets: read every time
	}
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()
	for {
		// What the last second's failed pushes counted, written in one go.
		p.alerts().Flush(ctx)
		if gate.Due(ctx, time.Now()) {
			// nil means the config store did not answer: keep the lanes as
			// they are rather than stopping every customer's receipts over
			// one failed read.
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

// dlrJob is one claimed receipt with the client's own reference for it.
type dlrJob struct {
	model.ClientDLR
	ref string
}

// receiptOrder is a receipt's place among its message's (pushpool.Order): the
// id the client holds, and 0 for a progress receipt (the message is still
// `sent`: ACCEPTD, ENROUTE), 1 for a final one.
func receiptOrder(j dlrJob) (string, int) {
	if model.Status(j.Status).IsTerminal() {
		return j.ClientMsgID, 1
	}
	return j.ClientMsgID, 0
}

// pushDLR sends one receipt to the account's webhook and settles it: ack on
// success, otherwise back off or give up by the endpoint's retry policy.
//
// The webhook row is an ordinary HTTP endpoint (pushcall, PO 2026-09-14): its
// headers, auth and TLS always apply, and a row that shapes its own request
// (parameters or a body template) is sent exactly as configured. A row with
// none gets the gateway's own JSON body, as before.
func (p *Process) pushDLR(ctx context.Context, client *http.Client, tgt dlrTarget, j dlrJob) {
	// Every try is a row (dlr_sub_, D-140), the call on it; a push that
	// could not even be made is one too, with no answer and why.
	try := receiptTry(j.ClientDLR, tgt)
	// Past the account's limit already (its bind used the tries, or the
	// limit was lowered): given up without one more try.
	if tgt.maxAttempts > 0 && j.Attempts > tgt.maxAttempts {
		p.retryOrGiveUp(ctx, j.ClientDLR, tgt, "no tries left", false)
		return
	}
	unmade := func(failure string) {
		try.Result, try.Error = dal.TryNoAnswer, failure
		p.tries.Note(try)
		p.push.NoteFailed(failure, 0)
		p.byAcct.NoteFailed(j.AccountID, failure)
		p.retryOrGiveUp(ctx, j.ClientDLR, tgt, failure, false)
	}
	if tgt.epErr != nil {
		// Its settings cannot be built (e.g. a secret that will not
		// resolve): fail the push with the reason rather than send it
		// without the auth the customer asked for.
		unmade("webhook settings: " + tgt.epErr.Error())
		return
	}
	req, err := tgt.ep.Request(ctx, dlrFields(j), func(ctx context.Context) (*http.Request, error) {
		return dlrsvc.NewWebhookRequest(ctx, tgt.url, webhookBody(j), tgt.names)
	})
	if err != nil {
		unmade(err.Error())
		return
	}
	sentAt := time.Now()
	_, call, err := tgt.ep.SendCaptured(tgt.ep.Client(client), req)
	took := time.Since(sentAt)
	pushcall.Fill(&try, call, p.callMode)
	p.tries.Note(try)
	if err != nil {
		p.push.NoteFailed(err.Error(), took)
		p.byAcct.NoteFailed(j.AccountID, err.Error())
		// P5-B02: back off by the policy, and stop entirely once it is
		// exhausted. Before this the only schedule was the claim's flat
		// 60s lease, so a dead endpoint was hammered once a minute for
		// ever and nothing recorded that receipts were being lost. An
		// answer the row calls final (not one of its retry codes) gives up
		// at once.
		p.retryOrGiveUp(ctx, j.ClientDLR, tgt, err.Error(), errors.Is(err, httpadapter.ErrPermanent))
		return
	}
	p.push.NoteOK(took)
	p.byAcct.NoteOK(j.AccountID)
	// It answered: the endpoint is not failing, whatever it was doing before.
	p.clearWebhookFailure(ctx, tgt.endpointID)
	// An ack the queue store refuses is kept and made again until it takes
	// it: the customer has this receipt, and unacked it would be posted
	// again when its lease ends (retest of 19 Oct 2026, dlr.AckKeeper). The
	// receipts posted in the same moment are acked together (ackbatch.go).
	p.log.Printf("dlr posted acct=%d msg=%s status=%s -> %s", j.AccountID, j.ClientMsgID, j.Status, tgt.url)
	if made, n, err := p.ackPosted(j.ID); made && err != nil {
		p.log.Printf("ack client_dlr of %d posted receipt(s) with id=%d: %v", n, j.ID, err)
	}
}

// webhookBody is the gateway's own body for one receipt (a row that shapes
// its own request gets dlrFields instead). doneAt is the receipt's own time;
// a receipt queued before that was carried says when it was queued.
func webhookBody(j dlrJob) dlrsvc.WebhookBody {
	done := j.DoneAt
	if done.IsZero() {
		done = j.CreatedAt
	}
	return dlrsvc.WebhookBody{MessageID: j.ClientMsgID, Reference: j.ref, Status: j.Status,
		Stat: j.Stat, Receipt: j.Receipt, ErrorCode: dlrsvc.ErrorCodeOf(j.Receipt),
		To: j.Dst, From: j.Src,
		SubmittedAt: dlrsvc.WebhookTime(dlrsvc.SubmittedAt(j.ClientMsgID)),
		DoneAt:      dlrsvc.WebhookTime(done)}
}

// dlrFields are a receipt's values for a webhook row's own request: the same
// facts as the gateway's own body, under the row's placeholder names.
func dlrFields(j dlrJob) map[string]string {
	b := webhookBody(j)
	return map[string]string{
		"msgid": j.ClientMsgID, "reference": j.ref, "status": j.Status, "stat": j.Stat, "receipt": j.Receipt,
		"error_code": b.ErrorCode, "to": j.Dst, "from": j.Src,
		"submitted_at": b.SubmittedAt, "done_at": b.DoneAt,
	}
}

// retryOrGiveUp applies the endpoint's retry policy to one failed push: delay
// the next attempt, or abandon the receipt and record the failing endpoint so
// the Admin API can tell the company (the engine has no mailer, and the Admin
// API may not read the queue store — webhook_delivery_alerts is the handoff).
func (p *Process) retryOrGiveUp(ctx context.Context, j model.ClientDLR, tgt dlrTarget, failure string, final bool) {
	now := time.Now()
	up, why := tgt.policy.GiveUp(j.Attempts, j.CreatedAt, now)
	code := "WEBHOOK_UNREACHABLE"
	if final {
		up, why = true, "the endpoint's answer is final (not one of its retry codes)"
	}
	// The account's own limit (config/0173): every try counts, bind and
	// webhook; whichever limit comes first.
	if tgt.maxAttempts > 0 && j.Attempts >= tgt.maxAttempts {
		up, why, code = true, fmt.Sprintf("reached the account's limit of %d tries", tgt.maxAttempts), "MAX_ATTEMPTS_REACHED"
	}
	if up {
		if err := p.queue.GiveUpClientDLR(ctx, j.ID); err != nil {
			p.log.Printf("give up dlr id=%d: %v", j.ID, err)
			return
		}
		p.push.NoteGaveUp(1)
		p.byAcct.NoteGaveUp(j.AccountID)
		// The gateway's giving up is a row too: no try, and why.
		gave := receiptTry(j, tgt)
		gave.Channel, gave.EndpointID, gave.Result, gave.ErrorCode, gave.Error = "", 0, dal.TryExpired, code, why+": "+failure
		p.tries.Note(gave)
		p.log.Printf("GIVING UP on receipt %s for acct=%d after %d attempt(s) — %s (%s) -> %s",
			j.ClientMsgID, j.AccountID, j.Attempts, why, failure, tgt.url)
		p.recordWebhookFailure(ctx, tgt, j.AccountID, "dlr", failure, true)
		return
	}
	// Not abandoned, but the endpoint did not answer — recorded on every
	// attempt (PO 2026-09-16) so the customer hears about an outage in
	// minutes rather than when the first receipt is discarded a day later.
	p.recordWebhookFailure(ctx, tgt, j.AccountID, "dlr", failure, false)
	delay := tgt.policy.Delay(j.Attempts)
	if err := p.queue.RescheduleClientDLR(ctx, j.ID, delay); err != nil {
		p.log.Printf("reschedule dlr id=%d: %v", j.ID, err)
		return
	}
	p.log.Printf("post dlr acct=%d id=%s: %s (attempt %d, retrying in %s)",
		j.AccountID, j.ClientMsgID, failure, j.Attempts, delay)
}

// recordWebhookFailure notes a push the customer's endpoint did not take, for
// the one row per endpoint the Admin API's alert poller reads. One row, not
// one per message: a customer whose endpoint is down loses thousands of
// receipts and must be told ONCE. Written by internal/webhookalert at most
// about once a second per endpoint (it was one config-store write per failed
// push).
func (p *Process) recordWebhookFailure(ctx context.Context, tgt dlrTarget, accountID int64, kind, failure string, gaveUp bool) {
	p.alerts().Failure(ctx, tgt.endpointID, accountID, tgt.url, failure, gaveUp)
}

// clearWebhookFailure forgets an endpoint's outage as soon as anything reaches
// it (PO 2026-09-16). The row IS the counters, including how many mails this
// outage has already produced, so dropping it resets all of them: the next
// outage is a new one and the customer is told about it again.
// internal/webhookalert does it at once after a failure this process saw,
// else at most every few seconds (it was one DELETE per delivered receipt).
func (p *Process) clearWebhookFailure(ctx context.Context, endpointID int64) {
	p.alerts().OK(ctx, endpointID)
}

// alerts is this role's webhook alert writer, made on first use.
func (p *Process) alerts() *webhookalert.Tracker {
	p.alertOnce.Do(func() { p.alertT = webhookalert.New(p.traffic.WebhookAlerts(), "dlr", p.log.Printf) })
	return p.alertT
}

func trunc(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// webhookGate is the gate for a role's customer webhook targets of one kind
// ("dlr" or "mo"): they are made of the endpoint rows of that kind, their
// accounts and the accounts' dialects. Looked at every
// YAKU_WEBHOOK_RELOAD_SEC (3), read in full at least every 30 s.
func webhookGate(marks dal.ChangeMarks, kind string) *reloadgate.Gate {
	look := 3
	if n, err := strconv.Atoi(strings.TrimSpace(os.Getenv("YAKU_WEBHOOK_RELOAD_SEC"))); err == nil && n >= 1 {
		look = n
	}
	return &reloadgate.Gate{Look: time.Duration(look) * time.Second, Backstop: 30 * time.Second,
		Sig: reloadgate.Tables(marks, "http_endpoints WHERE type = '"+kind+"'", "sms_service_config", "sms_service_dialects")}
}

// loadDLRUrls returns account_id -> dlr_url for accounts that have one set.
// Client DLR webhooks are http_endpoints rows since D-087 (migration
// config/0052 moved them out of sms_service_config.dlr_config and DROPPED
// that column — this query read the dropped column until 2026-08-10, so the
// dlr_url poster silently delivered NOTHING; found by the P5 matrix campaign).
func (p *Process) loadDLRUrls(ctx context.Context) map[int64]dlrTarget {
	// P5-B03: the account's delivery channel comes along, because it decides
	// whether this leg may claim the row at all and from which attempt.
	// The rows are read by the DAL (dal.CustomerWebhooks); a row that cannot
	// be read is left out, as before.
	m := map[int64]dlrTarget{}
	err := p.traffic.CustomerWebhooks().DLRWebhooks(ctx, func(w dal.WebhookRow, err error) {
		epID, acct, company := w.ID, w.AccountID, w.CompanyID
		url, retryJSON, channel, respMap := w.URL, w.Retry, w.Channel, w.ResponseMap
		bindAttempts, ratePerSec, concurrency, timeoutMS := w.BindAttempts, w.RateLimit, w.Concurrency, w.TimeoutMS
		deployJSON := w.Deployments
		raw := pushcall.Raw{Name: w.Name, Method: w.Method, Headers: w.Headers, Auth: w.Auth, BodyTemplate: w.BodyTemplate, TLS: w.TLS, Config: w.Config}
		if err == nil {
			if channel == "bind" {
				return // the SMPP bind owns this account's receipts outright
			}
			// Per-endpoint retry policy on top of the global default — the
			// `retry` column has existed since D-087 and was read by nothing
			// (P5-B08); this is the half of it that now works.
			// `both` hands over only after the bind has spent its budget;
			// `url` (and a pre-0077 empty value) claims from the first attempt.
			minAttempts := 0
			if channel == "both" && bindAttempts > 0 {
				minAttempts = bindAttempts
			}
			raw.ID, raw.URL = epID, url
			var ep *pushcall.Endpoint
			var epErr error
			if p.webhooks != nil {
				ep, epErr = p.webhooks.Get(ctx, raw)
			}
			// This gateway's share of the account's receipt rate
			// (config/0123). An account on the All row is uncapped here.
			deps, _ := deployment.Parse(deployJSON)
			m[acct] = dlrTarget{endpointID: epID, url: url, minAttempts: minAttempts, companyID: company, maxAttempts: w.MaxAttempts,
				dlrThroughput: deps.Rate(deployment.ID(), deployment.DLR),
				ep:            ep, epErr: epErr, names: dialect.PushNamesJSON(respMap),
				policy: pushretry.Resolve(p.retry, retryJSON),
				limits: pushpool.Limits{RatePerSec: ratePerSec, Concurrency: concurrency,
					Timeout: time.Duration(timeoutMS) * time.Millisecond}}
		}
	})
	if err != nil {
		p.log.Printf("load dlr_urls: %v", err)
		return nil
	}
	return m
}

// dlrTarget is one account's receipt webhook and the policy governing pushes to it.
type dlrTarget struct {
	endpointID int64
	url        string
	// companyID is the account's company: the records of its messages are in
	// mt_<company>_<month> (D-136), where the client's own reference is.
	companyID int64
	// minAttempts is the bind's budget under dlr_channel='both': this leg
	// claims a receipt only once the bind has already failed that many times.
	minAttempts int
	// maxAttempts is the account's dlr_max_attempts (config/0173): the most
	// tries, bind and webhook together; 0 = the policy's alone.
	maxAttempts int
	policy      pushretry.Policy
	// limits is how hard this endpoint may be pushed: its rate_limit per
	// second, submit_concurrency in flight, timeout_ms per push.
	limits pushpool.Limits
	// dlrThroughput is THIS DEPLOYMENT's share of the account's receipt cap
	// per second (config/0123 — the account-level dlr_throughput is retired)
	// (dlr_throughput), on top of the endpoint's rate_limit; 0 = none.
	dlrThroughput int
	// names is the account's dialect applied to the push body (PO
	// 2026-09-17): our field names -> the customer's; nil = native shape.
	names map[string]string
	// ep is the row built into its push (headers, auth, TLS, its own
	// request); epErr says why it could not be built. nil ep = the
	// gateway's own request with nothing added.
	ep    *pushcall.Endpoint
	epErr error
}

// bindFirstSec is how old an untried receipt must be before the webhook takes
// it under dlr_channel 'both': a live bind gets that long to claim it first.
const bindFirstSec = 5

// claim is what the webhook poster asks the queue for, for this account: the
// attempt window its dlr_channel leaves the webhook, and how old an untried
// receipt must be. Under 'url' no bind ever claims, so nothing waits.
func (t dlrTarget) claim(acct int64) (model.DLRClaim, int) {
	if t.minAttempts == 0 {
		return model.DLRClaim{AccountID: acct}, 0
	}
	return model.DLRClaim{AccountID: acct, MinAttempts: t.minAttempts}, bindFirstSec
}

// Step lets the process satisfy worker.Worker; Run takes precedence.
func (p *Process) Step(ctx context.Context) error { return nil }

// handleWebhook takes a receipt callback on a door's default path,
// /dlr/<cid>[/<token>]. The cid names the vendor; the token is checked by the
// door, not used to find it, so a wrong secret reads as "forbidden" in the log
// and not as "unknown vendor" — an operator debugging a silent vendor must be
// able to tell the two apart.
func (p *Process) handleWebhook(w http.ResponseWriter, r *http.Request) {
	cid, urlToken := webhookauth.SplitPath(strings.TrimPrefix(r.URL.Path, "/dlr/"))
	p.serveDoor(w, r, cid, urlToken)
}

// handleDoorPath takes a receipt callback on any other path:
//
//	a door's own path        that door (issue 20, inbound_endpoints.custom_path)
//	a DLR dialect's own URL  the door of that dialect the caller's credentials
//	                         name (27 Oct, sms_service_dialects.url; serveDialect)
//	anything else            404
//
// Each is the whole path, or the path and then the token for a door that
// reads its token from the path. A path nothing names is 404, all of them in
// ONE bucket — the path is the caller's, and a count for each would let
// anyone grow this role's memory without limit.
func (p *Process) handleDoorPath(w http.ResponseWriter, r *http.Request) {
	if door, urlToken, ok := p.doors.ForPath(r.URL.Path, inboundauth.KindDLR); ok {
		p.serveDoor(w, r, door.VendorCID, urlToken)
		return
	}
	if doors, urlToken, ok := p.doors.ForDialectPath(r.URL.Path, inboundauth.KindDLR); ok {
		p.serveDialect(w, r, doors, urlToken)
		return
	}
	if !p.doors.Loaded() {
		http.Error(w, "inbound endpoints not loaded yet", http.StatusServiceUnavailable)
		return
	}
	p.inbound.Note(unknownVendor, "unknown_path")
	http.NotFound(w, r)
}

// serveDialect takes a receipt callback to a DLR dialect's own URL (27 Oct,
// Saleem: "dialects for mo and dlr will also have custom urls, similar to MT
// one"). Every door reading with the dialect shares the URL, so the caller's
// credentials say which door it is (inboundauth.Guard.Identify); the callback
// is then judged and read at that door exactly as on its own path.
// Credentials that name none of them are a bare 403, counted once.
func (p *Process) serveDialect(w http.ResponseWriter, r *http.Request, doors []inboundauth.Endpoint, urlToken string) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	door, res := p.guard.Identify(r, body, doors, urlToken)
	if !res.OK {
		p.log.Printf("REFUSED receipt at the dialect URL %s from %s: %s", doors[0].DialectURL, r.RemoteAddr, res.Reason)
		// ONE bucket: no door was named.
		p.inbound.Note(unknownVendor, "refused")
		if res.RetryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(res.RetryAfter))
		}
		http.Error(w, http.StatusText(res.Status), res.Status)
		return
	}
	p.serveFound(w, r, door.VendorCID, door, body, urlToken)
}

// serveDoor reads one receipt callback at a vendor's receipt door and applies
// it (issue 19, 2026-10-27 — Saleem: "it will be defined in inbounds", "no
// dialect should use default, but creating inbound is necessary"):
//
//	no receipt door       refused: 404, as an unknown vendor (the vendor's
//	                      "DLR webhook" call made a vendor known until
//	                      2026-10-27; config/0153 moved its reading onto a
//	                      dialect on the door)
//	a door, no dialect    read with the default reading (callbackmap
//	                      DefaultDLR: the usual names for the id, status,
//	                      error and number)
//	a door and a dialect  read with the dialect, as before
//
// The door's own security comes first in every case, and a refusal is always
// a bare 403 or 429.
func (p *Process) serveDoor(w http.ResponseWriter, r *http.Request, cid, urlToken string) {
	door, known := p.doors.For(cid, inboundauth.KindDLR)
	if !known || !door.Exists() || door.VendorID == 0 {
		if !p.doors.Loaded() {
			// Not read yet: refuse rather than let a callback through
			// unauthenticated, and say so plainly for the operator.
			http.Error(w, "inbound endpoints not loaded yet", http.StatusServiceUnavailable)
			return
		}
		// ONE bucket for every unknown cid, never one per cid: the path is
		// caller-controlled, and keying on it would let anyone grow this
		// role's memory and the dashboard without limit.
		p.inbound.Note(unknownVendor, "unknown_vendor")
		http.Error(w, "unknown vendor", http.StatusNotFound)
		return
	}
	// The body is read before authentication: a credential may be a field of
	// it. It is bounded, and nothing is done with it until the caller has
	// proved itself.
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	p.serveFound(w, r, cid, door, body, urlToken)
}

// serveFound judges a receipt callback at its door and, once the caller has
// proved itself, reads and applies it. The door's default path, its own path
// and its dialect's URL all end here.
func (p *Process) serveFound(w http.ResponseWriter, r *http.Request, cid string, door inboundauth.Endpoint, body []byte, urlToken string) {
	// P5-B01, rebuilt 2026-09-23: authenticate the caller with the account
	// edge's own machinery before a single byte of this receipt is allowed to
	// change a real message's status.
	if res := p.guard.Check(r, body, door, urlToken); !res.OK {
		// The REASON goes to our log only. Telling the caller which check
		// failed helps them hunt for the one that passes.
		p.log.Printf("REFUSED receipt for vendor %s from %s: %s", cid, r.RemoteAddr, res.Reason)
		p.inbound.Note(cid, "refused")
		if res.RetryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(res.RetryAfter))
		}
		http.Error(w, http.StatusText(res.Status), res.Status)
		return
	}
	reader := door.Request
	if reader == nil {
		reader = callbackmap.DefaultDLR()
	}
	// receipts may arrive as query params (GET) or a body (POST) — kept whole
	p.applyDoor(w, r, webhookVendor{id: door.VendorID, cid: cid}, door, reader, body, r.URL.RawQuery+"\n"+string(body))
}

// applyDoor reads a receipt callback with its door's reader — the door's
// dialect (config/0125, one model with a client's dialect since 2026-10-08),
// or the default reading of a door with none — and applies each receipt: the
// vendor's field names, a batch of receipts in one call, the dialect's status
// words, the number the receipt is about, and the answer the vendor expects.
//
// Unreadable receipts are kept as orphans and acknowledged — refusing only
// makes the vendor resend what we still cannot read — unless the dialect has
// an error rule for UNREADABLE. A receipt we could not APPLY (the store is
// down) is answered as STORE_UNAVAILABLE (503 when the door has no answer of
// its own), so the vendor sends it again; the ones already applied that time
// come back "unmatched", which changes nothing.
func (p *Process) applyDoor(w http.ResponseWriter, r *http.Request, v webhookVendor, door inboundauth.Endpoint,
	reader *callbackmap.Request, body []byte, raw string) {
	cid := v.cid
	where := "at the mapped path"
	if reader.IsDefault() {
		where = "under the usual names (the door has no dialect)"
	}
	items, err := reader.Read(r, body)
	if err != nil || len(items) == 0 {
		why := "no receipts in the callback"
		if err != nil {
			why = err.Error()
		}
		p.keepOrphan(r, v, "", "", raw, why)
		p.inbound.Note(cid, "no_message_id")
		door.Reply.Write(w, callbackmap.Outcome{Reason: callbackmap.ReasonUnreadable})
		return
	}
	monitor.Flow.NoteReceipt(int64(len(items)))
	failed := false
	readable := 0
	var last string
	for _, it := range items {
		id := strings.TrimSpace(it.Values["message_id"])
		st, errCode := it.Values["status"], strings.TrimSpace(it.Values["error_code"])
		if id == "" {
			p.keepOrphan(r, v, "", st, it.Raw, "no message id "+where)
			p.log.Printf("dlr vendor=%s: no message id %s — kept as orphan", cid, where)
			p.inbound.Note(cid, "no_message_id")
			continue
		}
		readable++
		last = id
		// When the vendor says the message reached the phone, if the door maps
		// it (done_at). A time that cannot be read is logged and left out;
		// the receipt still counts.
		doneAt, _, terr := reader.DoneAt(it.Values)
		if terr != nil {
			p.log.Printf("dlr vendor=%s id=%s: delivery time %q not read (%v) — check the delivery time's format on the door's dialect",
				cid, id, it.Values["done_at"], terr)
		}
		// The handset the receipt is about, when the vendor says (the "to"
		// field): a bulk call the vendor gave ONE id sends one receipt a
		// number, and each finds its own message by it (dlr groups.go, rule
		// 1; go-live test 18 Oct 2026, P4 B8 / P4-7).
		applied, err := p.applyReceipt(r.Context(), dlrsvc.ReceiptIn{
			VendorID: v.id, VendorMsgID: id, RawStatus: st, ErrorCode: errCode, Raw: it.Raw,
			StatusMap: reader.StatusMap, DoneAt: doneAt, Number: strings.TrimSpace(it.Values["to"])})
		switch {
		case err != nil:
			failed = true
			p.inbound.Note(cid, "error")
			p.log.Printf("apply dlr vendor=%s id=%s: %v", cid, id, err)
		case applied:
			p.inbound.Note(cid, "applied")
		default:
			// A receipt for an id we never issued, or for a message already
			// final. A vendor with a high share of these has its id format
			// wrong (hex vs decimal is the classic) — see msgid_transform.
			p.inbound.Note(cid, "unmatched")
			p.log.Printf("dlr vendor=%s id=%s unmatched", cid, id)
		}
	}
	out := callbackmap.Outcome{MessageID: last, Count: len(items)}
	switch {
	case failed:
		out.Reason = callbackmap.ReasonStore
	case readable == 0:
		// Not one receipt of the call had a message id.
		out.Reason = callbackmap.ReasonUnreadable
	}
	door.Reply.Write(w, out)
}

// applyReceipt hands one receipt to the receipt service: the door's workers
// when they run, else the service itself (or a test's seam).
func (p *Process) applyReceipt(ctx context.Context, in dlrsvc.ReceiptIn) (bool, error) {
	if p.applyIn != nil {
		return p.applyIn(ctx, in)
	}
	return p.gather.Apply(ctx, p.svc, in)
}

// keepOrphan files a receipt we could not tie to a message, with the reason.
// With the DB worker it is a task of the record queue, like every hot-store
// row (28 Oct 2026).
func (p *Process) keepOrphan(r *http.Request, v webhookVendor, id, st, raw, why string) {
	var err error
	switch {
	case p.recQ != nil:
		err = p.recQ.WriteOrphanDLR(r.Context(), v.id, id, st, "", raw, why)
	case p.hot != nil:
		err = p.hot.WriteOrphanDLR(r.Context(), v.id, id, st, "", raw, why)
	default:
		return
	}
	if err != nil {
		p.log.Printf("orphan receipt from vendor %s not recorded: %v", v.cid, err)
	}
}

// warnUnconfigured says, once per change, which inbound doors nothing guards.
//
// A door with no credential refuses every callback, and the vendor sees only
// "forbidden" — so this log line is the operator's one clue. Said once per
// change rather than every refresh: a message repeated every 30 seconds is
// scenery, and scenery protects nobody.
func (p *Process) warnUnconfigured(ctx context.Context) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	var warner, dialects, paths webhookauth.UnconfiguredWarner
	for {
		if fresh := warner.Warn(p.doors.Unconfigured()); len(fresh) > 0 {
			p.log.Printf("inbound endpoints with NO security configured (every callback is refused): %s",
				strings.Join(fresh, ", "))
		}
		// A receipt door whose dialect could not be loaded reads callbacks
		// the old way; this line is the only thing that says so.
		for _, line := range dialects.Warn(p.doors.MappingProblems(inboundauth.KindDLR)) {
			p.log.Printf("receipt door of vendor %s — its dialect is NOT in use, callbacks are read as if it had none", line)
		}
		// A path not used, or a token two doors on one dialect URL share
		// (27 Oct): callbacks there are refused, and only this line says why.
		for _, line := range paths.Warn(p.doors.PathProblems(inboundauth.KindDLR)) {
			p.log.Printf("receipt door of vendor %s", line)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// loadVendors loads which vendors are one carrier (vendorgroup): a receipt
// that comes in at one vendor's door is matched against what any vendor of
// its company sent (D-135).
//
// It also built the table of receipt "webhook" readings — the vendor's "DLR
// webhook" HTTP call (adapter_config.endpoints.webhook) and a hand-written
// profile's status.extract — until issue 19 (2026-10-27): a vendor's receipts
// are read by its receipt door and nothing else, and config/0153 moved those
// readings onto a DLR dialect on each vendor's door. A PAUSED vendor's door
// stays open (go-live test 18 Oct 2026, P4 G2): the doors load every vendor
// that is not disabled (inboundauth cache).
func (p *Process) loadVendors(ctx context.Context) {
	if err := p.groups.Load(ctx, p.traffic.VendorCompanies()); err != nil {
		p.log.Printf("vendor companies for receipts: %v", err)
	}
}

// clientRefs maps our message ids to the ids their clients gave them, for one
// claimed batch. Best effort: a lookup failure costs the `reference` field on
// this batch's receipts, never the receipts themselves.
func (p *Process) clientRefs(ctx context.Context, companyID int64, jobs []model.ClientDLR) map[string]string {
	if p.hot == nil || len(jobs) == 0 {
		return nil
	}
	ids := make([]model.RecordRef, 0, len(jobs))
	for _, j := range jobs {
		ids = append(ids, model.RecordRef{MsgID: j.ClientMsgID, CompanyID: companyID})
	}
	refs, err := p.hot.ClientRefsFor(ctx, ids)
	if err != nil {
		p.log.Printf("client reference lookup failed (%v) — receipts sent without it", err)
		return nil
	}
	return refs
}

// receiptTry is a receipt's try to reach its customer's webhook, as a row
// (D-140): filed with the message the receipt is about, the id the client
// was told beside it.
func receiptTry(j model.ClientDLR, tgt dlrTarget) dal.ClientTry {
	t := dal.ClientTry{Kind: dal.TryDLR, MsgID: j.MsgID, ClientMsgID: j.ClientMsgID, CompanyID: tgt.companyID,
		AccountID: j.AccountID, Status: j.Status, Stat: j.Stat, Attempt: j.Attempts, Channel: dal.TryHTTP,
		EndpointID: tgt.endpointID, At: time.Now()}
	if t.MsgID == "" {
		t.MsgID = j.ClientMsgID
	}
	if t.Stat == "" {
		t.Stat = string(dlrsvc.StatFor(model.Status(j.Status)))
	}
	return t
}
