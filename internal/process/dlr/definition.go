// Package dlr implements the "dlr" process (P5): the delivery-receipt receiver.
// It exposes the vendors' receipt doors so vendors can PUSH receipts, reads
// each with its door's DLR dialect (or the default reading of a door with
// none — issue 19, 2026-10-27), and funnels them through the shared
// internal/dlr service (correlate -> raw_dlr history -> status -> client push).
// SMPP deliver_sm receipts arrive on the P3 bind and use the same service. [SCOPE P5]
//
// Definition: types + construction. The Run loop lives in workings.go.
package dlr

import (
	"context"
	"github.com/yakugate/yakugate/internal/billing"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/auth"
	"github.com/yakugate/yakugate/internal/crypto"
	"github.com/yakugate/yakugate/internal/dal"
	redisstore "github.com/yakugate/yakugate/internal/dal/redis"
	"github.com/yakugate/yakugate/internal/dal/runtimequeue"
	"github.com/yakugate/yakugate/internal/deployment"
	dlrsvc "github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/handoff"
	"github.com/yakugate/yakugate/internal/inboundauth"
	"github.com/yakugate/yakugate/internal/lockout"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/platform/worker"
	"github.com/yakugate/yakugate/internal/pushcall"
	"github.com/yakugate/yakugate/internal/pushretry"
	"github.com/yakugate/yakugate/internal/queuecheck"
	"github.com/yakugate/yakugate/internal/ratelimit"
	"github.com/yakugate/yakugate/internal/receiptfacts"
	"github.com/yakugate/yakugate/internal/receiptpolicy"
	"github.com/yakugate/yakugate/internal/recordq"
	"github.com/yakugate/yakugate/internal/seclog"
	"github.com/yakugate/yakugate/internal/vendorgroup"
	"github.com/yakugate/yakugate/internal/vendorstatus"
	"github.com/yakugate/yakugate/internal/webhookalert"
)

// dlrQueue is the client_dlr backing this poster reads (stale claim) and acks —
// MySQL or, under the Redis gate, Redis.
type dlrQueue interface {
	ClaimClientDLRStale(ctx context.Context, claims []model.DLRClaim, minAgeSec, n int) ([]model.ClientDLR, error)
	AckClientDLR(ctx context.Context, ids ...int64) error
	// Retry policy (P5-B02): back a failed push off, abandon it once the
	// policy is exhausted, and sweep rows nobody is even trying.
	RescheduleClientDLR(ctx context.Context, id int64, delay time.Duration) error
	GiveUpClientDLR(ctx context.Context, id int64) error
	SweepGiveUpClientDLR(ctx context.Context, olderThan time.Duration) (int64, error)
}

// webhookVendor is the vendor a receipt door belongs to. How its receipts are
// read is the door's (its dialect, or the default reading): the vendor's own
// "DLR webhook" call and a hand-written profile's status.extract are no longer
// read (issue 19, 2026-10-27; config/0153 moved them onto the doors).
type webhookVendor struct {
	id  int64
	cid string
}

// Process is the DLR webhook receiver (Runnable).
type Process struct {
	// facts is where the messages waiting for a receipt are filed when Redis
	// is set (D-136); nil = open_msgs.
	facts *receiptfacts.Store
	// drain counts what a stop waits for (Stop, stop.go): the door's
	// shutdown, the dlr_url push loop, the no-receipt sweep, the orphan
	// re-match.
	drain sync.WaitGroup
	// grant paces receipt pushes by each account's DLR throughput
	// (2026-09-14): the shared Redis bucket under the gate, else in-memory.
	grant ratelimit.Granter
	// rateHeld counts the checks an account's rate on this gateway gave 0.
	rateHeld ratelimit.Holds
	cfg      config.Config
	log      *logging.Logger
	addr     string
	hot      dal.RecordStore
	queue    dlrQueue
	// eng is the config store's parts the shared engine packages read
	// (attempt caps, split modes, receipt policies, doors, change marks).
	eng *dal.EngineConfig
	// traffic is the small parts of the config store this role reads and
	// writes beside its work (vendor health, status codes, companies,
	// webhook alerts, MO rules), on the DAL; nil without a config store.
	traffic *dal.TrafficConfig
	// The customers' webhook outage rows (webhook_delivery_alerts), written
	// by internal/webhookalert: counted here, flushed about once a second.
	alertOnce sync.Once
	alertT    *webhookalert.Tracker
	vstatus   *vendorstatus.Cache
	svc       *dlrsvc.Service
	// groups is which vendors are one carrier (vendorgroup): a callback that
	// comes in for one vendor of a company is matched against what any of
	// them sent. Loaded with the vendors.
	groups *vendorgroup.Groups
	// Shared with P1's rule (D-091): X-Forwarded-For is honoured only from
	// listed trusted proxies, so an IP allow-list cannot be spoofed by a
	// header the caller sets for itself. nil = peer address only.
	ipres *auth.IPResolver
	// Global client-push retry policy; each endpoint may override it from its
	// own http_endpoints.retry column.
	retry pushretry.Policy
	// webhooks builds each customer receipt webhook row into its push
	// (headers, auth, TLS, a request of its own) and keeps it until the row
	// changes (pushcall, 2026-09-14).
	webhooks *pushcall.Cache
	// acks takes the receipts a webhook took off client_dlr, and again while
	// the queue store refuses (dlr.AckKeeper, retest of 19 Oct 2026). nil (a
	// test's own Process) = acked once, as before.
	acks *dlrsvc.AckKeeper
	// ackBatch gathers the receipts the webhooks took for one ack call
	// (ackbatch.go).
	ackBatch ackBatch

	// Monitoring (2026-09-11). This role used to publish nothing beyond "I am
	// alive" while applying every HTTP vendor's receipts and pushing every
	// dlr_url — the busiest invisible role in the gateway.
	inbound monitor.Inbound  // vendor webhooks, per vendor and outcome
	push    monitor.Delivery // receipts pushed to customers' dlr_url
	// byAcct is the same push leg seen per CUSTOMER (2026-09-16), so one
	// customer's failing endpoint is visible beside the healthy ones instead
	// of being summed away into the totals above.
	byAcct monitor.PushAccounts

	// handoff parks receipts for clients on other gateways and imports the
	// ones parked for this gateway into handoffTo, its own client queue
	// (issue 5); strays is the queue Redis, walked once for what an older
	// build queued there for another gateway. nil = none.
	handoff   *handoff.HandOff
	handoffTo *dlrsvc.QueuePusher
	strays    handoff.StrayReceipts
	// live is the main Redis's live view: the one-time drop of the waiting
	// sets monitoring kept until 2026-10-15 (monitor.DropAwaitSets). nil
	// without Redis.
	live dal.LiveStatus

	// policies is the per-account receipt policy table (config/0119).
	policies *receiptpolicy.Cache

	// gather applies the receipts that come in at this role's door many at a
	// time (dlr.Gather, 2026-10-13); nil = each by itself, as before.
	gather *dlrsvc.Gather

	// recQ is the record queue's client, set with the DB worker: a receipt
	// finished in Redis leaves its database work there. nil = none is queued
	// by this role.
	recQ *recordq.Client

	// tries writes every try to give a customer a receipt (dlr_sub_, D-140),
	// the webhook call on it as callMode says (YAKU_WEBHOOK_CALL_LOG).
	tries    *dal.TryLog
	callMode string

	// rcptQueue is the receipts' own list (rcptw) with the records written
	// directly: a receipt finished in Redis leaves its database work there,
	// and this role writes it out (rcptSink). nil with the DB worker, or
	// without Redis.
	rcptQueue *receiptfacts.Queue
	rcptSink  receiptfacts.Sink

	// doors and guard are inbound security (config/0121): one endpoint row per
	// vendor per kind, judged with the account edge's own machinery.
	doors *inboundauth.Cache
	guard *inboundauth.Guard

	// dlrTargets replaces loadDLRUrls when set — the seam a test uses to run
	// the real push loop without a config store. nil in production.
	dlrTargets func(context.Context) map[int64]dlrTarget

	// applyIn replaces the receipt service at the door when set — the seam a
	// test uses to see what a door read. nil in production.
	applyIn func(context.Context, dlrsvc.ReceiptIn) (bool, error)
}

// doorWorkers is how many workers apply the receipts that come in at the door
// (each owns the message ids that hash to it, so a message's receipts keep
// their order).
const doorWorkers = 8

// New wires the hot + config stores and the shared DLR service.
func New(cfg config.Config) worker.Worker {
	p := &Process{cfg: cfg, log: logging.New("dlr"), addr: env("YAKU_DLR_ADDR", ":8090"),
		retry: pushretry.Defaults()}
	p.acks = dlrsvc.NewAckKeeper(func(failed bool, text string) {
		if failed {
			p.log.Printf("ERROR %s", text)
			return
		}
		p.log.Printf("%s", text)
	})
	if res, invalid := auth.NewIPResolver(env("YAKU_TRUSTED_PROXIES", "")); res != nil {
		p.ipres = res
		if len(invalid) > 0 {
			p.log.Printf("YAKU_TRUSTED_PROXIES: ignoring invalid entries %v", invalid)
		}
	}
	var pusher dlrsvc.ClientPusher
	var ownQ dlrsvc.Enqueuer // this gateway's own client queue, for receipts handed to it
	// client_dlr is on the DAL the settings pick (dal.QueuesOn): Redis only
	// with YAKU_QUEUE_BACKEND=redis (D-122), else the SQL database.
	var qredis *redisstore.Store
	if q, on, err := dal.OpenQueues(cfg); err == nil {
		p.queue = q
		pusher = dlrsvc.NewQueuePusher(q) // smsc leg -> client_dlr (D-022)
		ownQ = q
		if s, ok := q.(handoff.StrayReceipts); ok {
			p.strays = s // what another gateway parked for this one (Redis)
		}
		// The receipts' write-behind needs the Redis queue itself: the client's
		// receipt and its record work go in one Redis transaction.
		qredis, _ = q.(*redisstore.Store)
	} else {
		p.log.Printf("client receipt queue unavailable on %s: %v", on, err)
	}
	// Items left on the other side of YAKU_QUEUE_BACKEND move over (D-122).
	runtimequeue.HandOver(cfg, p.log.Printf)
	// The throughput counter and the monitoring read use Redis whenever it is
	// set.
	p.grant = ratelimit.New()
	if cfg.RedisEnabled() {
		// Monitoring: the main Redis. The waiting set (await:v:*) is
		// monitoring data, written there by every role.
		if l, err := dal.OpenLiveStatus(cfg); err == nil {
			p.live = l
		}
	}
	if l, err := dal.OpenLimits(cfg); err == nil {
		// A counter: the main Redis (dal.CountersOn).
		p.grant = ratelimit.NewShared(l)
	}
	if h, err := dal.OpenRecordStore(cfg); err == nil {
		p.hot = h
		// With the DB worker (YAKU_RECORDS_VIA=worker) a receipt that comes
		// in at this role's door is finished in Redis when the vendor role
		// filed its message's facts there (internal/dlr fast.go): found,
		// decided once, sent on to the client, and its row and the message's
		// final status put on the record queue, on the message's own lane,
		// in the same Redis transaction as the client's receipt (recordq
		// receipts.go, 2026-10-13). A receipt with no facts takes the record
		// store's path, as every receipt here did. Only with the worker: it
		// is the role sure to write that queue.
		fast := recordq.ViaWorker(cfg) && receiptfacts.On()
		if recordq.ViaWorker(cfg) {
			// Worker mode: every row this role files goes through the record
			// queue, whether or not receipts are finished in Redis (28 Oct 2026).
			_, p.recQ = recordq.ForRole(context.Background(), cfg, h, p.log.Printf)
		}
		if fast {
			if qredis != nil && p.recQ != nil {
				pusher = dlrsvc.NewQueuePusher(recordq.WithReceipts(qredis, p.recQ))
			}
		}
		p.svc = dlrsvc.New(h, pusher)
		if p.recQ != nil {
			p.svc.SetRecordWrites(p.recQ)
		}
		// A receipt for a client on another gateway is parked for it, and
		// the receipts parked for this one are filed here (issue 5).
		p.handoff = handoff.Attach(cfg, pusher, p.log.Printf)
		if ownQ != nil {
			p.handoffTo = dlrsvc.NewQueuePusher(ownQ)
		}
		p.groups = vendorgroup.New()
		p.svc.SetGroups(p.groups)
		// With Redis set, the messages waiting for a receipt are in its
		// facts and nowhere else (D-136: "Redis if set, else MySQL, never
		// both"): a receipt is finished there, its key SET at send and
		// GETDEL'd by the receipt that closes it (29 Oct 2026). With
		// YAKU_RECEIPTS_IN_REDIS=0 no facts are filed and the record store
		// matches every receipt, as on a gateway without Redis.
		// The facts and the receipts' own list on the main Redis, on one
		// client (dal.OpenReceipts).
		var rcpts *dal.Receipts
		if cfg.RedisEnabled() && receiptfacts.On() {
			if r, err := dal.OpenReceipts(cfg, deployment.Owner()); err == nil {
				rcpts = r
				p.facts = receiptfacts.New(r.Facts)
			} else {
				p.log.Printf("receipts not finished in Redis: %v", err)
			}
		}
		if fast && p.recQ != nil && p.facts != nil {
			p.svc.SetFast(p.facts, p.recQ)
			p.log.Printf("receipts are finished in Redis when their message's facts are there (the DB worker writes their rows)")
			// A vendor's callback is one request a receipt. The receipts that
			// arrive while a worker is busy are applied together, in one read,
			// one close and one filing (dlr.Gather → Service.ApplyMany).
			p.gather = dlrsvc.NewGather(p.svc, doorWorkers)
		} else if p.facts != nil {
			// With the records written directly, a receipt is finished in
			// Redis too (go-live test 18 Oct 2026), as the SMPP vendor role's
			// have been since 2026-10-07: found, decided once, sent on to the
			// client, and its row and its message's status put on the
			// receipts' own list (rcptw), which this role writes out — one
			// process of the deployment at a time. It went through the record
			// store, and a receipt that came before its record, or while
			// MySQL was away, was answered and lost (2,090 of 9,000 in one
			// test). The list waits for a record that is late, and for a
			// MySQL that is away.
			p.rcptQueue = receiptfacts.NewQueue(rcpts.Writes)
			p.rcptQueue.Check = queuecheck.For(queuecheck.ReceiptWrites) // YAKU_QUEUE_RECEIPT_WRITES_* (issue 7)
			p.rcptSink = receiptfacts.ToRecordStore(h)
			p.svc.SetFast(p.facts, p.rcptQueue)
			p.log.Printf("receipts are finished in Redis when their message's facts are there (this role writes their rows)")
			p.gather = dlrsvc.NewGather(p.svc, doorWorkers)
		} else if !cfg.RedisEnabled() {
			// No Redis (DAL plan, step 3, 30 Oct 2026): every receipt is
			// matched and closed in the record store. The callbacks that
			// arrive while a worker is busy are closed together, in one
			// transaction a receipt table, and their clients' receipts
			// filed in one go (dlr.Gather → ApplyMany → closemany.go); one
			// at a time each was two commits behind the summary rows every
			// writer updates.
			p.gather = dlrsvc.NewGather(p.svc, doorWorkers)
		}
		// A receipt applied later (from the orphans) has no status words of
		// its own: it is read with its vendor's.
		p.svc.SetStatusMaps(p.statusMapOf)
	}
	// The config store's parts the shared engine packages read
	// (dal.EngineConfig, on the same pool as traffic; empty when it cannot
	// be opened, as traffic is then nil).
	p.eng = dal.EngineConfigOrNone(cfg)
	if t, err := dal.OpenTrafficConfig(cfg); err == nil {
		p.traffic = t
		p.webhooks = pushcall.NewCache(config.Secrets)
	}
	// What a receipt decides is booked when the client is given it (issues 3
	// and 18; Saleem, 26 Oct 2026): the receipt fee, a delivery charge point,
	// the vendor's sides. The billing role is retired.
	// With no Redis the money is booked in the config store (step 3).
	if (p.live != nil || !cfg.RedisEnabled()) && p.traffic != nil && p.svc != nil {
		if bs, err := dal.OpenBilling(cfg); err == nil {
			p.svc.SetBiller(billing.NewMomentsBooking(context.Background(), bs, p.log.Printf))
		} else {
			p.log.Printf("billing unavailable: %v", err)
		}
	}
	// Receipt-code overrides for the webhook leg — the third and last caller
	// of dlr.Apply, all three now answered by one cache.
	p.vstatus = vendorstatus.New(p.traffic.StatusCodes())
	p.svc.SetStatusOverrides(p.vstatus)
	// The accounts' receipt policies (config/0119). This role needs them
	// twice over: to replace a status on an arriving receipt, and to run the
	// no_receipt sweep for messages whose vendor never answered at all.
	p.policies = receiptpolicy.New(p.engine().Policies)
	p.svc.SetPolicies(p.policies)
	// Inbound security (config/0121, PO 2026-09-23): a vendor's callbacks are
	// authenticated with the account edge's own machinery — JWT, token,
	// password, signature, IP allow-list, throughput, lockout ladders, spray
	// guard, security events. One door per vendor per kind.
	p.doors = inboundauth.NewCache(p.engine().Doors)
	sec := seclog.Sink("dlr")
	lad := lockout.New(envIntDefault("YAKU_INBOUND_FAIL_THRESHOLD", 5))
	ipLad := lockout.New(envIntDefault("YAKU_INBOUND_IP_THRESHOLD", 20))
	spray := lockout.NewSprayGuard(envIntDefault("YAKU_SPRAY_DISTINCT_THRESHOLD", 3))
	p.guard = inboundauth.New(
		inboundauth.WithPepper(env("YAKU_TOKEN_PEPPER", "yaku-dev-token-pepper")),
		inboundauth.WithLadder(lad),
		inboundauth.WithIPThrottle(ipLad),
		inboundauth.WithSprayGuard(spray),
		inboundauth.WithThroughput(p.grant),
		inboundauth.WithSecrets(config.Secrets),
		inboundauth.WithReporter(sec),
		inboundauth.WithIPResolver(func(r *http.Request) string { return p.ipres.FromRequest(r) }),
	)
	// Receipts of encrypted messages keep their text: part encrypted
	// (PO 2026-09-13). Without crypto here that part is removed instead.
	if cs, err := dal.OpenDataKeys(cfg); err == nil && p.svc != nil {
		if c, err := crypto.FromEnv(context.Background(), cs, config.Secrets); err != nil {
			p.log.Printf("crypto init: %v — receipt text of encrypted messages will be removed, not kept", err)
		} else if c != nil {
			p.svc.SetSealer(c)
		}
	}
	// Every try to give a customer a receipt (dlr_sub_, D-140). A
	// receipt's push holds no message text: its call is kept as sent.
	var tries dal.ClientTries
	if p.recQ != nil {
		tries = p.recQ
	} else if p.hot != nil {
		tries = p.hot
	}
	p.callMode = pushcall.CallLogMode()
	p.tries = pushcall.NewTryLog(tries, nil, p.log.Printf)
	return p
}

// Name returns the process role name.
func (p *Process) Name() string { return "dlr" }

// statusMapOf is a vendor's own words for a status — its receipt door's
// dialect's (dlr.Service.SetStatusMaps): what a receipt applied later from the
// orphans is read with. A door with no dialect has none: the common spellings.
func (p *Process) statusMapOf(vendorID int64) map[string]string {
	if d, ok := p.doors.ForVendor(vendorID, inboundauth.KindDLR); ok && d.Request != nil {
		return d.Request.StatusMap
	}
	return nil
}

func env(k, def string) string {
	if v, ok := lookupEnv(k); ok && v != "" {
		return v
	}
	return def
}

// envIntDefault reads a positive integer setting, def when unset or invalid.
func envIntDefault(k string, def int) int {
	if v, ok := lookupEnv(k); ok {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}
