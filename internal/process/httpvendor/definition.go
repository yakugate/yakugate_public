// Package httpvendor implements the "http-vendor" process (P4): the upstream HTTP
// egress. It claims each HTTP vendor's queue and delivers messages through the
// CONFIG-DRIVEN httpadapter (a per-vendor JSON Profile), so a new HTTP/REST/XML/
// SOAP vendor is onboarded by config only — no code. Mirrors P3 (smppvendor) in
// shape: Claim -> deliver -> record + vendor_msg_id -> Ack, and poll status ->
// UpdateStatus. Reuses the same stores as P1/P3. [SCOPE P4]
//
// Definition: types + construction. The Step loop lives in workings.go.
package httpvendor

import (
	"context"
	"errors"
	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/queuecheck"
	"github.com/yakugate/yakugate/internal/receiptfacts"
	"github.com/yakugate/yakugate/internal/recordq"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/ackkeep"
	"github.com/yakugate/yakugate/internal/crypto"
	"github.com/yakugate/yakugate/internal/dal"
	redisstore "github.com/yakugate/yakugate/internal/dal/redis"
	"github.com/yakugate/yakugate/internal/dal/runtimequeue"
	"github.com/yakugate/yakugate/internal/deployment"
	dlrsvc "github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/handoff"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/platform/worker"
	"github.com/yakugate/yakugate/internal/queues"
	"github.com/yakugate/yakugate/internal/ratelimit"
	"github.com/yakugate/yakugate/internal/receiptpolicy"
	"github.com/yakugate/yakugate/internal/vendorgroup"
	"github.com/yakugate/yakugate/internal/vendorhealth"
	"github.com/yakugate/yakugate/internal/vendors/httpadapter"
	"github.com/yakugate/yakugate/internal/vendorstatus"
)

// vendorAdapter is one loaded HTTP vendor: its cid + a built adapter.
type vendorAdapter struct {
	id  int64
	cid string
	ad  *httpadapter.Adapter
	// split is how the vendor's rate is shared between its queues (issue
	// 22): fair (the zero value) or priority.
	split queues.Mode
	// The vendor's receipt settings (dlr_config, D-136).
	sendsNoReceipts  bool
	receiptWaitHours int
	// submitTPS is THIS DEPLOYMENT's share of the vendor's rate
	// (vendors.deployments[].mt_throughput, config/0124): the most CALLS a second we may
	// make to this vendor. 0 = no limit, which is every vendor's value today
	// and the behaviour before this column was read at all.
	//
	// A bulk chunk is ONE call and costs one, because that is what the vendor
	// meters and bills — the ruling was to count what we hand them, not how
	// many messages were inside it.
	//
	// PER PROCESS by ruling (PO 2026-09-09): N running http-vendor processes
	// send N times this rate.
	submitTPS int
	// batch and concurrency are the send endpoint's rate_limit and
	// submit_concurrency (D-088): how many messages one claim takes, and how
	// many vendor calls may be in flight at once. 50 and 1 — the fixed values
	// before these were read — for a vendor with no send endpoint row.
	batch       int
	concurrency int
	// ownIntermediateDLR: this vendor sends its own ACCEPTD/ENROUTE, so the
	// gateway does not send a submit notice of its own for it (config/0120).
	ownIntermediateDLR bool
	// downAfter / recheckAfter (config/0103, PO 2026-09-14): how long calls
	// may keep failing before routing counts this vendor as down, and while
	// down, how often one real message is let through to test it.
	downAfter, recheckAfter time.Duration
	// breaker is the send row's Circuit Breaker (JSON) (QA EP-018).
	breaker breakerConfig
}

// vendorQueue is what P4 needs from the per-vendor queue backend — satisfied
// by the MySQL store and, under the YAKU_REDIS_ADDR hot-path gate, the Redis
// store (2026-07-23). ONLY the vendor queue moves behind this interface; the
// client_dlr pusher below keeps its direct MySQL handle regardless.
type vendorQueue interface {
	Claim(ctx context.Context, vendor string, n int) ([]model.Message, error)
	Ack(ctx context.Context, vendor string, msgIDs ...string) error
	FetchConcatGroup(ctx context.Context, vendor string, accountID int64,
		sender, dest string, ref, total int) ([]model.Message, error)
}

// Process is the http-vendor egress (poll-style Worker).
type Process struct {
	// mt is how often each vendor's worker checks its queue and how much one
	// check takes (YAKU_QUEUE_MT_*, internal/queuecheck; issue 7). Zero in a
	// Process a test builds by hand: see mtCheck.
	mt queuecheck.Check
	// facts is where the messages waiting for a receipt are filed when Redis
	// is set (receipt facts and waiting sets, D-136); nil = open_msgs.
	facts  *receiptfacts.Store
	cfg    config.Config
	log    *logging.Logger
	queue  vendorQueue // per-vendor queue (Claim/Ack/FetchConcatGroup)
	mysqlQ dal.Queues  // the queue database's own handle (dal.OpenSQLQueues), opened beside the queues as before
	// sessions holds the vendors' shared login sessions (vendor_sessions):
	// the HLR store, YAKU_HLR_DSN, else the queue database (D-124) — not a queue.
	sessions dal.VendorSessions
	hot      dal.RecordStore // MDR records (Write/UpdateStatus/ResolveVendorMsgID)
	// recs is where this role's records go: the record store itself, or —
	// with YAKU_RECORDS_VIA=worker — the queue the DB worker writes out
	// (recordq). recQ is that queue's client, nil when the role writes its own.
	recs    recordq.Recorder
	recQ    *recordq.Client
	vstatus *vendorstatus.Cache
	// policies is the per-account receipt policy (config/0119). An HTTP
	// vendor is the main reason it exists: most answer the send call and
	// never send a receipt at all.
	policies *receiptpolicy.Cache
	// eng is the config store's parts the shared engine packages read
	// (attempt caps, split modes, receipt policies, doors, change marks).
	eng *dal.EngineConfig
	// traffic is the small parts of the config store this role reads and
	// writes beside its work (vendor health, status codes, companies,
	// webhook alerts, MO rules), on the DAL; nil without a config store.
	traffic *dal.TrafficConfig
	// bill books the money of what this role sees (billing.Moments); nil
	// without Redis or a config store.
	bill *billing.Moments
	svc  *dlrsvc.Service // shared DLR application (P5)
	// groups is which vendors are one carrier (vendorgroup), for matching
	// receipts. Loaded with the vendors.
	groups *vendorgroup.Groups
	// vendors is replaced by every reload while the per-vendor workers read
	// it, hence vmu. lanes holds each running worker's stop function.
	vmu     sync.Mutex
	vendors []vendorAdapter
	lanes   map[int64]context.CancelFunc
	// lanesWG counts the vendor workers running, so a stop can wait for them (Stop).
	lanesWG sync.WaitGroup
	// noID paces the log line of a vendor whose accepted answers carry no id
	// where its send call reads one (noid.go).
	noID noIDWarn
	// bulkAlone paces the line that says a bulk vendor's calls are carrying
	// one message each, and why (noid.go).
	bulkAlone noIDWarn
	// bulkOneID and bulkIDCount pace the lines that say what a bulk call's
	// ids were when they were not one a message (noid.go, noteBulkIDs).
	bulkOneID, bulkIDCount noIDWarn
	// sessTries is where each session vendor stands with the login made when
	// it is loaded (sessions.go); sessMu guards it — the logins run off the
	// Step goroutine.
	sessMu    sync.Mutex
	sessTries map[int64]*sessionTry
	lastPoll  map[int64]time.Time // per-vendor last status poll — honors status.interval_sec (2026-08-19)
	// polls paces each waiting message's status polls (pollsched.go); used
	// from Step only, like lastPoll.
	polls *pollSched
	// turns is the main Redis, where a vendor's poll turn is held
	// (pollturn.go); nil without Redis. instance names this process there.
	turns    dal.PollTurns
	instance string
	// breakers pause a vendor whose calls keep failing, by its send row's
	// circuit breaker settings (extras.go).
	breakers *breakers
	// rate paces calls per vendor against that deployment's share. In
	// memory, so the limit is PER PROCESS by ruling — see vendorAdapter.
	// rate paces calls to a vendor. It is the shared limiter when the
	// counters are kept (the main Redis, else the SQL database), so every
	// vendor process in this deployment draws from one bucket (config/0124);
	// in-memory and per process otherwise. rateShared says which.
	//
	// An interface, not a concrete type, for exactly that swap — the same
	// shape the pipeline's RateAllower takes.
	rate interface {
		Allow(key string, limit, n int) bool
	}
	// rateShared: rate is the shared limiter (every process of the
	// deployment draws from one bucket), not this process's own.
	rateShared bool

	loadedAt time.Time
	reload   time.Duration // vendor reload cadence (YAKU_VENDOR_RELOAD_SEC, default 30s — same knob as P3)
	// What the vendors and their calls looked like at the last look, and when
	// that was: a change is loaded within seconds, not at the next reload
	// (settingsChanged, 2026-10-12).
	settingsSig   string
	settingsSigAt time.Time
	cry           *crypto.Service // decrypt bodies for the vendor call (nil = plaintext) [P1-CRYPTO]
	// callMode is YAKU_VENDOR_CALL_LOG: which answers keep the call as it
	// went (hot/0077, vendorcall.go); "" = all.
	callMode string
	// stats are the per-vendor monitoring counters (status.go). Its own lock,
	// so reading the dashboard can never block a send.
	stats *vendorStats
	// health is what this process sees of each vendor — are its calls being
	// answered — published to vendor_health for routing, and the gate that
	// stops sending to a down vendor (PO 2026-09-14). startOnce starts the
	// publisher and the quality counter with the first Step.
	health    *vendorhealth.Tracker
	startOnce sync.Once

	// acks makes a refused queue ack again (see ack); built on first use.
	acks     *ackkeep.Keeper
	acksOnce sync.Once
}

// New wires the queue, hot and config stores.
func New(cfg config.Config) worker.Worker {
	p := &Process{cfg: cfg, log: logging.New("http-vendor"), reload: 30 * time.Second, mt: queuecheck.For(queuecheck.MT),
		lastPoll: map[int64]time.Time{},
		polls:    newPollSched(),
		breakers: newBreakers(),
		lanes:    map[int64]context.CancelFunc{},
		rate:     ratelimit.New(),
		stats:    newVendorStats(),
		health:   vendorhealth.NewTracker()}
	if v := os.Getenv("YAKU_VENDOR_RELOAD_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			p.reload = time.Duration(n) * time.Second
		}
	}
	if q, err := dal.OpenSQLQueues(cfg); err == nil {
		p.mysqlQ = q // the queue database itself
	}
	// The sessions are on the HLR store's pool, the queue database's when it
	// is the same database (one pool a database a process). None when no
	// HLR store is set or its database keeps none.
	if s, err := dal.OpenVendorSessions(cfg); err == nil {
		p.sessions = s
	} else if !errors.Is(err, dal.ErrNotServed) {
		p.log.Printf("vendor sessions store (YAKU_HLR_DSN) not reachable: %v — a session-auth vendor logs in per process", err)
	}
	// What was queued before this build is moved into today's queues at
	// start (issue 22), here too: a deployment may run only vendor roles.
	runtimequeue.HandOver(cfg, p.log.Printf)
	// The vendor MT queue and the client_dlr queue are on the DAL the
	// settings pick (dal.QueuesOn): Redis only when YAKU_QUEUE_BACKEND=redis
	// (D-122, 2026-10-02), else the SQL database even when Redis is set.
	var qs dal.Queues
	var qredis *redisstore.Store // the receipts' write-behind needs the Redis queue itself
	if q, on, err := dal.OpenQueues(cfg); err == nil {
		qs, p.queue = q, q
		if qredis, _ = q.(*redisstore.Store); qredis != nil {
			if err := qredis.Ping(context.Background()); err != nil {
				p.log.Printf("REDIS UNREACHABLE at %s: %v — vendor claims will fail until it returns", cfg.ForQueueRedis().RedisWhere(), err)
			}
		}
	} else {
		p.log.Printf("queues unavailable on %s: %v", on, err)
	}
	// A counter, not a queue: it stays where the counters are whatever
	// YAKU_QUEUE_BACKEND says. One counter per DEPLOYMENT, not per process
	// (config/0124). A carrier's rate is something you allocate across
	// gateways, so every vendor process on this one has to draw from the same
	// bucket: the main Redis when it is set (not the queues' own,
	// YAKU_QUEUE_REDIS_*: a counter belongs with the other counters), else
	// the SQL database's limit_buckets (dal.CountersOn, DAL step 3). Only
	// where neither keeps them does it stay per process, which the role says
	// out loud when it binds.
	if l, err := dal.OpenLimits(cfg); err == nil {
		p.rate = ratelimit.NewShared(l)
		p.rateShared = true
	}
	if cfg.RedisEnabled() {
		// The vendors' poll turns (pollturn.go) are in the main Redis too.
		if t, err := dal.OpenPollTurns(cfg); err == nil {
			p.turns = t
		}
		p.instance = vendorhealth.Instance()
	}
	if h, err := dal.OpenRecordStore(cfg); err == nil {
		p.hot = h
		p.recs, p.recQ = recordq.ForRole(context.Background(), cfg, h, p.log.Printf)
		var pusher dlrsvc.ClientPusher
		if qredis != nil && p.recQ != nil {
			// With the DB worker, a receipt's database work is a task on the
			// record queue, and it is queued in the same Redis transaction
			// as the client's receipt (recordq receipts.go, 2026-10-13).
			pusher = dlrsvc.NewQueuePusher(recordq.WithReceipts(qredis, p.recQ))
		} else if qs != nil {
			pusher = dlrsvc.NewQueuePusher(qs) // smsc leg -> client_dlr
		}
		p.svc = dlrsvc.New(h, pusher)
		if p.recQ != nil {
			// Worker mode: every receipt row this role files goes through the
			// record queue; the store is only read (28 Oct 2026).
			p.svc.SetRecordWrites(p.recQ)
		}
		// A receipt for a client on another gateway is parked for it
		// (issue 5): the status poll (one process of the gateways at a time)
		// closes every gateway's messages.
		handoff.Attach(cfg, pusher, p.log.Printf)
		p.groups = vendorgroup.New()
		p.svc.SetGroups(p.groups)
		// With Redis set an HTTP vendor's receipts are finished in Redis as
		// an SMPP vendor's are (internal/dlr fast.go): the message's facts
		// are SET when the vendor takes it (noteSent), and the receipt that
		// closes it GETDELs them and is sent on to the client without MySQL
		// (29 Oct 2026: no MySQL call on the receipt path). Its row and the
		// message's close go on the record queue with the DB worker, else on
		// the receipts' own list (rcptw), which the dlr role and the SMPP
		// vendor role of the deployment write out. YAKU_RECEIPTS_IN_REDIS=0:
		// no facts, and the record store matches every receipt.
		if cfg.RedisEnabled() && receiptfacts.On() {
			// The facts and the list on the main Redis, on one client
			// (dal.OpenReceipts).
			if rcpts, err := dal.OpenReceipts(cfg, deployment.Owner()); err == nil {
				p.facts = receiptfacts.New(rcpts.Facts)
				var writes dlrsvc.RecordWriter = receiptfacts.NewQueue(rcpts.Writes)
				if p.recQ != nil {
					writes = p.recQ
				}
				p.svc.SetFast(p.facts, writes)
				p.log.Printf("receipts are finished in Redis")
			} else {
				p.log.Printf("receipts not finished in Redis: %v", err)
			}
		}
	}
	// The config store's parts the shared engine packages read
	// (dal.EngineConfig, on the same pool as traffic; empty when it cannot
	// be opened, as traffic is then nil).
	p.eng = dal.EngineConfigOrNone(cfg)
	if t, err := dal.OpenTrafficConfig(cfg); err == nil {
		p.traffic = t
	}
	// Money is booked at the moments this role sees (issues 3 and 18; Saleem,
	// 26 Oct 2026): each call's answer, the receipts its polls bring. The
	// billing role is retired.
	// With no Redis the money is booked in the config store (step 3).
	if (p.turns != nil || !cfg.RedisEnabled()) && p.traffic != nil {
		if bs, err := dal.OpenBilling(cfg); err == nil {
			p.bill = billing.NewMoments(context.Background(), bs, p.log.Printf)
			if p.svc != nil {
				p.svc.SetBiller(p.bill)
			}
		} else {
			p.log.Printf("billing unavailable: %v", err)
		}
	}
	// Receipt-code overrides for the status-poll leg, from the same cache the
	// other two receipt paths use.
	p.vstatus = vendorstatus.New(p.traffic.StatusCodes())
	p.svc.SetStatusOverrides(p.vstatus)
	p.policies = receiptpolicy.New(p.engine().Policies)
	p.svc.SetPolicies(p.policies)
	p.callMode = vendorCallMode()
	if cs, err := dal.OpenDataKeys(cfg); err == nil {
		if c, err := crypto.FromEnv(context.Background(), cs, config.Secrets); err == nil {
			p.cry = c
			if c != nil && p.svc != nil {
				// Receipts of encrypted messages keep their text: part
				// encrypted (PO 2026-09-13).
				p.svc.SetSealer(c)
			}
		} else {
			p.log.Printf("crypto init: %v", err)
		}
	}
	return p
}

// Name returns the process role name.
func (p *Process) Name() string { return "http-vendor" }

// rec is where this role's records go (see Process.recs).
func (p *Process) rec() recordq.Recorder {
	if p.recs != nil {
		return p.recs
	}
	if p.hot == nil {
		return nil // not a nil store inside the interface
	}
	return p.hot
}

// rateUnit says whom a vendor's rate binds: the deployment's processes
// together (the counters are shared: the main Redis, else the SQL database),
// or this process alone.
func rateUnit(shared bool, role string) string {
	if shared {
		return "shared by its " + role + " processes"
	}
	return "per process (no shared counters)"
}
