// Package smppvendor implements the "smpp-vendor" process (P3): the upstream SMPP
// egress. It binds OUT to each SMPP vendor's SMSC as a client, claims that
// vendor's queue, delivers messages as submit_sm within an async window, and
// correlates submit_sm_resp (vendor message id) and inbound deliver_sm (DLRs)
// back onto the MDR records — reusing the same PDU codec as P2 (server side) and
// the same stores as P1. [SCOPE P3]
//
// Definition: types + construction. The Run loop lives in workings.go.
package smppvendor

import (
	"context"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/ackkeep"
	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/concat"
	"github.com/yakugate/yakugate/internal/crypto"
	"github.com/yakugate/yakugate/internal/dal"
	redisstore "github.com/yakugate/yakugate/internal/dal/redis"
	"github.com/yakugate/yakugate/internal/dal/runtimequeue"
	"github.com/yakugate/yakugate/internal/deployment"
	dlrsvc "github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/handoff"
	mosvc "github.com/yakugate/yakugate/internal/mo"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/platform/worker"
	"github.com/yakugate/yakugate/internal/queuecheck"
	"github.com/yakugate/yakugate/internal/ratelimit"
	"github.com/yakugate/yakugate/internal/receiptfacts"
	"github.com/yakugate/yakugate/internal/receiptpolicy"
	"github.com/yakugate/yakugate/internal/recordq"
	"github.com/yakugate/yakugate/internal/vendorgroup"
	"github.com/yakugate/yakugate/internal/vendorhealth"
	"github.com/yakugate/yakugate/internal/vendorstatus"
	"github.com/yakugate/yakugate/lib/smpp/session"
)

// sessionKey identifies one concurrent bind of a vendor: Idx ranges over
// [0, vendorConn.sessions). A vendor configured for a single session keeps
// exactly one key, {vendorID, 0} — unchanged behavior for anyone who never
// touches the new multi-session config. [SCOPE P3 multi-session]
type sessionKey struct {
	VendorID int64
	Idx      int
}

// vendorQueue is what P3 needs from the per-vendor queue backend — satisfied
// by the MySQL store and, under the YAKU_REDIS_ADDR hot-path gate, the Redis
// store (2026-07-23). ONLY the vendor queue moves behind this interface;
// client_dlr (DLR pusher) and mo_queue (MO ingestion) keep the direct MySQL
// handle regardless.
type vendorQueue interface {
	Claim(ctx context.Context, vendor string, n int) ([]model.Message, error)
	Ack(ctx context.Context, vendor string, msgIDs ...string) error
}

// Process is the smpp-vendor egress. It is a Runnable (persistent binds).
type Process struct {
	cfg    config.Config
	log    *logging.Logger
	queue  vendorQueue     // per-vendor queue (Claim/Ack)
	mysqlQ dal.Queues      // the queue database's own handle (dal.OpenSQLQueues), opened beside the queues as before
	hot    dal.RecordStore // MDR records (Write/UpdateStatus/ResolveVendorMsgID)
	// partBooks are each session's long messages waiting for every part's
	// answer (parts.go): sessionKey -> *partsBook.
	partBooks sync.Map
	// recs is where this role's records go: the record store itself, or —
	// with YAKU_RECORDS_VIA=worker — the queue the DB worker writes out
	// (recordq). recQ is that queue's client, nil when the role writes its own.
	recs recordq.Recorder
	recQ *recordq.Client
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
	// groups is which vendors are one carrier (vendorgroup): a receipt that
	// comes in on a carrier's RX row is matched against what its TX row sent.
	// Loaded with the vendors.
	groups *vendorgroup.Groups
	// mt is how often each session checks its vendor's queue and how much
	// one check takes (YAKU_QUEUE_MT_*, internal/queuecheck; issue 7). Zero
	// in a Process a test builds by hand: see mtCheck.
	mt queuecheck.Check
	// mainRedis says the main Redis is set (counters, receipt facts). rcptQueue and rcptSink are the receipts' write-behind:
	// their database work waiting in Redis, and the batch write that takes
	// it to the record store (nil = receipts use the record store directly).
	mainRedis bool
	rcptQueue *receiptfacts.Queue
	rcptSink  receiptfacts.Sink
	// sendHolds counts the ticks on which a session did not claim because the
	// receipt workers were behind (inboundPool.behind); sendHoldSaid is when
	// the vendor's log last said so.
	sendHolds    atomic.Int64
	sendHoldSaid atomic.Int64
	mo           *mosvc.Service // shared MO ingestion (P6): non-receipt deliver_sm -> mo_queue
	// inbound applies receipts and MO off the session readers (inbound.go).
	// Created by New, closed by Run on shutdown; nil (tests) means apply
	// inline on the reader.
	inbound *inboundPool
	cry     *crypto.Service // decrypt bodies for the wire (nil = plaintext) [P1-CRYPTO]

	// Supervisor state (2026-07-26): dynamic vendor reload + live status for the
	// monitor surface. Sessions (2026-07-29): a vendor can run more than one
	// concurrent bind, so state/sup are keyed per session, not per vendor. mu
	// guards every field below. [SCOPE P3]
	mu    sync.Mutex
	state map[sessionKey]*vendorState // per-session live status (bind, window, counters)
	sup   map[sessionKey]*supHandle   // running session goroutines (cancel + done + fingerprint)

	// rate paces sends per vendor against that deployment's share. In
	// memory, so the limit is PER PROCESS by ruling — see vendorConn.submitTPS.
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

	current map[int64]vendorConn // latest loaded config per vendor id (command surface + cross-session lookups)
	// vstatus is the shared vendor_status_codes cache (submit leg here, and
	// the receipt leg through the DLR service). Replaces a private map that
	// could not read a hex-written code at all.
	vstatus *vendorstatus.Cache
	// policies is the per-account receipt policy (config/0119): what a client
	// is told when the vendor's answer is not what the operator wants passed
	// on. Read on the submit-answer path, so it is cached, never queried.
	policies        *receiptpolicy.Cache
	breakers        map[int64]*breakerState // vendor-level circuit breaker
	resumeRequested map[int64]bool          // manual reconnect requests (max_reconnect_attempts recovery)

	// health is what this process sees of each vendor's connection, published
	// to vendor_health for routing (PO 2026-09-14).
	health *vendorhealth.Tracker

	// moParts buffers the parts of incoming multi-part messages until their
	// group is complete (2026-09-14): Redis when the queue is on Redis, else
	// the queue store's concat_assembly table — the same buffer MT ingress
	// uses, under its own MO keys. nil = parts are staged as they come.
	moParts concat.Buffer

	reloadEvery  time.Duration // how often to re-scan the vendors table
	reloadNow    chan struct{} // admin-triggered immediate reconcile ("reload now" command)
	drainTimeout time.Duration // graceful-shutdown window drain budget

	// acks makes a refused queue ack again (see ack); built on first use.
	acks     *ackkeep.Keeper
	acksOnce sync.Once
}

// supHandle tracks one running session goroutine: its cancel func, a done
// channel closed when it fully exits (so a config-change restart can wait for
// the old goroutine before starting the new one), and the config fingerprint
// it was started with.
type supHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
	fp     string

	// The connection the session holds right now, so a session that does not
	// stop when told can have it closed from outside (stopSession).
	mu sync.Mutex
	cl *session.Client
}

// holds records the session's current connection (nil when it has none).
func (h *supHandle) holds(cl *session.Client) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.cl = cl
	h.mu.Unlock()
}

// closeConn closes the session's connection, if it has one. It reports
// whether there was one to close.
func (h *supHandle) closeConn() bool {
	h.mu.Lock()
	cl := h.cl
	h.mu.Unlock()
	if cl == nil {
		return false
	}
	_ = cl.Close()
	return true
}

// New constructs the process, wiring the queue, hot and config stores.
func New(cfg config.Config) worker.Worker {
	p := &Process{
		cfg:          cfg,
		log:          logging.New("smpp-vendor"),
		mt:           queuecheck.For(queuecheck.MT),
		state:        map[sessionKey]*vendorState{},
		inbound:      newInboundPool(inboundWorkers(cfg.RedisEnabled())),
		sup:          map[sessionKey]*supHandle{},
		current:      map[int64]vendorConn{},
		rate:         ratelimit.New(),
		health:       vendorhealth.NewTracker(),
		reloadEvery:  envSeconds("YAKU_VENDOR_RELOAD_SEC", 30),
		reloadNow:    make(chan struct{}, 1),
		drainTimeout: envSeconds("YAKU_SMPP_DRAIN_SEC", 5),
	}
	if q, err := dal.OpenSQLQueues(cfg); err == nil {
		p.mysqlQ = q // the queue database itself
	}
	// What was queued before this build is moved into today's queues at
	// start (issue 22), here too: a deployment may run only vendor roles.
	runtimequeue.HandOver(cfg, p.log.Printf)
	// The vendor MT queue, client_dlr (DLR) and mo_queue (MO) are on the DAL
	// the settings pick (dal.QueuesOn): Redis only when YAKU_QUEUE_BACKEND=
	// redis (D-122, 2026-10-02), else the SQL database even when Redis is set.
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
	if cfg.RedisEnabled() {
		p.mainRedis = true
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
		// (issue 5): a vendor bound from both gateways returns receipts on
		// any bind.
		handoff.Attach(cfg, pusher, p.log.Printf)
		p.groups = vendorgroup.New()
		p.svc.SetGroups(p.groups)
		// A receipt read on a vendor row with no status words of its own (an
		// RX row) is read with the words of the vendor that sent it.
		p.svc.SetStatusMaps(func(id int64) map[string]string {
			p.mu.Lock()
			defer p.mu.Unlock()
			return p.current[id].statusMap
		})
		// The receipt workers apply what they have waiting in one call
		// (inbound.go, dlr.Service.ApplyMany).
		p.inbound.applyMany = p.applyReceipts
		// With Redis set, a receipt is finished in Redis — found, decided,
		// pushed to its client — and its two database writes follow in
		// batches (Saleem 2026-10-07; internal/dlr fast.go). The record
		// store used to be on the path the bind's reader waits on.
		// Both on the main Redis, on one client of their own (dal.OpenReceipts).
		var rcpts *dal.Receipts
		if p.mainRedis && receiptsInRedis() {
			if r, err := dal.OpenReceipts(cfg, deployment.Owner()); err == nil {
				rcpts = r
			} else {
				p.log.Printf("receipts not finished in Redis: %v", err)
			}
		}
		if rcpts != nil {
			p.rcptQueue = receiptfacts.NewQueue(rcpts.Writes)
			p.rcptQueue.Check = queuecheck.For(queuecheck.ReceiptWrites) // YAKU_QUEUE_RECEIPT_WRITES_* (issue 7)
			p.rcptSink = receiptfacts.ToRecordStore(h)
			// Where those two writes wait: the DB worker's record queue when
			// it runs (the message's own lane), else the receipts' own list,
			// which this role then writes out itself.
			var writes dlrsvc.RecordWriter = p.rcptQueue
			if p.recQ != nil {
				writes = p.recQ
			}
			p.svc.SetFast(receiptfacts.New(rcpts.Facts), writes)
		}
		// YAKU_RECEIPTS_IN_REDIS=0: no facts are filed, and the record store
		// matches every receipt, as on a gateway without Redis.
	}
	// The config store's parts the shared engine packages read
	// (dal.EngineConfig, on the same pool as traffic; empty when it cannot
	// be opened, as traffic is then nil).
	p.eng = dal.EngineConfigOrNone(cfg)
	if t, err := dal.OpenTrafficConfig(cfg); err == nil {
		p.traffic = t
	}
	// Money is booked at the moments this role sees (issues 3 and 18; Saleem,
	// 26 Oct 2026): each submit's answer (the vendor's side, an account
	// charged on submit), the receipts (the fee, a delivery charge point),
	// an incoming message. The billing role is retired.
	// With no Redis the money is booked in the config store (step 3).
	if (p.mainRedis || !cfg.RedisEnabled()) && p.traffic != nil {
		if bs, err := dal.OpenBilling(cfg); err == nil {
			p.bill = billing.NewMoments(context.Background(), bs, p.log.Printf)
			if p.svc != nil {
				p.svc.SetBiller(p.bill)
			}
		} else {
			p.log.Printf("billing unavailable: %v", err)
		}
	}
	// The per-vendor status-code table, shared by the submit leg (classify)
	// and the receipt leg (the DLR service below). One cache, so both legs
	// cannot disagree about what a vendor's code means.
	p.vstatus = vendorstatus.New(p.traffic.StatusCodes())
	p.svc.SetStatusOverrides(p.vstatus)
	// The accounts' receipt policies: this role answers for a vendor the
	// moment it accepts or refuses a submit, so it needs them in memory too.
	p.policies = receiptpolicy.New(p.engine().Policies)
	p.svc.SetPolicies(p.policies)
	if p.traffic != nil {
		// MO ingestion stages onto the queues' DAL.
		var stager mosvc.Stager
		if qs != nil {
			stager = qs
		}
		if stager != nil {
			// An MO for a client on another gateway is parked for it (issue 5).
			stager = handoff.FromConfig(cfg, p.log.Printf).WrapStager(stager)
			p.mo = mosvc.New(context.Background(), p.traffic.MORules(), stager, p.log)
			if p.hot != nil {
				// Unowned MO is kept, not dropped: through the record queue with the
				// DB worker (every hot-store row is queued, 28 Oct 2026), else here.
				var orphans mosvc.OrphanRecorder = p.hot
				if p.recQ != nil {
					orphans = p.recQ
				}
				p.mo.SetOrphanRecorder(orphans)
				p.mo.SetHotWriter(p.moRec()) // and owned MO is logged when it arrives, not when it lands
			}
			p.mo.SetBilling(p.bill)
		}
	}
	// Where an incoming message's parts wait: YAKU_CONCAT_MO_STORE (Saleem,
	// 28 Oct 2026), the main Redis by default — a vendor bound from every
	// gateway may send one message's parts over any of them, and the gateway
	// that gets the last part joins it. Grouped under the vendor's company,
	// so a carrier's TX and RX rows meet. The DAL opens it (the main Redis,
	// or the queue database's pool).
	p.moParts = concat.OpenFor(cfg, true, envSeconds("YAKU_CONCAT_ASSEMBLY_TIMEOUT_SEC", 60), p.log.Printf)
	if p.moParts != nil {
		concat.SetScope(p.moParts, func(v int64) int64 { return p.groups.Scope(v) })
	}
	if cs, err := dal.OpenDataKeys(cfg); err == nil {
		if c, err := crypto.FromEnv(context.Background(), cs, config.Secrets); err == nil {
			p.cry = c
			// Encrypted companies (PO 2026-09-13): their MO is encrypted
			// before staging, and their receipts keep the text: part
			// encrypted. Without crypto here both are refused/removed rather
			// than kept plain.
			if c != nil {
				if p.mo != nil {
					p.mo.SetEncryptor(c)
				}
				if p.svc != nil {
					p.svc.SetSealer(c)
				}
			}
		} else {
			p.log.Printf("crypto init: %v", err)
		}
	}
	return p
}

// Name returns the process role name.
func (p *Process) Name() string { return "smpp-vendor" }

// envSeconds reads a positive integer env var as a duration in seconds.
func envSeconds(key string, def int) time.Duration {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return time.Duration(def) * time.Second
}

// receiptsInRedis is YAKU_RECEIPTS_IN_REDIS: on unless it says 0, false, off
// or no. It only matters when Redis is set; without Redis receipts use the
// record store, as they always did.
func receiptsInRedis() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("YAKU_RECEIPTS_IN_REDIS"))) {
	case "0", "false", "off", "no":
		return false
	}
	return true
}

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

// moRec is where an incoming message's record and outcome go: the record
// store, or the record queue (see Process.recs).
func (p *Process) moRec() mosvc.HotWriter {
	if p.recQ != nil {
		return p.recQ
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
