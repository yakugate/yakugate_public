// Package mo implements the MO Receiver role: it ingests mobile-originated
// messages from HTTP vendor callbacks and delivers staged MO to clients that
// aren't SMPP-bound by POSTing their accounts.mo_url. (SMPP-vendor MO ingestion
// lives in smppvendor; SMPP-bound client delivery lives in smppserver.) [SCOPE P6]
//
// Definition: types + construction. The Run loop lives in workings.go.
package mo

import (
	"context"
	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/recordq"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/auth"
	"github.com/yakugate/yakugate/internal/crypto"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/dal/runtimequeue"
	"github.com/yakugate/yakugate/internal/handoff"
	"github.com/yakugate/yakugate/internal/inboundauth"
	"github.com/yakugate/yakugate/internal/lockout"
	mosvc "github.com/yakugate/yakugate/internal/mo"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/platform/worker"
	"github.com/yakugate/yakugate/internal/pushcall"
	"github.com/yakugate/yakugate/internal/pushretry"
	"github.com/yakugate/yakugate/internal/ratelimit"
	"github.com/yakugate/yakugate/internal/seclog"
	"github.com/yakugate/yakugate/internal/webhookalert"
	"github.com/yakugate/yakugate/internal/webhookauth"
)

// moQueue is what the MO role needs: stage MO (via mosvc), stale-claim, and
// ack (via mosvc.Finalize) — MySQL or, under the Redis gate, Redis.
type moQueue interface {
	StageMO(ctx context.Context, m model.Message) error
	ClaimMOStale(ctx context.Context, claims []model.MOClaim, minAgeSec, n int) ([]model.Message, error)
	AckMO(ctx context.Context, msgIDs ...string) error
	// Retry policy (P6-B1): back a failed delivery off, abandon it once the
	// policy is exhausted, and sweep MO nobody is even trying (which is also
	// where valid_until finally gets enforced).
	RescheduleMO(ctx context.Context, msgID string, delay time.Duration) error
	GiveUpMO(ctx context.Context, msgID string) error
	SweepGiveUpMO(ctx context.Context, olderThan time.Duration) (int64, error)
}

// Process is the MO receiver (Runnable): webhook intake + mo_url delivery drain.
type Process struct {
	// drain counts what a stop waits for (Stop, stop.go): the door's
	// shutdown and the mo_url push loop.
	drain sync.WaitGroup
	// moTargets and moRecorder, when a test sets them, stand in for
	// loadMOUrls and for the record writer.
	moTargets  func(ctx context.Context) map[int64]moTarget
	moRecorder mosvc.HotWriter
	// grant paces pushes by each account's MO throughput (2026-09-14): the
	// cluster-wide Redis bucket under the gate, shared with smpp-server's
	// binds; in-memory otherwise (this process only).
	grant ratelimit.Granter
	// rateHeld counts the checks an account's rate on this gateway gave 0.
	rateHeld ratelimit.Holds
	cfg      config.Config
	log      *logging.Logger
	addr     string
	hot      dal.RecordStore
	// recs is where this role's records go: the record store itself, or —
	// with YAKU_RECORDS_VIA=worker — the queue the DB worker writes out
	// (recordq). recQ is that queue's client, nil when the role writes its own.
	recs  recordq.Recorder
	recQ  *recordq.Client
	queue moQueue
	// handoff parks an MO for a client on another gateway and imports the
	// MO parked for this one into queue (issue 5); strays is the queue
	// Redis, walked once for what an older build queued there for another
	// gateway. nil = none.
	handoff *handoff.HandOff
	strays  handoff.StrayMO
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
	svc       *mosvc.Service
	// Inbound security (P5-B01's twin, 2026-08-31). A forged MO is worse than a
	// forged receipt: it is billed, it lands in the client's logs as genuine,
	// and it can trigger their auto-reply into sending real traffic.
	// doors and guard are inbound security (config/0121): one endpoint row
	// per vendor per kind, judged with the account edge's own machinery. The
	// pre-0121 map of the vendors' old shared settings is gone (18 Oct 2026):
	// it fed only a warning, and an untrue one.
	doors  *inboundauth.Cache
	guard  *inboundauth.Guard
	ipres  *auth.IPResolver
	warner webhookauth.UnconfiguredWarner
	// dialectWarner remembers which MO doors' dialect problems were said.
	dialectWarner webhookauth.UnconfiguredWarner
	// pathWarner remembers which MO doors' path notes were said (27 Oct).
	pathWarner webhookauth.UnconfiguredWarner
	// Global client-push retry policy; each mo endpoint may override it from
	// its own http_endpoints.retry column.
	retry pushretry.Policy
	// webhooks builds each customer MO webhook row into its push (headers,
	// auth, TLS, a request of its own), kept until the row changes
	// (pushcall, 2026-09-14).
	webhooks *pushcall.Cache

	// Monitoring (2026-09-11). Named hooks/push because `inbound` was
	// once the auth config. Until now this role published nothing about
	// the MO it took in or delivered.
	hooks monitor.Inbound  // vendor MO webhooks, per vendor and outcome
	push  monitor.Delivery // MO pushed to customers' mo_url
	// byAcct is the same push leg seen per CUSTOMER (2026-09-16): one
	// customer's endpoint refusing everything is invisible in the totals
	// above, which is exactly the case an operator is asked about.
	byAcct monitor.PushAccounts

	// cry encrypts MO of encrypted companies and opens it for the push. nil
	// = no crypto here.
	cry *crypto.Service
	// delivered is the MO pushed to its customer whose record is still to be
	// written: claimed again, it is recorded, not pushed (delivered.go).
	delivered deliveredSet
	// numbers is our HLR data for the subscriber's number and vendors each
	// vendor's cid and name: what the webhook says beside the message
	// (pushfacts.go, 4 Nov 2026). nil = not known.
	numbers mosvc.NumberLook
	vendors *vendorNames
	// tries writes every try to give a customer its MO (mo_sub_, D-140),
	// the webhook call on it as callMode says (YAKU_WEBHOOK_CALL_LOG); nil
	// keeps nothing. An MO's final try is written before it leaves the queue.
	tries    *dal.TryLog
	callMode string
}

// New wires the queue (staging), hot (records), and config (routes/mo_url) stores.
func New(cfg config.Config) worker.Worker {
	p := &Process{cfg: cfg, log: logging.New("mo"), addr: env("YAKU_MO_ADDR", ":8091"),
		retry: pushretry.Defaults()}
	if res, invalid := auth.NewIPResolver(env("YAKU_TRUSTED_PROXIES", "")); res != nil {
		p.ipres = res
		if len(invalid) > 0 {
			p.log.Printf("YAKU_TRUSTED_PROXIES: ignoring invalid entries %v", invalid)
		}
	}
	// mo_queue is on the DAL the settings pick (dal.QueuesOn): Redis only
	// with YAKU_QUEUE_BACKEND=redis (D-122), else the SQL database.
	if q, on, err := dal.OpenQueues(cfg); err == nil {
		p.queue = q
		if s, ok := q.(handoff.StrayMO); ok {
			p.strays = s // what another gateway parked for this one (Redis)
		}
	} else {
		p.log.Printf("MO queue unavailable on %s: %v", on, err)
	}
	// Items left on the other side of YAKU_QUEUE_BACKEND move over (D-122).
	runtimequeue.HandOver(cfg, p.log.Printf)
	// The inbound throughput counter uses Redis whenever it is set.
	p.grant = ratelimit.New()
	if l, err := dal.OpenLimits(cfg); err == nil {
		// A counter: the main Redis (dal.CountersOn).
		p.grant = ratelimit.NewShared(l)
	}
	p.handoff = handoff.FromConfig(cfg, p.log.Printf)
	if h, err := dal.OpenRecordStore(cfg); err == nil {
		p.hot = h
		p.recs, p.recQ = recordq.ForRole(context.Background(), cfg, h, p.log.Printf)
	}
	// The config store's parts the shared engine packages read
	// (dal.EngineConfig, on the same pool as traffic; empty when it cannot
	// be opened, as traffic is then nil).
	p.eng = dal.EngineConfigOrNone(cfg)
	if t, err := dal.OpenTrafficConfig(cfg); err == nil {
		p.traffic = t
		p.webhooks = pushcall.NewCache(config.Secrets)
		if n, ok := t.Vendors.(dal.VendorNamer); ok {
			p.vendors = &vendorNames{read: n.VendorNames}
		}
	}
	p.numbers = numberLookFor(cfg, p.log.Printf)
	// Inbound security (config/0121, PO 2026-09-23). This door matters most:
	// a forged MO is billed, reaches the customer's log as genuine and can
	// trigger their auto-reply into sending real traffic.
	p.doors = inboundauth.NewCache(p.engine().Doors)
	sec := seclog.Sink("mo")
	p.guard = inboundauth.New(
		inboundauth.WithPepper(env("YAKU_TOKEN_PEPPER", "yaku-dev-token-pepper")),
		inboundauth.WithLadder(lockout.New(envIntDefault("YAKU_INBOUND_FAIL_THRESHOLD", 5))),
		inboundauth.WithIPThrottle(lockout.New(envIntDefault("YAKU_INBOUND_IP_THRESHOLD", 20))),
		inboundauth.WithSprayGuard(lockout.NewSprayGuard(envIntDefault("YAKU_SPRAY_DISTINCT_THRESHOLD", 3))),
		inboundauth.WithThroughput(p.grant),
		inboundauth.WithSecrets(config.Secrets),
		inboundauth.WithReporter(sec),
		inboundauth.WithIPResolver(func(r *http.Request) string { return p.ipres.FromRequest(r) }),
	)
	if p.traffic != nil && p.queue != nil {
		// An MO for a client on another gateway is parked for it (issue 5).
		p.svc = mosvc.New(context.Background(), p.traffic.MORules(), p.handoff.WrapStager(p.queue), p.log)
		if p.hot != nil {
			// Unowned MO is kept, not dropped: through the record queue with the
			// DB worker (every hot-store row is queued, 28 Oct 2026), else here.
			var orphans mosvc.OrphanRecorder = p.hot
			if p.recQ != nil {
				orphans = p.recQ
			}
			p.svc.SetOrphanRecorder(orphans)
			p.svc.SetHotWriter(p.moRec()) // and owned MO is logged when it arrives, not when it lands
		}
		// An incoming message is priced and booked when it is taken in
		// (issues 3 and 18; the billing role is retired).
		// With Redis on its live balances; with none in the config store
		// (step 3).
		{
			if bs, err := dal.OpenBilling(cfg); err == nil {
				p.svc.SetBilling(billing.NewMoments(context.Background(), bs, p.log.Printf))
			} else {
				p.log.Printf("billing unavailable: %v", err)
			}
		}
	}
	// Encryption for companies set to encrypted (PO 2026-09-13): their MO is
	// encrypted before it is staged and opened again for the push. Without it
	// such MO is refused (503 to the vendor), never kept in plain text.
	if cs, err := dal.OpenDataKeys(cfg); err == nil {
		c, err := crypto.FromEnv(context.Background(), cs, config.Secrets)
		switch {
		case err != nil:
			p.log.Printf("crypto init: %v — MO for encrypted companies will be refused", err)
		case c != nil:
			p.cry = c
			if p.svc != nil {
				p.svc.SetEncryptor(c)
			}
		}
	}
	p.callMode = pushcall.CallLogMode()
	p.tries = triesFor(p.recQ, p.hot, p.cry, p.log.Printf)
	return p
}

// triesFor is the log of tries to give customers their MO (mo_sub_, D-140):
// through the record queue in worker mode, else the record store; a private
// try's call sealed with the role's encryption, when it has one.
func triesFor(recQ *recordq.Client, hot dal.RecordStore, cry *crypto.Service, logf func(string, ...any)) *dal.TryLog {
	var w dal.ClientTries
	if recQ != nil {
		w = recQ
	} else if hot != nil {
		w = hot
	}
	var seal pushcall.Sealer
	if cry != nil {
		seal = cry
	}
	return pushcall.NewTryLog(w, seal, logf)
}

// sealer is the role's encryption as a pushcall.Sealer; nil when it has none.
func (p *Process) sealer() pushcall.Sealer {
	if p.cry == nil {
		return nil
	}
	return p.cry
}

// Name returns the process role name.
func (p *Process) Name() string { return "mo" }

// envIntDefault reads a positive integer setting, else the default — the
// inbound ladders' thresholds, spelled the same way the dlr role spells them.
func envIntDefault(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// moRec is where an incoming message's record and outcome go: the record
// store, or the record queue (see Process.recs).
func (p *Process) moRec() mosvc.HotWriter {
	if p.moRecorder != nil {
		return p.moRecorder
	}
	if p.recQ != nil {
		return p.recQ
	}
	return p.hot
}
