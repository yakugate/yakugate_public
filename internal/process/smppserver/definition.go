// Package smppserver implements the "smpp-server" transport-edge process:
// SMPP v3.4 client ingress. It is a Runnable server that accepts binds, decodes
// submit_sm, and runs each message through the SAME shared pipeline as the HTTP
// Submit API (Process 1) — no business logic is duplicated here. [SCOPE P2]
//
// definition.go = types + wiring; workings.go = the Run loop and PDU handling.
package smppserver

import (
	"context"
	"errors"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/abusecount"
	"github.com/yakugate/yakugate/internal/auth"
	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/campaignwin"
	"github.com/yakugate/yakugate/internal/concat"
	"github.com/yakugate/yakugate/internal/crypto"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/dal/redis"
	"github.com/yakugate/yakugate/internal/dal/runtimequeue"
	"github.com/yakugate/yakugate/internal/dedupe"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/handoff"
	"github.com/yakugate/yakugate/internal/hlr"
	"github.com/yakugate/yakugate/internal/lockout"
	"github.com/yakugate/yakugate/internal/mo"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/internal/pipeline"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/platform/worker"
	"github.com/yakugate/yakugate/internal/queuecheck"
	"github.com/yakugate/yakugate/internal/ratelimit"
	"github.com/yakugate/yakugate/internal/recordq"
	"github.com/yakugate/yakugate/internal/refdata"
	"github.com/yakugate/yakugate/internal/rejectlog"
	"github.com/yakugate/yakugate/internal/seclog"
	"github.com/yakugate/yakugate/internal/vendorhealth"
	"github.com/yakugate/yakugate/internal/vendorquality"
)

// boundQueue is what smpp-server needs to drain client_dlr + mo_queue to its
// bound clients — satisfied by both the MySQL store and, under the Redis
// gate (2026-07-24), the Redis store. mo.Finalize takes p.queue as its Acker.
type boundQueue interface {
	ClaimClientDLR(ctx context.Context, claims []model.DLRClaim, n int) ([]model.ClientDLR, error)
	AckClientDLR(ctx context.Context, ids ...int64) error
	ClaimMO(ctx context.Context, claims []model.MOClaim, n int) ([]model.Message, error)
	AckMO(ctx context.Context, msgIDs ...string) error
	// The receipts smpp-server raises itself (a joined long message refused
	// after its parts were acked, or parts that never all arrived), and the
	// part ids saved so a vendor receipt can reach every part (PO 2026-09-11).
	EnqueueClientDLR(ctx context.Context, d model.ClientDLR) error
	SaveClientPartIDs(ctx context.Context, msgID string, partIDs []string) error
	DropClientPartIDs(ctx context.Context, msgID string) error
}

// Process is the SMPP Submit Server. It shares the pipeline with httpserver.
type Process struct {
	// rater prices a message at accept so a prepaid account charged at
	// acceptance has the exact amount reserved; reloaded with the cache.
	rater *billing.Rater
	// grant paces delivery to bound clients by each account's MO and DLR
	// throughput (2026-09-14). nil = no pacing.
	grant ratelimit.Granter
	allow ratelimit.Allowance // what the legs hold of each account's bucket this second
	// gateway is this deployment (deployment.ID()): the MO and receipt rates
	// are read for it and their buckets are named with it — the limit and
	// the key from this one field.
	gateway string
	// moHeld, dlrHeld count the checks an account's rate gave 0 (rate_held).
	moHeld, dlrHeld ratelimit.Holds
	// When each leg last said a claim failed (claimFailed).
	dlrClaimSaid, moClaimSaid atomic.Int64
	cfg                       config.Config
	addr                      string // plaintext SMPP listen addr
	tlsAddr                   string // optional TLS listen addr ("" = disabled)
	tlsCert                   string
	tlsKey                    string
	ref                       *refdata.Mem
	pipe                      *pipeline.Pipeline
	queue                     boundQueue // draining client_dlr + mo_queue to bound clients (MySQL or Redis)
	// receipts queues a refused duplicate's REJECTD receipt (dlr.RefuseDuplicate).
	receipts dlr.ClientPusher
	// handoff imports the receipts and MO parked for this gateway by
	// another one into queue (issue 5); nil = none.
	handoff *handoff.HandOff
	hot     dal.RecordStore // for finalizing MO into hot records (direction=MO)
	// queueOK says whether the queue store a submit is written to answers
	// (queue_ready.go): ReadyForTraffic follows it.
	queueOK *pipeline.QueueHealth
	// recQ is the record queue's client when the DB worker writes this
	// gateway's records (YAKU_RECORDS_VIA=worker), nil when each role writes
	// its own: rejects and incoming-message outcomes then go through it.
	recQ *recordq.Client
	// moDelivered is the MO a client's bind confirmed whose record is still
	// to be written: claimed again, it is recorded, not sent (mo.Delivered).
	// moRecorder, when a test sets it, stands in for the record writer.
	moDelivered mo.Delivered
	moRecorder  mo.HotWriter
	// tries writes every try to give a client an MO or a receipt on a bind
	// (mo_sub_, dlr_sub_; D-140), off the delivery path, in batches; an MO's
	// final try is written before it leaves the queue (mo.Finalize).
	triesMu sync.Mutex
	tries   *dal.TryLog
	// cry opens the text of MO staged encrypted (an encrypted company,
	// PO 2026-09-13) just before it goes down the bind. nil = no crypto here.
	cry *crypto.Service
	// health is the vendor-health view routing reads (routing rules review,
	// PO 2026-09-14); nil = only the operator's switch counts.
	health *vendorhealth.View
	binds  *bindRegistry // account_id -> live sessions (ALL modes since 2026-07-26; delivery picks RX/TRX)
	// Redis reference-snapshot plumbing (2026-07-24) — non-nil only when gated.
	sync        *refdata.Syncer
	redisLoader *refdata.RedisLoader

	// 2026-07-26 hardening batch:
	lock   auth.Locker      // identity bind-auth ladder (5 fails/min, rungs 1/5/30/60m)
	ipLock auth.Locker      // pre-auth source-IP throttle (20 fails/min)
	spray  auth.SprayLocker // credential-spray guard, distinct system_ids/IP (PO 2026-07-27)
	// sec is the security/alert stream (P13 Type 2), the same sink the HTTP
	// edge uses. Wired 2026-09-05: until then P2 reported nothing to it, so a
	// bind being brute-forced left no security record at all while the
	// identical attack over HTTP did — one door watched, the other not.
	sec     func(seclog.Event)
	rejects RejectWriter // P1-REJECTLOG parity; nil = disabled
	// rejectQ: those records through a bounded queue to a few writers, so a
	// storm of refusals never takes the hot store's connections (2026-10-06).
	rejectQ    *rejectlog.Queue
	rejectOnce sync.Once
	// rejectSeen is rejectQ once made, for the status to read: the status
	// runs on its own goroutine and must not read rejectQ while it is made.
	rejectSeen atomic.Pointer[rejectlog.Queue]
	// abuseCounts counts refusals for the abuse monitor (nil where no
	// database keeps the counters).
	abuseCounts *abusecount.Counter
	stats       *monitor.Stats // shared traffic counters [SCOPE P1-STATUS]
	stats2      edgeCounters   // SMPP-edge extras (receipts/MO/timeouts/authfail)
	conns       *connTable     // every live session (graceful shutdown + cap)
	// legs counts the two delivery legs (receipts and MO to bound clients):
	// a stop waits for the round each has out to be answered and acked
	// before it unbinds anyone (stopDrain).
	legs sync.WaitGroup
	// acks takes the receipts a client answered off client_dlr, and again
	// while the queue store refuses (dlr.AckKeeper, retest of 19 Oct 2026).
	// Made at first use (receiptAcks).
	acks atomic.Pointer[dlr.AckKeeper]
	// The sequence numbers of the unbinds this server sends (a bind it ends,
	// and the shutdown broadcast): one counter in its own band.
	unbindMu  sync.Mutex
	unbindSeq uint32
	connCount int64 // atomic live-connection count
	maxConns  int   // YAKU_SMPP_MAX_CONNS (0 = uncapped)
	// window is the default number of submit_sm one bind may have in flight
	// (YAKU_SMPP_WINDOW). An account's own smpp_config.window_size overrides
	// it. [SCOPE P2-WINDOW]
	window int
	// windowGrace is how long a submit waits for a free slot before it is
	// refused ESME_RTHROTTLED (YAKU_SMPP_WINDOW_GRACE_MS). Waiting stops the
	// read loop, which is plain TCP backpressure; 0 refuses immediately.
	windowGrace time.Duration
	// limit is how many submits this process works on at once, all binds
	// together (YAKU_SMPP_SERVER_MAX_IN_FLIGHT, inflight.go); nil = no limit.
	limit        *acceptLimit
	deliverBatch int // YAKU_SMPP_DELIVER_BATCH per claim
	// deliverWindow is the default number of deliver_sm — receipts and MO
	// together — one bind may have out without an answer
	// (YAKU_SMPP_DELIVER_WINDOW, 64); an account's smpp_config.deliver_window
	// overrides it (pump.go).
	deliverWindow int
	// pumpCtx is the life of the binds' pumps (Run's), set before the legs start.
	pumpCtx context.Context
	// dlrFreed and moFreed wake the legs when a bind's window has room again.
	freedOnce         sync.Once
	dlrFreed, moFreed chan struct{}
	// dlrSeq and moSeq number the receipts and the MO every pump sends.
	dlrSeq, moSeq seqGen
	// finals are the messages whose final receipt this process has handed
	// to a bind lately: a progress receipt of theirs is not sent (receipts.go).
	finals       finalsTold
	deliverEvery time.Duration // the legs' check when dlrCheck/moCheck are unset (a Process built by hand)
	// dlrCheck and moCheck are how often the receipt and MO legs check their
	// queues and how much one check takes (YAKU_QUEUE_DLR_*, YAKU_QUEUE_MO_*,
	// internal/queuecheck; issue 7).
	dlrCheck, moCheck queuecheck.Check
	// moMaxParts caps how many deliver_sm PDUs one inbound MO may split into
	// when delivered to a bound client (P6-B6, PO 2026-09-03: reuse the SAME
	// ceiling MT already enforces, YAKU_MAX_SEGMENTS, rather than a second
	// number to reason about). A body that would need more parts than this
	// is truncated at the cap instead — the pre-existing safety valve, now
	// only reached by genuinely excessive bodies instead of every one over
	// 254 raw octets.
	moMaxParts int
	storeMu    sync.Mutex // guards queue/hot self-heal rebuilds
	// assembler is the multipart assemble-first buffer (D-096), mirroring the
	// queue backend (MySQL table or Redis hash+index): parts collected across
	// binds/instances, acked on durable insert, pipeline run once on the
	// assembled whole. nil (any other queue backend) keeps the classic D-014
	// inline per-part path.
	assembler concat.Buffer
}

// RejectWriter persists a refused submit (mirrors httpserver's; the MySQL hot
// store implements it). nil disables reject logging.
type RejectWriter interface {
	WriteRejected(ctx context.Context, m model.Message) error
}

// edgeCounters are the SMPP-edge-specific numbers on the status page.
type edgeCounters struct {
	receipts     monitor.Counter // deliver_sm receipts confirmed by clients
	mo           monitor.Counter // MO messages confirmed by clients
	respTimeouts monitor.Counter // pushes that timed out awaiting deliver_sm_resp
	authFails    monitor.Counter // bind-auth failures (both ladders)
	// throttled counts submits refused ESME_RTHROTTLED because the bind
	// already had its full window in flight [SCOPE P2-WINDOW]. A rising
	// number means a client is pushing harder than its window allows —
	// either raise window_size for that account, or the client should open
	// another bind.
	throttled monitor.Counter

	// Multipart assembly (PO 2026-09-11): parts taken into the buffer, groups
	// completed and run once, groups that timed out incomplete, and parts
	// that fell back to the per-part path because the buffer was unavailable.
	concatParts, concatCompleted, concatExpired, concatFallback monitor.Counter
	// concatTook is part 1's arrival to the group completing, seconds, under
	// concatMu.
	concatMu   sync.Mutex
	concatTook *monitor.HourHist
}

// concatBoundsSec buckets assembly time. The timeout is 60 s by default, so
// anything past it is a group that only just made it.
var concatBoundsSec = []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60}

// countThrottled records a submit refused for a full ingress window.
func (p *Process) countThrottled() { p.stats2.throttled.Add(1) }

func (p *Process) countAuthFail() {
	p.stats2.authFails.Add(1)
	if p.stats != nil {
		p.stats.AuthFail.Add(1)
	}
}

// connTable tracks every live session for the connection cap and graceful
// shutdown (the bind registry only holds authenticated sessions).
type connTable struct {
	mu sync.Mutex
	m  map[*session]struct{}
}

func newConnTable() *connTable { return &connTable{m: map[*session]struct{}{}} }

func (c *connTable) add(s *session) {
	c.mu.Lock()
	c.m[s] = struct{}{}
	c.mu.Unlock()
}

func (c *connTable) remove(s *session) {
	c.mu.Lock()
	delete(c.m, s)
	c.mu.Unlock()
}

func (c *connTable) all() []*session {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*session, 0, len(c.m))
	for s := range c.m {
		out = append(out, s)
	}
	return out
}

// New wires the reference cache, queue backend, and the shared accept pipeline.
// [SCOPE P2 — reuses pipeline.NewSubmit verbatim; only the transport is new]
func New(cfg config.Config) worker.Worker {
	loader, hlrStore := buildLoader(cfg)

	// Reference data + HLR topology per the Redis gate (PO 2026-07-24) — same
	// wiring as httpserver: gated hydrates Mem from the Redis snapshot and
	// reads HLR direct from Redis; ungated keeps the original in-memory-from-
	// MySQL wiring.
	var (
		ref         *refdata.Mem
		hlrSvc      *hlr.Service
		syncer      *refdata.Syncer
		redisLoader *refdata.RedisLoader
	)
	var hlrRedis *hlr.RedisStore // dip write-back target under the gate
	if cfg.RedisEnabled() {
		rc := redis.FromConfig(cfg).Client()
		snaps := redis.NewReferenceSnapshots(rc)
		redisLoader = refdata.NewRedisLoader(snaps)
		ref = refdata.New(redisLoader)
		ds, _ := hlrStore.(hlr.DeltaSource)
		syncer = refdata.NewSyncer(loader, ds, snaps, logging.New("redis-sync").Printf)
		hlrRedis = hlr.NewRedisStore(redis.NewHLRCopies(rc))
		// A number Redis lacks is read from our HLR data in the database
		// before a paid lookup (Saleem, 6 Nov 2026).
		if hlrStore != nil {
			hlrRedis.SetDatabase(hlrStore)
		}
		hlrSvc = hlr.New(hlrRedis)
	} else {
		ref = refdata.New(loader)
		if hlrStore != nil {
			hlrSvc = hlr.New(hlrStore) // direct reads; the cache variant is P1-owned
		}
	}

	// HLR (2026-09-14): the same per-account order as http-server — our HLR
	// data, number ranges, the account's own live providers, the country code
	// — through the shared hlr stage. Live answers go to the Redis copy (under
	// the gate) and the hlr_<first4> table.
	var hlrWriters []hlr.Writer
	if hlrRedis != nil {
		hlrWriters = append(hlrWriters, hlrRedis) // keep the direct-read copy current NOW
	}
	// HLR shards live in the HLR store: YAKU_HLR_DSN, else the queue
	// database where they always were (D-124).
	if _, hd := cfg.HLRStore(); hd != "" { // MySQL or SQL Server: the DAL of the HLR store
		if w, err := dal.OpenHLRShardWriter(cfg); err == nil { // the HLR store's one pool
			hlrWriters = append(hlrWriters, hlr.NewPersister(w, logging.New("hlr-dip").Printf))
		}
	}
	var hlrCfgDB dal.HLRLookupConfig
	if dal.ConfigOnSQL(cfg) {
		if e, err := dal.OpenEngineConfig(cfg); err == nil { // the config store's one pool
			hlrCfgDB = e.HLR
		}
	}
	hlrRanges, hlrLive := hlr.StartLookups(context.Background(), hlrCfgDB, hlr.MultiWriter(hlrWriters...),
		hlr.LiveSettingsFrom(env), logging.New("hlr").Printf)

	// Encrypt-at-rest on the enqueue write: for everyone when
	// YAKU_CRYPTO_AT_REST=on, and for any company set to encrypted.
	// When encryption is required but cannot start (no usable key), the
	// pipeline gets crypto.Broken, which refuses every message 503 — the start
	// error used to be discarded, and the process then stored everything in
	// plain text without a word. [P1-CRYPTO]
	var enc pipeline.Encryptor
	var cry *crypto.Service
	if cs, err := dal.OpenDataKeys(cfg); err == nil {
		c, cerr := crypto.FromEnv(context.Background(), cs, config.Secrets)
		switch {
		case cerr != nil:
			logging.New("smpp-server").Error(logging.SystemScope, "crypto",
				"encryption is required but failed to start — every message will be refused 503 until it is fixed",
				logging.Fields{"error": cerr.Error()})
			enc = crypto.Broken{Err: cerr}
		case c != nil:
			enc, cry = c, c
		}
	}

	// Synchronous balance reservation (2026-07-23) — shared with httpserver's
	// wiring; see pipeline.Reserver's doc comment. Off by default.
	var reserver pipeline.Reserver
	var fxResolver *billing.FXResolver
	var rater *billing.Rater // prices a message at accept, for the exact reservation (PO 2026-09-17)
	// Vendor health (PO 2026-09-14): the same view http-server routes by, so
	// both ingress edges skip a vendor with no working connection alike. The
	// MOVER runs in http-server only — one place moving waiting messages is
	// enough, and every deployment runs it.
	var health *vendorhealth.View
	// Billing's stores, each on the DAL the settings pick (dal.OpenBilling).
	bs, berr := dal.OpenBilling(cfg)
	if berr != nil {
		logging.New("smpp-server").Printf("billing unavailable: %v", berr)
	}
	if tc, err := dal.OpenTrafficConfig(cfg); err == nil && berr == nil {
		// See httpserver's definition.go for why this is bounded.
		health = vendorhealth.NewView(tc.VendorHealth()) // on the config store's one pool

		// Same handle, second use (S-03, PO 2026-09-03): loss detection's SMPP
		// parity — the shared pipeline means this stage runs for P2 submits
		// exactly as it does for P1's, so P2 needs the same FX resolver, not a
		// parallel one.
		fxResolver = billing.NewFXResolver(bs.Store).NoWait() // never waited for on a submit (see httpserver)
		rater = billing.NewRater(bs.Store)
		if cfg.RedisEnabled() {
			// Prepaid balance on Redis — same instance/counters as httpserver,
			// so both ingress edges decrement ONE live number. The write-back
			// loop runs in every gated process; all copy the same live value
			// to MySQL, so the duplication is harmless. Balance resync after a
			// top-up is the HTTP edge's POST /admin/balance/refresh.
			reserver = billing.NewRedisReserver(context.Background(), bs.Live, bs.Queue, bs.Store,
				logging.New("smpp-server").Printf)
		} else if sr := billing.NewSQLReserver(bs); sr != nil {
			// No Redis (step 3): the hold and its money events in one
			// transaction of the config store.
			reserver = sr
		} else {
			reserver = billing.DBReserver{DB: bs.Store}
		}
	}

	// Per-request counters (PO 2026-07-24): cluster-accurate Redis versions
	// when gated, shared with httpserver's keys; else in-memory single-node.
	var rate pipeline.RateAllower = ratelimit.New()
	// Dedupe window is env-tunable (PO 2026-07-26); 30 minutes unless set
	// (Saleem, 29 Oct 2026; it was 24 hours). An account's own wins.
	dedupeTTL := dedupe.DefaultTTL
	if n, err := strconv.Atoi(env("YAKU_DEDUPE_TTL_SEC", "1800")); err == nil && n > 0 {
		dedupeTTL = time.Duration(n) * time.Second
	}
	var dedu pipeline.Deduper = dedupe.NewMem(dedupeTTL)
	// The shared counters and blocks, on the DAL the settings pick
	// (dal.CountersOn) — same wiring as httpserver: the main Redis when it is
	// set, all on ONE client; else the SQL database, which keeps all six
	// since step 3 of the DAL plan. What a database does not keep stays in
	// memory.
	shared, _, _ := dal.OpenCounters(cfg)
	if shared == nil {
		shared = &dal.Counters{}
	}
	// Durable dedupe (PO 2026-07-27) — same rationale as httpserver's wiring:
	// without Redis, share dedupe_keys across instances/restarts when the
	// Config store is MySQL; MSSQL/Mongo still fall back to Mem.
	if shared.Dedupe != nil {
		ded := dedupe.NewShared(shared.Dedupe, dedupeTTL)
		go ded.StartSweep(context.Background(), time.Hour, logging.New("smpp-server").Printf)
		dedu = ded
	}
	// Bind-auth ladders (2026-07-26 — the "wired then discarded" gap closed):
	// identity 5 fails/min + source-IP 20 fails/min, same rungs as P1.
	var lock auth.Locker = lockout.New(5)
	var ipLock auth.Locker = lockout.New(20)
	// Credential-spray guard (PO 2026-07-27): same distinct-identity detector
	// as P1, keyed by bind system_id instead of username/token.
	sprayThreshold, err := strconv.Atoi(env("YAKU_SPRAY_DISTINCT_THRESHOLD", "3"))
	if err != nil || sprayThreshold < 1 {
		sprayThreshold = 3
	}
	var spray auth.SprayLocker = lockout.NewSprayGuard(sprayThreshold)
	// Quota-usage counter (chunk-6 ruling 6.1): in-memory single-node by
	// default, cluster-accurate + restart-surviving under the Redis gate.
	var usage pipeline.UsageCounter = ratelimit.NewUsage()
	// Per-destination frequency cap counter (config/0096). Its own instance,
	// not a share of `usage`: these keys carry the destination, so there are
	// vastly more of them and they must expire soon after their period. Two
	// days outlives a daily key with room for clock skew.
	var destCap pipeline.UsageCounter = ratelimit.NewUsageIdle(ratelimit.DestCapKeyLife)
	var campaigns pipeline.CampaignNoter // nil where no database keeps the window: the signature is still recorded
	// The abuse monitor's counters (abusecount, 2026-10-13). nil where no
	// database keeps them: nothing is counted and the monitor reads the
	// records.
	var abuseNoter pipeline.AbuseNoter
	var abuseCounts *abusecount.Counter
	if shared.Limits != nil {
		// The shared buckets, asked a chunk at a time (29 Oct 2026): a
		// message costs no call to the store (the main Redis, else the SQL
		// database's limit_buckets) unless this process's chunk ran out.
		rate = ratelimit.NewChunked(ratelimit.NewShared(shared.Limits))
	}
	if shared.Blocks != nil {
		lock = lockout.NewShared(shared.Blocks, 5)
		ipLock = lockout.NewShared(shared.Blocks, 20)
		spray = lockout.NewSharedSprayGuard(shared.Blocks, sprayThreshold)
	}
	if shared.Usage != nil {
		usage = ratelimit.NewSharedUsage(shared.Usage)
		destCap = ratelimit.NewSharedUsageTTL(shared.Usage, ratelimit.DestCapKeyLife)
	}
	if shared.Campaigns != nil {
		// The live campaign window (P10 register, ruled 2026-09-09): counts
		// buffered in memory, flushed to the store once a second.
		campaigns = campaignwin.New(context.Background(), shared.Campaigns, logging.New("campaign-window").Printf)
	}
	if shared.Abuse != nil {
		abuseCounts = abusecount.New(context.Background(), shared.Abuse, logging.New("abuse-counters").Printf)
		abuseNoter = abuseCounts
	}

	// Gateway-wide TPS ceiling (chunk-1 ruling 1.5, PO 2026-07-31): one
	// segments/sec cap over ALL tenants; 0/unset = disabled. Cluster-wide
	// under the Redis gate (shared bucket), per-instance otherwise.
	globalTPS, _ := strconv.Atoi(env("YAKU_GLOBAL_TPS", "0"))

	// Multipart assemble-first buffer (D-096). The buffer MIRRORS the queue
	// backend (PO 2026-08-09) so parts arriving on different binds/instances
	// meet in the same store already serving the hot-path queue — MySQL uses
	// the concat_assembly table, Redis a hash-per-group + index ZSET. Any other
	// backend keeps the classic inline per-part path, logged so the difference
	// is visible. Timeout is choice A on both: incomplete groups are recorded
	// expired and dropped, never flushed part-by-part.
	assemblyTimeoutSec, err := strconv.Atoi(env("YAKU_CONCAT_ASSEMBLY_TIMEOUT_SEC", "60"))
	if err != nil || assemblyTimeoutSec < 1 {
		assemblyTimeoutSec = 60
	}
	assemblyTimeout := time.Duration(assemblyTimeoutSec) * time.Second
	alog := logging.New("smpp-server")

	// Where a client's long-message parts wait: YAKU_CONCAT_MT_STORE (Saleem,
	// 28 Oct 2026) — mysql, redis (the MAIN Redis) or memory (this process's
	// own, shared by all its sessions); the main Redis by default when one is
	// set, else MySQL. No buffer = each part goes through the pipeline alone.
	// The DAL opens it: the main Redis, or the queue database's pool.
	assembler := concat.OpenFor(cfg, false, assemblyTimeout, alog.Printf)
	if assembler == nil {
		alog.Warn(logging.SystemScope, "concat_buffer", "no assembly buffer — concat parts use the inline per-part path",
			logging.Fields{"queueBackend": cfg.QueueBackend})
	}

	// One sink, built before the pipeline so BOTH the bind path (via p.sec)
	// and the accept pipeline report into the same Type-2 stream. Until the
	// trap-number alarm (config/0097) the pipeline had no security refusal of
	// its own on this edge, so SecLog was simply never passed here — a trap
	// hit over SMPP would have refused in silence.
	sec := seclog.Sink("smpp-server")

	if qs := vendorquality.Open(cfg); qs != nil {
		ref.SetQualitySource(qs, time.Duration(cfg.QualityWindowMin)*time.Minute)
	}
	var vh pipeline.VendorHealth
	if health != nil {
		vh = health
	}
	queue := buildQueue(cfg)
	// Least busy reads depths from this same queue store, wherever the queues
	// are (MySQL or Redis) — not from the HLR database (2026-10-06).
	if src := refdata.DepthsFromQueue(queue); src != nil && ref != nil {
		ref.SetQueueDepthSource(src)
	}
	// An accepted message's record (mt_) is written at acceptance (the
	// three-table design, 27 Oct 2026): through the DB worker's queue — with
	// the vendor queue in one Redis step — else the store itself.
	var accepted pipeline.AcceptRecorder
	var acceptQ *recordq.Client
	if h, err := dal.OpenRecordStore(cfg); err == nil {
		if _, rq := recordq.ForRole(context.Background(), cfg, h, logging.New("smpp-server").Printf); rq != nil {
			accepted, acceptQ = rq, rq
		} else if !cfg.RedisEnabled() {
			// No Redis: the records of the submits in flight are committed
			// together (dal.AcceptGroup), not one transaction a message.
			accepted = dal.NewAcceptGroup(h)
		} else {
			accepted = h
		}
	}
	pipe := pipeline.NewSubmit(pipeline.SubmitDeps{
		Accepted:         accepted,
		VendorHealth:     vh,
		QualityMinSample: int64(cfg.QualityMinSample), // route_type='quality' evidence threshold
		Ref:              ref,
		HLR:              hlrSvc,
		HLRLive:          hlrLive,   // per-account providers (2026-09-14)
		HLRRanges:        hlrRanges, // number ranges and country codes
		Rate:             rate,
		Dedupe:           dedu,
		Queue:            queue,
		Crypto:           enc,
		Reserve:          reserver,
		Pricer:           pipeline.RaterPricer{R: rater, FX: fxResolver},
		DedupeDefaultTTL: dedupeTTL,
		GlobalTPS:        globalTPS,
		Usage:            usage,
		DestCapUsage:     destCap,
		Campaigns:        campaigns,
		AbuseCounts:      abuseNoter,
		FX:               fxResolver,
		SecLog:           sec,
	})

	deliverBatch, _ := strconv.Atoi(env("YAKU_SMPP_DELIVER_BATCH", "200"))
	if deliverBatch < 1 {
		deliverBatch = 200
	}
	// A bind's delivery window (Saleem, 28 Oct 2026): any whole number above
	// 0; anything else is the default, 64.
	deliverWindow, _ := strconv.Atoi(env("YAKU_SMPP_DELIVER_WINDOW", "64"))
	if deliverWindow < 1 {
		deliverWindow = 64
	}
	maxConns, _ := strconv.Atoi(env("YAKU_SMPP_MAX_CONNS", "2000"))
	// Ingress window default (PO 2026-09-04). 16 is chosen to lift a single
	// bind well clear of its old 1/latency ceiling (~320 msg/s measured)
	// without letting one client hold an unbounded number of pipeline runs.
	window, _ := strconv.Atoi(env("YAKU_SMPP_WINDOW", "16"))
	graceMS, _ := strconv.Atoi(env("YAKU_SMPP_WINDOW_GRACE_MS", "200"))
	// Submits worked on at once, sized from the Redis pool the accept path
	// waits on (load test of 30 Oct 2026; inflight.go).
	maxInFlight, lowered := inFlightLimit(env("YAKU_SMPP_SERVER_MAX_IN_FLIGHT", ""),
		redis.PoolSize(cfg), redis.PoolTimeout(cfg))
	if lowered {
		alog.Warn(logging.SystemScope, "in_flight_limit",
			"YAKU_SMPP_SERVER_MAX_IN_FLIGHT is more than the Redis pool can serve in its timeout; lowered (raise YAKU_REDIS_POOL_SIZE to raise it)",
			logging.Fields{"limit": maxInFlight, "pool": redis.PoolSize(cfg)})
	}
	moMaxParts, _ := strconv.Atoi(env("YAKU_MAX_SEGMENTS", "10"))
	if moMaxParts < 1 {
		moMaxParts = 10
	}
	// The same limiter hands out the per-account MO and receipt throughput
	// (2026-09-14); both kinds implement ratelimit.Granter.
	grant, _ := rate.(ratelimit.Granter)
	// With Redis the legs ask on a client of their own (28 Oct 2026): on
	// the accept path's client a receipt claim waited behind thousands of
	// submits for a connection (ratelimit.Allowance). dal.OpenLimits opens
	// the buckets on a connection of their own.
	if l, err := dal.OpenLimits(cfg); err == nil {
		grant = ratelimit.NewShared(l)
	}
	p := &Process{
		rater:         rater,
		grant:         grant,
		gateway:       deployment.ID(),
		cfg:           cfg,
		ref:           ref,
		pipe:          pipe,
		assembler:     assembler,
		binds:         newBindRegistry(),
		abuseCounts:   abuseCounts,
		addr:          env("YAKU_SMPP_ADDR", ":2775"),
		tlsAddr:       env("YAKU_SMPP_TLS_ADDR", ""),
		tlsCert:       env("YAKU_SMPP_TLS_CERT", ""),
		tlsKey:        env("YAKU_SMPP_TLS_KEY", ""),
		sync:          syncer,
		redisLoader:   redisLoader,
		lock:          lock,
		ipLock:        ipLock,
		spray:         spray,
		sec:           sec,
		stats:         monitor.New(),
		conns:         newConnTable(),
		maxConns:      maxConns,
		window:        window,
		windowGrace:   time.Duration(graceMS) * time.Millisecond,
		limit:         limitOf(maxInFlight, env("YAKU_SMPP_SERVER_OVER_LIMIT", "hold")),
		deliverBatch:  deliverBatch,
		deliverWindow: deliverWindow,
		deliverEvery:  200 * time.Millisecond,
		dlrCheck:      queuecheck.For(queuecheck.DLR), // reads the retired YAKU_SMPP_DELIVER_MS while unset
		moCheck:       queuecheck.For(queuecheck.MO),
		moMaxParts:    moMaxParts,
		health:        health,
		queueOK:       queueHealthOf(queue),
	}
	// P1-REJECTLOG parity (2026-07-26): refused SMPP submits persist to the
	// same records_* partitions as HTTP rejects. MySQL hot store only, like P1.
	if h, err := dal.OpenRecordStore(cfg); err == nil {
		// One record-queue client for the role: the one acceptance writes
		// through (made above), else a new one.
		p.recQ = acceptQ
		if p.recQ == nil {
			_, p.recQ = recordq.ForRole(context.Background(), cfg, h, logging.New("smpp-server").Printf)
		}
		if env("YAKU_LOG_REJECTS", "on") != "off" {
			p.rejects = h
			if p.recQ != nil {
				// A refusal whose task cannot reach Redis is written to the
				// store itself (Saleem 2026-10-14, recordq.RejectsOrDirect).
				p.rejects = recordq.RejectsOrDirect{Queue: p.recQ, Direct: h}
			}
		}
	}
	// client_dlr + mo_queue drain from the DAL the settings pick
	// (dal.QueuesOn; Redis with YAKU_QUEUE_BACKEND=redis, D-122 — the same
	// instance the vendor MT queue uses).
	if q, on, err := dal.OpenQueues(cfg); err == nil {
		p.queue = q
	} else {
		logging.New("smpp-server").Printf("queues unavailable on %s: %v", on, err)
	}
	if p.queue != nil {
		// A refused duplicate's REJECTD receipt (Saleem, 28 Oct 2026).
		p.receipts = dlr.NewQueuePusher(p.queue)
	}
	// Receipts and MO that came in at another gateway for this one's
	// clients are moved into its own queues here too (issue 5), so a
	// deployment with no dlr or mo role still takes them in.
	p.handoff = handoff.FromConfig(cfg, logging.New("smpp-server").Printf)
	if h, err := dal.OpenRecordStore(cfg); err == nil {
		p.hot = h
	}
	p.cry = cry
	return p
}

// Name returns the process role name.
func (p *Process) Name() string { return "smpp-server" }

// Step satisfies worker.Worker; unused because Process is a Runnable (worker.Run
// dispatches to Run). [SCOPE P2-00]
func (p *Process) Step(context.Context) error { return nil }

// --- wiring helpers (mirror httpserver's; transport glue, not business logic) ---

// buildLoader returns the reference-cache loader AND the HLR store.
//
// The two are chosen INDEPENDENTLY, because they live in different stores: the
// reference cache reads the CONFIG store (accounts, routes, templates,
// anti-abuse config) while the sharded hlr_<first4> tables live in the QUEUE
// store. *refdata.MySQL happens to serve both when everything is MySQL, which
// is why they used to be selected together — but that coupling is what made
// "HLR on SQL Server" look like it needed a whole MSSQL reference loader.
// It does not: config on MySQL with queue and HLR on SQL Server is a perfectly
// ordinary deployment, and this is what makes it work.
func buildLoader(cfg config.Config) (refdata.Loader, hlr.Store) {
	log := logging.New("smpp-server")
	var loader refdata.Loader
	var hlrStore hlr.Store

	if dal.ConfigOnSQL(cfg) {
		if l, err := refdata.For(cfg); err == nil {
			loader = l
			// HLR from the HLR store when it is MySQL (YAKU_HLR_DSN, else the
			// queue database — D-124).
			if cfg.HLRStoreMySQL() {
				hlrStore = l
			}
		} else {
			log.Error(logging.SystemScope, "refdata_loader", "reference loader ("+string(dal.SQL(cfg))+") init failed", logging.Fields{"error": err.Error()})
		}
	}

	// HLR on SQL Server, whatever the config store is (register rows 1.6.1 and
	// 1.6.2, migration queue/mssql/0004). Shards are memory-optimized when the
	// migration has been applied; this reader works against either shape.
	if hb, hd := cfg.HLRStore(); hlrStore == nil && hb == config.BackendMSSQL && hd != "" {
		if s, err := dal.OpenHLRShards(cfg); err == nil { // the same store HLRStore names
			hlrStore = hlr.FromShards(s)
		} else {
			log.Error(logging.SystemScope, "hlr_store", "HLR store (mssql) init failed — every lookup will miss",
				logging.Fields{"error": err.Error()})
		}
	}

	if loader == nil {
		// Worth saying loudly. With no reference loader the account cache is
		// empty, so EVERY request fails to authenticate — the gateway comes up
		// and then refuses all traffic, which reads like a credentials problem
		// rather than a configuration one.
		log.Error(logging.SystemScope, "refdata_loader",
			"no reference loader for this config backend — the account cache stays EMPTY and every request will be refused 401",
			logging.Fields{"configBackend": cfg.ConfigBackend})
	}
	if hlrStore == nil {
		log.Warn(logging.SystemScope, "hlr_store", "no HLR store (YAKU_HLR_DSN, else the queue database); operator lookups will always miss",
			logging.Fields{"queueBackend": cfg.QueueBackend})
	}
	return loader, hlrStore
}

func buildQueue(cfg config.Config) pipeline.QueueWriter {
	log := logging.New("smpp-server")
	// Items left on the other side of YAKU_QUEUE_BACKEND move over (D-122).
	runtimequeue.HandOver(cfg, log.Printf)
	// Same policy as httpserver's buildQueue. The DAL the settings pick (dal.QueuesOn): with YAKU_QUEUE_BACKEND=redis
	// (D-122) the vendor queue is on Redis, NO SQL fallback — the claim side
	// (P3/P4) is picked the same way, and a silent fallback would enqueue
	// where nothing is reading.
	q, on, err := dal.OpenQueues(cfg)
	if err != nil {
		log.Error(logging.SystemScope, "queue_backend", "queue backend unavailable; submits will be refused ESME_RMSGQFUL",
			logging.Fields{"queueBackend": on, "error": err.Error()})
		return unavailableQueue{}
	}
	if rs, ok := q.(*redis.Store); ok {
		if err := rs.Ping(context.Background()); err != nil {
			log.Error(logging.SystemScope, "queue_backend", "queue Redis unreachable — enqueues will fail until it returns",
				logging.Fields{"addr": cfg.ForQueueRedis().RedisWhere(), "error": err.Error()})
		} else {
			log.Info(logging.SystemScope, "queue_backend", "vendor queue on redis", logging.Fields{"addr": cfg.ForQueueRedis().RedisWhere(),
				"own_redis": cfg.QueueRedisSet()})
		}
	}
	return q
}

type unavailableQueue struct{}

func (unavailableQueue) Enqueue(ctx context.Context, m model.Message) error {
	return errors.New("queue backend unavailable")
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// envSeconds reads a positive integer env var as a duration in seconds
// (mirrors httpserver's helper; the two packages don't share a utils file).
func envSeconds(key string, def int) time.Duration {
	if n, err := strconv.Atoi(env(key, strconv.Itoa(def))); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	return time.Duration(def) * time.Second
}

// security reports one security event to the P13 Type-2 stream. Safe when the
// sink was never wired (tests build a bare Process), so no call site needs a
// nil check. [SCOPE P13-SECLOG]
func (p *Process) security(e seclog.Event) {
	if p.sec == nil {
		return
	}
	p.sec(e)
}

// accountFor resolves a system_id to its account username for a security
// event, so a refusal that never got as far as authenticating can still be
// filed under the tenant it was aimed at. Empty when the system_id is unknown
// — such an event has no tenant and belongs to the system stream alone.
func (p *Process) accountFor(systemID string) string {
	if p.ref == nil {
		return ""
	}
	if a, ok := p.ref.AccountBySMPPSystemID(systemID); ok {
		return a.Username
	}
	return ""
}

// modeName renders a bind mode for a log record — "tx"/"rx"/"trx" reads
// better in a customer-facing file than the enum's integer.
func modeName(m bindMode) string {
	switch m {
	case modeTX:
		return "tx"
	case modeRX:
		return "rx"
	case modeTRX:
		return "trx"
	}
	return "unbound"
}

// acctScope names the account a record belongs to, so tenant lines land in
// that customer's own daily file as well as the system one (P13 log scoping).
// Falls back to the system scope when the account cannot be resolved — a
// record with nowhere to file it still belongs in the operator's stream.
func (p *Process) acctScope(accountID int64) logging.Scope {
	if p.ref != nil {
		if a, ok := p.ref.AccountByID(accountID); ok {
			return logging.AccountScope(a.Username)
		}
	}
	return logging.SystemScope
}

// logBind/logBindWarn write ONE bind-lifecycle event to BOTH streams: the
// account's own file, so a customer can see their own connections, and the
// system file, so an operator can answer "when did this client last connect"
// without opening every tenant's log.
//
// The double write is deliberate and follows seclog's precedent (P13 Type 2):
// an account-scoped record goes to that account's file ONLY, which is right
// for per-message traffic — it stops one busy tenant drowning the console —
// but wrong for the handful of events an operator depends on. Bind events are
// per CONNECTION, not per message, so the second copy costs almost nothing.
// acctName is the bound account's username, or "" on a session that has not
// authenticated yet. A capture is always scoped to a named account, so an
// unbound session simply records nothing rather than guessing.
func (s *session) acctName() string {
	if s == nil || s.acct == nil {
		return ""
	}
	return s.acct.Username
}

func (p *Process) logBind(log *logging.Logger, typ, msg, username string, f logging.Fields) {
	log.Info(logging.AccountScope(username), typ, msg, f)
	log.Info(logging.SystemScope, typ, msg, withAccount(f, username))
}

func (p *Process) logBindWarn(log *logging.Logger, typ, msg, username string, f logging.Fields) {
	log.Warn(logging.AccountScope(username), typ, msg, f)
	log.Warn(logging.SystemScope, typ, msg, withAccount(f, username))
}

// withAccount copies f and names the account in it. On a SYSTEM record
// "account" is a free field (it is only reserved on account-scoped records,
// where the scope emits it already), so this is how the system copy says who
// the event was about.
func withAccount(f logging.Fields, username string) logging.Fields {
	out := make(logging.Fields, len(f)+1)
	for k, v := range f {
		out[k] = v
	}
	out["account"] = username
	return out
}

// moRec is where an incoming message's outcome goes: the record store, or
// the record queue (see Process.recQ).
func (p *Process) moRec() mo.HotWriter {
	if p.moRecorder != nil {
		return p.moRecorder
	}
	if p.recQ != nil {
		return p.recQ
	}
	return p.hot
}

// legCheck is a delivery leg's check: its setting, or, in a Process a test
// builds by hand, every deliverEvery taking 50 rounds of deliverBatch (as
// before issue 7).
func (p *Process) legCheck(c queuecheck.Check) queuecheck.Check {
	if c.Every <= 0 {
		c.Every = p.deliverEvery
		if c.Every <= 0 {
			c.Every = 200 * time.Millisecond
		}
	}
	if c.PerInterval <= 0 {
		c.PerInterval = 50 * max(p.deliverBatch, 1)
	}
	return c
}

// tryLog is the role's log of tries (D-140), made at first use on the
// record writer it has then (the record queue, else the record store); nil
// while it has none.
func (p *Process) tryLog() *dal.TryLog {
	p.triesMu.Lock()
	defer p.triesMu.Unlock()
	if p.tries == nil {
		if w, ok := p.moRec().(dal.ClientTries); ok && w != nil {
			p.tries = dal.NewTryLog(w, nil, logging.New("smpp-server").Printf)
		}
	}
	return p.tries
}

// companyOf is an account's company (0 when the cache does not know it).
func (p *Process) companyOf(acct int64) int64 {
	if p.ref != nil {
		if a, ok := p.ref.AccountByID(acct); ok {
			return a.CompanyID
		}
	}
	return 0
}

// bindTry is what one delivery down a bind was, as a try (D-140).
func bindTry(s *session, a answer) dal.ClientTry {
	t := dal.ClientTry{Channel: dal.TrySMPP, CommandStatus: a.status, At: a.sent, TookMS: a.took.Milliseconds()}
	switch {
	case a.ok:
		t.Result = dal.TryAccepted
	case a.status == nil:
		t.Result, t.Error = dal.TryNoAnswer, "no deliver_sm_resp in time"
	default:
		t.Result = dal.TryRefused
	}
	if s != nil {
		t.Session = "bind " + strconv.Itoa(s.slot)
	}
	return t
}
