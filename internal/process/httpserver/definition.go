// Package httpserver implements Process 1 — the HTTP ingress / Submit API.
// It is a server-style process (worker.Runnable): an HTTP server that runs each
// submitted message through the canonical accept pipeline (internal/pipeline)
// and enqueues it. The process is thin: transport + wiring only; all business
// logic lives in the shared service packages.
package httpserver

import (
	"context"
	"errors"
	"github.com/yakugate/yakugate/internal/abusecount"
	"github.com/yakugate/yakugate/internal/recordq"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/audit"
	"github.com/yakugate/yakugate/internal/auth"
	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/campaignwin"
	"github.com/yakugate/yakugate/internal/crypto"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/dal/redis"
	"github.com/yakugate/yakugate/internal/dal/runtimequeue"
	"github.com/yakugate/yakugate/internal/dedupe"
	dlrsvc "github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/hlr"
	"github.com/yakugate/yakugate/internal/lockout"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/internal/pcap"
	"github.com/yakugate/yakugate/internal/pipeline"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/platform/worker"
	"github.com/yakugate/yakugate/internal/ratelimit"
	"github.com/yakugate/yakugate/internal/receiptpolicy"
	"github.com/yakugate/yakugate/internal/refdata"
	"github.com/yakugate/yakugate/internal/rejectlog"
	"github.com/yakugate/yakugate/internal/reroute"
	"github.com/yakugate/yakugate/internal/seclog"
	"github.com/yakugate/yakugate/internal/store"
	"github.com/yakugate/yakugate/internal/vendorhealth"
	"github.com/yakugate/yakugate/internal/vendorquality"
)

// Process is the Submit API process.
type Process struct {
	// receipts queues the REJECTD receipt of a refused duplicate (Saleem,
	// 28 Oct 2026); nil = the queue store cannot take client receipts.
	receipts  dlrsvc.ClientPusher
	cfg       config.Config
	addr      string
	ref       *refdata.Mem
	pipe      *pipeline.Pipeline
	auth      *auth.Authenticator
	hlrCache  *hlr.Cache      // nil = direct reads (redis backend or no delta source)
	hlrSvc    *hlr.Service    // HLR lookups for GET /sms/v4/operator (P1 scope 2026-08-09)
	hlrLive   *hlr.Live       // per-account live lookups for /operator (2026-09-14)
	hlrRanges *hlr.Ranges     // number ranges for /operator's range and country steps
	hotRead   dal.RecordStore // hot-store reader for GET /messages/{id}; nil = unavailable (non-mysql hot store)
	maxRcpt   int             // max recipients per request (YAKU_MAX_RECIPIENTS, default 500)
	maxBatch  int             // max messages[] items per request (YAKU_MAX_BATCH, default 500) [P1 scope 2026-08-09]
	// ipres resolves the client IP behind trusted proxies (YAKU_TRUSTED_PROXIES,
	// chunk-2 ruling 2.5); nil-safe — nil means TCP peer only, as before. The
	// SAME resolver is handed to auth, so ladders, allowlists, and the reject
	// log all agree on what "the client IP" is.
	ipres *auth.IPResolver
	// aud records operator actions on the admin endpoints to audit_log
	// (PO 2026-09-05). nil = no config store reachable, in which case the
	// endpoints work exactly as before and simply record nothing.
	// aud is an interface, not the concrete writer, so a test can assert what
	// an admin call records without a database.
	aud audit.Writer
	// secLog is the shared security sink (see sec in New). Held on the Process
	// so the unblock endpoint can report an operator LIFTING a lockout into
	// the same stream as the lockout itself.
	secLog  func(seclog.Event)
	rejects RejectWriter // nil = reject logging disabled (S-01)
	// recQ is the record queue this role writes through when the DB worker
	// writes the records (YAKU_RECORDS_VIA=worker), nil otherwise: its
	// figures are this role's record_queue on its status.
	recQ *recordq.Client
	// rejectQ puts those records through a bounded queue to a few writers,
	// so a storm of refusals never takes the hot store's connections from
	// the accept path (2026-10-06). Made on first use.
	rejectQ    *rejectlog.Queue
	rejectOnce sync.Once
	// rejectSeen is rejectQ once made, for the status to read: the status
	// runs on its own goroutine and must not read rejectQ while it is made.
	rejectSeen atomic.Pointer[rejectlog.Queue]
	balAdmin   *billing.RedisReserver // non-nil only under the redis gate: POST /admin/balance/refresh
	// abuseCounts counts refusals for the abuse monitor (nil where no
	// database keeps the counters).
	abuseCounts *abusecount.Counter
	// shown is the balance a reply shows when there is no Redis (reply_price.go).
	shown     *billing.BalanceCache
	shownOnce sync.Once
	// rater prices the optional accept-time cost/balance disclosure
	// (sms_service_config.send_sms_cost/send_remaining_balance) — the same
	// route/rate-card logic P9's async billing uses, called synchronously
	// here instead. nil when the config store isn't reachable, in which case
	// the fields are simply omitted from the response. [SCOPE P1-COST]
	rater *billing.Rater
	// Redis reference-snapshot plumbing (2026-07-24) — non-nil only when
	// gated: sync owns the MySQL->Redis leg, redisLoader the Redis->memory leg.
	sync        *refdata.Syncer
	redisLoader *refdata.RedisLoader
	log         *logging.Logger
	stats       *monitor.Stats // /status counters (accounts, TPS) [SCOPE P1-STATUS]
	statusRedis dal.LiveStatus // non-nil under the redis gate: /status/cluster reads pushed snapshots
	// campaignWin is the live campaign window GET /admin/campaign reads; nil
	// when no database keeps it (no Redis).
	campaignWin dal.CampaignWindow
	// queueDepthSrc reports live per-vendor queue depth/age for /status when
	// the queue backend supports it (MySQL, and Redis since 2026-09-11); nil =
	// section omitted.
	queueDepthSrc store.QueueDepthStats
	// queueOK says whether the queue store answers (queue_ready.go): /readyz
	// is not ready while it does not.
	queueOK *pipeline.QueueHealth
	// queued finds a message still waiting in a vendor queue for GET
	// /messages/{id} (QA BUG-YK-EP-016, status_queued.go); nil = a backend
	// without the lookup (MSSQL/Mongo), where a waiting message reads as not
	// found. statusRead replaces the record reader in tests only.
	queued     store.QueuedFinder
	statusRead statusReader
	// pending answers GET /messages/{id} for a message that was sent and
	// whose record is still in the DB worker's queue (YAKU_RECORDS_VIA=
	// worker); nil when every role writes its own records.
	pending pendingFinder
	// cry backs POST /admin/decrypt (the portal-view integration point,
	// eocean-multi-tenant-kms-architecture.md) — the same Service instance
	// passed into the pipeline as Crypto above, held directly here too since
	// pipeline.Encryptor's interface doesn't expose Decrypt. nil = at-rest
	// encryption disabled; the endpoint then degrades to plaintext passthrough
	// (crypto.Service's own Decrypt already does this for any nil receiver
	// call site — here it's simply never encrypted in the first place).
	// [SCOPE P1-KMS-MULTITENANT]
	cry *crypto.Service
	// Manual-unblock targets for POST /admin/unblock (P1 scope finalize
	// 2026-08-09 §7): the identity ladder, the per-IP ladder, and the spray
	// guard. Held as the narrow Clear interface so both the in-memory and the
	// Redis implementations fit; under the Redis gate a Clear is cluster-wide
	// and also releases P2's bind-auth blocks (shared keys).
	idUnblock, ipUnblock, sprayUnblock interface{ Clear(string) }
	// Who is locked right now, for GET /admin/blocks (2026-10-09). Under the
	// Redis gate the ladders file every lock in ONE list (lockout/blocked.go),
	// so one of them answers for all three and for every process on that
	// Redis (blocksShared); without Redis these are this process's own three.
	blockLists   []lockout.Lister
	blocksShared bool
	blockLimits  blockLimits
	// The "Will this send?" check (POST /admin/send/check, PO 2026-09-12)
	// builds its dry-run pipeline from the SAME dependencies as the real one,
	// so it reads the same caches, counters and HLR. traffic reads the one
	// setting the cache does not hold (an account's default selling card),
	// and the webhook rows and packet capture rows of the tools; balances is
	// the balance of record a reply shows without Redis; fx converts a quoted
	// price into the company's currency as billing does. traffic, balances
	// and fx are nil when the config store is unreachable.
	checkDeps pipeline.SubmitDeps
	traffic   *dal.TrafficConfig
	balances  dal.CompanyBalances

	// pcap runs Process 13 Type 4 packet captures on this host (admin_pcap.go).
	// nil until Run starts it.
	pcap *pcap.Runner
	fx   *billing.FXResolver

	// health is the vendor-health view routing reads (nil = config store
	// unreachable: only the operator's switch counts); mover moves messages
	// waiting for a down vendor to the next route (nil = off).
	health *vendorhealth.View
	mover  *reroute.Mover
}

// New wires the reference cache, the queue backend, and the pipeline. [SCOPE P1-PIPE builds P1-01..P1-17]
func New(cfg config.Config) worker.Worker {
	loader, hlrStore := buildLoader(cfg)

	// Reference data + HLR topology per the Redis gate (PO 2026-07-24):
	//
	//   GATED:   Mem hydrates from the Redis snapshot (RedisLoader); one
	//            elected instance syncs MySQL->Redis every 30s (Syncer, which
	//            keeps the MYSQL loader as its source); HLR lives ONLY in
	//            Redis and is read direct per lookup — no hlr.Cache.
	//   UNGATED: the original wiring, untouched.
	var (
		ref         *refdata.Mem
		hlrSvc      *hlr.Service
		hlrCache    *hlr.Cache
		hlrRedis    *hlr.RedisStore // Redis HLR store ref (also a dip Writer, hot-path backend)
		syncer      *refdata.Syncer
		redisLoader *refdata.RedisLoader
	)
	if cfg.RedisEnabled() {
		rc := redis.FromConfig(cfg).Client()
		snaps := redis.NewReferenceSnapshots(rc)
		redisLoader = refdata.NewRedisLoader(snaps)
		ref = refdata.New(redisLoader)
		ds, _ := hlrStore.(hlr.DeltaSource) // nil-tolerant: syncer skips HLR without a delta source
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
		// HLR is a shared core component (internal/hlr). [SCOPE P1-14]
		// Redis queue backend: direct per-message reads (fast enough).
		// SQL backends: in-memory reference (hlr.Cache) refreshed by 5-min delta
		// sync on last_check, with POST /admin/hlr/refresh as the immediate hook.
		if hlrStore != nil {
			if ds, ok := hlrStore.(hlr.DeltaSource); ok && cfg.QueueBackend != config.BackendRedis {
				hlrCache = hlr.NewCache(ds, logging.New("hlr-cache").Printf)
				hlrSvc = hlr.New(hlrCache)
			} else {
				hlrSvc = hlr.New(hlrStore)
			}
		}
	}

	// HLR (2026-09-14): each account's order decides how a number's network
	// is found — our HLR data, the operators table's number ranges, live
	// lookups with the account's own providers, the country code. The range
	// index and the providers load from the config store and stay current in
	// the background. Live answers go to the in-memory cache, the Redis copy
	// and the hlr_<first4> table together.
	var hlrWriters []hlr.Writer
	if hlrCache != nil {
		hlrWriters = append(hlrWriters, hlrCache) // SQL backend: immediate in-memory cache availability
	}
	if hlrRedis != nil {
		hlrWriters = append(hlrWriters, hlrRedis) // Redis hot-path: keep the direct-read copy current NOW (else re-dips until the 30s sync)
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
	var cry *crypto.Service
	var enc pipeline.Encryptor
	if cs, err := dal.OpenDataKeys(cfg); err == nil {
		c, cerr := crypto.FromEnv(context.Background(), cs, config.Secrets)
		switch {
		case cerr != nil:
			logging.New("http-server").Error(logging.SystemScope, "crypto",
				"encryption is required but failed to start — every message will be refused 503 until it is fixed",
				logging.Fields{"error": cerr.Error()})
			enc = crypto.Broken{Err: cerr}
		case c != nil:
			cry, enc = c, c
		}
	}

	// Synchronous balance reservation (2026-07-23) — see pipeline.Reserver's
	// doc comment for why. Shares the same config-store DSN every account/
	// route/dialect read already uses; a real *sql.DB (not the mysql.Store
	// wrapper above, which is queue/hot-store shaped) since billing.Reserve
	// needs raw transactions. off by default (ReservePricePerSegment<=0) —
	// an operator opts in via YAKU_RESERVE_PRICE_PER_SEGMENT, same pattern as
	// YAKU_CRYPTO_AT_REST.
	var reserver pipeline.Reserver
	var balAdmin *billing.RedisReserver
	var rater *billing.Rater
	var fxResolver *billing.FXResolver
	var checkCfg *dal.TrafficConfig
	var balances dal.CompanyBalances
	// sqlBook: no Redis, and the money events are booked in the config
	// store (billing.SQLReserver).
	var sqlBook bool
	// Billing's stores, each on the DAL the settings pick (dal.OpenBilling):
	// prices, terms and the balance of record on the config store, the live
	// balances and the money events in Redis when it is set.
	bs, berr := dal.OpenBilling(cfg)
	if berr != nil {
		logging.New("http-server").Printf("billing unavailable: %v", berr)
	}
	if tc, err := dal.OpenTrafficConfig(cfg); err == nil && berr == nil {
		// The config store's small parts, on its one pool (the pool
		// billing's store is on too).
		checkCfg, balances = tc, bs.Store
		// Bounded pool: under a heavy concurrent burst against ONE account,
		// row-lock contention already serializes the actual work — capping
		// open connections here just makes excess requests queue in-process
		// for a pool slot instead of each opening its own raw MySQL
		// connection, which a live load test found could blow past the
		// server's max_connections (default 151, shared with every other
		// pool this process and every other process already hold) well
		// before row-lock contention itself became the bottleneck.
		// Same handle, second use: pricing reads (rate_cards/rate_card_rates/
		// routes) are read-only and infrequent (one Reload per cache-refresh
		// cycle), so sharing this pool costs nothing extra. Reload happens in
		// server.go's Run — construction only here.
		rater = billing.NewRater(bs.Store)
		// Same handle, third use (S-03, PO 2026-09-03): loss detection's live
		// FX dip is the SAME resolver P9 already trusts, not a duplicate —
		// cheap to construct (just wraps this pool; no network call until a
		// route actually needs converting).
		// NoWait: a rate provider is never waited for while a client waits
		// for its reply (QA billing report #1) — the rate known is used and
		// the provider is asked in the background.
		fxResolver = billing.NewFXResolver(bs.Store).NoWait()
		if cfg.RedisEnabled() {
			// Prepaid balance on Redis (2026-07-23): Lua check-and-decrement,
			// lazily seeded from this same MySQL handle, written back every
			// 30s. See billing.RedisReserver for the full lifecycle.
			balAdmin = billing.NewRedisReserver(context.Background(), bs.Live, bs.Queue, bs.Store,
				logging.New("http-server").Printf)
			reserver = balAdmin
		} else if sr := billing.NewSQLReserver(bs); sr != nil {
			// No Redis (step 3): the hold and its money events in one
			// transaction of the config store.
			reserver, sqlBook = sr, true
		} else {
			reserver = billing.DBReserver{DB: bs.Store}
		}
	}

	// Per-request counters (PO 2026-07-24): cluster-accurate Redis versions
	// when gated — one rate limit / duplicate lock / lockout across every
	// instance — else the original in-memory single-node ones.
	var rate pipeline.RateAllower = ratelimit.New()
	// Dedupe window is env-tunable (PO 2026-07-26); 30 minutes unless set
	// (Saleem, 29 Oct 2026; it was 24 hours). An account's own wins.
	dedupeTTL := dedupe.DefaultTTL
	if n, err := strconv.Atoi(env("YAKU_DEDUPE_TTL_SEC", "1800")); err == nil && n > 0 {
		dedupeTTL = time.Duration(n) * time.Second
	}
	var dedu pipeline.Deduper = dedupe.NewMem(dedupeTTL)
	// The shared counters and blocks, on the DAL the settings pick
	// (dal.CountersOn): the main Redis when it is set — all of them on ONE
	// client, as before — else the SQL database, which keeps all six since
	// step 3 of the DAL plan (MySQL; config/0164-0166). An area a database
	// does not keep stays in this process's memory.
	shared, _, _ := dal.OpenCounters(cfg)
	if shared == nil {
		shared = &dal.Counters{}
	}
	// Durable dedupe (PO 2026-07-27): without Redis, a plain in-memory Deduper
	// loses its duplicate locks on restart and doesn't share them across
	// instances. When the Config store is MySQL, the DAL backs it with
	// dedupe_keys (an explicit expires_at column, reclaimed in place — see
	// dalmysql.Dedupe's doc comment) instead, swept here. MSSQL/Mongo config
	// backends still fall back to Mem — a durable backend for those isn't
	// implemented yet (tracked in the process-1 backlog).
	if shared.Dedupe != nil {
		ded := dedupe.NewShared(shared.Dedupe, dedupeTTL)
		go ded.StartSweep(context.Background(), time.Hour, logging.New("http-server").Printf)
		dedu = ded
	}
	// Two auth ladders (PO 2026-07-26): identity lock (5 fails/min, 423) and
	// per-source-IP throttle (20 fails/min, pre-auth 429) — same escalation
	// rungs (1m/5m/30m/60m), see internal/lockout's package doc.
	const loginFails, addressFails = 5, 20
	var lock auth.Locker = lockout.New(loginFails)
	var ipLock auth.Locker = lockout.New(addressFails)
	// Credential-spray guard (PO 2026-07-27): distinct-identity threshold per
	// source IP before the accelerated ladder trips — see lockout.SprayGuard's
	// doc comment. Tunable since the right threshold depends on how many
	// legitimate accounts share one NAT egress IP in a deployment.
	sprayThreshold, err := strconv.Atoi(env("YAKU_SPRAY_DISTINCT_THRESHOLD", "3"))
	if err != nil || sprayThreshold < 1 {
		sprayThreshold = 3
	}
	var spray auth.SprayLocker = lockout.NewSprayGuard(sprayThreshold)
	blockLists := listers(lock, ipLock, spray)
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
	var statusRC dal.LiveStatus // /status/cluster reader [SCOPE P1-STATUS]
	if cfg.RedisEnabled() {
		statusRC = redis.NewLiveStatus(redis.FromConfig(cfg).Client())
	}
	if shared.Limits != nil {
		// The shared buckets, asked a chunk at a time (29 Oct 2026): a
		// message costs no call to the store (the main Redis, else the SQL
		// database's limit_buckets) unless this process's chunk ran out.
		rate = ratelimit.NewChunked(ratelimit.NewShared(shared.Limits))
	}
	if shared.Blocks != nil {
		lock = lockout.NewShared(shared.Blocks, loginFails)
		ipLock = lockout.NewShared(shared.Blocks, addressFails)
		spray = lockout.NewSharedSprayGuard(shared.Blocks, sprayThreshold)
		blockLists = listers(lock) // one shared list (Redis, or the SQL database)
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

	q := buildQueue(cfg)
	// Queue depth/age per vendor (PO 2026-07-27) — only the backends that
	// implement store.QueueDepthStats (MySQL, and Redis since 2026-09-11)
	// report it; others simply omit the section rather than fail.
	queueDepthSrc, _ := q.(store.QueueDepthStats)
	// Least busy reads depths from this same queue store, wherever the queues
	// are (MySQL or Redis) — not from the HLR database (2026-10-06).
	if src := refdata.DepthsFromQueue(q); src != nil && ref != nil {
		ref.SetQueueDepthSource(src)
	}
	// A waiting message for GET /messages/{id} (QA BUG-YK-EP-016).
	queued, _ := q.(store.QueuedFinder)

	// Gateway-wide TPS ceiling (chunk-1 ruling 1.5, PO 2026-07-31): one
	// segments/sec cap over ALL tenants; 0/unset = disabled. Cluster-wide
	// under the Redis gate (shared bucket), per-instance otherwise.
	globalTPS, _ := strconv.Atoi(env("YAKU_GLOBAL_TPS", "0"))

	// ONE security sink for the whole process, shared by the pipeline, the
	// authenticator and the unblock endpoint. It matters that this is a single
	// instance: the sink owns the flood damper, and three separate sinks would
	// each fold repeats against their own private window instead of together.
	sec := seclog.Sink("http-server")

	deps := pipeline.SubmitDeps{
		QualityMinSample: int64(cfg.QualityMinSample), // route_type='quality' evidence threshold
		// Process 13 Type 2: IP-allowlist violations join the security stream.
		SecLog:    sec,
		Ref:       ref,
		HLR:       hlrSvc,
		HLRLive:   hlrLive,
		HLRRanges: hlrRanges,
		Rate:      rate,
		Dedupe:    dedu,
		Queue:     q,
		Crypto:    enc,
		Reserve:   reserver,
		// The exact sell charge, priced as billing prices it (PO 2026-09-17:
		// reserve the exact amount — YAKU_RESERVE_PRICE_PER_SEGMENT is retired).
		Pricer:           pipeline.RaterPricer{R: rater, FX: fxResolver},
		DedupeDefaultTTL: dedupeTTL,
		GlobalTPS:        globalTPS,
		Usage:            usage,
		DestCapUsage:     destCap,
		Campaigns:        campaigns,
		AbuseCounts:      abuseNoter,
		FX:               fxResolver,
	}
	// Routing rules review (PO 2026-09-14). Vendor health: a vendor with no
	// working connection for its down_after_sec is skipped like a paused one,
	// read from vendor_health every few seconds. Quality: per vendor and
	// network, from the counts the verdict-seeing roles keep. The mover takes
	// messages already waiting for a down vendor to the next route.
	var health *vendorhealth.View
	if checkCfg != nil {
		health = vendorhealth.NewView(checkCfg.VendorHealth()) // the config store's one pool
		deps.VendorHealth = health
	}
	if qs := vendorquality.Open(cfg); qs != nil {
		ref.SetQualitySource(qs, time.Duration(cfg.QualityWindowMin)*time.Minute)
	}
	// Build the hot-store reject writer once and reuse its handle for GET
	// /messages/{id} status reads (P1 scope 2026-08-09) — no second connection.
	//
	// The handle is given back beside the writer, not taken from it: with
	// the DB worker (YAKU_RECORDS_VIA=worker) the writer is the record queue,
	// and "the writer is not a *mysql.Store" used to leave this role with no
	// record store at all — no status read of a written message, and nothing
	// to fail a message with (below). Found on the dev box the afternoon the
	// worker was switched on, 2026-10-13: 3,616 messages sat in the re-route
	// inbox, each "should fail … but this process cannot write records".
	rejectWriter, inboxDone, hotRead, recQ := buildRecords(cfg)
	// An accepted message's record (mt_) is written at acceptance (the
	// three-table design, 27 Oct 2026): through the DB worker's queue — with
	// the vendor queue in one Redis step — else the store itself.
	switch {
	case recQ != nil:
		deps.Accepted = recQ
	case hotRead != nil && !cfg.RedisEnabled():
		// No Redis: the records of the submits in flight are committed
		// together (dal.AcceptGroup), not one transaction a message.
		deps.Accepted = dal.NewAcceptGroup(hotRead)
	case hotRead != nil:
		deps.Accepted = hotRead
	}
	pipe := pipeline.NewSubmit(deps)
	var mover *reroute.Mover
	if mq, ok := q.(reroute.Queue); ok && health != nil && env("YAKU_REROUTE", "on") != "off" {
		var seal reroute.Sealer
		if cry != nil {
			seal = cry
		}
		mover = reroute.New(mq, health, pipeline.NewRerouter(deps), ref, seal, logging.New("reroute"))
	}

	// Global recipient ceiling ruled 500 in the P1 scope finalize (2026-08-09).
	maxRcpt, err := strconv.Atoi(env("YAKU_MAX_RECIPIENTS", "500"))
	if err != nil || maxRcpt < 1 {
		maxRcpt = 500
	}
	// messages[] personalized-bulk cap (P1 scope 2026-08-09), independent of the
	// recipient ceiling above; total recipients are still bounded by maxRcpt.
	maxBatch, err := strconv.Atoi(env("YAKU_MAX_BATCH", "500"))
	if err != nil || maxBatch < 1 {
		maxBatch = 500
	}

	// Trusted reverse proxies (chunk-2 ruling 2.5, closing B-02): X-Forwarded-
	// For is honoured ONLY when the TCP peer is on this list — unset keeps the
	// peer-only behaviour, so bare deployments are unaffected and spoofing
	// stays impossible. Feeds the IP ladders, spray guard, per-account
	// allowlists, and reject-log client IP alike.
	ipres, badProxies := auth.NewIPResolver(env("YAKU_TRUSTED_PROXIES", ""))
	for _, bad := range badProxies {
		logging.New("http-server").Warn(logging.SystemScope, "config", "YAKU_TRUSTED_PROXIES entry is not a valid IP/CIDR — ignored",
			logging.Fields{"entry": bad})
	}
	// Opaque static tokens are HMAC'd with this pepper before lookup; running
	// production on the publicly known dev default makes every stored digest
	// brute-forceable offline (chunk-2 ruling 2.6).
	pepper := env("YAKU_TOKEN_PEPPER", "yaku-dev-token-pepper")
	if pepper == "yaku-dev-token-pepper" {
		logging.New("http-server").Warn(logging.SystemScope, "config",
			"YAKU_TOKEN_PEPPER is the known dev default — set a real pepper in production", nil)
	}

	// The re-route inbox (PO 2026-09-15): messages an SMPP vendor's status
	// code sent another way (failover, hlr_retry, pause_vendor). The mover
	// routes them again; one that has to fail is recorded through the hot
	// store and its failed receipt queued for the customer.
	if mover != nil {
		if iq, ok := q.(reroute.InboxQueue); ok {
			// A failed message's record goes the way this role's other
			// records go: through the record queue when the DB worker
			// writes them (so a message's records stay in order), else
			// straight to the hot store.
			done := inboxDone
			var push reroute.ClientPusher
			if eq, ok := q.(dlrsvc.Enqueuer); ok {
				push = dlrsvc.NewQueuePusher(eq)
			}
			mover.SetInbox(iq, done, push)
		}
		// What the client of a message that fails here is told: its
		// account's receipt policy, else the gateway default's (issue 23).
		if checkCfg != nil {
			mover.SetPolicies(receiptpolicy.New(dal.EngineConfigOrNone(cfg).Policies)) // on the config store's pool
		}
		// A message that fails here books its money (issues 3 and 18).
		if balAdmin != nil || sqlBook {
			mover.SetBilling(billing.NewMomentsBooking(context.Background(), bs,
				logging.New("http-server").Printf))
		}
	}

	var receipts dlrsvc.ClientPusher
	if eq, ok := q.(dlrsvc.Enqueuer); ok {
		receipts = dlrsvc.NewQueuePusher(eq)
	}
	return &Process{
		receipts: receipts,
		cfg:      cfg,
		addr:     env("YAKU_API_ADDR", ":8080"),
		ref:      ref,
		pipe:     pipe,
		auth: auth.New(ref, lock, auth.WithSecrets(config.Secrets),
			auth.WithTokenPepper(pepper),
			auth.WithIPThrottle(ipLock),    // pre-auth 429 ladder per source IP (20 fails/min)
			auth.WithSprayGuard(spray),     // credential-spray guard (PO 2026-07-27)
			auth.WithTrustedProxies(ipres), // X-Forwarded-For behind trusted proxies only (chunk-2 ruling 2.5)
			// Process 13 Type 2: EVERY auth refusal is a security record in
			// the system log and (when the account is known) that account's
			// own file. The plain "[audit] …" console line that used to sit
			// alongside this was REMOVED 2026-09-05 (PO): it duplicated the
			// same three third-rung events as an unfilterable sentence, and
			// stored them at info while the real record is at error.
			auth.WithSecurityLog(sec)),
		ipres:         ipres,
		secLog:        sec,
		aud:           adminAudit(cfg),
		hlrCache:      hlrCache,
		hlrSvc:        hlrSvc,
		hlrLive:       hlrLive,
		hlrRanges:     hlrRanges,
		maxRcpt:       maxRcpt,
		maxBatch:      maxBatch,
		rejects:       rejectWriter,
		recQ:          recQ,
		hotRead:       hotRead,
		balAdmin:      balAdmin,
		abuseCounts:   abuseCounts,
		rater:         rater,
		sync:          syncer,
		redisLoader:   redisLoader,
		log:           logging.New("http-server"),
		stats:         monitor.New(),
		statusRedis:   statusRC,
		campaignWin:   shared.Campaigns,
		queueDepthSrc: queueDepthSrc,
		queueOK:       newQueueHealth(q),
		queued:        queued,
		pending:       pendingRecords(cfg),
		cry:           cry,
		idUnblock:     asClearer(lock),
		ipUnblock:     asClearer(ipLock),
		sprayUnblock:  asClearer(spray),
		blockLists:    blockLists,
		blocksShared:  shared.Blocks != nil,
		blockLimits:   newBlockLimits(loginFails, addressFails, sprayThreshold),
		checkDeps:     deps,
		traffic:       checkCfg,
		balances:      balances,
		fx:            fxResolver,
		health:        health,
		mover:         mover,
	}
}

// asClearer safely narrows a lockout implementation to its manual-unblock
// capability; nil when the implementation has none (the endpoint then reports
// that target as unsupported instead of panicking at wiring time).
func asClearer(v any) interface{ Clear(string) } {
	c, _ := v.(interface{ Clear(string) })
	return c
}

// buildRejectWriter returns the hot-store handle used to persist refused
// submits (S-01), or nil to disable the feature. nil is a supported state: the
// accept path must work whether or not reject logging is available, so every
// failure mode here degrades to "no reject rows" rather than to a broken API.
// buildRecords opens the hot store for this role and says how its records
// are written. Three things, which are NOT one thing:
//
//	rejects   where refused submits are filed; nil with YAKU_LOG_REJECTS=off
//	finisher  what records a re-routed message that has to fail (the
//	          re-route inbox); nil only when there is no hot store
//	hot       the store itself, for the status read; nil when the hot store
//	          is not MySQL or cannot be opened
//	recQ      the record queue's client with the DB worker, else nil: what
//	          the status shows as record_queue
//
// With the DB worker (YAKU_RECORDS_VIA=worker) the first two are the record
// queue and the third is still the store. The fourth is given apart for the
// same reason: the status asked p.rejects for a *recordq.Client, and since
// D-135 that is a recordq.RejectsOrDirect, so the http-server never showed
// record_queue (go-live test P14 A7, 18 Oct 2026). Until 2026-10-13 one value stood
// for all three and the store was taken back out of it by its type — so in
// worker mode, and with reject logging off, this role had no record store at
// all: no status read of a written message, and a message the re-route inbox
// had to fail was "kept in the re-route inbox" for ever (3,616 on the dev box
// within forty minutes of switching the worker on).
func buildRecords(cfg config.Config) (rejects RejectWriter, finisher reroute.Finisher, hot dal.RecordStore, recQ *recordq.Client) {
	// Implemented where the hot store's DAL keeps records (MySQL today);
	// other backends simply skip it.
	if !dal.ServesRecords(cfg) {
		return nil, nil, nil, nil
	}
	s, err := dal.OpenRecordStore(cfg)
	if err != nil {
		logging.New("http-server").Warn(logging.SystemScope, "reject_log",
			"the record store cannot be opened: refused submits are not recorded, a message's status cannot be read from its record, and a re-routed message cannot be failed",
			logging.Fields{"error": err.Error()})
		return nil, nil, nil, nil
	}
	rejects, finisher, hot = s, s, s
	// With YAKU_RECORDS_VIA=worker this role's records go to the DB worker
	// (recordq), which writes them many to a statement and in their order.
	if _, q := recordq.ForRole(context.Background(), cfg, s, logging.New("http-server").Printf); q != nil {
		// A refusal whose task cannot reach Redis is written to the store
		// itself (Saleem 2026-10-14, recordq.RejectsOrDirect).
		rejects, finisher, recQ = recordq.RejectsOrDirect{Queue: q, Direct: s}, q, q
	}
	if env("YAKU_LOG_REJECTS", "on") == "off" {
		rejects = nil
	}
	return rejects, finisher, hot, recQ
}

// pendingFinder reads what is known of a message whose record is still in
// the record queue (recordq.Queue).
type pendingFinder interface {
	Pending(ctx context.Context, msgID string) (recordq.PendingRecord, bool, error)
}

// pendingRecords is the record queue of this deployment when its roles hand
// their records to the DB worker, nil otherwise.
func pendingRecords(cfg config.Config) pendingFinder {
	if !recordq.ViaWorker(cfg) {
		return nil
	}
	return recordq.FromConfig(cfg)
}

// Name returns the process role name.
func (p *Process) Name() string { return "http-server" }

// buildQueue selects the queue backend per config; falls back to a safe no-op.
func buildQueue(cfg config.Config) pipeline.QueueWriter {
	log := logging.New("http-server")
	// Items left on the other side of YAKU_QUEUE_BACKEND move over (D-122).
	runtimequeue.HandOver(cfg, log.Printf)
	// The DAL the settings pick (dal.QueuesOn): with YAKU_QUEUE_BACKEND=redis
	// (D-122) the vendor queue is on Redis (primary + failover, PO
	// 2026-07-26), NO SQL fallback — the claim side (P3/P4) is picked the
	// same way, and a silent fallback would enqueue where nothing is reading.
	// An unreachable Redis surfaces as 503 QUEUE_UNAVAILABLE per request.
	q, on, err := dal.OpenQueues(cfg)
	if err != nil {
		log.Error(logging.SystemScope, "queue_backend", "queue backend unavailable; submits will answer 503 until it is fixed",
			logging.Fields{"queueBackend": on, "error": err.Error()})
		return unavailableQueue{}
	}
	if rs, ok := q.(*redis.Store); ok {
		if err := rs.Ping(context.Background()); err != nil {
			log.Error(logging.SystemScope, "queue_backend", "queue Redis unreachable — submits will answer 503 until it returns",
				logging.Fields{"addr": cfg.ForQueueRedis().RedisWhere(), "error": err.Error()})
		} else {
			log.Info(logging.SystemScope, "queue_backend", "vendor queue on redis", logging.Fields{"addr": cfg.ForQueueRedis().RedisWhere(),
				"own_redis": cfg.QueueRedisSet()})
		}
	}
	return q
}

// unavailableQueue is the fallback when no queue backend could be built. It
// REFUSES messages (pipeline answers 503) — never silently accept-and-drop:
// an accepted message is a promise.
type unavailableQueue struct{}

func (unavailableQueue) Enqueue(ctx context.Context, m model.Message) error {
	return errors.New("queue backend unavailable")
}

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
	log := logging.New("http-server")
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

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// adminAudit opens the config store for the admin-endpoint audit trail
// (PO 2026-09-05). A failure here is deliberately not fatal: the engine must
// still serve traffic without an audit connection, so it logs loudly and
// records nothing rather than refusing to start.
func adminAudit(cfg config.Config) audit.Writer {
	if !dal.ConfigOnSQL(cfg) {
		return nil // an untyped nil: p.aud == nil must stay true
	}
	lg := logging.New("http-server")
	log, err := dal.OpenAuditLog(cfg) // the process's one pool for the config store
	if err != nil {
		lg.Error(logging.SystemScope, "audit_write", "admin audit trail unavailable — actions will not be recorded",
			logging.Fields{"error": err.Error()})
		return nil
	}
	return audit.New(log, func(err error) {
		lg.Error(logging.SystemScope, "audit_write", "admin audit row not stored",
			logging.Fields{"error": err.Error()})
	})
}
