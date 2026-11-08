package config

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/secrets"
)

// Backend identifies a storage engine.
type Backend string

const (
	BackendMySQL Backend = "mysql"
	BackendMSSQL Backend = "mssql"
	BackendRedis Backend = "redis"
	BackendMongo Backend = "mongo"
)

// Config holds per-role connection selection. Each store role (queue, hot, cold) [SCOPE P1-CFG four stores]
// picks its own backend so a deployment can mix engines.
type Config struct {
	Role string

	// Process 13 logging. LogDir is the root; each role writes into its own
	// subfolder under it. LogLevel is the process-wide default an account or
	// vendor with no log_level of its own falls back to (the D-069 shape).
	LogDir           string
	LogLevel         string
	LogMaxSizeMB     int
	LogRetentionDays int
	// LogRedact strips subscriber numbers and one-time codes from log files
	// (P1 register 1.9.2). ON unless explicitly turned off: forgetting to
	// switch it on leaves customer numbers sitting in a file for the whole
	// retention period, which is the failure worth defaulting against.
	// Time-boxed flow captures are exempt by design and are NOT affected.
	LogRedact bool

	// route_type='quality' (P1-15). QualityWindowMin is how far back the
	// per-vendor delivery ratio looks; QualityMinSample is how many judged
	// messages a vendor needs before that ratio is held against it.
	QualityWindowMin int
	QualityMinSample int

	// Store 1 — Queues (per-vendor queues, client receipts, MO, the multi-part
	// buffer). YAKU_QUEUE_BACKEND decides where they run (D-122); the DSN is the
	// MySQL queue database, used with `mysql` only.
	QueueBackend Backend
	QueueDSN     string

	// Store 1b — HLR and the gateway's other non-queue data: the HLR shards,
	// HTTP vendor login sessions, JWT revocations (Saleem 2026-10-02, D-124).
	// These lived in the queue database from the start; unset, HLRStore still
	// finds them there, so an install changes nothing until it sets these.
	HLRBackend Backend
	HLRDSN     string

	// Store 2 — Configurations (accounts, vendors, routes, templates, anti-abuse config)
	ConfigBackend Backend
	ConfigDSN     string

	// Store 3 — Hot backup (recent MDR / records)
	HotBackend Backend
	HotDSN     string

	// Store 4 — Cold backup (filesystem; ColdDSN = cold root path when ColdBackend=fs)
	ColdBackend Backend
	ColdDSN     string

	// A read DSN beside each store's own (readwrite.go, 2026-10-13:
	// YAKU_CONFIG_READ_DSN, YAKU_HOT_READ_DSN …): the whole DSN that store's
	// reads may use, "" when it is read where it is written.
	ConfigReadDSN, HotReadDSN, QueueReadDSN, HLRReadDSN, ColdReadDSN string

	// Redis hot-path offload — ONE Redis, primary + failover (PO 2026-07-26,
	// superseding the earlier two-Redis "queue on #2" model):
	//
	//   no Redis      → everything on the configured DATABASE / in-memory
	//                   (the original behavior).
	//   Redis set     → ALL hot-path data on that ONE Redis: the per-vendor
	//                   message queue, mo_queue/client_dlr, prepaid balance,
	//                   the reference-data snapshot, HLR, and the per-request
	//                   counters.
	//   + failover    → RedisFailoverAddr is a STANDBY: every new connection
	//                   dials the primary first and falls back when it is
	//                   unreachable (and returns to the primary automatically
	//                   once it dials again). Same password/db as the primary;
	//                   it should replicate the primary for data continuity.
	//
	//   or Sentinel   → RedisSentinelAddrs + RedisMasterName (2026-10-02,
	//                   Saleem): the Sentinels say which server is the main
	//                   one, and every connection goes to it — after a
	//                   failover too. RedisAddr and RedisFailoverAddr are
	//                   then not used (RedisProblem refuses a mix).
	//
	// delivery_reports/raw_dlr history and every other table stay on the
	// database regardless.
	RedisAddr         string // primary: host:port
	RedisPassword     string
	RedisDB           int
	RedisFailoverAddr string // optional standby, host:port

	RedisSentinelAddrs    []string // Sentinels, host:port each
	RedisMasterName       string   // the name the Sentinels monitor the main server by
	RedisSentinelPassword string   // the Sentinels' own password, when they have one

	// Connections one Redis client keeps (YAKU_REDIS_POOL_SIZE) and how long
	// a call waits for one before it fails (YAKU_REDIS_POOL_TIMEOUT_MS), for
	// every client of the main and the queue Redis. 0 = go-redis's own:
	// 10 a CPU, and 4 s (load test of 30 Oct 2026).
	RedisPoolSize      int
	RedisPoolTimeoutMS int

	// How long a balance deduction (and every other call on the live
	// balances' and billing queues' client) waits for Redis: first for a free
	// connection, then for the answer (YAKU_REDIS_BALANCE_TIMEOUT_MS). Saleem
	// 2026-11-02: under load the deduction failed at go-redis's 4 s; at least
	// 30 s, and settable.
	RedisBalanceTimeoutMS int

	// The queues' own Redis (Saleem 2026-10-02): optional, set up exactly like
	// the main one above — one address (+ standby) or Sentinel. Unset, the
	// queues use the main Redis. Used for the queues only (YAKU_QUEUE_BACKEND=
	// redis); balances, limits, counters, reference data and monitoring stay
	// on the main Redis. With `mysql` its lines name where queues WERE, so the
	// start-up hand-over can empty it into MySQL.
	QueueRedisAddr             string
	QueueRedisPassword         string
	QueueRedisDB               int
	QueueRedisFailoverAddr     string
	QueueRedisSentinelAddrs    []string
	QueueRedisMasterName       string
	QueueRedisSentinelPassword string
}

// RedisEnabled reports whether Redis is configured — the gate for the whole
// hot path (queues, balance, reference snapshot, HLR, counters).
func (c Config) RedisEnabled() bool { return c.RedisAddr != "" || len(c.RedisSentinelAddrs) > 0 }

// QueuesInRedis reports whether the live queues run in Redis — outgoing
// messages, MO, client receipts, the multi-part buffer and the re-route inbox
// (Saleem 2026-10-02, D-122). YAKU_QUEUE_BACKEND decides: `redis` runs them in
// the Redis set by YAKU_REDIS_ADDR or the Sentinel lines, and ONLY there — no
// MySQL copy (Saleem, same day: "if I set redis then no need to store in
// mysql"); `mysql` runs them in MySQL only, and Redis, when set, carries
// nothing but the cache and counters.
func (c Config) QueuesInRedis() bool {
	return c.QueueBackend == BackendRedis && (c.QueueRedisSet() || c.RedisEnabled())
}

// QueueRedisSet reports whether the queues have their own Redis lines.
func (c Config) QueueRedisSet() bool {
	return c.QueueRedisAddr != "" || len(c.QueueRedisSentinelAddrs) > 0
}

// ForQueueRedis is the Config to build the queues' Redis client from
// (redis.FromConfig): the YAKU_QUEUE_REDIS_* lines put in the Redis fields
// when they are set — standalone, standby or Sentinel alike — else the main
// Redis unchanged.
func (c Config) ForQueueRedis() Config {
	if !c.QueueRedisSet() {
		return c
	}
	q := c
	q.RedisAddr, q.RedisPassword, q.RedisDB, q.RedisFailoverAddr =
		c.QueueRedisAddr, c.QueueRedisPassword, c.QueueRedisDB, c.QueueRedisFailoverAddr
	q.RedisSentinelAddrs, q.RedisMasterName, q.RedisSentinelPassword =
		c.QueueRedisSentinelAddrs, c.QueueRedisMasterName, c.QueueRedisSentinelPassword
	return q
}

// QueueRedisSeparate reports whether the queue Redis is set AND is another
// Redis (server and database) than the main one — when queue keys left in
// the main Redis are handed over to it.
func (c Config) QueueRedisSeparate() bool {
	if !c.QueueRedisSet() {
		return false
	}
	return !c.RedisEnabled() || redisIdentity(c) != redisIdentity(c.ForQueueRedis())
}

// redisIdentity names the Redis a Config's Redis fields reach.
func redisIdentity(c Config) string {
	if len(c.RedisSentinelAddrs) > 0 {
		addrs := append([]string(nil), c.RedisSentinelAddrs...)
		sort.Strings(addrs)
		return fmt.Sprintf("sentinel|%s|%s|%d", strings.Join(addrs, ","), c.RedisMasterName, c.RedisDB)
	}
	return fmt.Sprintf("addr|%s|%d", c.RedisAddr, c.RedisDB)
}

// QueueStoreMySQL reports whether a MySQL queue database is configured: the
// queues themselves with `mysql`; with `redis`, an older database the queues
// left (items still waiting there are handed over to Redis at start).
func (c Config) QueueStoreMySQL() bool { return c.QueueStoreSQL() == BackendMySQL }

// QueueStoreSQL is the database the queue DSN names, "" when none: the
// queues themselves with `mysql` or `mssql`; with `redis`, the database kept
// beside them for the HLR data (D-124) — SQL Server when its DSN is written
// sqlserver://, else MySQL. A Redis-queue install on SQL Server is not a
// MySQL one.
func (c Config) QueueStoreSQL() Backend {
	if !c.queueDSNIsSQL() {
		return ""
	}
	switch c.QueueBackend {
	case BackendMySQL, BackendMSSQL:
		return c.QueueBackend
	case BackendRedis:
		if IsSQLServerDSN(c.QueueDSN) {
			return BackendMSSQL
		}
		return BackendMySQL
	}
	return ""
}

func (c Config) queueDSNIsSQL() bool {
	return c.QueueDSN != "" && !strings.HasPrefix(strings.ToLower(strings.TrimSpace(c.QueueDSN)), "redis")
}

// HLRStore is where the HLR data and the gateway's other non-queue tables
// live — HLR shards, HTTP vendor sessions, JWT revocations (D-124):
// YAKU_HLR_DSN (YAKU_HLR_BACKEND, mysql when unset), else the queue database,
// where they have always been. ("", "") = nowhere configured.
func (c Config) HLRStore() (Backend, string) {
	if c.HLRDSN != "" {
		if c.HLRBackend == "" {
			return BackendMySQL, c.HLRDSN
		}
		return c.HLRBackend, c.HLRDSN
	}
	// With redis, the queue DSN is the database kept for the HLR data (set
	// before D-124): MySQL, or SQL Server by its DSN.
	if b := c.QueueStoreSQL(); b != "" {
		return b, c.QueueDSN
	}
	return "", ""
}

// HLRStoreMySQL reports whether the HLR store is a MySQL database.
func (c Config) HLRStoreMySQL() bool {
	b, d := c.HLRStore()
	return b == BackendMySQL && d != ""
}

// HLRDSNOf is the HLR store's DSN ("" when none).
func (c Config) HLRDSNOf() string { _, d := c.HLRStore(); return d }

// HLRSeparate reports whether the HLR store is its own database, not the
// queue database — the case where its tables are created and filled there.
func (c Config) HLRSeparate() bool {
	return c.HLRDSN != "" && strings.TrimSpace(c.HLRDSN) != strings.TrimSpace(c.QueueDSN)
}

// RedisSentinel reports whether Redis is reached through Sentinel.
func (c Config) RedisSentinel() bool { return len(c.RedisSentinelAddrs) > 0 }

// RedisWhere says where Redis is, for log lines: the address, or the
// Sentinels and the name they know the main server by.
func (c Config) RedisWhere() string {
	db := ""
	if c.RedisDB != 0 {
		db = fmt.Sprintf(" db %d", c.RedisDB)
	}
	if c.RedisSentinel() {
		return fmt.Sprintf("sentinel %q via %s%s", c.RedisMasterName, strings.Join(c.RedisSentinelAddrs, ","), db)
	}
	return c.RedisAddr + db
}

// RedisProblem says what is wrong with the Redis settings, or "". A role
// refuses to start on a problem: half a Sentinel setup would otherwise run
// without Redis, or on a server the Sentinels may already have demoted.
func (c Config) RedisProblem() string {
	if c.QueueBackend == BackendRedis {
		if !c.QueuesInRedis() {
			return "YAKU_QUEUE_BACKEND=redis keeps the queues in Redis: set the queues' own Redis (YAKU_QUEUE_REDIS_ADDR, or YAKU_QUEUE_REDIS_SENTINEL_ADDRS + YAKU_QUEUE_REDIS_MASTER_NAME) or the main one (YAKU_REDIS_ADDR or its Sentinel lines), or set YAKU_QUEUE_BACKEND=mysql"
		}
		if c.QueueDSN != "" && !c.queueDSNIsSQL() {
			return "with YAKU_QUEUE_BACKEND=redis the queues are in Redis (YAKU_QUEUE_REDIS_* or the main Redis lines); YAKU_QUEUE_DSN is not a Redis address — remove it"
		}
		if _, d := c.HLRStore(); d == "" {
			return "with YAKU_QUEUE_BACKEND=redis no queue is kept in MySQL, but the HLR data and HTTP vendor sessions are: set YAKU_HLR_DSN to their MySQL database"
		}
	}
	switch c.HLRBackend {
	case "", BackendMySQL, BackendMSSQL:
	default:
		return fmt.Sprintf("YAKU_HLR_BACKEND=%s: the HLR data lives in mysql (or mssql)", c.HLRBackend)
	}
	// The main Redis and the queues' own: the same rules for each.
	if p := redisLinesProblem(redisNames{"YAKU_REDIS_ADDR", "YAKU_REDIS_FAILOVER_ADDR", "YAKU_REDIS_SENTINEL_ADDRS", "YAKU_REDIS_MASTER_NAME"},
		c.RedisAddr, c.RedisFailoverAddr, c.RedisSentinelAddrs, c.RedisMasterName); p != "" {
		return p
	}
	if p := redisLinesProblem(redisNames{"YAKU_QUEUE_REDIS_ADDR", "YAKU_QUEUE_REDIS_FAILOVER_ADDR", "YAKU_QUEUE_REDIS_SENTINEL_ADDRS", "YAKU_QUEUE_REDIS_MASTER_NAME"},
		c.QueueRedisAddr, c.QueueRedisFailoverAddr, c.QueueRedisSentinelAddrs, c.QueueRedisMasterName); p != "" {
		return p
	}
	if c.QueueRedisFailoverAddr != "" && c.QueueRedisAddr == "" && len(c.QueueRedisSentinelAddrs) == 0 {
		return "YAKU_QUEUE_REDIS_FAILOVER_ADDR is a standby for YAKU_QUEUE_REDIS_ADDR: set that too, or remove it"
	}
	return ""
}

// redisNames are one Redis's setting names, for the messages.
type redisNames struct{ addr, failover, sentinels, master string }

// redisLinesProblem checks one Redis's lines (the main one or the queues'
// own): either an address (with an optional standby) or Sentinel, never half
// of Sentinel and never both.
func redisLinesProblem(n redisNames, addr, failover string, sentinels []string, master string) string {
	if len(sentinels) == 0 {
		if master != "" {
			return n.master + " is set but " + n.sentinels + " is not: give the Sentinels' addresses too, or remove the name"
		}
		return ""
	}
	if master == "" {
		return n.sentinels + " is set but " + n.master + " is not: give the name the Sentinels monitor the main Redis by (often \"mymaster\")"
	}
	if addr != "" || failover != "" {
		return "with Sentinel (" + n.sentinels + ") the Sentinels say where the main Redis is: remove " + n.addr + " and " + n.failover
	}
	for _, a := range sentinels {
		if _, port, err := net.SplitHostPort(a); err != nil || port == "" {
			return fmt.Sprintf("%s: %q is not host:port (Sentinel's port is usually 26379)", n.sentinels, a)
		}
	}
	return ""
}

// envList reads a comma-separated list, spaces trimmed, blanks dropped.
func envList(k string) []string {
	var out []string
	for _, v := range strings.Split(os.Getenv(k), ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// envInt reads a whole number from the environment. An unset or unreadable
// value keeps the default rather than collapsing to zero, which for a log
// limit would silently mean "no limit".
func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// Load reads configuration from the environment. The settings file
// (yakugate.config) is copied into the environment first: main does that with
// LoadFile so it can honour --config and report errors; any other caller gets
// the same default search here.
func Load() Config {
	if !fileSearched {
		if _, err := LoadFile(""); err != nil {
			log.Printf("[config] %v", err)
		}
	}
	c := Config{
		QueueBackend:      Backend(env("YAKU_QUEUE_BACKEND", "mysql")),
		QueueDSN:          env("YAKU_QUEUE_DSN", ""),
		HLRBackend:        Backend(strings.ToLower(strings.TrimSpace(env("YAKU_HLR_BACKEND", "")))),
		HLRDSN:            env("YAKU_HLR_DSN", ""),
		ConfigBackend:     Backend(env("YAKU_CONFIG_BACKEND", "mysql")),
		ConfigDSN:         env("YAKU_CONFIG_DSN", ""),
		HotBackend:        Backend(env("YAKU_HOT_BACKEND", "mysql")),
		HotDSN:            env("YAKU_HOT_DSN", ""),
		ColdBackend:       Backend(env("YAKU_COLD_BACKEND", "mysql")),
		ColdDSN:           env("YAKU_COLD_DSN", ""),
		RedisAddr:         env("YAKU_REDIS_ADDR", ""),
		RedisPassword:     env("YAKU_REDIS_PASSWORD", ""),
		RedisFailoverAddr: env("YAKU_REDIS_FAILOVER_ADDR", ""),

		RedisSentinelAddrs:    envList("YAKU_REDIS_SENTINEL_ADDRS"),
		RedisMasterName:       strings.TrimSpace(env("YAKU_REDIS_MASTER_NAME", "")),
		RedisSentinelPassword: env("YAKU_REDIS_SENTINEL_PASSWORD", ""),
		RedisPoolSize:         envInt("YAKU_REDIS_POOL_SIZE", 0),
		RedisPoolTimeoutMS:    envInt("YAKU_REDIS_POOL_TIMEOUT_MS", 0),
		RedisBalanceTimeoutMS: envInt("YAKU_REDIS_BALANCE_TIMEOUT_MS", 30000),

		QueueRedisAddr:             strings.TrimSpace(env("YAKU_QUEUE_REDIS_ADDR", "")),
		QueueRedisPassword:         env("YAKU_QUEUE_REDIS_PASSWORD", ""),
		QueueRedisFailoverAddr:     strings.TrimSpace(env("YAKU_QUEUE_REDIS_FAILOVER_ADDR", "")),
		QueueRedisSentinelAddrs:    envList("YAKU_QUEUE_REDIS_SENTINEL_ADDRS"),
		QueueRedisMasterName:       strings.TrimSpace(env("YAKU_QUEUE_REDIS_MASTER_NAME", "")),
		QueueRedisSentinelPassword: env("YAKU_QUEUE_REDIS_SENTINEL_PASSWORD", ""),
		LogDir:                     env("YAKU_LOG_DIR", "/var/log/yakugate"),
		LogLevel:                   env("YAKU_LOG_LEVEL", "info"),
		LogMaxSizeMB:               envInt("YAKU_LOG_MAX_SIZE_MB", 100),
		LogRetentionDays:           envInt("YAKU_LOG_RETENTION_DAYS", 30),
		LogRedact:                  !strings.EqualFold(strings.TrimSpace(env("YAKU_LOG_REDACT", "on")), "off"),
		QualityWindowMin:           envInt("YAKU_QUALITY_WINDOW_MIN", 60),
		QualityMinSample:           envInt("YAKU_QUALITY_MIN_SAMPLE", 20),
	}
	if v := env("YAKU_REDIS_DB", ""); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.RedisDB = n
		}
	}
	if v := env("YAKU_QUEUE_REDIS_DB", ""); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.QueueRedisDB = n
		}
	}
	// Back-compat: if the Config store isn't set explicitly, reuse the Hot store
	// connection (older single-DB deployments kept config + records together).
	if c.ConfigDSN == "" {
		c.ConfigDSN, c.ConfigBackend = c.HotDSN, c.HotBackend
	}
	c.resolveDSNRefs()
	// A store whose backend is not set and whose DSN is a SQL Server one is
	// on SQL Server (DAL plan, step 4): an install on SQL Server need not
	// repeat "mssql" for every store. A MySQL install changes nothing.
	c.backendsFromDSNs()
	// Each store's read DSN (YAKU_<STORE>_READ_DSN): before the session time
	// zone below, so a read DSN gets it too.
	c.applyReadDSNs()
	if c.HotBackend == BackendMySQL {
		c.HotDSN = UTCSession(c.HotDSN)
		c.HotReadDSN = UTCSession(c.HotReadDSN)
	}
	// The config store too (PO 2026-09-17): its created_at/updated_at columns
	// take the SERVER clock (DEFAULT CURRENT_TIMESTAMP), and on a server set
	// to local time every ledger row, vendor-health stamp and audit line the
	// engine wrote landed five hours off what the Admin API — which reads the
	// store as UTC — showed (E2E run 2026-09-16, I-23).
	if c.ConfigBackend == BackendMySQL {
		c.ConfigDSN = UTCSession(c.ConfigDSN)
		c.ConfigReadDSN = UTCSession(c.ConfigReadDSN)
	}
	return c
}

// backendsFromDSNs puts a store on SQL Server when its backend setting is
// not set and its DSN is in the sqlserver:// form. A setting that IS set is
// never changed: SQLProblem says when it does not match its DSN.
func (c *Config) backendsFromDSNs() {
	for _, s := range []struct {
		name string
		b    *Backend
		dsn  string
	}{
		{"YAKU_CONFIG_BACKEND", &c.ConfigBackend, c.ConfigDSN},
		{"YAKU_HOT_BACKEND", &c.HotBackend, c.HotDSN},
		{"YAKU_QUEUE_BACKEND", &c.QueueBackend, c.QueueDSN},
		{"YAKU_HLR_BACKEND", &c.HLRBackend, c.HLRDSN},
	} {
		if strings.TrimSpace(os.Getenv(s.name)) == "" && IsSQLServerDSN(s.dsn) {
			*s.b = BackendMSSQL
		}
	}
}

// SQLProblem says what is wrong with the SQL stores' settings, or "": a
// store set to one database whose DSN is written for the other (a
// sqlserver:// DSN on mysql, a MySQL DSN on mssql). A role refuses to start
// on it, as on RedisProblem: it would otherwise fail later, on its first
// query, with the driver's words.
func (c Config) SQLProblem() string {
	for _, s := range []struct {
		name, dsnName string
		b             Backend
		dsn           string
	}{
		{"YAKU_CONFIG_BACKEND", "YAKU_CONFIG_DSN", c.ConfigBackend, c.ConfigDSN},
		{"YAKU_HOT_BACKEND", "YAKU_HOT_DSN", c.HotBackend, c.HotDSN},
		{"YAKU_QUEUE_BACKEND", "YAKU_QUEUE_DSN", c.QueueBackend, c.QueueDSN},
		{"YAKU_HLR_BACKEND", "YAKU_HLR_DSN", c.HLRBackend, c.HLRDSN},
	} {
		dsn := strings.TrimSpace(s.dsn)
		if dsn == "" {
			continue
		}
		b := Backend(strings.ToLower(strings.TrimSpace(string(s.b))))
		switch {
		case b == BackendMSSQL && !IsSQLServerDSN(dsn):
			return fmt.Sprintf("%s=mssql, but %s is not a SQL Server DSN: write it sqlserver://USER:PASSWORD@HOST:1433?database=NAME", s.name, s.dsnName)
		case (b == BackendMySQL || b == "") && IsSQLServerDSN(dsn):
			return fmt.Sprintf("%s is a SQL Server DSN, but %s says mysql: set %s=mssql (or remove that line)", s.dsnName, s.name, s.name)
		}
	}
	return ""
}

// UTCSession makes a MySQL DSN's sessions run in UTC (GW-39, PO 2026-09-14).
//
// The engine stamps the hot store with NOW() and column defaults
// (CURRENT_TIMESTAMP), which follow the SESSION time zone — the database
// server's local clock unless told otherwise. A DATETIME carries no offset,
// so rows written that way only mean something while the server's zone never
// changes, and every reader had to guess (the Admin API used to discover the
// server's offset and read the hot store in it; invoices billed by
// summary_mt.bucket_start moved hours between periods). Setting the session
// zone on the connection makes every write, default and NOW() comparison UTC
// at once, with no per-query changes.
//
// A DSN that already names a time_zone is left alone — an operator's explicit
// choice wins. Rows written before the switch stay in local time (PO
// 2026-09-14: pre-alpha, not converted).
func UTCSession(dsn string) string {
	if dsn == "" || strings.Contains(dsn, "time_zone=") {
		return dsn
	}
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + "time_zone=%27%2B00%3A00%27"
}

// Secrets is the process-wide secret resolver, built from YAKU_KMS_* and used for
// DSN refs here and jwt.secret_ref in the auth path. [SCOPE P1-SECRETS]
var Secrets = secrets.New(secrets.WithAudit(func(ref string, err error) {
	if err != nil {
		// The REF is safe to log (it names a location); the value never is.
		log.Printf("[secrets] resolve %s FAILED: %v", ref, err)
	}
}))

// resolveDSNRefs lets any DSN be a secret ref (env://, file://) instead of a
// literal, so database credentials need not sit in yakugate.config. A plain DSN is passed
// through untouched, so this is transparent for existing deployments.
func (c *Config) resolveDSNRefs() {
	ctx := context.Background()
	for _, p := range []struct {
		name string
		dst  *string
	}{
		{"YAKU_QUEUE_DSN", &c.QueueDSN}, {"YAKU_HLR_DSN", &c.HLRDSN}, {"YAKU_CONFIG_DSN", &c.ConfigDSN},
		{"YAKU_HOT_DSN", &c.HotDSN}, {"YAKU_COLD_DSN", &c.ColdDSN},
	} {
		if !secrets.IsRef(*p.dst) {
			continue
		}
		v, err := Secrets.ResolveString(ctx, *p.dst)
		if err != nil {
			// Leave the ref in place: connecting will fail loudly with a bad DSN
			// rather than silently starting against the wrong database.
			log.Printf("[secrets] %s is a ref that could not be resolved: %v", p.name, err)
			continue
		}
		*p.dst = v
	}
}
