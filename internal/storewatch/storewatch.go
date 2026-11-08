// Package storewatch reads what MySQL and Redis say about themselves, for
// monitoring (Saleem 2026-10-05, D-129: "build monitoring system for redis and
// mysql, whatever they natively produce").
//
// Nothing runs on the database machines: every figure here is one the server
// gives any client — MySQL's SHOW GLOBAL STATUS / VARIABLES and replica
// status, Redis's INFO and its Sentinels' view — plus the round trip it took to
// ask. The http-server of each deployment asks every time it reports (at most
// once a second), so the stores are seen as that deployment sees them.
//
// Every store the settings name is watched: the config, queue, hot and HLR
// databases (grouped by the MySQL server they are on) and the main Redis and
// the queues' own Redis. Each comes back with a health word — ok, warning or
// down — and the reasons, judged here once so the portal, the console and the
// events agree.
//
// Counters MySQL and Redis keep since they started become rates between two
// reads; the first read after this process starts, or after the server
// restarted, has no rates yet.
//
// Asking the servers is the DAL's (dal.SQLServerProbe, dal.RedisServerProbe:
// internal/dal/mysql and internal/dal/redis storereadings.go); judging what
// they say is here.
package storewatch

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// Rules are the limits that make a store's health a warning.
type Rules struct {
	PingMS        float64 // a round trip slower than this
	MySQLConnPct  float64 // connections in use over this share of max_connections
	RedisMemPct   float64 // memory in use over this share of maxmemory
	ReplicaLagSec float64 // a replica this far behind
}

// DefaultRules are the settings' defaults.
func DefaultRules() Rules {
	return Rules{PingMS: 100, MySQLConnPct: 80, RedisMemPct: 80, ReplicaLagSec: 30}
}

// The health words.
const (
	OK      = "ok"
	Warning = "warning"
	Down    = "down"
)

// Section is what a role publishes as `stores`.
type Section struct {
	CheckedAt string        `json:"checked_at"`
	MySQL     []MySQLServer `json:"mysql,omitempty"`
	Redis     []RedisServer `json:"redis,omitempty"`
}

// Watcher watches every store one gateway's settings name.
type Watcher struct {
	rules Rules
	mysql []*mysqlTarget
	redis []*redisTarget

	mu     sync.Mutex
	last   *Section
	lastAt time.Time
}

// New builds a Watcher for the stores cfg names. Nothing is dialled until the
// first Section.
func New(cfg config.Config, rules Rules) *Watcher {
	w := &Watcher{rules: rules}
	w.mysql = mysqlTargets(cfg)
	w.redis = redisTargets(cfg)
	return w
}

// Empty reports whether there is nothing to watch.
func (w *Watcher) Empty() bool { return w == nil || (len(w.mysql) == 0 && len(w.redis) == 0) }

// Section reads every store now — or gives the last reading when it is less
// than a second old — within about three seconds whatever the stores do.
func (w *Watcher) Section(ctx context.Context) *Section {
	if w.Empty() {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.last != nil && time.Since(w.lastAt) < time.Second {
		return w.last
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	out := &Section{CheckedAt: time.Now().UTC().Format(time.RFC3339Nano),
		MySQL: make([]MySQLServer, len(w.mysql)), Redis: make([]RedisServer, len(w.redis))}
	var wg sync.WaitGroup
	for i, t := range w.mysql {
		wg.Add(1)
		go func(i int, t *mysqlTarget) { defer wg.Done(); out.MySQL[i] = t.read(ctx, w.rules) }(i, t)
	}
	for i, t := range w.redis {
		wg.Add(1)
		go func(i int, t *redisTarget) { defer wg.Done(); out.Redis[i] = t.read(ctx, w.rules) }(i, t)
	}
	wg.Wait()
	w.last, w.lastAt = out, time.Now()
	return out
}

// Close closes every connection the watcher opened.
func (w *Watcher) Close() {
	if w == nil {
		return
	}
	for _, t := range w.mysql {
		for _, d := range t.dbs {
			_ = d.db.Close()
		}
	}
	for _, t := range w.redis {
		if t.client != nil {
			_ = t.client.Close() // its Sentinels' clients too
		}
	}
}

// Key is how a store is named in monitoring: letters, digits and _ only (it
// is a JSON path in the history).
func Key(kind, name string) string {
	b := []byte(strings.ToLower(kind + "_" + name))
	for i, c := range b {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_') {
			b[i] = '_'
		}
	}
	return string(b)
}

// judge sets a store's health from its reasons: down when it cannot be
// reached, a warning when anything is wrong, else ok.
func judge(reachable bool, reasons []string) string {
	switch {
	case !reachable:
		return Down
	case len(reasons) > 0:
		return Warning
	}
	return OK
}

func ms(d time.Duration) float64 { return round2(float64(d) / float64(time.Millisecond)) }

func round2(f float64) float64 {
	if f < 0 {
		return -float64(int64(-f*100+0.5)) / 100
	}
	return float64(int64(f*100+0.5)) / 100
}

func pct(part, whole float64) float64 {
	if whole <= 0 {
		return 0
	}
	return round2(part / whole * 100)
}

// short keeps an error to one readable line.
func short(err error) string {
	if err == nil {
		return ""
	}
	s := strings.Join(strings.Fields(err.Error()), " ")
	if len(s) > 200 {
		s = s[:200] + "…"
	}
	return s
}

func ago(sec int64) string {
	d := time.Duration(sec) * time.Second
	switch {
	case d >= time.Hour:
		return fmt.Sprintf("%dh%02dm", int(d.Hours()), int(d.Minutes())%60)
	case d >= time.Minute:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	return fmt.Sprintf("%ds", sec)
}

// ---------------------------------------------------------------- MySQL

// MySQLServer is one SQL server and the gateway's databases on it: a MySQL
// or, since 2 Nov 2026, a SQL Server (Kind). A SQL Server answers MySQL's
// names (internal/dal/mssql/storereadings.go); the section keeps its name
// "mysql" so older readers still find the servers.
type MySQLServer struct {
	Key       string    `json:"key"`
	Kind      string    `json:"kind"` // mysql | mssql
	Addr      string    `json:"addr"`
	Databases []MySQLDB `json:"databases"`
	OK        bool      `json:"ok"`
	Health    string    `json:"health"`
	Reasons   []string  `json:"reasons,omitempty"`
	Notes     []string  `json:"notes,omitempty"`
	Error     string    `json:"error,omitempty"`
	PingMS    float64   `json:"ping_ms"`

	Version   string `json:"version,omitempty"`
	UptimeSec int64  `json:"uptime_sec,omitempty"`
	ReadOnly  bool   `json:"read_only,omitempty"`

	Connections MySQLConnections `json:"connections"`
	Rates       *MySQLRates      `json:"rates,omitempty"`
	InnoDB      MySQLInnoDB      `json:"innodb"`
	Replica     MySQLReplica     `json:"replica"`
}

// MySQLDB is one of the gateway's databases on the server.
type MySQLDB struct {
	Use       string  `json:"use"` // config | queue | hot | hlr
	Name      string  `json:"name"`
	OK        bool    `json:"ok"`
	PingMS    float64 `json:"ping_ms"`
	Error     string  `json:"error,omitempty"`
	SizeBytes int64   `json:"size_bytes,omitempty"` // read every 5 minutes
	Tables    int64   `json:"tables,omitempty"`
}

// MySQLConnections: how many, and how many of the server's limit.
type MySQLConnections struct {
	Current int64   `json:"current"`
	Running int64   `json:"running"`
	Max     int64   `json:"max"`
	MaxUsed int64   `json:"max_used"`
	Pct     float64 `json:"pct"`
}

// MySQLRates are a second, between the last two reads.
type MySQLRates struct {
	Queries         float64 `json:"queries"`
	Selects         float64 `json:"selects"`
	Writes          float64 `json:"writes"`
	Slow            float64 `json:"slow"`
	AbortedConnects float64 `json:"aborted_connects"`
	Refused         float64 `json:"refused_max_connections"`
	TmpDiskTables   float64 `json:"tmp_disk_tables"`
	RowLockWaits    float64 `json:"row_lock_waits"`
	Deadlocks       float64 `json:"deadlocks"`
	BytesIn         float64 `json:"bytes_in"`
	BytesOut        float64 `json:"bytes_out"`
	// BufferPoolHitPct: reads served from memory, between the two reads; -1
	// when there were none.
	BufferPoolHitPct float64 `json:"buffer_pool_hit_pct"`
}

// MySQLInnoDB is InnoDB's memory and locks.
type MySQLInnoDB struct {
	BufferPoolBytes   int64   `json:"buffer_pool_bytes"`
	BufferPoolUsedPct float64 `json:"buffer_pool_used_pct"`
	RowLockWaitsNow   int64   `json:"row_lock_waits_now"`
	RowLockTimeAvgMS  float64 `json:"row_lock_time_avg_ms"`
}

// MySQLReplica is the server's replica status. Known=false when this user
// may not ask (it needs REPLICATION CLIENT).
type MySQLReplica = dal.ReplicaStatus

type mysqlTarget struct {
	key, addr string
	kind      config.Backend // mysql | mssql
	dbs       []*mysqlDB
	// readsOnly: the gateway only READS from this server (a read instance).
	// Being read-only is then what it is for, not a fault.
	readsOnly bool

	prev       map[string]float64
	prevAt     time.Time
	prevUptime int64
	sizesAt    time.Time
	sizes      map[string][2]int64
}

type mysqlDB struct {
	use, name string
	db        dal.SQLServerProbe
}

// sqlKind is a store's SQL database, "" when it is none of the two
// (MongoDB, or no store).
func sqlKind(b config.Backend) config.Backend {
	switch b {
	case "", config.BackendMySQL:
		return config.BackendMySQL
	case config.BackendMSSQL:
		return config.BackendMSSQL
	}
	return ""
}

// mysqlTargets groups the gateway's SQL databases — MySQL or SQL Server —
// by server (one kind per install, dal.CheckOneSQL; a server is keyed by its
// kind as well).
func mysqlTargets(cfg config.Config) []*mysqlTarget {
	type use struct {
		use, dsn string
		kind     config.Backend
	}
	var uses []use
	if k := sqlKind(cfg.ConfigBackend); k != "" {
		uses = append(uses, use{"config", cfg.ConfigDSN, k})
	}
	if k := cfg.QueueStoreSQL(); k != "" {
		uses = append(uses, use{"queue", cfg.QueueDSN, k})
	}
	if k := sqlKind(cfg.HotBackend); k != "" {
		uses = append(uses, use{"hot", cfg.HotDSN, k})
	}
	if b, d := cfg.HLRStore(); sqlKind(b) != "" && b != "" && d != "" && strings.TrimSpace(d) != strings.TrimSpace(cfg.QueueDSN) {
		uses = append(uses, use{"hlr", d, b})
	}
	// The read DSN beside each store (YAKU_<STORE>_READ_DSN, one per store,
	// 2026-10-13): watched as a server of its own, by its own
	// address — it is the one the reads go to, and the one that can fall
	// behind.
	// A read DSN is of its store's kind.
	kindOf := map[string]config.Backend{}
	for _, u := range uses {
		kindOf[u.use] = u.kind
	}
	for _, rd := range []use{{"config (read)", cfg.ConfigReadDSN, kindOf["config"]}, {"queue (read)", cfg.QueueReadDSN, kindOf["queue"]},
		{"hot (read)", cfg.HotReadDSN, kindOf["hot"]}, {"hlr (read)", cfg.HLRReadDSN, kindOf["hlr"]}} {
		if strings.TrimSpace(rd.dsn) != "" && rd.kind != "" {
			uses = append(uses, rd)
		}
	}
	byAddr := map[string]*mysqlTarget{}
	var out []*mysqlTarget
	for _, u := range uses {
		if strings.TrimSpace(u.dsn) == "" {
			continue
		}
		// The server's address from the DSN, and a small pool of its own
		// (two connections): the DAL's probe. A DSN that cannot be read is
		// left out.
		db, err := dal.OpenSQLServerProbe(u.kind, u.dsn)
		if err != nil {
			continue
		}
		addr := db.Addr()
		t := byAddr[string(u.kind)+"|"+addr]
		if t == nil {
			t = &mysqlTarget{key: Key(string(u.kind), addr), addr: addr, kind: u.kind}
			byAddr[string(u.kind)+"|"+addr] = t
			out = append(out, t)
		}
		t.dbs = append(t.dbs, &mysqlDB{use: u.use, name: db.Database(), db: db})
	}
	// A server the gateway only reads from: every database it holds for us
	// is a read instance's.
	for _, t := range out {
		t.readsOnly = len(t.dbs) > 0
		for _, d := range t.dbs {
			if !strings.HasSuffix(d.use, "(read)") {
				t.readsOnly = false
			}
		}
	}
	return out
}

var statusNames = []string{"Threads_connected", "Threads_running", "Max_used_connections", "Aborted_connects",
	"Connection_errors_max_connections", "Questions", "Com_select", "Com_insert", "Com_update", "Com_delete",
	"Com_replace", "Slow_queries", "Created_tmp_disk_tables", "Bytes_received", "Bytes_sent",
	"Innodb_buffer_pool_read_requests", "Innodb_buffer_pool_reads", "Innodb_buffer_pool_pages_data",
	"Innodb_buffer_pool_pages_total", "Innodb_row_lock_waits", "Innodb_row_lock_current_waits",
	"Innodb_row_lock_time_avg", "Uptime"}

var variableNames = []string{"max_connections", "version", "read_only", "super_read_only", "innodb_buffer_pool_size"}

func (t *mysqlTarget) read(ctx context.Context, r Rules) MySQLServer {
	s := MySQLServer{Key: t.key, Kind: string(t.kind), Addr: t.addr, Replica: MySQLReplica{}}
	var talk dal.SQLServerProbe
	var firstErr error
	for _, d := range t.dbs {
		c, cancel := context.WithTimeout(ctx, 2*time.Second)
		start := time.Now()
		err := d.db.Ping(c)
		cancel()
		row := MySQLDB{Use: d.use, Name: d.name, OK: err == nil, PingMS: ms(time.Since(start)), Error: short(err)}
		if err == nil {
			if talk == nil {
				talk = d.db
				s.PingMS = row.PingMS
			}
		} else if firstErr == nil {
			firstErr = err
		}
		s.Databases = append(s.Databases, row)
	}
	if talk == nil {
		s.Error = short(firstErr)
		s.Health = judge(false, nil)
		s.Reasons = []string{"not reachable: " + s.Error}
		return s
	}
	s.OK = true
	status := talk.Global(ctx, "STATUS", statusNames)
	vars := talk.Global(ctx, "VARIABLES", variableNames)
	now := time.Now()

	s.Version = vars["version"]
	s.ReadOnly = vars["read_only"] == "ON" || vars["read_only"] == "1" || vars["super_read_only"] == "ON"
	num := func(k string) float64 { return parseNum(status[k]) }
	s.UptimeSec = int64(num("Uptime"))
	s.Connections = MySQLConnections{Current: int64(num("Threads_connected")), Running: int64(num("Threads_running")),
		Max: int64(parseNum(vars["max_connections"])), MaxUsed: int64(num("Max_used_connections"))}
	s.Connections.Pct = pct(float64(s.Connections.Current), float64(s.Connections.Max))
	s.InnoDB = MySQLInnoDB{BufferPoolBytes: int64(parseNum(vars["innodb_buffer_pool_size"])),
		BufferPoolUsedPct: pct(num("Innodb_buffer_pool_pages_data"), num("Innodb_buffer_pool_pages_total")),
		RowLockWaitsNow:   int64(num("Innodb_row_lock_current_waits")), RowLockTimeAvgMS: num("Innodb_row_lock_time_avg")}

	// Rates since the last read.
	cur := map[string]float64{}
	for _, k := range statusNames {
		cur[k] = num(k)
	}
	if d, ok := talk.Deadlocks(ctx); ok {
		cur["deadlocks"] = d
	}
	restarted := t.prev != nil && s.UptimeSec < t.prevUptime
	if t.prev != nil && !restarted {
		el := now.Sub(t.prevAt).Seconds()
		if el > 0 {
			rate := func(k string) float64 { return round2(max0(cur[k]-t.prev[k]) / el) }
			s.Rates = &MySQLRates{Queries: rate("Questions"), Selects: rate("Com_select"),
				Writes:          round2(max0(cur["Com_insert"]+cur["Com_update"]+cur["Com_delete"]+cur["Com_replace"]-t.prev["Com_insert"]-t.prev["Com_update"]-t.prev["Com_delete"]-t.prev["Com_replace"]) / el),
				Slow:            rate("Slow_queries"),
				AbortedConnects: rate("Aborted_connects"), Refused: rate("Connection_errors_max_connections"),
				TmpDiskTables: rate("Created_tmp_disk_tables"), RowLockWaits: rate("Innodb_row_lock_waits"),
				Deadlocks: rate("deadlocks"), BytesIn: rate("Bytes_received"), BytesOut: rate("Bytes_sent"),
				BufferPoolHitPct: -1}
			req := cur["Innodb_buffer_pool_read_requests"] - t.prev["Innodb_buffer_pool_read_requests"]
			if req > 0 {
				s.Rates.BufferPoolHitPct = round2(100 - max0(cur["Innodb_buffer_pool_reads"]-t.prev["Innodb_buffer_pool_reads"])/req*100)
			}
		}
	}
	t.prev, t.prevAt, t.prevUptime = cur, now, s.UptimeSec

	s.Replica = talk.Replica(ctx)
	t.readSizes(ctx, talk, &s)

	var reasons []string
	for _, d := range s.Databases {
		if !d.OK {
			reasons = append(reasons, fmt.Sprintf("the %s database (%s) is not reachable: %s", d.Use, d.Name, d.Error))
		}
	}
	if s.PingMS > r.PingMS {
		reasons = append(reasons, fmt.Sprintf("slow to answer: %.0f ms (limit %.0f ms)", s.PingMS, r.PingMS))
	}
	if s.Connections.Max > 0 && s.Connections.Pct > r.MySQLConnPct {
		reasons = append(reasons, fmt.Sprintf("%d of %d connections in use (%.0f%%)", s.Connections.Current, s.Connections.Max, s.Connections.Pct))
	}
	if s.Rates != nil && s.Rates.Refused > 0 {
		reasons = append(reasons, "refusing connections: max_connections reached")
	}
	switch {
	case s.ReadOnly && t.readsOnly:
		s.Notes = append(s.Notes, "a read instance: read-only, as it should be")
	case s.ReadOnly:
		reasons = append(reasons, "the server is read-only — the gateway's writes fail")
	case t.readsOnly:
		s.Notes = append(s.Notes, "a read instance (it also takes writes: the gateway sends it none)")
	}
	if rp := s.Replica; rp.IsReplica {
		switch {
		case !rp.IORunning || !rp.SQLRunning:
			reasons = append(reasons, "replication stopped"+suffix(rp.LastError))
		case rp.LagSec != nil && float64(*rp.LagSec) > r.ReplicaLagSec:
			reasons = append(reasons, fmt.Sprintf("replica %ds behind its source", *rp.LagSec))
		}
	}
	if restarted || (s.UptimeSec > 0 && s.UptimeSec < 600) {
		s.Notes = append(s.Notes, "restarted "+ago(s.UptimeSec)+" ago")
	}
	if s.Rates != nil && s.Rates.Deadlocks > 0 {
		s.Notes = append(s.Notes, fmt.Sprintf("deadlocks: %.2f a second", s.Rates.Deadlocks))
	}
	if !s.Replica.Known {
		if t.kind == config.BackendMSSQL {
			s.Notes = append(s.Notes, "replica status unknown: the gateway's SQL Server login lacks VIEW SERVER PERFORMANCE STATE")
		} else {
			s.Notes = append(s.Notes, "replica status unknown: the gateway's MySQL user lacks REPLICATION CLIENT")
		}
	}
	s.Reasons = reasons
	s.Health = judge(true, reasons)
	return s
}

func suffix(s string) string {
	if s == "" {
		return ""
	}
	return ": " + s
}

func max0(f float64) float64 {
	if f < 0 {
		return 0
	}
	return f
}

// readSizes fills each database's size, read again every 5 minutes (the
// information schema is not free on a store with thousands of tables).
func (t *mysqlTarget) readSizes(ctx context.Context, db dal.SQLServerProbe, s *MySQLServer) {
	if time.Since(t.sizesAt) >= 5*time.Minute {
		var names []string
		for _, d := range t.dbs {
			names = append(names, d.name)
		}
		if sizes, err := db.SchemaSizes(ctx, names); err == nil {
			t.sizes, t.sizesAt = sizes, time.Now()
		}
	}
	for i := range s.Databases {
		if sz, ok := t.sizes[s.Databases[i].Name]; ok {
			s.Databases[i].SizeBytes, s.Databases[i].Tables = sz[0], sz[1]
		}
	}
}

// ---------------------------------------------------------------- Redis

// RedisServer is one Redis the gateway uses: the main one, the queues' own,
// or one serving both.
type RedisServer struct {
	Key     string   `json:"key"`
	Uses    []string `json:"uses"`  // main | queues
	Where   string   `json:"where"` // as the settings name it
	Addr    string   `json:"addr,omitempty"`
	OK      bool     `json:"ok"`
	Health  string   `json:"health"`
	Reasons []string `json:"reasons,omitempty"`
	Notes   []string `json:"notes,omitempty"`
	Error   string   `json:"error,omitempty"`
	PingMS  float64  `json:"ping_ms"`

	Version        string `json:"version,omitempty"`
	Role           string `json:"role,omitempty"`
	UptimeSec      int64  `json:"uptime_sec,omitempty"`
	Clients        int64  `json:"clients"`
	BlockedClients int64  `json:"blocked_clients"`
	Keys           int64  `json:"keys"`
	Expires        int64  `json:"expires"`

	Memory      RedisMemory      `json:"memory"`
	Rates       *RedisRates      `json:"rates,omitempty"`
	OpsPerSec   float64          `json:"ops_per_sec"`
	Persistence RedisPersistence `json:"persistence"`
	Replication RedisReplication `json:"replication"`
	Sentinel    *RedisSentinel   `json:"sentinel,omitempty"`
}

// RedisMemory is Redis's memory and its limit.
type RedisMemory struct {
	Used          int64   `json:"used"`
	RSS           int64   `json:"rss"`
	Peak          int64   `json:"peak"`
	Max           int64   `json:"max"` // 0 = no limit
	SystemTotal   int64   `json:"system_total"`
	Pct           float64 `json:"pct"` // of max, else of the machine
	Policy        string  `json:"policy"`
	Fragmentation float64 `json:"fragmentation"`
}

// RedisRates are a second, between the last two reads.
type RedisRates struct {
	Commands   float64 `json:"commands"`
	Evicted    float64 `json:"evicted"`
	Expired    float64 `json:"expired"`
	Rejected   float64 `json:"rejected_connections"`
	NetInKbps  float64 `json:"net_in_kbps"`
	NetOutKbps float64 `json:"net_out_kbps"`
	CPUPct     float64 `json:"cpu_pct"`
	HitPct     float64 `json:"hit_pct"` // -1 when there were no lookups
}

// RedisPersistence is whether Redis is saving to disk, and how that went.
type RedisPersistence struct {
	AOF              bool  `json:"aof"`
	AOFOK            bool  `json:"aof_ok"`
	RDBOK            bool  `json:"rdb_ok"`
	ChangesSinceSave int64 `json:"changes_since_save"`
	LastSaveAgeSec   int64 `json:"last_save_age_sec"`
}

// RedisReplication is this Redis's replicas (or, on a replica, its link).
type RedisReplication struct {
	Replicas   int    `json:"replicas"`
	MaxLagSec  int64  `json:"max_lag_sec"`
	MasterLink string `json:"master_link,omitempty"`
}

// RedisSentinel is what the Sentinels say about the master.
type RedisSentinel struct {
	MasterName string `json:"master_name"`
	Master     string `json:"master,omitempty"`
	Flags      string `json:"flags,omitempty"`
	Replicas   int    `json:"replicas"`
	Sentinels  int    `json:"sentinels"` // the master's count (others + the one asked)
	Quorum     int    `json:"quorum"`
	Reachable  int    `json:"reachable"`
	Of         int    `json:"of"`
}

type redisTarget struct {
	key, where string
	uses       []string
	queues     bool
	client     dal.RedisServerProbe
	sentinels  []dal.RedisSentinelProbe
	masterName string

	prev       map[string]float64
	prevAt     time.Time
	prevUptime int64
}

func redisTargets(cfg config.Config) []*redisTarget {
	var out []*redisTarget
	add := func(c config.Config, key string, uses []string) {
		// The Redis c names and, under Sentinel, each Sentinel: the DAL's
		// probe (a client of its own, as FromConfig gives).
		client, err := dal.OpenRedisServerProbe(c)
		if err != nil {
			return
		}
		t := &redisTarget{key: Key("redis", key), where: c.RedisWhere(), uses: uses, client: client}
		for _, u := range uses {
			if u == "queues" {
				t.queues = true
			}
		}
		if c.RedisSentinel() {
			t.masterName = c.RedisMasterName
			t.sentinels = client.Sentinels()
		}
		out = append(out, t)
	}
	if cfg.RedisEnabled() {
		uses := []string{"main"}
		if cfg.QueuesInRedis() && !cfg.QueueRedisSeparate() {
			uses = append(uses, "queues")
		}
		add(cfg, "main", uses)
	}
	if cfg.QueueRedisSeparate() {
		add(cfg.ForQueueRedis(), "queues", []string{"queues"})
	}
	return out
}

func (t *redisTarget) read(ctx context.Context, r Rules) RedisServer {
	s := RedisServer{Key: t.key, Uses: t.uses, Where: t.where}
	if len(t.sentinels) > 0 {
		s.Sentinel = t.sentinelView(ctx)
		if s.Sentinel.Master != "" {
			s.Addr = s.Sentinel.Master
		}
	}
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	start := time.Now()
	if err := t.client.Ping(c); err != nil {
		s.Error = short(err)
		s.Health = Down
		s.Reasons = []string{"not reachable: " + s.Error}
		return s
	}
	s.PingMS = ms(time.Since(start))
	s.OK = true
	raw, err := t.client.Info(c)
	if err != nil {
		s.Reasons = []string{"INFO refused: " + short(err)}
		s.Health = Warning
		return s
	}
	info := parseInfo(raw)
	now := time.Now()
	n := func(k string) float64 { return parseNum(info[k]) }
	s.Version, s.Role = info["redis_version"], info["role"]
	s.UptimeSec = int64(n("uptime_in_seconds"))
	s.Clients, s.BlockedClients = int64(n("connected_clients")), int64(n("blocked_clients"))
	s.OpsPerSec = n("instantaneous_ops_per_sec")
	s.Keys, s.Expires = keyspace(info)
	s.Memory = RedisMemory{Used: int64(n("used_memory")), RSS: int64(n("used_memory_rss")), Peak: int64(n("used_memory_peak")),
		Max: int64(n("maxmemory")), SystemTotal: int64(n("total_system_memory")), Policy: info["maxmemory_policy"],
		Fragmentation: round2(n("mem_fragmentation_ratio"))}
	if s.Memory.Max > 0 {
		s.Memory.Pct = pct(float64(s.Memory.Used), float64(s.Memory.Max))
	} else {
		s.Memory.Pct = pct(float64(s.Memory.Used), float64(s.Memory.SystemTotal))
	}
	s.Persistence = RedisPersistence{AOF: info["aof_enabled"] == "1", AOFOK: info["aof_last_write_status"] != "err" && info["aof_last_bgrewrite_status"] != "err",
		RDBOK: info["rdb_last_bgsave_status"] != "err", ChangesSinceSave: int64(n("rdb_changes_since_last_save"))}
	if t := n("rdb_last_save_time"); t > 0 {
		s.Persistence.LastSaveAgeSec = int64(now.Unix()) - int64(t)
	}
	s.Replication = replication(info, now)
	if s.Addr == "" {
		s.Addr = t.client.Addr()
	}

	cur := map[string]float64{"total_commands_processed": n("total_commands_processed"), "evicted_keys": n("evicted_keys"),
		"expired_keys": n("expired_keys"), "rejected_connections": n("rejected_connections"),
		"keyspace_hits": n("keyspace_hits"), "keyspace_misses": n("keyspace_misses"),
		"cpu": n("used_cpu_sys") + n("used_cpu_user")}
	restarted := t.prev != nil && s.UptimeSec < t.prevUptime
	if t.prev != nil && !restarted {
		el := now.Sub(t.prevAt).Seconds()
		if el > 0 {
			rate := func(k string) float64 { return round2(max0(cur[k]-t.prev[k]) / el) }
			s.Rates = &RedisRates{Commands: rate("total_commands_processed"), Evicted: rate("evicted_keys"),
				Expired: rate("expired_keys"), Rejected: rate("rejected_connections"),
				NetInKbps: round2(n("instantaneous_input_kbps")), NetOutKbps: round2(n("instantaneous_output_kbps")),
				CPUPct: round2(max0(cur["cpu"]-t.prev["cpu"]) / el * 100), HitPct: -1}
			hits := cur["keyspace_hits"] - t.prev["keyspace_hits"]
			miss := cur["keyspace_misses"] - t.prev["keyspace_misses"]
			if hits+miss > 0 {
				s.Rates.HitPct = round2(hits / (hits + miss) * 100)
			}
		}
	}
	t.prev, t.prevAt, t.prevUptime = cur, now, s.UptimeSec

	var reasons []string
	if s.PingMS > r.PingMS {
		reasons = append(reasons, fmt.Sprintf("slow to answer: %.0f ms (limit %.0f ms)", s.PingMS, r.PingMS))
	}
	if s.Role != "" && s.Role != "master" {
		reasons = append(reasons, "this is a replica — the gateway's writes fail ("+s.Role+")")
	}
	if s.Memory.Max > 0 && s.Memory.Pct > r.RedisMemPct {
		reasons = append(reasons, fmt.Sprintf("memory %.0f%% of its limit (%s of %s)", s.Memory.Pct, bytesText(s.Memory.Used), bytesText(s.Memory.Max)))
	}
	if s.Rates != nil && s.Rates.Evicted > 0 {
		reasons = append(reasons, fmt.Sprintf("evicting keys: %.1f a second", s.Rates.Evicted))
	}
	if t.queues && s.Memory.Policy != "" && s.Memory.Policy != "noeviction" {
		reasons = append(reasons, "holds the queues but maxmemory-policy is "+s.Memory.Policy+" — must be noeviction")
	}
	if !s.Persistence.AOFOK || !s.Persistence.RDBOK {
		reasons = append(reasons, "saving to disk failed")
	}
	if s.Rates != nil && s.Rates.Rejected > 0 {
		reasons = append(reasons, "refusing connections: maxclients reached")
	}
	if s.Replication.MasterLink == "down" {
		reasons = append(reasons, "replica link to its master is down")
	}
	if sn := s.Sentinel; sn != nil && sn.Of > 0 && sn.Quorum > 0 && sn.Reachable < sn.Quorum {
		reasons = append(reasons, fmt.Sprintf("only %d of %d Sentinels answer (quorum %d) — a failover cannot happen", sn.Reachable, sn.Of, sn.Quorum))
	}
	if t.queues && !s.Persistence.AOF {
		s.Notes = append(s.Notes, "AOF is off: queued messages since the last save are lost if Redis stops")
	}
	if restarted || (s.UptimeSec > 0 && s.UptimeSec < 600) {
		s.Notes = append(s.Notes, "restarted "+ago(s.UptimeSec)+" ago")
	}
	if s.Memory.Max == 0 {
		s.Notes = append(s.Notes, "no maxmemory set: memory is shown as a share of the machine")
	}
	s.Reasons = reasons
	s.Health = judge(true, reasons)
	return s
}

// sentinelView asks the Sentinels, first that answers, what they know.
func (t *redisTarget) sentinelView(ctx context.Context) *RedisSentinel {
	v := &RedisSentinel{MasterName: t.masterName, Of: len(t.sentinels)}
	for _, sc := range t.sentinels {
		c, cancel := context.WithTimeout(ctx, 2*time.Second)
		m, err := sc.Master(c, t.masterName)
		cancel()
		if err != nil {
			continue
		}
		v.Reachable++
		if v.Master == "" {
			v.Master = m["ip"] + ":" + m["port"]
			v.Flags = m["flags"]
			v.Replicas = int(parseNum(m["num-slaves"]))
			v.Sentinels = int(parseNum(m["num-other-sentinels"])) + 1
			v.Quorum = int(parseNum(m["quorum"]))
		}
	}
	return v
}

// parseInfo turns INFO's "key:value" lines into a map.
func parseInfo(raw string) map[string]string {
	out := map[string]string{}
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || line[0] == '#' {
			continue
		}
		if k, v, ok := strings.Cut(line, ":"); ok {
			out[k] = v
		}
	}
	return out
}

// keyspace adds up "db0:keys=1,expires=0,avg_ttl=0" lines.
func keyspace(info map[string]string) (keys, expires int64) {
	for k, v := range info {
		if !strings.HasPrefix(k, "db") {
			continue
		}
		f := fields(v)
		keys += int64(parseNum(f["keys"]))
		expires += int64(parseNum(f["expires"]))
	}
	return
}

// replication reads connected_slaves and each "slaveN:ip=…,lag=N" line.
func replication(info map[string]string, now time.Time) RedisReplication {
	r := RedisReplication{Replicas: int(parseNum(info["connected_slaves"])), MasterLink: info["master_link_status"]}
	keys := make([]string, 0)
	for k := range info {
		if strings.HasPrefix(k, "slave") && k != "slave_read_only" && k != "slave_repl_offset" && k != "slave_priority" && k != "slave_read_repl_offset" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		f := fields(info[k])
		if lag := int64(parseNum(f["lag"])); lag > r.MaxLagSec {
			r.MaxLagSec = lag
		}
	}
	return r
}

func fields(v string) map[string]string {
	out := map[string]string{}
	for _, kv := range strings.Split(v, ",") {
		if k, val, ok := strings.Cut(kv, "="); ok {
			out[k] = val
		}
	}
	return out
}

func parseNum(s string) float64 {
	s = strings.TrimSpace(s)
	var f float64
	if _, err := fmt.Sscanf(s, "%g", &f); err != nil {
		return 0
	}
	return f
}

func bytesText(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// RulesFromEnv reads the limits from the settings (yakugate.config or the
// environment), each falling back to its default.
func RulesFromEnv() Rules {
	r := DefaultRules()
	read := func(name string, into *float64) {
		if v := strings.TrimSpace(os.Getenv(name)); v != "" {
			if f := parseNum(v); f > 0 {
				*into = f
			}
		}
	}
	read("YAKU_MONITOR_STORE_PING_MS", &r.PingMS)
	read("YAKU_MONITOR_MYSQL_CONN_PCT", &r.MySQLConnPct)
	read("YAKU_MONITOR_REDIS_MEM_PCT", &r.RedisMemPct)
	read("YAKU_MONITOR_REPLICA_LAG_SEC", &r.ReplicaLagSec)
	return r
}
