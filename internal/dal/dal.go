// Package dal is the gateway's data access layer (plan of 29 Oct 2026,
// branch 1.1.0-beta): the one door between the engine and its databases.
//
// The interfaces here say what the data is (queues, records, billing,
// limits, …), never which database holds it. Each database type has its own
// DAL package that implements them, written for that database only:
//
//	internal/dal/mysql   today's MySQL code
//	internal/dal/redis   today's Redis code
//	internal/dal/mssql   SQL Server 2022 (step 4)
//
// A DAL package registers itself at init (Register), the way database/sql
// drivers do, and Open picks one DAL per area ONCE at start: Redis for what
// Redis serves when the settings put that area in Redis, else the install's
// SQL database. A role never asks which database is under it, and nothing
// outside internal/dal imports a database client (imports_test.go).
//
// The areas move in one by one (step 2); an area not moved yet is still
// reached the old way, and imports_test.go lists the packages that do.
package dal

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/yakugate/yakugate/internal/platform/config"
)

// ErrNotServed is what an opener answers when its database cannot serve an
// area in the settings it was given.
var ErrNotServed = errors.New("dal: area not served by this database")

// Driver is one database type's DAL: a name and one opener per area. A nil
// opener means the database does not serve that area (Redis keeps no
// config, for one). An opener builds a fresh handle; the caller closes it.
type Driver struct {
	Name config.Backend

	// Queues opens the queue family: the vendor queues, the client receipt
	// and MO queues, the SMPP part ids and the multi-part buffer.
	Queues func(cfg config.Config) (Queues, error)

	// Counters opens the shared counters and blocks (counters.go) on one
	// connection: rate limits, usage, login ladders, the duplicate check,
	// the campaign window, the abuse counters. An area it does not keep is
	// left nil.
	Counters func(cfg config.Config) (*Counters, error)

	// BillingStore opens billing's part of the SQL database: the balance of
	// record, the usage books, prices and terms (billing.go).
	BillingStore func(cfg config.Config) (BillingStore, error)
	// LiveBalances opens the live balance counters (the main Redis).
	LiveBalances func(cfg config.Config) (LiveBalances, error)
	// BillingQueue opens a deployment's billing queues: on the queue Redis,
	// or (main) the main Redis's queues of earlier builds.
	BillingQueue func(cfg config.Config, main bool) (BillingQueue, error)

	// Config opens the config store as provision writes it (config.go): a
	// handle of its own on YAKU_CONFIG_DSN, closed by the caller.
	Config func(cfg config.Config) (ConfigStore, error)
	// VendorQueueTables opens the queue store's vendor tables (config.go).
	VendorQueueTables func(cfg config.Config) (VendorQueueTables, error)
	// ReferenceData opens the reference cache's reads of the config store
	// at dsn (reference.go): the process's one pool, and its read instance
	// when one is set.
	ReferenceData func(dsn string) (ReferenceData, error)
	// HLRNumbers opens the HLR store at dsn (reference.go): the number
	// shards and the token revocations.
	HLRNumbers func(dsn string) (HLRNumbers, error)

	// RecordLanes opens a deployment's record queue, the DB worker's lists
	// (records.go): on the queue Redis.
	RecordLanes func(cfg config.Config, owner string) (RecordLanes, error)
	// Receipts opens the receipt facts and a deployment's receipt
	// write-behind list on one connection (receipts.go): the main Redis.
	Receipts func(cfg config.Config, owner string) (*Receipts, error)
	// HandOffs opens the lists receipts and MO wait on for another gateway,
	// in the main Redis, with this gateway's queue Redis when its queues are
	// there (receipts.go).
	HandOffs func(cfg config.Config) (HandOffs, error)
	// ConcatBuffer opens the buffer a long message's parts wait in, MT or
	// MO (concat.go): the main Redis, or the queue database.
	ConcatBuffer func(cfg config.Config, mo bool) (ConcatBuffer, error)
	// VendorQuality opens the counts quality routing reads (traffic.go): the
	// main Redis, or the hot store.
	VendorQuality func(cfg config.Config) (VendorQuality, error)
	// TrafficConfig opens the small parts of the config store the traffic
	// roles read and write beside their main work (traffic.go).
	TrafficConfig func(cfg config.Config) (*TrafficConfig, error)

	// MonitorHistory opens the monitoring history on a small pool of its own
	// (monitoring.go): the hot store.
	MonitorHistory func(cfg config.Config) (MonitorHistory, error)
	// LiveStatus opens the live view (monitoring.go): the main Redis.
	LiveStatus func(cfg config.Config) (LiveStatus, error)
	// SQLServerProbe opens one database to ask its server about itself
	// (storereadings.go); RedisServerProbe the Redis cfg names.
	SQLServerProbe   func(dsn string) (SQLServerProbe, error)
	RedisServerProbe func(cfg config.Config) (RedisServerProbe, error)

	// Leases opens the named leases and the register of gateway-wide roles
	// beside them (locks.go): the config store.
	Leases func(cfg config.Config) (*Leases, error)
	// EngineConfig opens the config store's parts the shared engine
	// packages read (engineconfig.go): send settings, receipt policies,
	// change marks, inbound doors, HLR ranges and providers, the audit
	// trail, the log settings.
	EngineConfig func(cfg config.Config) (*EngineConfig, error)
	// HLRShardWriter opens the HLR store at dsn for live answers' writes;
	// HLRShards opens it for the HLR cache's whole reads; HLRCopies opens
	// the numbers' copies (the main Redis). hlrstore.go.
	HLRShardWriter func(dsn string) (HLRShardWriter, error)
	HLRShards      func(dsn string) (HLRShards, error)
	HLRCopies      func(cfg config.Config) (HLRCopies, error)

	// Schema opens one store's schema work at start (schema.go): its
	// database, its migrations, its clone tables, its seed. store is
	// "config", "queue", "hot" or "hlr"; dsn is that store's.
	Schema func(cfg config.Config, store, dsn string) (Schema, error)

	// TableDumps opens a database at dsn as a backup reads it, on the
	// process's one pool and its read instance; Restorer opens a target
	// database at dsn for a restore, on a connection of its own
	// (backup.go).
	TableDumps func(dsn string) (TableDumps, error)
	Restorer   func(ctx context.Context, dsn string) (Restorer, error)

	// AbuseData opens the abuse monitor's data (abuse.go): the config and
	// hot stores' pools of this process, and the hot store's read instance.
	AbuseData func(cfg config.Config) (*AbuseData, error)

	// Retention opens the retention role's upkeep of the config store and
	// the two servers it compares (roleconfig.go).
	Retention func(cfg config.Config) (*Retention, error)
	// PollTurns opens the HTTP vendors' poll turns (roleconfig.go): the
	// main Redis.
	PollTurns func(cfg config.Config) (PollTurns, error)

	// What the console opens for itself (console.go): the main Redis's live
	// parts on one client, the monitoring history on a handle of its own,
	// and a SQL server's setup.
	LiveParts     func(cfg config.Config) (*LiveParts, error)
	MonitorReader func(dsn string) (MonitorReader, error)
	ServerSetup   func() ServerSetup

	// BalanceBook opens the balance of record and the billing queue kept in
	// one SQL database, for an install with no Redis (billing.go): a balance
	// move and its money events are one transaction.
	BalanceBook func(cfg config.Config) (BalanceBook, error)

	// RecordStore opens the record store (recordstore.go): the hot store's
	// records of every message, on the process's one pool for YAKU_HOT_DSN.
	RecordStore func(cfg config.Config) (RecordStore, error)
	// RecordFault reads the error of a failed record write when it is this
	// database's own (mine): whether it is about the values of the row
	// (row), and whether it is a duplicate key (dup). RecordAway and
	// RecordDuplicate ask each DAL; nil = this DAL's errors are not known
	// (a record write that fails with one counts as the database away).
	RecordFault func(err error) (mine, row, dup bool)
	// DataKeys opens the wrapped data keys and the KMS rows of the config
	// store (datakeys.go), on its one pool.
	DataKeys func(cfg config.Config) (DataKeys, error)
	// VendorSessions opens the HTTP vendors' login sessions in the HLR store
	// at dsn (datakeys.go).
	VendorSessions func(dsn string) (VendorSessions, error)
}

var (
	mu      sync.RWMutex
	drivers = map[config.Backend]Driver{}
)

// Register makes a database type's DAL known. Called from the DAL
// package's init; a second registration under one name is a programming
// error.
func Register(d Driver) {
	mu.Lock()
	defer mu.Unlock()
	if d.Name == "" {
		panic("dal: Register with no name")
	}
	if _, dup := drivers[d.Name]; dup {
		panic(fmt.Sprintf("dal: %s registered twice", d.Name))
	}
	drivers[d.Name] = d
}

// Lookup returns the registered DAL of a database type.
func Lookup(name config.Backend) (Driver, bool) {
	mu.RLock()
	defer mu.RUnlock()
	d, ok := drivers[name]
	return d, ok
}

// Registered names the DAL packages linked into this binary, sorted.
func Registered() []config.Backend {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]config.Backend, 0, len(drivers))
	for n := range drivers {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
