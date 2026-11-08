package dal

import (
	"context"
	"fmt"

	"github.com/yakugate/yakugate/internal/platform/config"
)

// What the stores say about themselves (internal/storewatch, D-129): the
// figures any client may ask a MySQL server (SHOW GLOBAL STATUS / VARIABLES,
// InnoDB's deadlock count, replica status, the sizes of the gateway's
// databases) or a Redis (INFO, and its Sentinels' view). How the figures are
// judged stays in internal/storewatch; asking for them is here. Each probe
// has a small connection of its own, apart from the traffic's.

// ReplicaStatus is a SQL server's replica status. Known=false when the
// gateway's user may not ask (it needs REPLICATION CLIENT).
type ReplicaStatus struct {
	Known      bool   `json:"known"`
	IsReplica  bool   `json:"is_replica"`
	IORunning  bool   `json:"io_running,omitempty"`
	SQLRunning bool   `json:"sql_running,omitempty"`
	LagSec     *int64 `json:"lag_sec,omitempty"`
	LastError  string `json:"last_error,omitempty"`
}

// SQLServerProbe is one of the gateway's SQL databases, opened to ask its
// server about itself. Each read is bounded by two seconds.
type SQLServerProbe interface {
	// Addr is the server as the DSN names it ("unix:<path>" for a socket);
	// Database the database's name.
	Addr() string
	Database() string
	// Ping asks the database once.
	Ping(ctx context.Context) error
	// Global reads SHOW GLOBAL <what> (STATUS or VARIABLES) for the names
	// given; nothing when it cannot be read.
	Global(ctx context.Context, what string, names []string) map[string]string
	// Deadlocks is the server's deadlock count since start; ok=false when
	// it cannot be read.
	Deadlocks(ctx context.Context) (float64, bool)
	// Replica is the server's replica status.
	Replica(ctx context.Context) ReplicaStatus
	// SchemaSizes is each named database's size and table count.
	SchemaSizes(ctx context.Context, schemas []string) (map[string][2]int64, error)
	// Close closes the probe's connection.
	Close() error
}

// RedisServerProbe is one Redis the gateway uses, opened to ask about
// itself, with its Sentinels when the settings name them.
type RedisServerProbe interface {
	// Ping asks once; Info reads INFO.
	Ping(ctx context.Context) error
	Info(ctx context.Context) (string, error)
	// Addr is the address the client dials.
	Addr() string
	// Sentinels are the Sentinels the settings name, in order.
	Sentinels() []RedisSentinelProbe
	// Close closes the client and the Sentinels'.
	Close() error
}

// RedisSentinelProbe is one Sentinel.
type RedisSentinelProbe interface {
	// Master is what the Sentinel knows of the named master.
	Master(ctx context.Context, name string) (map[string]string, error)
}

// OpenSQLServerProbe opens a probe of one database of the kind named (the
// stores storewatch watches are MySQL). An error when the DSN cannot be read
// or no DAL of that kind asks servers.
func OpenSQLServerProbe(kind config.Backend, dsn string) (SQLServerProbe, error) {
	d, ok := Lookup(kind)
	if !ok {
		return nil, fmt.Errorf("dal: no %s DAL in this binary (have %v)", kind, Registered())
	}
	if d.SQLServerProbe == nil {
		return nil, fmt.Errorf("dal: server probe on %s: %w", kind, ErrNotServed)
	}
	return d.SQLServerProbe(dsn)
}

// OpenRedisServerProbe opens a probe of the Redis cfg names (the main one,
// or cfg.ForQueueRedis() for the queues' own).
func OpenRedisServerProbe(cfg config.Config) (RedisServerProbe, error) {
	d, ok := Lookup(config.BackendRedis)
	if !ok {
		return nil, fmt.Errorf("dal: no %s DAL in this binary (have %v)", config.BackendRedis, Registered())
	}
	if d.RedisServerProbe == nil {
		return nil, fmt.Errorf("dal: server probe on %s: %w", config.BackendRedis, ErrNotServed)
	}
	return d.RedisServerProbe(cfg)
}
