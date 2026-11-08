package dal

import (
	"context"
	"fmt"

	"github.com/yakugate/yakugate/internal/platform/config"
)

// What the console (internal/process/console) opens for itself, beside the
// config store and the queue store's vendor tables it opens with OpenConfig
// and OpenVendorQueueTables (30 Oct 2026): the live parts of the main Redis,
// the monitoring history on a handle of its own, and, on its database setup
// screen, a SQL server reached before any database exists. Every handle is
// the console's own and is closed by it.

// LiveParts is the main Redis's live balances and live view on ONE client,
// as the console held them.
type LiveParts struct {
	Balances LiveBalances
	Status   LiveStatus
	// Release closes the client.
	Release func() error
}

// Close closes the client.
func (l *LiveParts) Close() error {
	if l == nil || l.Release == nil {
		return nil
	}
	return l.Release()
}

// OpenLiveParts opens them when Redis is set; ErrNotServed when it is off.
func OpenLiveParts(cfg config.Config) (*LiveParts, error) {
	if !cfg.RedisEnabled() {
		return nil, fmt.Errorf("dal: live parts: %w", ErrNotServed)
	}
	d, ok := Lookup(config.BackendRedis)
	if !ok {
		return nil, fmt.Errorf("dal: no %s DAL in this binary (have %v)", config.BackendRedis, Registered())
	}
	if d.LiveParts == nil {
		return nil, fmt.Errorf("dal: live parts on %s: %w", config.BackendRedis, ErrNotServed)
	}
	return d.LiveParts(cfg)
}

// MonitorReader is the monitoring history read on a handle of its own.
type MonitorReader interface {
	MonitorStatements
	Close() error
}

// OpenMonitorReader opens the history in the hot store at dsn, of the kind
// named.
func OpenMonitorReader(kind config.Backend, dsn string) (MonitorReader, error) {
	d, ok := Lookup(kind)
	if !ok {
		return nil, fmt.Errorf("dal: no %s DAL in this binary (have %v)", kind, Registered())
	}
	if d.MonitorReader == nil {
		return nil, fmt.Errorf("dal: monitoring history on %s: %w", kind, ErrNotServed)
	}
	return d.MonitorReader(dsn)
}

// ServerSetup is a SQL server as the console's setup screen reaches it: the
// settings' connection strings read and written, a login to the server
// itself, the databases it lacks, and what a refusal of the server says.
type ServerSetup interface {
	// ReadDSN reads a connection string; ok=false when it cannot be read.
	ReadDSN(dsn string) (addr, user, password, database string, ok bool)
	// BuildDSN is a connection string to database ("" = the server) at
	// host:port as user, with base's other parameters kept (a base that
	// cannot be read, or "", gives the defaults), its times read as times.
	BuildDSN(base, host, port, user, password, database string) string
	// Ping logs in to dsn, within five seconds.
	Ping(ctx context.Context, dsn string) error
	// MissingDatabases lists the names the server at dsn has no database of.
	MissingDatabases(ctx context.Context, dsn string, names []string) ([]string, error)
	// Refusal is the server's own error number and words, when err is one.
	Refusal(err error) (number int, message string, ok bool)
}

// SetupOf is the setup of a SQL database kind.
func SetupOf(kind config.Backend) (ServerSetup, error) {
	d, ok := Lookup(kind)
	if !ok {
		return nil, fmt.Errorf("dal: no %s DAL in this binary (have %v)", kind, Registered())
	}
	if d.ServerSetup == nil {
		return nil, fmt.Errorf("dal: server setup on %s: %w", kind, ErrNotServed)
	}
	return d.ServerSetup(), nil
}
