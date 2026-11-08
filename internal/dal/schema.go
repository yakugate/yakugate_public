package dal

import (
	"context"
	"fmt"

	"github.com/yakugate/yakugate/internal/platform/config"
)

// Schema is one SQL store's schema work at start (plan of 29 Oct 2026, "each
// SQL DAL owns its schema"). internal/platform/bootstrap decides: which
// stores, which migration folder, which files are still to run, when to seed,
// what to log. A Schema does the database's part: make the database, keep
// the record of applied migrations under the store's lock, run one migration
// file, keep the tables cloned from a template in step with it, seed, add the
// late keys, fill a separate HLR store.
//
// A Schema is opened for one store ("config", "queue", "hot", "hlr") and one
// DSN. Opening it reaches no database: each step opens what it needs, as the
// bootstrap always has (the seed, the late keys and the HLR copy each on a
// connection of their own), and Close closes what Connect opened.
//
// Its error messages are the ones the bootstrap has always given (they start
// "bootstrap:"), so an operator reads the same words as before.
type Schema interface {
	// MakeDatabase creates the store's database when it is missing, and
	// answers its name.
	MakeDatabase(ctx context.Context) (string, error)
	// Connect opens the store's own database for the migration steps below
	// and waits until it answers.
	Connect(ctx context.Context) error
	// TrackMigrations makes the table the applied migrations are written in
	// (schema_migrations), when it is missing.
	TrackMigrations(ctx context.Context) error
	// LockMigrations takes the store's migration lock, waiting a while for a
	// process that holds it. held is false when the wait ran out (log says
	// so); release is always safe to call.
	LockMigrations(ctx context.Context, log Printer) (release func(), held bool, err error)
	// AppliedMigrations lists what schema_migrations holds: applied files
	// ("<store>/<file>"), the statement markers of a file part-applied
	// ("<store>/<file>#<n>"), and the seed and copy markers.
	AppliedMigrations(ctx context.Context) (map[string]bool, error)
	// ApplyMigration runs one migration file (body) and records it as
	// version. Statements a marker in applied says have run are passed; a
	// statement the database's own rules allow to be skipped (a table that
	// belongs to the Admin API, an ALTER whose end state already holds) is
	// logged and passed.
	ApplyMigration(ctx context.Context, version, body string, applied map[string]bool, log Printer) error
	// SyncClones brings every table cloned from one of the store's templates
	// up to it: columns added, widened, dropped as the template was, and the
	// keys a clone must have.
	SyncClones(ctx context.Context, log Printer) error
	// Seed runs seedSQL once per marker, on a connection of its own; seeded
	// is false when the marker says it has run.
	Seed(ctx context.Context, marker, seedSQL string) (seeded bool, err error)
	// AddLateKeys adds to the clones the template keys that only make a
	// screen fast, on a small pool of its own; one process at a time does it
	// and nobody waits for the other.
	AddLateKeys(ctx context.Context, log Printer) error
	// CopyHLRFrom fills this store (a separate HLR store) from the queue
	// database at queueDSN, once, and answers the rows inserted. log may be
	// nil.
	CopyHLRFrom(ctx context.Context, queueDSN string, log Printer) (int64, error)
	// Close closes what Connect opened.
	Close() error
}

// Printer is where a Schema writes its progress lines: the bootstrap's
// logger.
type Printer interface {
	Printf(format string, args ...any)
}

// OpenSchema opens the schema work of one store on the database kind named
// (the store's backend), with the store's DSN.
func OpenSchema(cfg config.Config, on config.Backend, store, dsn string) (Schema, error) {
	d, ok := Lookup(on)
	if !ok {
		return nil, fmt.Errorf("dal: no %s DAL in this binary (have %v)", on, Registered())
	}
	if d.Schema == nil {
		return nil, fmt.Errorf("dal: schema on %s: %w", on, ErrNotServed)
	}
	return d.Schema(cfg, store, dsn)
}
