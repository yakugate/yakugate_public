package dal

import (
	"context"
	"fmt"
	"os"

	"github.com/yakugate/yakugate/internal/platform/config"
)

// Backup and restore (P8, internal/process/backup): a database's tables as
// the backup role reads them into a run's files, and a target database as the
// restore loads a run's files back into it. Reading a table's rows and
// writing its file, and reading the file and inserting its rows, are one call
// each here, so the file a run holds is the database DAL's own work; the role
// keeps the schedule, the run's names, its manifest and every decision.
//
// The backup is MySQL-only (S8-08, PO 2026-09-17): the role and the restore
// open MySQL whatever backend the settings name, as they always did.

// BodySeal is how a table dump treats the message body column ("body"):
// sealed with the gateway crypto envelope when On, as it was read otherwise.
type BodySeal struct {
	// On: the gateway key is there, so plain bodies are sealed.
	On bool
	// Seal seals one non-empty body of the row-th row (counted from 1) of
	// the account given (0 when the table has no account_id). It answers
	// the body as it is when it is already sealed, and an error that leaves
	// the table out of the run.
	Seal func(ctx context.Context, row int, account int64, body string) (string, error)
	// Plain is called once per table that has a body column and is dumped
	// with On false.
	Plain func()
}

// TableDumps is one database as a backup reads it.
type TableDumps interface {
	// Reads is where one dump of the database reads from: its read instance
	// when it has one that answers, else the database itself. A dump keeps
	// one for the list of tables and every table, and asks again after the
	// database went away in the middle.
	Reads() TableReads
	// Ping asks the database (where Reads reads) for an answer.
	Ping(ctx context.Context) error
	// ConnLost says an error is the database connection going away (the
	// server restarted, refusing connections, a network cut) rather than
	// something about the table being read.
	ConnLost(err error) bool
}

// TableReads is one database's tables, read into a run's files. Every file
// is written with the mode given.
type TableReads interface {
	// Tables lists the database's base tables, in name order.
	Tables(ctx context.Context) ([]string, error)
	// DumpTable writes every row of the table to path as gzipped JSON, one
	// object per row, and says how many rows it wrote. Binary values are
	// written as {"b64": "<base64>"}, everything else as text or null.
	DumpTable(ctx context.Context, table, path string, mode os.FileMode, seal BodySeal) (int, error)
	// DumpSchema writes the table's CREATE TABLE statement to path.
	DumpSchema(ctx context.Context, table, path string, mode os.FileMode) error
	// DumpTriggers writes the database's triggers to path as JSON
	// ([{name, table, create}]); no triggers, no file.
	DumpTriggers(ctx context.Context, path string, mode os.FileMode) error
}

// Restorer is one target database a run is restored into, on one
// connection the whole restore long, with the session set for loading
// (foreign keys off, an id of 0 kept as 0, strict). Close it when done.
type Restorer interface {
	// Target is the database's name.
	Target() string
	// TableState says whether the table exists and whether it has rows.
	TableState(ctx context.Context, table string) (exists, hasRows bool, err error)
	// InsertTriggers names the table's triggers that fire on INSERT.
	InsertTriggers(ctx context.Context, table string) ([]string, error)
	// CreateTable runs a CREATE TABLE statement saved in the run.
	CreateTable(ctx context.Context, ddl string) error
	// RestoreTable loads one data file (DumpTable's form) into the table in
	// one transaction. When want is given and the rows read differ from it,
	// the table is rolled back.
	RestoreTable(ctx context.Context, table, path string, want *int64) (int64, error)
	// RestoreTriggers creates the triggers saved at path (DumpTriggers's
	// form) that the target does not have yet, and names those it created.
	RestoreTriggers(ctx context.Context, path string) ([]string, error)
	// Close lets go of the connection.
	Close() error
}

// backupOn is the database type backup and restore use for a DSN: SQL
// Server when it is in the sqlserver:// form, else MySQL, as before
// (S8-08). One install is on one SQL database (CheckOneSQL), so the DSN
// says it.
func backupOn(dsn string) config.Backend {
	if config.IsSQLServerDSN(dsn) {
		return config.BackendMSSQL
	}
	return config.BackendMySQL
}

// OpenTableDumps opens the database at dsn for a backup: the process's one
// pool on it, and its read instance when one is set.
func OpenTableDumps(dsn string) (TableDumps, error) {
	backupOn := backupOn(dsn)
	d, ok := Lookup(backupOn)
	if !ok {
		return nil, fmt.Errorf("dal: no %s DAL in this binary (have %v)", backupOn, Registered())
	}
	if d.TableDumps == nil {
		return nil, fmt.Errorf("dal: table dumps on %s: %w", backupOn, ErrNotServed)
	}
	return d.TableDumps(dsn)
}

// OpenRestorer opens the target database at dsn for a restore: a connection
// of its own, apart from any pool, with its session set for loading.
func OpenRestorer(ctx context.Context, dsn string) (Restorer, error) {
	backupOn := backupOn(dsn)
	d, ok := Lookup(backupOn)
	if !ok {
		return nil, fmt.Errorf("dal: no %s DAL in this binary (have %v)", backupOn, Registered())
	}
	if d.Restorer == nil {
		return nil, fmt.Errorf("dal: restore on %s: %w", backupOn, ErrNotServed)
	}
	return d.Restorer(ctx, dsn)
}
