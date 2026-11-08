package mysql

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	driver "github.com/go-sql-driver/mysql"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/dal/stmt"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// A store's schema work at start (dal.Schema) — moved here unchanged on 30 Oct
// 2026 (the DAL plan, "each SQL DAL owns its schema") from
// internal/platform/bootstrap (bootstrap.go, latekeys.go, hlrcopy.go). The
// bootstrap still decides which stores, which migration folder, which files
// are still to run, when to seed and what to log; this is the MySQL part:
// create the database, schema_migrations and its lock, run one file statement
// by statement, the clone tables, the seed, the late keys, the HLR copy.
//
// The error messages are the bootstrap's own, word for word ("bootstrap: …").

// SchemaStmts is every fixed statement of the schema work, by name. DDL built
// per database or per table (CREATE DATABASE, a clone's ALTER, a migration
// file's own statements, the HLR copy's pages) is built as it always was, at
// the call.
var SchemaStmts = stmt.New("mysql")

// SchemaStatements is the registry of these statements (the lint test).
func SchemaStatements() *stmt.Registry { return SchemaStmts }

var (
	stSchemaTrack = SchemaStmts.Add("schema.track_migrations", `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    VARCHAR(255) NOT NULL PRIMARY KEY,
		applied_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
	) ENGINE=InnoDB`, false)
	stSchemaApplied     = SchemaStmts.Add("schema.applied", "SELECT version FROM schema_migrations", false)
	stSchemaMark        = SchemaStmts.Add("schema.mark", "INSERT IGNORE INTO schema_migrations (version) VALUES (?)", false)
	stSchemaRecord      = SchemaStmts.Add("schema.record", "INSERT INTO schema_migrations (version) VALUES (?)", false)
	stSchemaForgetMarks = SchemaStmts.Add("schema.forget_marks", "DELETE FROM schema_migrations WHERE version LIKE ?", false)
	stSchemaSeeded      = SchemaStmts.Add("schema.seeded", "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = ?)", false)
	stSchemaDatabase    = SchemaStmts.Add("schema.database", "SELECT DATABASE()", false)
	stSchemaLock        = SchemaStmts.Add("schema.lock", "SELECT GET_LOCK(?, ?)", false)
	stSchemaRelease     = SchemaStmts.Add("schema.release", "DO RELEASE_LOCK(?)", false)
)

// The stores' schema work at start (the bootstrap's runner): schema*.go.
var _ = serves(func(d *dal.Driver) { d.Schema = openSchema })

// openSchema is the schema opener: it reaches no database (dal.Schema).
func openSchema(_ config.Config, store, dsn string) (dal.Schema, error) {
	return &schema{store: store, dsn: dsn}, nil
}

// schema is one store's schema work on MySQL.
type schema struct {
	store, dsn string
	// cfg is the store's DSN as MakeDatabase read it; db is Connect's pool.
	cfg *driver.Config
	db  *sql.DB
}

var _ dal.Schema = (*schema)(nil)

// MakeDatabase creates the store's database when it is missing.
func (s *schema) MakeDatabase(ctx context.Context) (string, error) {
	cfg, err := driver.ParseDSN(s.dsn)
	if err != nil {
		return "", fmt.Errorf("bootstrap: bad DSN for %s store: %w", s.store, err)
	}
	dbName := cfg.DBName
	if dbName == "" {
		return "", fmt.Errorf("bootstrap: %s store DSN has no database name", s.store)
	}
	if err := createDatabase(ctx, cfg, dbName); err != nil {
		return "", err
	}
	s.cfg = cfg
	return dbName, nil
}

// createDatabase runs CREATE DATABASE IF NOT EXISTS on the server.
func createDatabase(ctx context.Context, cfg *driver.Config, dbName string) error {
	server := *cfg
	server.DBName = ""
	db, err := sql.Open("mysql", server.FormatDSN())
	if err != nil {
		return fmt.Errorf("bootstrap: open server: %w", err)
	}
	defer db.Close()
	if err := waitReady(ctx, db); err != nil {
		return fmt.Errorf("bootstrap: server not reachable: %w", err)
	}
	q := "CREATE DATABASE IF NOT EXISTS `" + strings.ReplaceAll(dbName, "`", "``") +
		"` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
	if _, err := db.ExecContext(ctx, q); err != nil {
		return fmt.Errorf("bootstrap: create database %q: %w", dbName, err)
	}
	return nil
}

// Connect opens the store's database with multi-statement files allowed (a
// migration file's statement may be several, a seed is one script).
func (s *schema) Connect(ctx context.Context) error {
	cfg := s.cfg
	if cfg == nil {
		c, err := driver.ParseDSN(s.dsn)
		if err != nil {
			return fmt.Errorf("bootstrap: bad DSN for %s store: %w", s.store, err)
		}
		cfg = c
	}
	dbName := cfg.DBName
	cfg.DBName = dbName
	cfg.MultiStatements = true
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return fmt.Errorf("bootstrap: open %s: %w", dbName, err)
	}
	if err := waitReady(ctx, db); err != nil {
		db.Close()
		return fmt.Errorf("bootstrap: %s not reachable: %w", dbName, err)
	}
	s.db = db
	return nil
}

// Close closes Connect's pool.
func (s *schema) Close() error {
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

// TrackMigrations makes schema_migrations.
func (s *schema) TrackMigrations(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, stSchemaTrack.SQL); err != nil {
		return fmt.Errorf("bootstrap: create schema_migrations: %w", err)
	}
	return nil
}

// AppliedMigrations reads schema_migrations.
func (s *schema) AppliedMigrations(ctx context.Context) (map[string]bool, error) {
	return appliedVersions(ctx, s.db)
}

func appliedVersions(ctx context.Context, db *sql.DB) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx, stSchemaApplied.SQL)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: read schema_migrations: %w", err)
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out[v] = true
	}
	return out, rows.Err()
}

// Seed runs a seed script once per marker, on a connection of its own.
func (s *schema) Seed(ctx context.Context, marker, seedSQL string) (bool, error) {
	cfg, err := driver.ParseDSN(s.dsn)
	if err != nil {
		return false, fmt.Errorf("bootstrap: seed DSN: %w", err)
	}
	cfg.MultiStatements = true
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return false, fmt.Errorf("bootstrap: open seed db: %w", err)
	}
	defer db.Close()
	if err := waitReady(ctx, db); err != nil {
		return false, err
	}
	return seedIfEmpty(ctx, db, marker, seedSQL)
}

// seedIfEmpty applies an idempotent seed once per marker version (recorded in
// the target DB's schema_migrations).
func seedIfEmpty(ctx context.Context, db *sql.DB, marker, seedSQL string) (bool, error) {
	var done bool
	if err := db.QueryRowContext(ctx, stSchemaSeeded.SQL, marker).
		Scan(&done); err != nil {
		return false, fmt.Errorf("bootstrap: check seed marker %s: %w", marker, err)
	}
	if done {
		return false, nil
	}
	if _, err := db.ExecContext(ctx, seedSQL); err != nil {
		return false, fmt.Errorf("bootstrap: apply seed %s: %w", marker, err)
	}
	if _, err := db.ExecContext(ctx, stSchemaMark.SQL, marker); err != nil {
		return false, fmt.Errorf("bootstrap: record seed marker %s: %w", marker, err)
	}
	return true, nil
}

func waitReady(ctx context.Context, db *sql.DB) error {
	var err error
	for i := 0; i < 10; i++ {
		pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err = db.PingContext(pctx)
		cancel()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return err
}

// migrateLockWaitSec is how long a role waits for whoever is migrating. Long
// enough for a real migration on a populated store, short enough that a
// forgotten lock does not hang a restart for ever.
const migrateLockWaitSec = 120

// migrateLockName is the advisory lock a store's migrations take: the store
// AND its database (go-live retest of 19 Oct 2026). GET_LOCK names are per
// SERVER, so "yaku_migrate_queue" alone made every gateway with databases of
// its own on one MySQL server wait for the others: eight test rigs started
// together migrated one after another, and one took 8 minutes to come up.
// Deployments of one gateway share the database, so they still share the
// lock. MySQL caps a name at 64 characters; a database name that would pass
// it is replaced by the start of its SHA-1.
//
// Upgrade note: a binary built before this takes the name without the
// database, so an old and a new process starting together on one store do
// not wait for each other. Start the roles of a deployment on one build.
func migrateLockName(store, database string) string {
	name := "yaku_migrate_" + store
	if database == "" {
		return name
	}
	if full := name + "@" + database; len(full) <= 64 {
		return full
	}
	sum := sha1.Sum([]byte(database))
	return name + "@" + hex.EncodeToString(sum[:])[:16]
}

// LockMigrations takes the store's migration lock (migrateLock).
func (s *schema) LockMigrations(ctx context.Context, log dal.Printer) (func(), bool, error) {
	return migrateLock(ctx, s.db, s.store, log)
}

// migrateLock takes the per-store advisory lock on a PINNED connection —
// MySQL scopes GET_LOCK to a session, so a lock taken from the pool would be
// released the moment that connection went back and be useless.
//
// Returns a release function that is always safe to call, and whether the
// lock was actually obtained.
func migrateLock(ctx context.Context, db *sql.DB, store string, log dal.Printer) (func(), bool, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return func() {}, false, fmt.Errorf("bootstrap: migrate lock: %w", err)
	}
	var database sql.NullString
	if err := conn.QueryRowContext(ctx, stSchemaDatabase.SQL).Scan(&database); err != nil {
		_ = conn.Close()
		return func() {}, false, fmt.Errorf("bootstrap: migrate lock: %w", err)
	}
	name := migrateLockName(store, database.String)
	release := func() {
		// Background context: the caller's may already be cancelled, and a
		// lock left held would block every other role's start.
		_, _ = conn.ExecContext(context.Background(), stSchemaRelease.SQL, name)
		_ = conn.Close()
	}
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, stSchemaLock.SQL, name, migrateLockWaitSec).Scan(&got); err != nil {
		release()
		return func() {}, false, fmt.Errorf("bootstrap: migrate lock: %w", err)
	}
	if !got.Valid || got.Int64 != 1 {
		release()
		log.Printf("%s store: waited %ds for another process migrating", store, migrateLockWaitSec)
		return func() {}, false, nil
	}
	return release, true, nil
}
