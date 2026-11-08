package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	mssql "github.com/microsoft/go-mssqldb"
	"github.com/microsoft/go-mssqldb/msdsn"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/dal/stmt"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// A store's schema work at start on SQL Server 2022 (dal.Schema; plan of 29
// Oct 2026, step 4, "each SQL DAL owns its schema"). Made like
// internal/dal/mysql/schema*.go: the bootstrap decides which stores, which
// migration folder (migrations/<store>/mssql), which files are still to run,
// when to seed and what to log; this is the SQL Server part — create the
// database (READ_COMMITTED_SNAPSHOT on; the queue database also gets its
// memory-optimized filegroup), schema_migrations and its lock
// (sp_getapplock), run one file batch by batch (GO lines), the clone tables,
// the seed, the late keys, the HLR copy.
//
// The error messages are the bootstrap's own words ("bootstrap: …"), as on
// MySQL, so an operator reads the same thing on either database.

// SchemaStmts is every fixed statement of the schema work, by name. DDL built
// per database or per table (CREATE DATABASE, a clone's CREATE and ALTER, a
// migration file's own batches, the HLR copy's pages) is built at the call.
var SchemaStmts = stmt.New("mssql")

// SchemaStatements is the registry of these statements (the lint test).
func SchemaStatements() *stmt.Registry { return SchemaStmts }

// The collation a new database is made with, and every text column of the
// base schema names (schemagen.TextCollation).
const databaseCollation = "Latin1_General_100_CI_AS_SC"

var (
	stSchemaTrack = SchemaStmts.Add("schema.track_migrations", `IF OBJECT_ID(N'schema_migrations', N'U') IS NULL
	CREATE TABLE schema_migrations (
		version    NVARCHAR(255) COLLATE Latin1_General_100_CI_AS_SC NOT NULL CONSTRAINT PK_schema_migrations PRIMARY KEY,
		applied_at DATETIME2(0) NOT NULL CONSTRAINT DF_schema_migrations_applied_at DEFAULT (SYSUTCDATETIME())
	)`, false)
	stSchemaApplied = SchemaStmts.Add("schema.applied", "SELECT version FROM schema_migrations", false)
	// INSERT IGNORE's twin: the row is written when it is not there.
	stSchemaMark = SchemaStmts.Add("schema.mark", `INSERT INTO schema_migrations (version)
		SELECT @p1 WHERE NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version = @p1)`, false)
	stSchemaRecord      = SchemaStmts.Add("schema.record", "INSERT INTO schema_migrations (version) VALUES (@p1)", false)
	stSchemaForgetMarks = SchemaStmts.Add("schema.forget_marks", "DELETE FROM schema_migrations WHERE version LIKE @p1", false)
	stSchemaSeeded      = SchemaStmts.Add("schema.seeded", `SELECT CASE WHEN EXISTS (SELECT 1 FROM schema_migrations WHERE version = @p1) THEN 1 ELSE 0 END`, false)
	stSchemaLock        = SchemaStmts.Add("schema.lock", `DECLARE @r INT;
		EXEC @r = sp_getapplock @Resource = @p1, @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = @p2;
		SELECT @r`, false)
	stSchemaRelease = SchemaStmts.Add("schema.release", `IF APPLOCK_MODE('public', @p1, 'Session') <> 'NoLock'
		EXEC sp_releaseapplock @Resource = @p1, @LockOwner = 'Session'`, false)
	stDatabaseExists = SchemaStmts.Add("schema.database_exists", "SELECT CASE WHEN DB_ID(@p1) IS NULL THEN 0 ELSE 1 END", false)
	stDatabaseFlags  = SchemaStmts.Add("schema.database_flags", `SELECT is_read_committed_snapshot_on, is_memory_optimized_elevate_to_snapshot_on
		FROM sys.databases WHERE name = @p1`, false)
	stDataFile = SchemaStmts.Add("schema.data_file", `SELECT TOP (1) physical_name FROM sys.master_files
		WHERE database_id = DB_ID(@p1) AND type = 0 ORDER BY file_id`, false)
)

// openSchema is the schema opener: it reaches no database (dal.Schema).
func openSchema(_ config.Config, store, dsn string) (dal.Schema, error) {
	return &schema{store: store, dsn: dsn}, nil
}

// schema is one store's schema work on SQL Server.
type schema struct {
	store, dsn string
	db         *sql.DB // Connect's pool
}

var _ dal.Schema = (*schema)(nil)

// parseDSN reads a SQL Server DSN in any of the driver's forms
// (sqlserver://…?database=x, "server=…;database=x", odbc:…).
func schemaParseDSN(dsn string) (msdsn.Config, error) {
	return msdsn.Parse(dsn)
}

// openDSN opens a pool on a parsed DSN, naming database (the DSN's own when
// empty).
func schemaOpenDSN(cfg msdsn.Config, database string) *sql.DB {
	if database != "" {
		cfg.Database = database
	}
	return sql.OpenDB(mssql.NewConnectorConfig(cfg))
}

// openStore opens a pool on a store's own database.
func schemaOpenStore(dsn string) (*sql.DB, error) {
	cfg, err := schemaParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return schemaOpenDSN(cfg, ""), nil
}

// quoteName brackets a database, table or column name.
func schemaQuote(name string) string { return "[" + strings.ReplaceAll(name, "]", "]]") + "]" }

// MakeDatabase creates the store's database when it is missing, with
// READ_COMMITTED_SNAPSHOT on (readers never wait for writers, as InnoDB's
// consistent reads); the queue database also gets its MEMORY_OPTIMIZED_DATA
// filegroup and MEMORY_OPTIMIZED_ELEVATE_TO_SNAPSHOT (the memory-optimized
// queue tables of a Redis-free install need both; the filegroup can never be
// removed again, so it is added to the queue database only).
func (s *schema) MakeDatabase(ctx context.Context) (string, error) {
	cfg, err := schemaParseDSN(s.dsn)
	if err != nil {
		return "", fmt.Errorf("bootstrap: bad DSN for %s store: %w", s.store, err)
	}
	name := cfg.Database
	if name == "" {
		return "", fmt.Errorf("bootstrap: %s store DSN has no database name", s.store)
	}
	db := schemaOpenDSN(cfg, "master")
	defer db.Close()
	if err := schemaWaitReady(ctx, db); err != nil {
		return "", fmt.Errorf("bootstrap: server not reachable: %w", err)
	}
	var exists int
	if err := db.QueryRowContext(ctx, stDatabaseExists.SQL, name).Scan(&exists); err != nil {
		return "", fmt.Errorf("bootstrap: create database %q: %w", name, err)
	}
	if exists == 0 {
		q := "CREATE DATABASE " + schemaQuote(name) + " COLLATE " + databaseCollation
		if _, err := db.ExecContext(ctx, q); err != nil && !isAlreadyThere(err) {
			return "", fmt.Errorf("bootstrap: create database %q: %w", name, err)
		}
	}
	var rcsi, elevate bool
	if err := db.QueryRowContext(ctx, stDatabaseFlags.SQL, name).Scan(&rcsi, &elevate); err != nil {
		return "", fmt.Errorf("bootstrap: read database %q: %w", name, err)
	}
	if !rcsi {
		// NO_WAIT: on a database others are using, the switch needs them all
		// gone; it then fails at once with this line rather than hang a start.
		q := "ALTER DATABASE " + schemaQuote(name) + " SET READ_COMMITTED_SNAPSHOT ON WITH NO_WAIT"
		if _, err := db.ExecContext(ctx, q); err != nil {
			return "", fmt.Errorf("bootstrap: database %q: READ_COMMITTED_SNAPSHOT is off and could not be turned on while it is in use (%w); run ALTER DATABASE … SET READ_COMMITTED_SNAPSHOT ON with nothing connected", name, err)
		}
	}
	if s.store == "queue" {
		if err := memoryOptimized(ctx, db, name, elevate); err != nil {
			return "", fmt.Errorf("bootstrap: database %q: memory-optimized filegroup: %w", name, err)
		}
	}
	return name, nil
}

// memoryOptimized gives a database its MEMORY_OPTIMIZED_DATA filegroup (its
// container a folder beside the database's data file) and turns on
// MEMORY_OPTIMIZED_ELEVATE_TO_SNAPSHOT.
func memoryOptimized(ctx context.Context, db *sql.DB, name string, elevate bool) error {
	var n int
	q := "SELECT COUNT(*) FROM " + schemaQuote(name) + ".sys.filegroups WHERE type = 'FX'"
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		return err
	}
	if n == 0 {
		var file string
		if err := db.QueryRowContext(ctx, stDataFile.SQL, name).Scan(&file); err != nil {
			return fmt.Errorf("data file: %w", err)
		}
		dir := file[:strings.LastIndexAny(file, `/\`)+1]
		fg := name + "_mod"
		if _, err := db.ExecContext(ctx, "ALTER DATABASE "+schemaQuote(name)+" ADD FILEGROUP "+schemaQuote(fg)+" CONTAINS MEMORY_OPTIMIZED_DATA"); err != nil && !isAlreadyThere(err) {
			return err
		}
		add := "ALTER DATABASE " + schemaQuote(name) + " ADD FILE (NAME = N'" + strings.ReplaceAll(fg, "'", "''") +
			"', FILENAME = N'" + strings.ReplaceAll(dir+fg, "'", "''") + "') TO FILEGROUP " + schemaQuote(fg)
		if _, err := db.ExecContext(ctx, add); err != nil && !isAlreadyThere(err) {
			return err
		}
	}
	if !elevate {
		if _, err := db.ExecContext(ctx, "ALTER DATABASE "+schemaQuote(name)+" SET MEMORY_OPTIMIZED_ELEVATE_TO_SNAPSHOT = ON"); err != nil {
			return err
		}
	}
	return nil
}

// Connect opens the store's own database for the migration steps.
func (s *schema) Connect(ctx context.Context) error {
	cfg, err := schemaParseDSN(s.dsn)
	if err != nil {
		return fmt.Errorf("bootstrap: bad DSN for %s store: %w", s.store, err)
	}
	db := schemaOpenDSN(cfg, "")
	if err := schemaWaitReady(ctx, db); err != nil {
		db.Close()
		return fmt.Errorf("bootstrap: %s not reachable: %w", cfg.Database, err)
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

// TrackMigrations makes schema_migrations, in the shape MySQL's has.
func (s *schema) TrackMigrations(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, stSchemaTrack.SQL); err != nil && !isAlreadyThere(err) {
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

// Seed runs a seed script once per marker, on a connection of its own. The
// script is T-SQL in batches (GO lines); a batch that is only
// "-- yaku:create-like <template> <table>" lines makes those tables from
// their templates (SQL Server has no CREATE TABLE … LIKE), as the gateway
// itself does.
func (s *schema) Seed(ctx context.Context, marker, seedSQL string) (bool, error) {
	db, err := schemaOpenStore(s.dsn)
	if err != nil {
		return false, fmt.Errorf("bootstrap: seed DSN: %w", err)
	}
	defer db.Close()
	if err := schemaWaitReady(ctx, db); err != nil {
		return false, err
	}
	var done int
	if err := db.QueryRowContext(ctx, stSchemaSeeded.SQL, marker).Scan(&done); err != nil {
		return false, fmt.Errorf("bootstrap: check seed marker %s: %w", marker, err)
	}
	if done == 1 {
		return false, nil
	}
	clones := NewCloner(db)
	for _, batch := range splitBatches(seedSQL) {
		if likes, ok := createLikeLines(batch); ok {
			for _, l := range likes {
				if err := clones.CreateLike(ctx, l[0], l[1]); err != nil {
					return false, fmt.Errorf("bootstrap: apply seed %s: %w", marker, err)
				}
			}
			continue
		}
		if _, err := db.ExecContext(ctx, batch); err != nil {
			return false, fmt.Errorf("bootstrap: apply seed %s: %w", marker, err)
		}
	}
	if _, err := db.ExecContext(ctx, stSchemaMark.SQL, marker); err != nil {
		return false, fmt.Errorf("bootstrap: record seed marker %s: %w", marker, err)
	}
	return true, nil
}

// createLikeLines reads a batch made only of comment lines, at least one of
// them "-- yaku:create-like T N": each such line's template and new table.
func createLikeLines(batch string) ([][2]string, bool) {
	var out [][2]string
	for _, line := range strings.Split(batch, "\n") {
		t := strings.TrimSpace(line)
		if t == "" {
			continue
		}
		if !strings.HasPrefix(t, "--") {
			return nil, false
		}
		rest, ok := strings.CutPrefix(t, "-- yaku:create-like ")
		if !ok {
			continue // a plain comment
		}
		f := strings.Fields(rest)
		if len(f) != 2 {
			return nil, false
		}
		out = append(out, [2]string{f[0], f[1]})
	}
	return out, len(out) > 0
}

func schemaWaitReady(ctx context.Context, db *sql.DB) error {
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

// migrateLockWaitSec is how long a role waits for whoever is migrating, as on
// MySQL.
const migrateLockWaitSec = 120

// migrateLockName is the application lock a store's migrations take. An
// application lock belongs to the database it is taken in, so the store's
// name is enough: two gateways with databases of their own on one server
// never wait for each other (MySQL's GET_LOCK is per server and needs the
// database in its name).
func migrateLockName(store string) string { return "yaku_migrate_" + store }

// LockMigrations takes the store's migration lock on a PINNED connection:
// sp_getapplock with a Session owner belongs to that connection, so a lock
// taken from the pool would go with the connection it was taken on.
func (s *schema) LockMigrations(ctx context.Context, log dal.Printer) (func(), bool, error) {
	return migrateLock(ctx, s.db, s.store, log)
}

func migrateLock(ctx context.Context, db *sql.DB, store string, log dal.Printer) (func(), bool, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return func() {}, false, fmt.Errorf("bootstrap: migrate lock: %w", err)
	}
	name := migrateLockName(store)
	release := func() {
		// Background context: the caller's may already be cancelled, and a
		// lock left held would block every other role's start.
		_, _ = conn.ExecContext(context.Background(), stSchemaRelease.SQL, name)
		_ = conn.Close()
	}
	var got int
	if err := conn.QueryRowContext(ctx, stSchemaLock.SQL, name, migrateLockWaitSec*1000).Scan(&got); err != nil {
		release()
		return func() {}, false, fmt.Errorf("bootstrap: migrate lock: %w", err)
	}
	// 0 granted at once, 1 granted after a wait; below 0: timed out (-1),
	// cancelled (-2), deadlock victim (-3), error (-999).
	if got < 0 {
		release()
		log.Printf("%s store: waited %ds for another process migrating", store, migrateLockWaitSec)
		return func() {}, false, nil
	}
	return release, true, nil
}
