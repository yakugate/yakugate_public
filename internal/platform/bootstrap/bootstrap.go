// Package bootstrap makes the engine self-provisioning: on startup it creates
// each SQL store database if missing, applies that store's migrations (tracked
// in schema_migrations, version-prefixed per store), and seeds demo/fixtures.
//
// Four stores: config, queue (incl. hlr + api_tokens), hot, cold(filesystem).
// MySQL and SQL Server (since step 4 of the DAL plan) are auto-provisioned,
// each from its own folder (migrations/<store>/mysql, …/mssql); mongo and
// redis are applied manually.
//
// Since 30 Oct 2026 (the DAL plan, "each SQL DAL owns its schema") the
// database's part — create the database, the migration lock, run one file,
// the clone tables, the seed, the late keys, the HLR copy — is the DAL's
// (dal.Schema; MySQL's is internal/dal/mysql/schema*.go). This package keeps
// the decisions: which stores, which migration folder, which files are still
// to run, when to seed, what is logged.
//
// Env: YAKU_AUTO_MIGRATE=false skips schema creation; YAKU_AUTO_SEED=true seeds demo data (default off, PO 2026-09-17).
package bootstrap

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sort"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/migrations"
	"github.com/yakugate/yakugate/scripts"
)

// Ensure provisions every MySQL store referenced by the config, then seeds. [SCOPE P1-BOOT]
func Ensure(ctx context.Context, cfg config.Config) error {
	return EnsureTo(ctx, cfg, nil)
}

// EnsureTo is Ensure with its progress lines written to out instead of the
// terminal (nil = the terminal, as Ensure). The console's database setup
// screen collects them, so some 200 "applied migration" lines do not scroll
// over its questions, and shows the last few only if building fails
// (2026-10-02).
func EnsureTo(ctx context.Context, cfg config.Config, out io.Writer) error {
	log := logging.New("bootstrap")
	if out != nil {
		log.SetOutput(out)
	}
	if !envBool("YAKU_AUTO_MIGRATE", true) {
		log.Printf("auto-migrate disabled (YAKU_AUTO_MIGRATE=false); skipping")
		return nil
	}

	stores := []struct {
		name    string
		backend config.Backend
		dsn     string
	}{
		{"config", cfg.ConfigBackend, cfg.ConfigDSN},
		// The MySQL queue database, when there is one: the queues with
		// YAKU_QUEUE_BACKEND=mysql (D-122), and the HLR data too unless
		// YAKU_HLR_DSN gives it its own database (D-124).
		{"queue", queueStoreBackend(cfg), cfg.QueueDSN},
		{"hot", cfg.HotBackend, cfg.HotDSN},
	}
	// A separate HLR store: its own database and migration set (D-124).
	if hb, hd := cfg.HLRStore(); cfg.HLRSeparate() && builds(hb) {
		stores = append(stores, struct {
			name    string
			backend config.Backend
			dsn     string
		}{"hlr", hb, hd})
	}
	for _, s := range stores {
		if !builds(s.backend) || s.dsn == "" {
			continue
		}
		if err := ensureStore(ctx, cfg, s.name, s.backend, s.dsn, log); err != nil {
			return err
		}
	}
	// What the queue database held moves into a separate HLR store, once.
	startHLRCopy(cfg, log)
	// Keys that only make a screen fast are added after start, not during it.
	startLateCloneKeys(cfg, log)

	// Each store seeds its OWN database (config and queue are now separate DBs).
	// Seeding is OFF unless asked (PO 2026-09-17): a production store must
	// never receive demo rows because nobody set a variable. A dev box turns
	// it on in its own yakugate.config (the sample ships it off).
	if envBool("YAKU_AUTO_SEED", false) {
		// Each database has its own seed scripts (scripts/seed/ for MySQL,
		// scripts/mssql/ for SQL Server).
		if builds(cfg.ConfigBackend) && cfg.ConfigDSN != "" {
			if err := seedInto(ctx, cfg, cfg.ConfigBackend, "config", cfg.ConfigDSN, "seed_config_v2", seedFor(cfg.ConfigBackend, "config"), log); err != nil {
				return err
			}
		}
		if b := cfg.QueueStoreSQL(); b != "" {
			if err := seedInto(ctx, cfg, b, "queue", cfg.QueueDSN, "seed_queue_v2", seedFor(b, "queue"), log); err != nil {
				return err
			}
		}
	}
	return nil
}

// builds reports whether the engine builds a store on this database itself:
// MySQL, and SQL Server since step 4 of the DAL plan.
func builds(b config.Backend) bool {
	return b == config.BackendMySQL || b == config.BackendMSSQL
}

// seedFor is a store's seed script written for one database.
func seedFor(b config.Backend, store string) string {
	switch {
	case b == config.BackendMSSQL && store == "config":
		return scripts.SeedConfigMSSQL
	case b == config.BackendMSSQL:
		return scripts.SeedQueueMSSQL
	case store == "config":
		return scripts.SeedConfigSQL
	}
	return scripts.SeedQueueSQL
}

// queueStoreBackend is the backend of the queue store's own database: the
// queues' database for `mysql` / `mssql`, and for `redis` the SQL database
// still named beside them (its HLR data lives there, D-124).
func queueStoreBackend(cfg config.Config) config.Backend {
	if b := cfg.QueueStoreSQL(); b != "" {
		return b
	}
	return cfg.QueueBackend
}

// ensureStore builds one store: its database, its migrations, its clone
// tables. The database's part is the DAL's (dal.Schema).
func ensureStore(ctx context.Context, cfg config.Config, store string, backend config.Backend, dsn string, log *logging.Logger) error {
	sch, err := dal.OpenSchema(cfg, backend, store, dsn)
	if err != nil {
		return fmt.Errorf("bootstrap: %s store: %w", store, err)
	}
	defer sch.Close()
	dbName, err := sch.MakeDatabase(ctx)
	if err != nil {
		return err
	}
	log.Printf("database %q present", dbName)
	if err := sch.Connect(ctx); err != nil {
		return err
	}
	return applyMigrations(ctx, sch, store, backend, log)
}

// migrationDir is the embedded folder a store's migrations are read from on
// one kind of database: <store>/<database> (config/mysql, hot/mysql, …).
func migrationDir(store string, backend config.Backend) string {
	return store + "/" + string(backend)
}

// applyMigrations runs migrations/<store>/<database>/*.sql not yet applied,
// recording each as "<store>/<file>" so stores can share a database without
// version clashes.
func applyMigrations(ctx context.Context, sch dal.Schema, store string, backend config.Backend, log *logging.Logger) error {
	if err := sch.TrackMigrations(ctx); err != nil {
		return err
	}
	// One migrator at a time. Every role auto-migrates at start and they all
	// start together — fifteen roles on one box today, and a role per
	// deployment on top of that once there is more than one gateway against
	// one store. Without this, two processes both read schema_migrations,
	// both find the same file unapplied, and both run its DDL: the loser dies
	// on "Duplicate column" at start, which looks like a corrupt schema and
	// is really just a race.
	//
	// The applied set is read AFTER the lock, so the winner's work is visible
	// to everyone who waited.
	release, held, err := sch.LockMigrations(ctx, log)
	if err != nil {
		return err
	}
	defer release()

	applied, err := sch.AppliedMigrations(ctx)
	if err != nil {
		return err
	}
	dir := migrationDir(store, backend)
	if !held {
		// Somebody else has been migrating for longer than we are willing to
		// wait. If they have finished the work, there is nothing to do and
		// this role starts normally; if there is still something pending we
		// refuse rather than race them for it.
		if pending := pendingCount(applied, store, dir); pending > 0 {
			return fmt.Errorf("bootstrap: %s store: another process is migrating and %d migration(s) are still pending; retry shortly", store, pending)
		}
		log.Printf("%s store: migrations already applied by another process", store)
		return nil
	}
	entries, err := fs.ReadDir(migrations.FS, dir)
	if err != nil {
		return fmt.Errorf("bootstrap: read embedded %s migrations: %w", store, err)
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	ran := 0
	for _, name := range names {
		version := store + "/" + name
		if applied[version] {
			continue
		}
		body, err := migrations.FS.ReadFile(dir + "/" + name)
		if err != nil {
			return fmt.Errorf("bootstrap: read %s: %w", version, err)
		}
		// The DAL runs the file statement by statement, marking each as it
		// goes so a failure half way resumes there (GW-07), and passes a
		// statement its database's rules allow (GW-08/09, a gateway-only
		// install).
		if err := sch.ApplyMigration(ctx, version, string(body), applied, log); err != nil {
			return err
		}
		log.Printf("applied migration %s", version)
		ran++
	}
	if ran == 0 {
		log.Printf("%s store schema up to date (%d migrations)", store, len(names))
	} else {
		log.Printf("%s store: applied %d migration(s)", store, ran)
	}
	// The tables cloned from a template are brought up to it.
	return sch.SyncClones(ctx, log)
}

// seedInto applies an idempotent seed to one store's database once per
// marker version (recorded in that database's schema_migrations).
func seedInto(ctx context.Context, cfg config.Config, backend config.Backend, store, dsn, marker, seedSQL string, log *logging.Logger) error {
	sch, err := dal.OpenSchema(cfg, backend, store, dsn)
	if err != nil {
		return fmt.Errorf("bootstrap: %s store: %w", store, err)
	}
	defer sch.Close()
	seeded, err := sch.Seed(ctx, marker, seedSQL)
	if err != nil {
		return err
	}
	if seeded {
		log.Printf("seeded %s", marker)
	}
	return nil
}

func envBool(key string, def bool) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if v == "" {
		return def
	}
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

// pendingCount reports how many embedded migrations for this store are not in
// the applied set — used only to decide whether losing the lock race matters.
func pendingCount(applied map[string]bool, store, dir string) int {
	entries, err := fs.ReadDir(migrations.FS, dir)
	if err != nil {
		return 0
	}
	n := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		if !applied[store+"/"+e.Name()] {
			n++
		}
	}
	return n
}
