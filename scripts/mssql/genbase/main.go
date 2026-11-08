// Command genbase writes the SQL Server starting schemas,
// migrations/<store>/mssql/0001_base.sql, from a fresh MySQL install (plan of
// 29 Oct 2026, step 4).
//
// The MySQL install it reads is built first, on a scratch server (never the
// dev box), with nothing seeded:
//
//	yakugate --config gen.config migrate          # YAKU_AUTO_SEED unset
//	cd p13-backend && DATABASE_HOST=… DATABASE_NAME=<config db> JWT_SECRET=x npm run migrate
//
// then, from the repository root:
//
//	go run ./scripts/mssql/genbase -mysql 'root:pw@tcp(127.0.0.1:15506)/' \
//	    -db config=g_config,queue=g_queue,hot=g_hot,hlr=g_hlr
//
// Each base records the MySQL migration it equals (and, for config, the
// Admin API's). A migration added after that is written for both databases;
// internal/dal/mssql's parity_twins_test.go holds that rule.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/yakugate/yakugate/internal/dal/mssql/schemagen"
)

func main() {
	server := flag.String("mysql", "", "MySQL server DSN without a database, e.g. root:pw@tcp(127.0.0.1:3306)/")
	dbs := flag.String("db", "config=yakugate_config,queue=yakugate_queue,hot=yakugate_hot,hlr=yakugate_hlr", "store=database pairs to read")
	root := flag.String("root", ".", "the repository root")
	flag.Parse()
	if *server == "" {
		fmt.Fprintln(os.Stderr, "genbase: -mysql is required")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	for _, pair := range strings.Split(*dbs, ",") {
		store, name, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if !ok {
			fmt.Fprintf(os.Stderr, "genbase: %q is not store=database\n", pair)
			os.Exit(2)
		}
		if err := one(ctx, *root, *server, store, name); err != nil {
			fmt.Fprintf(os.Stderr, "genbase: %s: %v\n", store, err)
			os.Exit(1)
		}
	}
}

func one(ctx context.Context, root, server, store, database string) error {
	db, err := sql.Open("mysql", server+database+"?parseTime=false")
	if err != nil {
		return err
	}
	defer db.Close()
	s, err := schemagen.ReadMySQL(ctx, db)
	if err != nil {
		return err
	}
	head, err := lastFile(filepath.Join(root, "migrations", store, "mysql"))
	if err != nil {
		return err
	}
	o := schemagen.Options{Store: store, MySQLHead: head, Skip: map[string]bool{"schema_migrations": true}}
	if store == "config" {
		if o.AdminHead, err = lastFile(filepath.Join(root, "p13-backend", "migrations")); err != nil {
			return err
		}
	}
	// The applied list must say the install is at the head: a MySQL
	// install a step behind would make a base that is too.
	var applied int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM schema_migrations WHERE version = ?", store+"/"+head).Scan(&applied); err != nil {
		return err
	}
	if applied == 0 {
		return fmt.Errorf("%s has not run %s/%s: build it with this tree's binary first", database, store, head)
	}
	text, notes, err := schemagen.Render(ctx, db, s, o)
	if err != nil {
		return err
	}
	out := filepath.Join(root, "migrations", store, "mssql", "0001_base.sql")
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(out, []byte(text), 0o644); err != nil {
		return err
	}
	fmt.Printf("%s: %d tables -> %s (head %s)\n", store, len(s.Tables)-len(o.Skip), out, head)
	report("ON UPDATE CURRENT_TIMESTAMP dropped", notes.OnUpdate)
	report("unique keys over a nullable column, filtered", notes.NullableUniques)
	report("primary keys too wide to cluster, NONCLUSTERED", notes.NonClusteredPKs)
	report("keys wider than 1700 bytes", notes.WideKeys)
	report("triggers translated by hand", notes.Triggers)
	return nil
}

func report(what string, items []string) {
	if len(items) == 0 {
		return
	}
	fmt.Printf("  %s (%d): %s\n", what, len(items), strings.Join(items, ", "))
}

// lastFile is the last .sql file of a folder, by name.
func lastFile(dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	if len(names) == 0 {
		return "", fmt.Errorf("no migrations in %s", dir)
	}
	sort.Strings(names)
	return names[len(names)-1], nil
}
