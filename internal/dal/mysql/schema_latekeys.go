package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	driver "github.com/go-sql-driver/mysql"

	"github.com/yakugate/yakugate/internal/dal"
)

// Keys added AFTER start (2026-10-08) — moved here unchanged on 30 Oct 2026
// from internal/platform/bootstrap/latekeys.go. When they are added (in the
// background, at every role's start; waited for by `yakugate migrate`) is
// still the bootstrap's.
//
// cloneKeys (schema_clones.go) adds a template's key to the tables cloned from
// it while a role starts, under the migration lock — right for a key the
// gateway cannot work correctly without. It is wrong for a key that only makes
// a screen fast: the three search keys of the Rejected list (hot/0058), added
// that way to a month of 6.7 million refusals, held every role's start for
// four and a half minutes on the dev box. The same holds for the receive-time
// key of an incoming-message month (hot/0060).
//
// The keys listed here are added in the background instead: one process does
// it (an advisory lock, not waited for), in ONE pass over each table, with
// writes to the table allowed throughout. Nothing waits for it and nothing
// depends on it — until it is done the screen is as slow as it was. A process
// stopped half way simply leaves the table without the keys; the next start
// tries again.

// lateKeysLock is the late keys' lock, named for THIS database.
const lateKeysLock = "LEFT(CONCAT('yaku_late_keys@', DATABASE()), 64)"

var (
	stLateKeysLock    = SchemaStmts.Add("schema.late_keys_lock", "SELECT GET_LOCK("+lateKeysLock+", 0)", false)
	stLateKeysRelease = SchemaStmts.Add("schema.late_keys_release", "DO RELEASE_LOCK("+lateKeysLock+")", false)
	stLateKeysTables  = SchemaStmts.Add("schema.late_keys_tables", `
			SELECT t.table_name FROM information_schema.tables t
			WHERE t.table_schema = DATABASE() AND t.table_name LIKE ? AND t.table_name <> ?
			  AND t.table_type = 'BASE TABLE' ORDER BY t.table_name DESC`, false)
)

// lateCloneKeys: per store, the template whose keys its clones should also
// have. Clones are named by a LIKE pattern, as in cloneKeys.
var lateCloneKeys = map[string][]struct {
	template, pattern string
	keys              []string
}{
	// No late keys for the hot store since D-136: the record tables by
	// company and month are made from templates that carry their keys.
	"hot": nil,
}

// AddLateKeys adds the store's late keys on a small pool of its own, and
// returns when done.
func (s *schema) AddLateKeys(ctx context.Context, log dal.Printer) error {
	c, err := driver.ParseDSN(s.dsn)
	if err != nil {
		return err
	}
	db, err := sql.Open("mysql", c.FormatDSN())
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	return syncLateCloneKeys(ctx, db, s.store, log)
}

// syncLateCloneKeys gives each listed clone the template keys it lacks.
func syncLateCloneKeys(ctx context.Context, db *sql.DB, store string, log dal.Printer) error {
	if len(lateCloneKeys[store]) == 0 {
		return nil
	}
	// One process at a time, per database; nobody waits for the other.
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, stLateKeysLock.SQL).Scan(&got); err != nil || got.Int64 != 1 {
		return nil // another process has it (or the lock cannot be asked): its job
	}
	defer conn.ExecContext(context.Background(), stLateKeysRelease.SQL)

	for _, fam := range lateCloneKeys[store] {
		// What each key covers, from the template. A key the template does not
		// have yet (its migration has not run) is left for a later start.
		cols := map[string][]string{}
		for _, k := range fam.keys {
			c, err := keyColumns(ctx, db, fam.template, k)
			if err == nil && len(c) > 0 {
				cols[k] = c
			}
		}
		if len(cols) == 0 {
			continue
		}
		rows, err := db.QueryContext(ctx, stLateKeysTables.SQL, fam.pattern, fam.template)
		if err != nil {
			return err
		}
		var tables []string
		for rows.Next() {
			var t string
			if rows.Scan(&t) == nil {
				tables = append(tables, t)
			}
		}
		rows.Close()
		for _, t := range tables { // newest first: it is the one the screen reads
			var adds []string
			for _, k := range fam.keys {
				c, ok := cols[k]
				if !ok {
					continue
				}
				if have, _ := keyColumns(ctx, db, t, k); len(have) > 0 {
					continue
				}
				adds = append(adds, fmt.Sprintf("ADD KEY `%s` (`%s`)", k, strings.Join(c, "`,`")))
			}
			if len(adds) == 0 {
				continue
			}
			began := time.Now()
			// One statement = one pass over the table for all its keys. INPLACE
			// with LOCK=NONE: rows go on being written while it runs; a server
			// that cannot do that says so, and it is then done the plain way.
			stmt := fmt.Sprintf("ALTER TABLE `%s` %s", t, strings.Join(adds, ", "))
			if _, err := db.ExecContext(ctx, stmt+", ALGORITHM=INPLACE, LOCK=NONE"); err != nil && !isAlreadyThereErr(err) {
				if _, err2 := db.ExecContext(ctx, stmt); err2 != nil && !isAlreadyThereErr(err2) {
					return fmt.Errorf("add search keys to %s: %w", t, err2)
				}
			}
			log.Printf("search keys: added %d key(s) to %s in %s", len(adds), t, time.Since(began).Round(time.Second))
		}
	}
	return nil
}
