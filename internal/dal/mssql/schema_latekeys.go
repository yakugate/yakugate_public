package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// Keys added AFTER start (made like internal/dal/mysql/schema_latekeys.go):
// a template key that only makes a screen fast is added to the clones in the
// background, one process at a time (an application lock not waited for),
// newest table first, never under the migration lock. The list is empty, as
// on MySQL: the record tables by company and day are made from templates
// that carry their keys.

var (
	stLateKeysTables = SchemaStmts.Add("schema.late_keys_tables", `SELECT name FROM sys.tables
		WHERE name LIKE @p1 ESCAPE '\' AND name <> @p2 ORDER BY name DESC`, false)
	stIndexExists = SchemaStmts.Add("schema.index_exists", `SELECT CASE WHEN EXISTS (SELECT 1 FROM sys.indexes
		WHERE object_id = OBJECT_ID(@p1) AND name = @p2) THEN 1 ELSE 0 END`, false)
)

// lateCloneKeys: per store, the template whose keys its clones should also
// have.
var lateCloneKeys = map[string][]struct {
	template, pattern string
	keys              []string
}{
	"hot": nil,
}

// AddLateKeys adds the store's late keys on a small pool of its own.
func (s *schema) AddLateKeys(ctx context.Context, log dal.Printer) error {
	if len(lateCloneKeys[s.store]) == 0 {
		return nil
	}
	db, err := schemaOpenStore(s.dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	return syncLateCloneKeys(ctx, db, s.store, log)
}

func syncLateCloneKeys(ctx context.Context, db *sql.DB, store string, log dal.Printer) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	const lock = "yaku_late_keys"
	var got int
	if err := conn.QueryRowContext(ctx, stSchemaLock.SQL, lock, 0).Scan(&got); err != nil || got < 0 {
		return nil // another process has it (or the lock cannot be asked): its job
	}
	defer conn.ExecContext(context.Background(), stSchemaRelease.SQL, lock)

	for _, fam := range lateCloneKeys[store] {
		var id int64
		var at sql.NullTime
		if err := db.QueryRowContext(ctx, stCloneTemplate.SQL, fam.template).Scan(&id, &at); err != nil {
			continue
		}
		sh, err := readShape(ctx, db, id)
		if err != nil {
			return err
		}
		keys := map[string]tplIndex{}
		for _, ix := range sh.indexes {
			for _, k := range fam.keys {
				if ix.name == k {
					keys[k] = ix
				}
			}
		}
		if len(keys) == 0 {
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
			n := 0
			began := time.Now()
			for _, k := range fam.keys {
				ix, ok := keys[k]
				if !ok {
					continue
				}
				var have int
				if err := db.QueryRowContext(ctx, stIndexExists.SQL, t, k).Scan(&have); err != nil || have == 1 {
					continue
				}
				// ONLINE: rows go on being written while it runs. An edition
				// that cannot (Standard) says so, and it is then done plainly.
				q := strings.TrimSuffix(ix.createSQL(t), ";\n")
				if _, err := db.ExecContext(ctx, q+" WITH (ONLINE = ON)"); err != nil && !isAlreadyThere(err) {
					if _, err2 := db.ExecContext(ctx, q); err2 != nil && !isAlreadyThere(err2) {
						return fmt.Errorf("add search keys to %s: %w", t, err2)
					}
				}
				n++
			}
			if n > 0 {
				log.Printf("search keys: added %d key(s) to %s in %s", n, t, time.Since(began).Round(time.Second))
			}
		}
	}
	return nil
}
