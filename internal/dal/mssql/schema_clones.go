package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
)

// The tables cloned from a template, kept in step with it — made like
// internal/dal/mysql/schema_clones.go: a migration that changes only the
// template leaves the clones made before it behind, so at start every clone
// gets the columns its template has gained (ADD only), the provable widenings
// of a text column, the columns its template lost by name (droppedColumns),
// and the keys a clone must have (cloneKeys). Read from sys.columns and
// sys.indexes in one scan each.

var (
	stCloneTables = SchemaStmts.Add("schema.clone_tables", `SELECT name FROM sys.tables
		WHERE name LIKE @p1 ESCAPE '\' AND name <> @p2 ORDER BY name`, false)
	stCloneTableColumns = SchemaStmts.Add("schema.clone_table_columns", `SELECT o.name, c.name, t.name, c.max_length
		FROM sys.tables o
		JOIN sys.columns c ON c.object_id = o.object_id
		JOIN sys.types t ON t.user_type_id = c.user_type_id
		WHERE o.name LIKE @p1 ESCAPE '\' AND o.name <> @p2`, false)
	stCloneKeyless = SchemaStmts.Add("schema.clones_without_key", `SELECT o.name FROM sys.tables o
		WHERE o.name LIKE @p1 ESCAPE '\' AND o.name <> @p2
		  AND NOT EXISTS (SELECT 1 FROM sys.indexes i WHERE i.object_id = o.object_id AND i.name = @p3)
		ORDER BY o.name`, false)
	stColumnDefault = SchemaStmts.Add("schema.column_default", `SELECT dc.name FROM sys.default_constraints dc
		JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
		WHERE dc.parent_object_id = OBJECT_ID(@p1) AND c.name = @p2`, false)
)

// cloneFamilies are the template → clone relationships of a store, as on
// MySQL (the patterns are LIKE patterns with \ as the escape).
var cloneFamilies = map[string][]struct {
	template, pattern string
	match             *regexp.Regexp
}{
	"queue": {
		{template: "queue_template", pattern: `queue\_%`},
		// The HLR shards hlr_<first4> (6 Nov 2026), as on MySQL.
		{template: "hlr_template", pattern: `hlr\_%`, match: hlrShard},
	},
	"hlr": {{template: "hlr_template", pattern: `hlr\_%`, match: hlrShard}},
	"hot": {
		{template: "tpl_day_mt", pattern: `mt\_%`, match: recordKind("mt")},
		{template: "tpl_day_sub", pattern: `sub\_%`, match: recordKind("sub")},
		{template: "tpl_day_dlr", pattern: `dlr\_%`, match: recordKind("dlr")},
		{template: "tpl_day_mo", pattern: `mo\_%`, match: recordKind("mo")},
		{template: "tpl_day_mo_out", pattern: `mo\_out\_%`, match: recordKind("mo_out")},
		{template: "tpl_day_hlr", pattern: `hlr\_%`, match: recordKind("hlr")},
		{template: "tpl_day_rejects", pattern: `rejects\_%`, match: recordKind("rejects")},
		{template: "tpl_day_orphans", pattern: `orphans\_%`, match: recordKind("orphans")},
		{template: "tpl_day_mo_sub", pattern: `mo\_sub\_%`, match: recordKind("mo_sub")},
		{template: "tpl_day_dlr_sub", pattern: `dlr\_sub\_%`, match: recordKind("dlr_sub")},
	},
}

// hlrShard is an HLR shard's name: hlr_ and the number's first four digits.
var hlrShard = regexp.MustCompile(`^hlr_[0-9]{4}$`)

// recordKind is the name of a log table of one kind: <kind>_<company>_<YYYYMMDD>.
func recordKind(kind string) *regexp.Regexp {
	return regexp.MustCompile(`^` + kind + `_[0-9]+_[0-9]{8}$`)
}

// droppedColumns are columns a template lost, by template: a clone that still
// has one has it dropped at start (keys first, then each column's default,
// then the column). Empty on SQL Server: its clones are made from the
// templates of the base schema, which have lost what MySQL's list names; a
// later migration that drops a template's column lists it here.
var droppedColumns = map[string]struct{ keys, cols []string }{}

// cloneKeys are template keys a clone must also have, by name (MySQL's list).
var cloneKeys = map[string][]struct{ template, pattern, key string }{
	"queue": {
		{template: "queue_template", pattern: `queue\_%`, key: "idx_owner_claim"},
		{template: "queue_template", pattern: `queue\_%`, key: "idx_pick"},
	},
}

// SyncClones brings the store's clone tables up to their templates.
func (s *schema) SyncClones(ctx context.Context, log dal.Printer) error {
	return syncClones(ctx, s.db, s.store, log)
}

func syncClones(ctx context.Context, db *sql.DB, store string, log dal.Printer) error {
	for _, fam := range cloneFamilies[store] {
		f, err := readCloneFamily(ctx, db, fam.template, fam.pattern, fam.match)
		if err != nil {
			return err
		}
		if f == nil {
			continue // no template here
		}
		missing := f.missing()
		tables := make([]string, 0, len(missing))
		for t := range missing {
			tables = append(tables, t)
		}
		sort.Strings(tables)
		for _, table := range tables {
			cols := missing[table]
			for _, c := range cols {
				q := "ALTER TABLE " + schemaQuote(table) + " ADD " + c.definition(fam.template, table)
				if _, err := db.ExecContext(ctx, q); err != nil && !isAlreadyThere(err) {
					return fmt.Errorf("bootstrap: sync %s.%s: %w", table, c.name, err)
				}
			}
			log.Printf("schema sync: added %d column(s) to %s", len(cols), table)
		}
		if err := widenClones(ctx, db, f, log); err != nil {
			return err
		}
		if err := dropCloneColumns(ctx, db, fam.template, f, log); err != nil {
			return err
		}
	}
	return syncCloneKeys(ctx, db, store, log)
}

// cloneFamily is one template and its clones.
type cloneFamily struct {
	template *tableShape
	tables   []string                       // every clone, sorted
	clones   map[string]map[string]cloneCol // clone -> lower(column) -> type
}

type cloneCol struct {
	typ    string
	maxLen int
}

// readCloneFamily reads a template's shape and every clone's columns in one
// scan; nil when the store has no such template.
func readCloneFamily(ctx context.Context, db *sql.DB, template, pattern string, match *regexp.Regexp) (*cloneFamily, error) {
	var id int64
	var at sql.NullTime
	err := db.QueryRowContext(ctx, stCloneTemplate.SQL, template).Scan(&id, &at)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("bootstrap: read template %s: %w", template, err)
	}
	sh, err := readShape(ctx, db, id)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: read template %s: %w", template, err)
	}
	f := &cloneFamily{template: sh, clones: map[string]map[string]cloneCol{}}
	rows, err := db.QueryContext(ctx, stCloneTables.SQL, pattern, template)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: list clones of %s: %w", template, err)
	}
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			rows.Close()
			return nil, err
		}
		if match != nil && !match.MatchString(t) {
			continue
		}
		f.tables = append(f.tables, t)
		f.clones[t] = map[string]cloneCol{}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(f.tables) == 0 {
		return f, nil
	}
	rows, err = db.QueryContext(ctx, stCloneTableColumns.SQL, pattern, template)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: read clones of %s: %w", template, err)
	}
	defer rows.Close()
	for rows.Next() {
		var t, col, typ string
		var maxLen int
		if err := rows.Scan(&t, &col, &typ, &maxLen); err != nil {
			return nil, err
		}
		if have, ok := f.clones[t]; ok {
			have[strings.ToLower(col)] = cloneCol{typ: typ, maxLen: maxLen}
		}
	}
	return f, rows.Err()
}

// missing is, per clone, the template columns it lacks that can be added to
// a table holding rows (nullable, or with a default), in template order.
func (f *cloneFamily) missing() map[string][]tplColumn {
	out := map[string][]tplColumn{}
	for _, table := range f.tables {
		have := f.clones[table]
		if len(have) == 0 {
			continue
		}
		for _, c := range f.template.columns {
			if _, ok := have[strings.ToLower(c.name)]; ok {
				continue
			}
			if !c.nullable && c.defSQL == "" {
				continue
			}
			out[table] = append(out[table], c)
		}
	}
	return out
}

// widenClones applies the provable widenings of a text column: the template
// is (n)varchar and holds at least as many characters as the clone's
// (n)char/(n)varchar, and does not go from unicode to not. Any other type
// drift is logged and left for a deliberate migration.
func widenClones(ctx context.Context, db *sql.DB, f *cloneFamily, log dal.Printer) error {
	for _, table := range f.tables {
		have := f.clones[table]
		for _, c := range f.template.columns {
			got, ok := have[strings.ToLower(c.name)]
			if !ok || (got.typ == c.typ && got.maxLen == c.maxLen) {
				continue
			}
			if !isWidening(got, c) {
				log.Printf("schema sync: %s.%s type %s(%d) differs from template %s — NOT auto-changed (not a provable widening)",
					table, c.name, got.typ, got.maxLen, c.typeSQL())
				continue
			}
			def := c.typeSQL()
			if c.collation != "" {
				def += " COLLATE " + c.collation
			}
			if c.nullable {
				def += " NULL"
			} else {
				def += " NOT NULL"
			}
			q := "ALTER TABLE " + schemaQuote(table) + " ALTER COLUMN " + schemaQuote(c.name) + " " + def
			if _, err := db.ExecContext(ctx, q); err != nil {
				return fmt.Errorf("bootstrap: widen %s.%s: %w", table, c.name, err)
			}
			log.Printf("schema sync: widened %s.%s to %s", table, c.name, c.typeSQL())
		}
	}
	return nil
}

// chars is a text column's length in characters (-1 = MAX).
func textChars(typ string, maxLen int) int {
	if maxLen < 0 {
		return -1
	}
	if typ == "nvarchar" || typ == "nchar" {
		return maxLen / 2
	}
	return maxLen
}

func isWidening(have cloneCol, want tplColumn) bool {
	text := map[string]bool{"char": true, "varchar": true, "nchar": true, "nvarchar": true}
	if !text[have.typ] || (want.typ != "varchar" && want.typ != "nvarchar") {
		return false
	}
	if want.typ == "varchar" && (have.typ == "nvarchar" || have.typ == "nchar") {
		return false
	}
	hn, wn := textChars(have.typ, have.maxLen), textChars(want.typ, want.maxLen)
	if wn < 0 {
		return true
	}
	return hn >= 0 && wn >= hn
}

// dropCloneColumns drops from every clone the columns its template lost
// (droppedColumns) that the clone still has — once the template has lost
// them.
func dropCloneColumns(ctx context.Context, db *sql.DB, template string, f *cloneFamily, log dal.Printer) error {
	gone, ok := droppedColumns[template]
	if !ok {
		return nil
	}
	for _, c := range f.template.columns {
		for _, g := range gone.cols {
			if strings.EqualFold(c.name, g) {
				return nil // the template's migration has not run yet
			}
		}
	}
	for _, table := range f.tables {
		have := f.clones[table]
		var todo []string
		for _, c := range gone.cols {
			if _, ok := have[strings.ToLower(c)]; ok {
				todo = append(todo, c)
			}
		}
		if len(todo) == 0 {
			continue
		}
		for _, k := range gone.keys {
			q := "DROP INDEX " + schemaQuote(k) + " ON " + schemaQuote(table)
			if _, err := db.ExecContext(ctx, q); err != nil && !isAlreadyThere(err) {
				return fmt.Errorf("bootstrap: drop %s.%s: %w", table, k, err)
			}
		}
		for _, c := range todo {
			// A column with a default constraint cannot be dropped before it.
			var dc string
			if err := db.QueryRowContext(ctx, stColumnDefault.SQL, table, c).Scan(&dc); err == nil {
				q := "ALTER TABLE " + schemaQuote(table) + " DROP CONSTRAINT " + schemaQuote(dc)
				if _, err := db.ExecContext(ctx, q); err != nil && !isAlreadyThere(err) {
					return fmt.Errorf("bootstrap: drop %s.%s: %w", table, dc, err)
				}
			}
			q := "ALTER TABLE " + schemaQuote(table) + " DROP COLUMN " + schemaQuote(c)
			if _, err := db.ExecContext(ctx, q); err != nil && !isAlreadyThere(err) {
				return fmt.Errorf("bootstrap: drop %s.%s: %w", table, c, err)
			}
		}
		log.Printf("schema sync: dropped %d column(s) from %s (the template lost them)", len(todo), table)
	}
	return nil
}

// syncCloneKeys adds each listed template key to the clones that lack it, as
// the template has it. ADD only.
func syncCloneKeys(ctx context.Context, db *sql.DB, store string, log dal.Printer) error {
	for _, k := range cloneKeys[store] {
		var id int64
		var at sql.NullTime
		if err := db.QueryRowContext(ctx, stCloneTemplate.SQL, k.template).Scan(&id, &at); err != nil {
			continue // no template here
		}
		sh, err := readShape(ctx, db, id)
		if err != nil {
			return fmt.Errorf("bootstrap: read template %s: %w", k.template, err)
		}
		var ix *tplIndex
		for i := range sh.indexes {
			if sh.indexes[i].name == k.key {
				ix = &sh.indexes[i]
			}
		}
		if ix == nil {
			continue // the template has no such key yet
		}
		rows, err := db.QueryContext(ctx, stCloneKeyless.SQL, k.pattern, k.template, k.key)
		if err != nil {
			return nil // the catalog cannot be read: the claim still works, only slower
		}
		var tables []string
		for rows.Next() {
			var t string
			if rows.Scan(&t) == nil {
				tables = append(tables, t)
			}
		}
		rows.Close()
		for _, t := range tables {
			if _, err := db.ExecContext(ctx, ix.createSQL(t)); err != nil && !isAlreadyThere(err) {
				return fmt.Errorf("bootstrap: add key %s to %s: %w", k.key, t, err)
			}
		}
		if len(tables) > 0 {
			log.Printf("schema sync: added key %s to %d table(s)", k.key, len(tables))
		}
	}
	return nil
}
