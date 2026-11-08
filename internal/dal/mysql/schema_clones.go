package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
)

// The tables cloned from a template, kept in step with it — moved here
// unchanged on 30 Oct 2026 from internal/platform/bootstrap/bootstrap.go.

var (
	stCloneTemplateCols = SchemaStmts.Add("schema.clone_template_columns", `
		SELECT column_name, column_type, is_nullable, COALESCE(column_default, ''), (column_default IS NOT NULL)
		  FROM information_schema.columns
		 WHERE table_schema = DATABASE() AND table_name = ?
		 ORDER BY ordinal_position`, false)
	stCloneTables = SchemaStmts.Add("schema.clone_tables", `
		SELECT table_name FROM information_schema.tables
		 WHERE table_schema = DATABASE() AND table_name LIKE ? AND table_name <> ?
		   AND table_type = 'BASE TABLE'`, false)
	stCloneCols = SchemaStmts.Add("schema.clone_columns", `
		SELECT table_name, column_name, column_type FROM information_schema.columns
		 WHERE table_schema = DATABASE() AND table_name LIKE ? AND table_name <> ?`, false)
	stCloneKeyless = SchemaStmts.Add("schema.clones_without_key", `
			SELECT t.table_name FROM information_schema.tables t
			WHERE t.table_schema = DATABASE() AND t.table_name LIKE ? AND t.table_name <> ?
			  AND t.table_type = 'BASE TABLE'
			  AND NOT EXISTS (SELECT 1 FROM information_schema.statistics s
			                  WHERE s.table_schema = DATABASE() AND s.table_name = t.table_name
			                    AND s.index_name = ?)`, false)
	stKeyColumns = SchemaStmts.Add("schema.key_columns", `
		SELECT column_name FROM information_schema.statistics
		WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?
		ORDER BY seq_in_index`, false)
)

// cloneFamilies are the template -> clone relationships this store uses. Tables
// are created with CREATE TABLE ... LIKE <template>, so a migration that ALTERs
// only the template leaves every existing clone behind — and an INSERT naming a
// new column then fails against the stale clones.
var cloneFamilies = map[string][]struct {
	template, pattern string
	// match, when set, is what a clone's name must be: a LIKE pattern cannot
	// tell mo_12_202610 from an old mo_log_2026_10.
	match *regexp.Regexp
}{
	"queue": {
		{template: "queue_template", pattern: "queue\\_%"},
		// The HLR shards hlr_<first4> (6 Nov 2026): a column added to the
		// template reaches every shard at start. Only four digits: the hot
		// day tables hlr_<company>_<day> share a database with them on
		// some installs and are not shards.
		{template: "hlr_template", pattern: "hlr\\_%", match: hlrShard},
	},
	"hlr": {{template: "hlr_template", pattern: "hlr\\_%", match: hlrShard}},
	// The log tables by company and DAY (issue 25.3, hot/0066): each kind is
	// made from its tpl_day_ template. The month tables of D-136 and every
	// layout before them are not ours any more and are not kept in step.
	"hot": {
		{template: "tpl_day_mt", pattern: "mt\\_%", match: recordKind("mt")},
		{template: "tpl_day_sub", pattern: "sub\\_%", match: recordKind("sub")},
		{template: "tpl_day_dlr", pattern: "dlr\\_%", match: recordKind("dlr")},
		{template: "tpl_day_mo", pattern: "mo\\_%", match: recordKind("mo")},
		{template: "tpl_day_mo_out", pattern: "mo\\_out\\_%", match: recordKind("mo_out")},
		{template: "tpl_day_hlr", pattern: "hlr\\_%", match: recordKind("hlr")},
		{template: "tpl_day_rejects", pattern: "rejects\\_%", match: recordKind("rejects")},
		{template: "tpl_day_orphans", pattern: "orphans\\_%", match: recordKind("orphans")},
		{template: "tpl_day_mo_sub", pattern: "mo\\_sub\\_%", match: recordKind("mo_sub")},
		{template: "tpl_day_dlr_sub", pattern: "dlr\\_sub\\_%", match: recordKind("dlr_sub")},
	},
}

// hlrShard is an HLR shard's name: hlr_ and the number's first four digits.
var hlrShard = regexp.MustCompile(`^hlr_[0-9]{4}$`)

// recordKind is the name of a log table of one kind: <kind>_<company>_<YYYYMMDD>.
// mo's never matches an mo_out_ table (its company part is digits).
func recordKind(kind string) *regexp.Regexp {
	return regexp.MustCompile(`^` + kind + `_[0-9]+_[0-9]{8}$`)
}

// SyncClones brings the store's clone tables up to their templates
// (syncCloneColumns).
func (s *schema) SyncClones(ctx context.Context, log dal.Printer) error {
	return syncCloneColumns(ctx, s.db, s.store, log)
}

// syncCloneColumns brings every clone table up to its template's column set.
//
// Migrations before this existed carried a manual note ("existing tables are
// backfilled at deploy"); this performs that reconciliation automatically and
// idempotently at startup. It only ever ADDs columns — it never drops or
// retypes anything, so it cannot destroy data, and a clone that already matches
// costs one information_schema query.
func syncCloneColumns(ctx context.Context, db *sql.DB, store string, log dal.Printer) error {
	for _, fam := range cloneFamilies[store] {
		// One read of the family serves both checks (see readCloneFamily).
		f, err := readCloneFamily(ctx, db, fam.template, fam.pattern, fam.match)
		if err != nil {
			return err
		}
		missing := f.missing()
		tables := make([]string, 0, len(missing))
		for table := range missing {
			tables = append(tables, table)
		}
		sort.Strings(tables)
		for _, table := range tables {
			cols := missing[table]
			for _, c := range cols {
				stmt := fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN %s", table, c.definition())
				if _, err := db.ExecContext(ctx, stmt); err != nil {
					return fmt.Errorf("bootstrap: sync %s.%s: %w", table, c.name, err)
				}
			}
			log.Printf("schema sync: added %d column(s) to %s", len(cols), table)
		}
		if err := widenCloneColumns(ctx, db, f, log); err != nil {
			return err
		}
		if err := dropCloneColumns(ctx, db, fam.template, f, log); err != nil {
			return err
		}
	}
	return syncCloneKeys(ctx, db, store, log)
}

// droppedColumns are columns a template LOST, by template: a clone that still
// has one has it dropped at start (Saleem, 27 Oct 2026: the three-table design
// — mt_ is written at acceptance and holds nothing about the vendor, "drop
// everywhere"). Only names listed here are ever dropped: a column a clone has
// and its template does not is otherwise left alone. The keys go first (a key
// on a dropped column would be left half), then the columns, one ALTER each,
// INSTANT where MySQL can.
var droppedColumns = map[string]struct{ keys, cols []string }{
	"tpl_day_mt": {
		keys: []string{"idx_vendor_msg"},
		cols: []string{"vendor_id", "vendor_msg_id", "vendor_protocol", "sent_parts", "attempts", "vendor_ack_at",
			"sent_deployment_id", "route_level", "send_round", "buy_rate_id", "buy_fx_rate", "buy_base_rate"},
	},
}

// dropCloneColumns drops from every clone the columns its template lost
// (droppedColumns) that the clone still has. The template must have lost
// them first (its migration applied): until then nothing is dropped.
func dropCloneColumns(ctx context.Context, db *sql.DB, template string, f *cloneFamily, log dal.Printer) error {
	gone, ok := droppedColumns[template]
	if !ok || len(f.template) == 0 {
		return nil
	}
	inTemplate := map[string]bool{}
	for _, c := range f.template {
		inTemplate[strings.ToLower(c.name)] = true
	}
	for _, c := range gone.cols {
		if inTemplate[c] {
			return nil // the template's migration has not run yet: drop nothing
		}
	}
	for _, table := range f.tables {
		have := f.clones[table]
		var todo []string
		for _, c := range gone.cols {
			if _, ok := have[c]; ok {
				todo = append(todo, c)
			}
		}
		if len(todo) == 0 {
			continue
		}
		for _, k := range gone.keys {
			_, err := db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE `%s` DROP INDEX `%s`", table, k))
			if err != nil && !isAlreadyThereErr(err) {
				return fmt.Errorf("bootstrap: drop %s.%s: %w", table, k, err)
			}
		}
		for _, c := range todo {
			stmt := fmt.Sprintf("ALTER TABLE `%s` DROP COLUMN `%s`, ALGORITHM=INSTANT", table, c)
			_, err := db.ExecContext(ctx, stmt)
			if err != nil && !isAlreadyThereErr(err) {
				// MySQL cannot drop this one instantly: the table is rebuilt.
				_, err = db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE `%s` DROP COLUMN `%s`", table, c))
			}
			if err != nil && !isAlreadyThereErr(err) {
				return fmt.Errorf("bootstrap: drop %s.%s: %w", table, c, err)
			}
		}
		log.Printf("schema sync: dropped %d column(s) from %s (the template lost them)", len(todo), table)
	}
	return nil
}

// cloneKeys are template keys a clone must also have, by name. Deliberately a
// short list, not every key: adding an index to hundreds of large daily log
// partitions at start would hold every role's start for minutes. A key goes
// here when a query depends on it on EVERY clone — idx_owner_claim keeps one
// deployment's claim from locking rows another deployment is about to take
// (queue/0037, D-121); idx_pick serves every take of issue 22 (queue/0044:
// by owner, short code, priority and pick time).
//
// A key that only makes a screen fast does NOT go here — see lateCloneKeys.
var cloneKeys = map[string][]struct{ template, pattern, key string }{
	"queue": {
		{template: "queue_template", pattern: "queue\\_%", key: "idx_owner_claim"},
		{template: "queue_template", pattern: "queue\\_%", key: "idx_pick"},
	},
}

// syncCloneKeys adds each listed template key to the clones that lack it,
// with the template's own columns in order. ADD only, never drop.
func syncCloneKeys(ctx context.Context, db *sql.DB, store string, log dal.Printer) error {
	for _, k := range cloneKeys[store] {
		cols, err := keyColumns(ctx, db, k.template, k.key)
		if err != nil || len(cols) == 0 {
			continue // the template has no such key yet (migration not applied): nothing to copy
		}
		rows, err := db.QueryContext(ctx, stCloneKeyless.SQL, k.pattern, k.template, k.key)
		if err != nil {
			return nil // information_schema unavailable: the claim still works, only slower
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
			// Every column the key names must exist on the clone first; the
			// column sync above has just added any that were missing.
			stmt := fmt.Sprintf("ALTER TABLE `%s` ADD KEY `%s` (`%s`)", t, k.key, strings.Join(cols, "`,`"))
			if _, err := db.ExecContext(ctx, stmt); err != nil && !isAlreadyThereErr(err) {
				return fmt.Errorf("bootstrap: add key %s to %s: %w", k.key, t, err)
			}
		}
		if len(tables) > 0 {
			log.Printf("schema sync: added key %s to %d table(s)", k.key, len(tables))
		}
	}
	return nil
}

// keyColumns lists a key's columns in order.
func keyColumns(ctx context.Context, db *sql.DB, table, key string) ([]string, error) {
	rows, err := db.QueryContext(ctx, stKeyColumns.SQL, table, key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// widenCloneColumns reconciles clone columns whose TYPE lags a template
// widening — e.g. request_id CHAR(26) → VARCHAR(64) (queue/0016, hot/0012).
// It applies ONLY provable widenings (template is varchar with capacity >=
// the clone's char/varchar length), so like the ADD-only sync above it can
// never destroy data; any other type drift is left for a deliberate
// migration and logged. [2026-07-26]
func widenCloneColumns(ctx context.Context, db *sql.DB, f *cloneFamily, log dal.Printer) error {
	type widen struct{ table, col, want, nullable string }
	var todo []widen
	for _, table := range f.tables {
		have := f.clones[table]
		for _, c := range f.template {
			got, ok := have[strings.ToLower(c.name)]
			if !ok || got == c.colType {
				continue // a missing column was just added with the template's type
			}
			if isWidening(got, c.colType) {
				todo = append(todo, widen{table: table, col: c.name, want: c.colType, nullable: c.nullable})
			} else {
				log.Printf("schema sync: %s.%s type %q differs from template %q — NOT auto-changed (not a provable widening)", table, c.name, got, c.colType)
			}
		}
	}
	for _, w := range todo {
		null := " NULL"
		if w.nullable == "NO" {
			null = " NOT NULL"
		}
		stmt := fmt.Sprintf("ALTER TABLE `%s` MODIFY `%s` %s%s", w.table, w.col, w.want, null)
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("bootstrap: widen %s.%s: %w", w.table, w.col, err)
		}
		log.Printf("schema sync: widened %s.%s to %s", w.table, w.col, w.want)
	}
	return nil
}

// isWidening reports whether changing a column from `have` to `want` is a
// lossless character-type widening: char(n)/varchar(n) → varchar(m), m >= n.
func isWidening(have, want string) bool {
	hn, hok := charLen(have)
	wn, wok := charLen(want)
	return hok && wok && strings.HasPrefix(strings.ToLower(want), "varchar") && wn >= hn
}

// charLen extracts n from "char(n)"/"varchar(n)" (any case); ok=false otherwise.
func charLen(t string) (int, bool) {
	t = strings.ToLower(strings.TrimSpace(t))
	if !strings.HasPrefix(t, "char(") && !strings.HasPrefix(t, "varchar(") {
		return 0, false
	}
	open := strings.Index(t, "(")
	close := strings.Index(t, ")")
	if open < 0 || close <= open+1 {
		return 0, false
	}
	n, err := strconv.Atoi(t[open+1 : close])
	return n, err == nil
}

type cloneCol struct {
	name, colType, nullable, defaultVal string
	hasDefault                          bool // column_default IS NOT NULL — distinct from an EMPTY default ('')
}

// definition renders the ADD COLUMN clause. Types come from information_schema
// for the template itself, so a clone always ends up with the template's type.
func (c cloneCol) definition() string {
	s := "`" + c.name + "` " + c.colType
	if c.nullable == "NO" {
		s += " NOT NULL"
	}
	if c.hasDefault {
		// information_schema stores the default UNQUOTED; string-ish types need
		// the quotes back (an empty-string default renders as '').
		if isNumericDefault(c.colType, c.defaultVal) {
			s += " DEFAULT " + c.defaultVal
		} else {
			s += " DEFAULT '" + strings.ReplaceAll(c.defaultVal, "'", "''") + "'"
		}
	}
	return s
}

// isNumericDefault reports whether the default should render unquoted.
func isNumericDefault(colType, def string) bool {
	ct := strings.ToLower(colType)
	for _, p := range []string{"int", "decimal", "float", "double", "bit"} {
		if strings.Contains(ct, p) {
			return true
		}
	}
	// expressions like CURRENT_TIMESTAMP stay unquoted too
	return strings.EqualFold(def, "CURRENT_TIMESTAMP") || strings.HasPrefix(def, "CURRENT_TIMESTAMP")
}

// cloneFamily is one template and its clones as information_schema has them.
type cloneFamily struct {
	template []cloneCol                   // the template's columns, in order
	tables   []string                     // every clone, sorted
	clones   map[string]map[string]string // clone -> lower(column name) -> column type
}

// readCloneFamily reads a template's columns and every clone's columns in one
// pass each.
//
// It used to be ONE query that, for every template column and every clone,
// asked information_schema whether the clone had that column (a correlated
// NOT EXISTS). MySQL answers each of those from its data dictionary one at a
// time: 337 mtlog tables took 10 seconds, the cost grew with every day's new
// log tables, and EVERY role paid it at EVERY start (found 2026-10-06, when
// the e2e harnesses stopped coming up inside their 15 seconds). The same
// facts read in one scan take a tenth of a second; the comparison is done
// here.
//
// A read that fails is not fatal: migrations already ran, and a stale clone
// surfaces loudly on its first INSERT. The family then reads as empty.
func readCloneFamily(ctx context.Context, db *sql.DB, template, pattern string, match *regexp.Regexp) (*cloneFamily, error) {
	f := &cloneFamily{clones: map[string]map[string]string{}}
	rows, err := db.QueryContext(ctx, stCloneTemplateCols.SQL, template)
	if err != nil {
		return f, nil
	}
	for rows.Next() {
		var c cloneCol
		if err := rows.Scan(&c.name, &c.colType, &c.nullable, &c.defaultVal, &c.hasDefault); err != nil {
			rows.Close()
			return nil, err
		}
		f.template = append(f.template, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(f.template) == 0 {
		return f, nil // no template here: nothing to compare with
	}

	// The clones: real tables only (a view of the same name pattern is not one).
	rows, err = db.QueryContext(ctx, stCloneTables.SQL, pattern, template)
	if err != nil {
		return &cloneFamily{clones: map[string]map[string]string{}}, nil
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
		f.clones[t] = map[string]string{}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Strings(f.tables)
	if len(f.tables) == 0 {
		return f, nil
	}

	rows, err = db.QueryContext(ctx, stCloneCols.SQL, pattern, template)
	if err != nil {
		return &cloneFamily{clones: map[string]map[string]string{}}, nil
	}
	defer rows.Close()
	for rows.Next() {
		var t, col, typ string
		if err := rows.Scan(&t, &col, &typ); err != nil {
			return nil, err
		}
		if have, ok := f.clones[t]; ok { // not a base table: not a clone
			have[strings.ToLower(col)] = typ
		}
	}
	return f, rows.Err()
}

// missing is, per clone, the template columns it lacks, in the template's
// order.
//
// Only nullable / defaulted columns: a NOT NULL column with no default cannot
// be added safely to a populated table, so it is left for a migration author
// to handle deliberately. hasDefault (not the value) makes the call — a
// DEFAULT ” column is perfectly addable, and the old value-based check
// silently skipped exactly that (found via hlr_source, 2026-08-27).
func (f *cloneFamily) missing() map[string][]cloneCol {
	out := map[string][]cloneCol{}
	for _, table := range f.tables {
		have := f.clones[table]
		if len(have) == 0 {
			continue // its columns could not be read: nothing is claimed about it
		}
		for _, c := range f.template {
			if _, ok := have[strings.ToLower(c.name)]; ok {
				continue
			}
			if c.nullable == "NO" && !c.hasDefault {
				continue
			}
			out[table] = append(out[table], c)
		}
	}
	return out
}
