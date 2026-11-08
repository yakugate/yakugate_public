package backup

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
)

// Restore loads one database of a backup run back into MySQL. [SCOPE P8 S8-04]
//
// What it promises:
//   - the run is checked against MANIFEST.json first; any missing, extra or
//     changed file refuses the whole restore;
//   - every target table is checked BEFORE the first row is written: a table
//     that already has rows refuses the restore unless IntoNonEmpty is set
//     (and then rows are only added — a duplicate key fails that table);
//   - nothing is ever dropped, truncated, updated or deleted. A table the
//     target does not have is created from the CREATE TABLE saved in the run;
//   - one transaction per table, batched INSERTs, and the row count must match
//     the manifest or the table is rolled back.
//
// The intended disaster-recovery path is an EMPTY database: the tables are
// created from the run, the rows go in, and the triggers are created last so
// they do not fire for restored rows (the way mysqldump orders it). A table
// that already exists WITH a trigger fires it for every restored row.

// RestoreOptions says what to restore and where.
type RestoreOptions struct {
	RunDir       string
	DB           string   // the run's sub-folder: config, cold or hot
	DSN          string   // target MySQL (the same setting the roles read)
	Tables       []string // empty = every table of DB in the run
	IntoNonEmpty bool     // allow adding rows to a table that already has some
	DryRun       bool     // verify and pre-flight only; write nothing
	Logf         func(format string, a ...any)
}

// RestoredTable is the outcome for one table.
type RestoredTable struct {
	Table   string
	Rows    int64
	Created bool // the table did not exist and was created from the run
}

// RestoreResult is what Restore did.
type RestoreResult struct {
	Target   string // database name on the target server
	Tables   []RestoredTable
	Triggers []string // triggers created
}

// Restore runs one restore. On an error, tables already listed in the result
// are committed; the failing table and everything after it are untouched.
func Restore(ctx context.Context, o RestoreOptions) (*RestoreResult, error) {
	logf := o.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	if o.DSN == "" {
		return nil, fmt.Errorf("no DSN set for the %q store", o.DB)
	}
	man, rep, err := Verify(o.RunDir)
	if err != nil {
		return nil, err
	}
	if !rep.OK() {
		return nil, fmt.Errorf("the run does not match its manifest (%s); nothing was restored", rep.String())
	}
	logf("manifest %s", rep.String())
	if !man.Whole() {
		// Still restored — it may be the best copy there is — but never
		// without saying what it lacks (2026-10-18).
		logf("NOTE: this run is not a whole backup: %d table(s) left out %v, database(s) not dumped %v",
			len(man.ExcludedTables), man.ExcludedTables, man.DatabasesFailed)
	}

	tables, err := selectTables(man, o.DB, o.Tables)
	if err != nil {
		return nil, err
	}

	// One connection for the whole restore, its session set for loading
	// (foreign keys off, an id of 0 kept as 0, strict): the database's DAL
	// (dal.Restorer). A DSN that names no database is refused there.
	conn, err := dal.OpenRestorer(ctx, o.DSN)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	res := &RestoreResult{Target: conn.Target()}

	// Pre-flight: every table, before the first write.
	missing := map[string]bool{}
	var refused []string
	for _, t := range tables {
		exists, hasRows, err := conn.TableState(ctx, t)
		if err != nil {
			return nil, err
		}
		switch {
		case !exists && man.File(o.DB+"/"+t+".sql") == nil:
			refused = append(refused, t+" (not in the target, and the run has no CREATE TABLE for it)")
		case !exists:
			missing[t] = true
		case hasRows && !o.IntoNonEmpty:
			refused = append(refused, t+" (already has rows)")
		}
		if exists {
			if trs, _ := conn.InsertTriggers(ctx, t); len(trs) > 0 {
				logf("WARNING: %s has trigger(s) %s — they fire for every restored row", t, strings.Join(trs, ", "))
			}
		}
	}
	if len(refused) > 0 {
		return res, fmt.Errorf("refused, nothing was written to %s: %s%s", res.Target, strings.Join(refused, "; "),
			map[bool]string{true: "", false: " — restore into an empty database, or pass --into-nonempty to add rows"}[o.IntoNonEmpty])
	}
	if o.DryRun {
		for _, t := range tables {
			res.Tables = append(res.Tables, RestoredTable{Table: t, Rows: *man.File(o.DB + "/" + t + ".jsonl.gz").Rows, Created: missing[t]})
		}
		return res, nil
	}

	for _, t := range tables {
		if missing[t] {
			ddl, err := os.ReadFile(filepath.Join(o.RunDir, o.DB, t+".sql"))
			if err != nil {
				return res, err
			}
			if err := conn.CreateTable(ctx, string(ddl)); err != nil {
				return res, fmt.Errorf("create %s: %w", t, err)
			}
		}
		want := man.File(o.DB + "/" + t + ".jsonl.gz").Rows
		n, err := conn.RestoreTable(ctx, t, filepath.Join(o.RunDir, o.DB, t+".jsonl.gz"), want)
		if err != nil {
			return res, fmt.Errorf("table %s: %w (this table was rolled back)", t, err)
		}
		res.Tables = append(res.Tables, RestoredTable{Table: t, Rows: n, Created: missing[t]})
		logf("%s: %d rows%s", t, n, map[bool]string{true: " (table created)", false: ""}[missing[t]])
	}

	// Triggers last, and only for a whole-database restore.
	if len(o.Tables) == 0 && man.File(o.DB+"/"+triggersFile) != nil {
		created, err := conn.RestoreTriggers(ctx, filepath.Join(o.RunDir, o.DB, triggersFile))
		res.Triggers = created
		if err != nil {
			return res, err
		}
	}
	return res, nil
}

// selectTables returns the tables of db to restore, in name order.
func selectTables(man *Manifest, db string, only []string) ([]string, error) {
	have := map[string]bool{}
	for _, f := range man.Files {
		dir, file, ok := strings.Cut(f.Path, "/")
		if !ok || dir != db || !strings.HasSuffix(file, ".jsonl.gz") {
			continue
		}
		t := strings.TrimSuffix(file, ".jsonl.gz")
		if strings.ContainsAny(t, "`/\\") || t == "" {
			return nil, fmt.Errorf("unsafe table name %q in the manifest", t)
		}
		if f.Rows == nil {
			return nil, fmt.Errorf("%s has no row count in the manifest", f.Path)
		}
		have[t] = true
	}
	if len(have) == 0 {
		return nil, fmt.Errorf("the run has no tables for database %q", db)
	}
	var out []string
	if len(only) == 0 {
		for t := range have {
			out = append(out, t)
		}
	} else {
		seen := map[string]bool{}
		for _, t := range only {
			if !have[t] {
				return nil, fmt.Errorf("table %q is not in the run's %q database", t, db)
			}
			if !seen[t] {
				seen[t] = true
				out = append(out, t)
			}
		}
	}
	sort.Strings(out)
	return out, nil
}
