package mssql

// The log tables as a whole, on SQL Server (mirrors
// internal/dal/mysql/records_tables.go): listing them, and archiving a day's
// table to the cold database on the same server — copied, every hot row
// proven to be in cold with the same values, and only then dropped.
//
// What differs in the writing: the cold database's tables are made from the
// hot templates' shape (clone.go) in the cold database
// (EXEC <cold>.sys.sp_executesql), as CREATE TABLE … LIKE does on MySQL; an
// IDENTITY id cannot be updated, so a table made again after its day moved
// gets its ids moved by copying its rows out and back with
// IDENTITY_INSERT, in one transaction; "the same, as bytes, NULL = NULL" is
// each column CAST to VARBINARY, in a CASE so a NULL on one side is a
// difference, not an unknown.

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/yakugate/yakugate/internal/billingstore"
	"github.com/yakugate/yakugate/internal/dal"
)

var recIdentRe = regexp.MustCompile(`[^a-zA-Z0-9_]`)

var (
	stRecAllTables = RecordStmts.Add("record.tables", "SELECT TOP (200000) name FROM sys.tables WHERE name LIKE N'%[_]%[_]%' AND name NOT LIKE N'tpl[_]%'", false)
	stRecColdDB    = RecordStmts.Add("record.cold.database", `IF DB_ID(@p1) IS NULL
	BEGIN
		DECLARE @yk_ddl NVARCHAR(400) = N'CREATE DATABASE ' + QUOTENAME(@p1) + N' COLLATE Latin1_General_100_CI_AS_SC';
		EXEC (@yk_ddl);
	END`, false)
	stRecColdHas = RecordStmts.Add("record.cold.has", "SELECT COUNT(*) WHERE OBJECT_ID(@p1, N'U') IS NOT NULL", false)
	stRecColdDDL = RecordStmts.Add("record.cold.ddl", "EXEC {cold}.sys.sp_executesql @p1", false)
	stRecColumns = RecordStmts.Add("record.columns", `SELECT c.name FROM sys.columns AS c
		WHERE c.object_id = OBJECT_ID(@p1, N'U') AND c.is_computed = 0 ORDER BY c.column_id`, false)
	stRecPruneMatched = RecordStmts.Add("record.orphans.prune_matched",
		"DELETE TOP (50000) FROM orphans_matched WHERE matched_at < DATEADD(DAY, -@p1, SYSUTCDATETIME())", false)
)

// ListRecordTables returns every log table of every kind (never a template).
func (s *RecordStore) ListRecordTables(ctx context.Context) ([]dal.RecordTable, error) {
	rows, err := rQuery(ctx, s.db, stRecAllTables, stRecAllTables.SQL)
	if err != nil {
		return nil, err
	}
	byKind := map[string][]dal.RecordTable{}
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			rows.Close()
			return nil, err
		}
		if rt, ok := billingstore.ParseRecordTable(t); ok {
			byKind[rt.Kind] = append(byKind[rt.Kind], rt)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	var out []dal.RecordTable
	for _, kind := range billingstore.Kinds {
		ts := byKind[kind]
		sort.Slice(ts, func(i, j int) bool { return ts[i].Name < ts[j].Name })
		out = append(out, ts...)
	}
	return out, nil
}

// recColdTable is a table of the cold database, three parts.
func recColdTable(coldDB, table string) string {
	return quoteIdent(coldDB) + ".[dbo]." + quoteIdent(table)
}

// recCloneInto makes table in the cold database from the hot template's
// shape, unless it is there.
func (s *RecordStore) recCloneInto(ctx context.Context, coldDB, template, table string) error {
	var has int
	if err := rRow(ctx, s.db, stRecColdHas, stRecColdHas.SQL, rText(coldDB+".dbo."+table)).Scan(&has); err != nil {
		return err
	}
	if has == 1 {
		return nil
	}
	sh, err := s.clone.shape(ctx, template)
	if err != nil {
		return fmt.Errorf("cold %s: %w", table, err)
	}
	_, err = rExec(ctx, s.db, stRecColdDDL, rOn(stRecColdDDL.SQL, "cold", coldDB), rText(sh.createDDL(template, table)))
	if err != nil && strings.Contains(err.Error(), "already an object named") {
		return nil // made by another process meanwhile
	}
	return err
}

// EnsureColdSchema creates the cold archive database (if absent) and a clone
// of every template inside it. coldDB is a bare database name on the same
// server.
func (s *RecordStore) EnsureColdSchema(ctx context.Context, coldDB string) error {
	coldDB = recIdentRe.ReplaceAllString(coldDB, "_")
	if _, err := rExec(ctx, s.db, stRecColdDB, stRecColdDB.SQL, rText(coldDB)); err != nil {
		return err
	}
	for _, k := range billingstore.Kinds {
		tpl := billingstore.Templates[k]
		if err := s.recCloneInto(ctx, coldDB, tpl, tpl); err != nil {
			return err
		}
	}
	return nil
}

// recArchiveKey is how a row of a family is known in cold (MySQL's
// archiveKey): key finds it, ident are columns a row never changes.
var recArchiveKey = map[string]struct {
	key   string
	ident []string
}{
	billingstore.KindMT:      {"msg_id", []string{"msg_id"}},
	billingstore.KindMO:      {"msg_id", []string{"msg_id"}},
	billingstore.KindMOOut:   {"msg_id", []string{"msg_id"}},
	billingstore.KindRejects: {"msg_id", []string{"msg_id"}},
	billingstore.KindSub:     {"id", []string{"msg_id", "part", "attempt", "result"}},
	billingstore.KindDLR:     {"id", []string{"msg_id", "created_at"}},
	billingstore.KindHLR:     {"id", []string{"looked_up_at", "destination"}},
	billingstore.KindOrphans: {"id", []string{"kind", "received_at"}},
	billingstore.KindMOSub:   {"id", []string{"try_id"}},
	billingstore.KindDLRSub:  {"id", []string{"try_id"}},
}

// recArchiveChecksumOff is YAKU_RETENTION_SKIP_CHECKSUM: check only that
// every hot row's key is in cold.
func recArchiveChecksumOff() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("YAKU_RETENTION_SKIP_CHECKSUM")))
	return v == "1" || v == "true" || v == "yes"
}

// ArchiveTableToCold copies a record table into the cold database and, ONLY
// once every hot row is proven to be in cold with the same values, drops the
// hot table (MySQL's ArchiveTableToCold, its four steps).
func (s *RecordStore) ArchiveTableToCold(ctx context.Context, coldDB, table string) (dal.ArchiveResult, error) {
	var res dal.ArchiveResult
	coldDB = recIdentRe.ReplaceAllString(coldDB, "_")
	rt, ok := billingstore.ParseRecordTable(table)
	if !ok {
		return res, fmt.Errorf("not a record table: %q", table)
	}
	k, ok := recArchiveKey[rt.Kind]
	if !ok {
		return res, fmt.Errorf("archive %s: no key for the family %q", table, rt.Kind)
	}
	if err := s.recCloneInto(ctx, coldDB, billingstore.Templates[billingstore.TableFamily(table)], table); err != nil {
		return res, err
	}
	cold, hot := recColdTable(coldDB, table), quoteIdent(table)
	cols, err := s.recColumns(ctx, table)
	if err != nil {
		return res, err
	}
	var rest []string
	for _, c := range cols {
		if c != "id" {
			rest = append(rest, c)
		}
	}
	quoted := func(prefix string, names []string) string {
		out := make([]string, len(names))
		for i, n := range names {
			out[i] = prefix + quoteIdent(n)
		}
		return strings.Join(out, ", ")
	}
	// same: every named column the same, as bytes, NULL = NULL — 1 or 0.
	same := func(names []string) string {
		out := make([]string, len(names))
		for i, n := range names {
			q := quoteIdent(n)
			out[i] = "(CAST(c." + q + " AS VARBINARY(MAX)) = CAST(h." + q + " AS VARBINARY(MAX)) OR (c." + q + " IS NULL AND h." + q + " IS NULL))"
		}
		return "CASE WHEN " + strings.Join(out, " AND ") + " THEN 1 ELSE 0 END"
	}
	key := quoteIdent(k.key)

	// 1. A table made again after its day moved: its ids clash with other rows.
	var clash int64
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT_BIG(*) FROM "+hot+" AS h INNER JOIN "+cold+" AS c ON c.id = h.id WHERE "+
		same(k.ident)+" = 0").Scan(&clash); err != nil {
		return res, err
	}
	if clash > 0 {
		var hotMax, coldMax sql.NullInt64
		if err := s.db.QueryRowContext(ctx, "SELECT (SELECT MAX(id) FROM "+hot+"), (SELECT MAX(id) FROM "+cold+")").Scan(&hotMax, &coldMax); err != nil {
			return res, err
		}
		offset := hotMax.Int64
		if coldMax.Int64 > offset {
			offset = coldMax.Int64
		}
		// Out, and back with their ids past every id there, in one
		// transaction: an IDENTITY column cannot be updated.
		move := `SET XACT_ABORT ON; SET NOCOUNT ON;
BEGIN TRAN;
SELECT id + @p1 AS id, ` + quoted("", rest) + ` INTO #yk_move FROM ` + hot + ` WITH (TABLOCKX);
DELETE FROM ` + hot + `;
SET IDENTITY_INSERT ` + hot + ` ON;
INSERT INTO ` + hot + ` (id, ` + quoted("", rest) + `) SELECT id, ` + quoted("", rest) + ` FROM #yk_move;
SET IDENTITY_INSERT ` + hot + ` OFF;
DROP TABLE #yk_move;
COMMIT;`
		if _, err := s.db.ExecContext(ctx, move, offset); err != nil {
			return res, err
		}
		res.Renumbered = true
	}
	// 2. The rows cold already has, written again where hot's differ.
	set := make([]string, len(rest))
	for i, n := range rest {
		set[i] = quoteIdent(n) + " = h." + quoteIdent(n)
	}
	r, err := s.db.ExecContext(ctx, "UPDATE c SET "+strings.Join(set, ", ")+" FROM "+cold+" AS c INNER JOIN "+hot+" AS h ON c."+key+" = h."+key+
		" WHERE "+same(rest)+" = 0")
	if err != nil {
		return res, err
	}
	res.Refreshed, _ = r.RowsAffected()
	// 3. The rows cold lacks, with their ids.
	var added int64
	if err := s.db.QueryRowContext(ctx, "SET NOCOUNT ON; SET IDENTITY_INSERT "+cold+" ON;\nINSERT INTO "+cold+" ("+quoted("", cols)+") SELECT "+
		quoted("h.", cols)+" FROM "+hot+" AS h WHERE NOT EXISTS (SELECT 1 FROM "+cold+" AS c WHERE c."+key+" = h."+key+");\n"+
		"DECLARE @yk_n BIGINT = @@ROWCOUNT; SET IDENTITY_INSERT "+cold+" OFF; SELECT @yk_n").Scan(&added); err != nil {
		return res, err
	}
	res.Added = added
	// 4. Every hot row is in cold, the same — counted in one statement.
	match := same(rest) + " = 1"
	if recArchiveChecksumOff() {
		match = "1 = 1"
	}
	var total, found int64
	if err := s.db.QueryRowContext(ctx, "SELECT (SELECT COUNT_BIG(*) FROM "+hot+"), (SELECT COUNT_BIG(*) FROM "+hot+" AS h WHERE EXISTS (SELECT 1 FROM "+
		cold+" AS c WHERE c."+key+" = h."+key+" AND "+match+"))").Scan(&total, &found); err != nil {
		return res, err
	}
	res.Rows = found
	if found < total {
		return res, fmt.Errorf("archive %s: %d of %d hot row(s) are not the same in cold — NOT dropping the hot table",
			table, total-found, total)
	}
	if _, err := s.db.ExecContext(ctx, "DROP TABLE "+hot); err != nil {
		return res, err
	}
	s.ensured.Delete(table)
	return res, nil
}

// recColumns lists a table's stored columns in order.
func (s *RecordStore) recColumns(ctx context.Context, table string) ([]string, error) {
	rows, err := rQuery(ctx, s.db, stRecColumns, stRecColumns.SQL, rText(table))
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("table %s has no columns (is it there?)", table)
	}
	return out, nil
}

// PruneOrphansMatched lets go of the marks of orphans applied after all
// older than the days kept in hot. Returns how many went.
func (s *RecordStore) PruneOrphansMatched(ctx context.Context, days int) (int64, error) {
	if days < 1 {
		days = 1
	}
	res, err := rExec(ctx, s.db, stRecPruneMatched, stRecPruneMatched.SQL, days+1)
	if err != nil {
		if MissingTable(err) {
			return 0, nil
		}
		return 0, err
	}
	return res.RowsAffected()
}
