package mysql

// The log tables as a whole (D-136; by day since 26 Oct 2026): listing them
// and archiving a day to cold. (The billing role's reads of what was unpriced
// and its prices went with the role, 26 Oct 2026: every record is priced when
// it is written, as rate ids.)

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/yakugate/yakugate/internal/billingstore"
	"github.com/yakugate/yakugate/internal/dal"
)

// RecordTable is one record table (billingstore). [SCOPE P8]
type RecordTable = billingstore.RecordTable

// ListRecordTables returns every log table of every kind (never a template,
// never a table of an older layout — the month tables of D-136 among them).
// [SCOPE P8]
func (s *Store) ListRecordTables(ctx context.Context) ([]RecordTable, error) {
	var out []RecordTable
	for _, kind := range billingstore.Kinds {
		// mo\_% also lists mo_out_ tables: each is kept under its own kind.
		rows, err := s.db.QueryContext(ctx, "SHOW TABLES LIKE '"+strings.ReplaceAll(kind, "_", `\_`)+`\_%'`)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var t string
			if err := rows.Scan(&t); err != nil {
				rows.Close()
				return nil, err
			}
			if rt, ok := billingstore.ParseRecordTable(t); ok && rt.Kind == kind {
				out = append(out, rt)
			}
		}
		rows.Close()
	}
	return out, nil
}

// templateForTable is the template a record table was made from.
func templateForTable(name string) string {
	return billingstore.Templates[billingstore.TableFamily(name)]
}

// coldTemplates are the templates mirrored into cold.
func coldTemplates() []string {
	out := make([]string, 0, len(billingstore.Templates))
	for _, k := range billingstore.Kinds {
		out = append(out, billingstore.Templates[k])
	}
	return out
}

// EnsureColdSchema creates the cold archive database (if absent) and a clone
// of every template inside it. coldDB is a bare schema name on the same
// server. [SCOPE P8]
func (s *Store) EnsureColdSchema(ctx context.Context, coldDB string) error {
	coldDB = identRe.ReplaceAllString(coldDB, "_")
	if _, err := s.db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+coldDB+" DEFAULT CHARSET=utf8mb4"); err != nil {
		return err
	}
	for _, tmpl := range coldTemplates() {
		if _, err := s.db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS "+coldDB+"."+tmpl+" LIKE "+tmpl); err != nil {
			return err
		}
	}
	return nil
}

// ArchiveResult is what one archive of a record table did (dal.ArchiveResult).
type ArchiveResult = dal.ArchiveResult

// archiveKey is how a row of a family is known in cold. `key` finds it: the
// message id for the two families that have one unique per message, the row
// id for the rest. `ident` are columns a row never changes after it is
// written: a cold row with the same id but other `ident` values is a
// DIFFERENT row (see ArchiveTableToCold, the table made again).
var archiveKey = map[string]struct {
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

// ArchiveTableToCold copies a record table into the cold schema and, ONLY once
// every hot row is proven to be in cold with the same values, drops the hot
// table. Same-server cross-schema move. [SCOPE P8 / S8-05]
//
// The verification is the point: without it the only copy could be dropped
// with rows missing. A table that fails is left untouched, retried on the
// next pass and reported every time.
//
// The copy brings cold up to date with hot (2026-10-18, go-live P8 E4/D6).
// It was INSERT IGNORE, which never writes a row cold already has, and then
// the two WHOLE tables were checksummed. So a table that changed after its
// first copy — billing still rating it, a receipt that came late, a copy cut
// short and tried again — was refused at every sweep for ever (about 20 s of
// MySQL each time on a 500k-row month), and with YAKU_RETENTION_SKIP_CHECKSUM
// it was dropped with its newer values lost. Now:
//
//  1. a table made AGAIN after its month moved (a late receipt makes it anew)
//     restarts its ids at 1, and those ids already belong to other rows in
//     cold: its ids are moved above both tables' (one UPDATE, the hot table
//     only), so every id names one row;
//  2. the rows cold already has (by `key`) are written again where hot's
//     values differ — hot is the truth until it is dropped;
//  3. the rows cold lacks are added;
//  4. in ONE statement, right before the drop: every hot row has a row in
//     cold with the same value in every column but the row id (compared as
//     bytes, so a change of letter case counts). Cold may hold more rows than
//     hot — the months the table was moved before. With
//     YAKU_RETENTION_SKIP_CHECKSUM only "a row with that key is there" is
//     checked; the values were brought over in 2 and 3 either way.
func (s *Store) ArchiveTableToCold(ctx context.Context, coldDB, table string) (ArchiveResult, error) {
	var res ArchiveResult
	coldDB = identRe.ReplaceAllString(coldDB, "_")
	rt, ok := billingstore.ParseRecordTable(table)
	if !ok {
		return res, fmt.Errorf("not a record table: %q", table)
	}
	k, ok := archiveKey[rt.Kind]
	if !ok {
		return res, fmt.Errorf("archive %s: no key for the family %q", table, rt.Kind)
	}
	cold := coldDB + "." + table
	// Cloned from the LIVE hot template, so the two match column for column.
	if _, err := s.db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS "+cold+" LIKE "+templateForTable(table)); err != nil {
		return res, err
	}
	cols, err := s.tableColumns(ctx, table)
	if err != nil {
		return res, err
	}
	var rest []string // every column but the row id
	for _, c := range cols {
		if c != "id" {
			rest = append(rest, c)
		}
	}
	quoted := func(prefix string, names []string) string {
		out := make([]string, len(names))
		for i, n := range names {
			out[i] = prefix + "`" + n + "`"
		}
		return strings.Join(out, ", ")
	}
	same := func(names []string) string { // every named column the same, as bytes, NULL = NULL
		out := make([]string, len(names))
		for i, n := range names {
			out[i] = "CAST(c.`" + n + "` AS BINARY) <=> CAST(h.`" + n + "` AS BINARY)"
		}
		return strings.Join(out, " AND ")
	}

	// 1. A table made again after its month moved: its ids clash with other rows.
	var clash int64
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" h JOIN "+cold+" c ON c.id = h.id WHERE NOT ("+same(k.ident)+")").
		Scan(&clash); err != nil {
		return res, err
	}
	if clash > 0 {
		var hotMax, coldMax sql.NullInt64
		if err := s.db.QueryRowContext(ctx, "SELECT (SELECT MAX(id) FROM "+table+"), (SELECT MAX(id) FROM "+cold+")").
			Scan(&hotMax, &coldMax); err != nil {
			return res, err
		}
		offset := hotMax.Int64
		if coldMax.Int64 > offset {
			offset = coldMax.Int64
		}
		// Highest first, and the step is past every id there: no id is ever
		// taken twice while the rows move. A row's updated_at is set to
		// itself, so moving its id is not taken for a change of the row.
		keepStamp := ""
		for _, c := range cols {
			if c == "updated_at" {
				keepStamp = ", updated_at = updated_at"
			}
		}
		if _, err := s.db.ExecContext(ctx, "UPDATE "+table+" SET id = id + ?"+keepStamp+" ORDER BY id DESC", offset); err != nil {
			return res, err
		}
		res.Renumbered = true
	}
	// 2. The rows cold already has, written again where hot's differ.
	set := make([]string, len(rest))
	for i, n := range rest {
		set[i] = "c.`" + n + "` = h.`" + n + "`"
	}
	r, err := s.db.ExecContext(ctx, "UPDATE "+cold+" c JOIN "+table+" h ON c.`"+k.key+"` = h.`"+k.key+"` SET "+
		strings.Join(set, ", ")+" WHERE NOT ("+same(rest)+")")
	if err != nil {
		return res, err
	}
	res.Refreshed, _ = r.RowsAffected()
	// 3. The rows cold lacks.
	r, err = s.db.ExecContext(ctx, "INSERT INTO "+cold+" ("+quoted("", cols)+") SELECT "+quoted("h.", cols)+" FROM "+table+
		" h WHERE NOT EXISTS (SELECT 1 FROM "+cold+" c WHERE c.`"+k.key+"` = h.`"+k.key+"`)")
	if err != nil {
		return res, err
	}
	res.Added, _ = r.RowsAffected()
	// 4. Every hot row is in cold, the same — counted in one statement.
	match := same(rest)
	if archiveChecksumDisabled() {
		match = "TRUE"
	}
	var total, found sql.NullInt64
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*), COALESCE(SUM(EXISTS (SELECT 1 FROM "+cold+" c WHERE c.`"+k.key+
		"` = h.`"+k.key+"` AND "+match+")), 0) FROM "+table+" h").Scan(&total, &found); err != nil {
		return res, err
	}
	res.Rows = found.Int64
	if found.Int64 < total.Int64 {
		return res, fmt.Errorf("archive %s: %d of %d hot row(s) are not the same in cold — NOT dropping the hot table",
			table, total.Int64-found.Int64, total.Int64)
	}
	if _, err := s.db.ExecContext(ctx, "DROP TABLE "+table); err != nil {
		return res, err
	}
	s.ensured.Delete(table)
	return res, nil
}

// tableColumns lists a table's stored columns in order.
func (s *Store) tableColumns(ctx context.Context, table string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT column_name FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = ? AND extra NOT LIKE '%GENERATED%' ORDER BY ordinal_position`, table)
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
// (orphans_matched) older than the days kept in hot: their orphan rows have
// gone to cold by then. Returns how many went.
func (s *Store) PruneOrphansMatched(ctx context.Context, days int) (int64, error) {
	if days < 1 {
		days = 1
	}
	res, err := s.db.ExecContext(ctx, "DELETE FROM orphans_matched WHERE matched_at < DATE_SUB(NOW(), INTERVAL ? DAY) LIMIT 50000", days+1)
	if err != nil {
		if isNoSuchTable(err) {
			return 0, nil
		}
		return 0, err
	}
	return res.RowsAffected()
}
