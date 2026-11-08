package mysql

// ap1/ap2/apg1/apg2 hold 255 characters (QA templates API-013, 2026-10-14).
//
// The client's limit and a template's is 255 characters, but the queue row
// and the MT record kept the four in VARCHAR(50) (queue/0015, hot/0013): a
// longer value failed the enqueue — the client was told 503
// QUEUE_UNAVAILABLE — and a template carrying one failed every message it
// matched. queue/0039_ap_wide, hot/0060_ap_wide and (for the record tables by
// company and month, D-136) hot/0063 widen the templates, and bootstrap widens
// the clones that exist at start (widenCloneColumns). What is left is a
// table still 50 wide when a long value reaches it — a clone made from a
// template that was not migrated yet, or a store that started before the
// migration ran — so, like the "Unknown column" self-repair (execRepair),
// the write that meets one widens that table and its template, then runs
// again. A message is never lost to the width of a column.

import (
	"context"
	"errors"
	"log"
	"strings"

	gomysql "github.com/go-sql-driver/mysql"
)

// apColumns are the client's pass-through fields.
var apColumns = []string{"ap1", "ap2", "apg1", "apg2"}

// apWidth is what each holds, in characters (utf8mb4).
const apWidth = "VARCHAR(255) NULL"

// isAPTooLong reports MySQL 1406 (ER_DATA_TOO_LONG) naming one of the ap
// columns — quoted, as MySQL writes it ("Data too long for column 'ap1'").
func isAPTooLong(err error) bool {
	var myErr *gomysql.MySQLError
	if !errors.As(err, &myErr) || myErr.Number != 1406 {
		return false
	}
	for _, c := range apColumns {
		if strings.Contains(myErr.Message, "'"+c+"'") {
			return true
		}
	}
	return false
}

// widenAP makes the four columns 255 characters wide on each table, all four
// in one ALTER (one rebuild of the table, not four). ok = the first table —
// the one written to — was widened.
func (s *Store) widenAP(ctx context.Context, tables ...string) bool {
	parts := make([]string, len(apColumns))
	for i, c := range apColumns {
		parts[i] = "MODIFY " + c + " " + apWidth
	}
	ok := false
	for i, t := range tables {
		t = identRe.ReplaceAllString(t, "_")
		if _, err := s.db.ExecContext(ctx, "ALTER TABLE `"+t+"` "+strings.Join(parts, ", ")); err != nil {
			log.Printf("[store] %s: could not widen ap1/ap2/apg1/apg2 to 255 characters: %v", t, err)
			continue
		}
		log.Printf("[store] %s: ap1/ap2/apg1/apg2 widened to 255 characters (it was made before queue/0039, hot/0060)", t)
		if i == 0 {
			ok = true
		}
	}
	return ok
}

// withWideAP runs write and, when it failed only because an ap value is too
// long for a table still 50 wide, widens the tables and runs it once more.
// tables[0] is the table written to; the rest are its template.
func (s *Store) withWideAP(ctx context.Context, tables []string, write func() error) error {
	err := write()
	if isAPTooLong(err) && s.widenAP(ctx, tables...) {
		err = write()
	}
	return err
}
