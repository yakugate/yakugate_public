package mssql

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	mssql "github.com/microsoft/go-mssqldb"

	"github.com/yakugate/yakugate/internal/dal"
)

// Running one migration file on SQL Server. Which files run, and in what
// order, is the bootstrap's (migrations/<store>/mssql/*.sql).
//
// A T-SQL migration file is a list of batches split by GO lines, as sqlcmd
// and SSMS read it. Each batch is one unit: it runs whole, and its marker
// (<version>#<n>) is written once it has, so a file that fails half way
// resumes at the batch that failed (GW-07, as on MySQL). A statement whose
// end state already holds — the column, key or constraint it adds is there,
// the one it drops is gone, the table an ALTER reaches for is absent on this
// install — is passed with a log line, as MySQL's runner does; that is
// judged per batch, so such a statement stands in a batch of its own.

// ApplyMigration runs one migration file and records it as version.
func (s *schema) ApplyMigration(ctx context.Context, version, body string, applied map[string]bool, log dal.Printer) error {
	db := s.db
	batches := splitBatches(body)
	for i, batch := range batches {
		marker := fmt.Sprintf("%s#%d", version, i)
		if len(batches) > 1 && applied[marker] {
			continue
		}
		if stripComments(batch) == "" {
			continue // comments only (a seed's "-- yaku:" lines mean nothing here)
		}
		if _, err := db.ExecContext(ctx, batch); err != nil {
			switch {
			case isMissingObject(err) && isAlterBatch(batch):
				log.Printf("migration %s statement %d skipped — target table absent on this deployment", version, i+1)
			case isAlreadyThere(err) && isAlterBatch(batch):
				log.Printf("migration %s statement %d already satisfied — the column or key is present", version, i+1)
			default:
				return fmt.Errorf("bootstrap: apply %s (statement %d): %w", version, i+1, err)
			}
		}
		if len(batches) > 1 {
			_, _ = db.ExecContext(ctx, stSchemaMark.SQL, marker)
		}
	}
	if _, err := db.ExecContext(ctx, stSchemaRecord.SQL, version); err != nil {
		return fmt.Errorf("bootstrap: record %s: %w", version, err)
	}
	_, _ = db.ExecContext(ctx, stSchemaForgetMarks.SQL, version+"#%")
	return nil
}

// goLine is a batch separator: GO alone on its line (any case).
var goLine = regexp.MustCompile(`(?i)^\s*GO\s*;?\s*$`)

// splitBatches breaks a T-SQL script into its batches at GO lines. A GO
// inside a /* … */ comment is not a separator. A batch that is only comments
// and blank lines is dropped. "GO n" (repeat) is not supported: a migration
// never needs it.
func splitBatches(body string) []string {
	var out []string
	var cur []string
	inComment := false
	flush := func() {
		s := strings.TrimSpace(strings.Join(cur, "\n"))
		// A batch of comments only is dropped, unless it carries a seed's
		// "-- yaku:" line (Seed reads those).
		if stripComments(s) != "" || strings.Contains(s, "-- yaku:") {
			out = append(out, s)
		}
		cur = cur[:0]
	}
	for _, line := range strings.Split(strings.ReplaceAll(body, "\r\n", "\n"), "\n") {
		if !inComment && goLine.MatchString(line) {
			flush()
			continue
		}
		cur = append(cur, line)
		inComment = blockCommentOpen(line, inComment)
	}
	flush()
	return out
}

// blockCommentOpen says whether a /* … */ comment is still open after line,
// given whether one was open before it. Comment marks inside a quoted string
// or after -- are not counted.
func blockCommentOpen(line string, open bool) bool {
	inQuote := false
	for i := 0; i < len(line); i++ {
		c := line[i]
		switch {
		case open:
			if c == '*' && i+1 < len(line) && line[i+1] == '/' {
				open = false
				i++
			}
		case inQuote:
			if c == '\'' {
				inQuote = false
			}
		case c == '\'':
			inQuote = true
		case c == '-' && i+1 < len(line) && line[i+1] == '-':
			return false
		case c == '/' && i+1 < len(line) && line[i+1] == '*':
			open = true
			i++
		}
	}
	return open
}

// stripComments removes -- line comments and /* */ comments (enough to see
// whether a batch says anything; batches run verbatim).
func stripComments(s string) string {
	var b strings.Builder
	open := false
	for _, line := range strings.Split(s, "\n") {
		for i := 0; i < len(line); i++ {
			if open {
				if line[i] == '*' && i+1 < len(line) && line[i+1] == '/' {
					open = false
					i++
				}
				continue
			}
			if line[i] == '-' && i+1 < len(line) && line[i+1] == '-' {
				break
			}
			if line[i] == '/' && i+1 < len(line) && line[i+1] == '*' {
				open = true
				i++
				continue
			}
			b.WriteByte(line[i])
		}
		b.WriteByte(' ')
	}
	return strings.TrimSpace(b.String())
}

// isAlterBatch reports whether a batch's first statement is an ALTER TABLE,
// a CREATE INDEX or a DROP INDEX — the only kind the two tolerances apply to.
func isAlterBatch(batch string) bool {
	up := strings.ToUpper(strings.Join(strings.Fields(stripComments(batch)), " "))
	for _, p := range []string{"ALTER TABLE ", "CREATE INDEX ", "CREATE UNIQUE INDEX ", "CREATE NONCLUSTERED INDEX ",
		"CREATE UNIQUE NONCLUSTERED INDEX ", "DROP INDEX "} {
		if strings.HasPrefix(up, p) {
			return true
		}
	}
	return false
}

// errNumbers lists every error number an error carries (a batch may raise
// several).
func sqlErrNumbers(err error) []int32 {
	var me mssql.Error
	if !errors.As(err, &me) {
		return nil
	}
	out := []int32{me.Number}
	for _, e := range me.All {
		out = append(out, e.Number)
	}
	return out
}

func errHasNumber(err error, nums ...int32) bool {
	for _, n := range sqlErrNumbers(err) {
		for _, want := range nums {
			if n == want {
				return true
			}
		}
	}
	return false
}

// isAlreadyThere is the "that is already how it is" family: 2705 (a column
// added is there), 1913 (an index added is there), 2714 (an object or
// constraint added is there), 1801 (the database is there), 4924 (a column
// dropped is gone), 3703 (an index dropped is gone), 3728 (a constraint
// dropped is gone).
func isAlreadyThere(err error) bool {
	return errHasNumber(err, 2705, 1913, 2714, 1801, 4924, 3703, 3728)
}

// isMissingObject: 208 (no such object), 4902 (ALTER TABLE on a table that
// is not there).
func isMissingObject(err error) bool {
	return errHasNumber(err, 208, 4902)
}
