package audit

// Verifying the tamper-evident trail (P1 register 1.9.5).
//
// The chain itself lives in the database (migration config/0088): a BEFORE
// INSERT trigger hashes each row together with the hash of the row before it,
// serialized through a one-row head table so concurrent writers cannot fork
// it. That covers every writer — the Go engine, the Node Admin API, and a DBA
// typing an INSERT at the console.
//
// This file is the other half. A chain nobody can check is decoration, so
// there has to be one command that says plainly whether the trail has been
// interfered with, and where.
//
// The digest is recomputed HERE, in Go, rather than asked of the database.
// Asking MySQL to re-run its own trigger's expression would prove only that
// MySQL agrees with itself: an attacker with enough access to edit the rows
// can edit the trigger too. An independent implementation of the same rule is
// what makes the answer worth anything, which also means this function and
// the trigger must define the digest identically — see canonical() below.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
)

// Status is what verification concluded about the trail.
type Status string

const (
	// StatusVerified: every chained row hashes to what it claims, and each
	// links to the one before it.
	StatusVerified Status = "verified"
	// StatusBroken: at least one row was altered, or a row was removed from
	// the middle of the chain.
	StatusBroken Status = "broken"
	// StatusEmpty: nothing has been written since the chain began.
	StatusEmpty Status = "empty"
)

// Problem is one failed row, named precisely enough to act on.
type Problem struct {
	ID     int64  `json:"id"`
	Reason string `json:"reason"`
}

// Report is the answer to "has anyone touched the audit trail".
type Report struct {
	Status Status `json:"status"`
	// Checked is how many chained rows were examined.
	Checked int64 `json:"checked"`
	// Unprotected counts rows written BEFORE the chain existed. They are not
	// failures and are not counted as verified either: nothing ever attested
	// to them, and saying otherwise would be the one lie this feature cannot
	// afford. Reported so the number is visible rather than quietly ignored.
	Unprotected int64 `json:"unprotected"`
	// FirstProtectedID is where the trustworthy part of the trail starts.
	FirstProtectedID int64 `json:"firstProtectedId,omitempty"`
	// Problems lists what failed, oldest first, capped so a wrecked table
	// cannot produce an unbounded response.
	Problems []Problem `json:"problems,omitempty"`
	// HeadMatches reports whether the stored head hash still equals the last
	// row's hash. A mismatch with no broken link above it means rows were
	// deleted from the END of the trail — the case the links alone cannot
	// see, because nothing remains to point at the missing rows.
	HeadMatches bool `json:"headMatches"`
}

// maxProblems bounds the report. Past this the answer is not "which rows" but
// "this trail is gone", and listing a million ids helps nobody.
const maxProblems = 100

// auditColumns is the digest input, in order, and MUST match the trigger in
// migration config/0088 exactly — same columns, same order. A column in one
// and not the other turns every row into a false alarm.
//
// Everything except id and the two hash columns is included: a column left
// out of the digest is a column an attacker may edit freely. id is absent
// because it is not yet assigned while the BEFORE INSERT trigger runs.
var auditColumns = []string{
	"actor", "actor_name", "actor_email", "account_id", "company_id", "company_name",
	"action", "method", "route", "params", "query", "response_code", "summary", "outcome", "reason",
	"entity", "feature", "module", "entity_id", "target_label",
	"detail", "before_data", "after_data", "ip", "user_agent", "content_type", "created_at",
}

// The DAL reads every column back AS CHAR (dal.AuditLog.WalkChain), which is
// the one detail that makes an independent implementation possible at all.
//
// CONCAT_WS inside the trigger casts each value to a string using MySQL's own
// rules. A driver does not have to agree: with parseTime enabled this one
// hands back a DATETIME as "2026-09-08T17:50:41Z" where MySQL wrote
// "2026-09-08 17:50:41", and that single field made every row on the trail
// report as edited on the first live run. Asking the server to do the cast
// removes the whole class of mismatch — for every column and for any column
// whose type changes later — instead of teaching Go to imitate MySQL one type
// at a time.

// canonical joins one row's values the way the trigger's CONCAT_WS does.
//
// NULL becomes the empty string rather than being skipped: CONCAT_WS drops
// NULLs outright, which would let ("a", NULL, "b") and (NULL, "a", "b")
// digest alike — a gap an attacker could move a value through.
func canonical(prev string, vals []dal.NullString) string {
	parts := make([]string, 0, len(vals)+1)
	parts = append(parts, prev)
	for _, v := range vals {
		parts = append(parts, v.String) // NullString.String is "" when NULL
	}
	return strings.Join(parts, "|")
}

func digest(prev string, vals []dal.NullString) string {
	sum := sha256.Sum256([]byte(canonical(prev, vals)))
	return hex.EncodeToString(sum[:])
}

// Verify walks the chained portion of audit_log in id order and reports what
// it finds.
//
// Deliberately a full walk rather than a sample: a spot check that happens to
// miss the altered row reports "verified", which is worse than not checking at
// all. The table is small (tens of thousands of rows over a deployment's life)
// and this runs on demand, never on a request path.
func Verify(ctx context.Context, db dal.AuditLog) (Report, error) {
	var rep Report

	n, err := db.UnprotectedCount(ctx)
	if err != nil {
		return rep, fmt.Errorf("audit verify: count unprotected: %w", err)
	}
	rep.Unprotected = n

	// The column list is derived, never a literal: a hand-counted column
	// count that drifts from the SELECT fails as a scan error at run time, in
	// the one function whose whole job is to be trustworthy.
	var (
		prevHash string // the hash the PREVIOUS row actually computed to
		lastHash string
		first    = true
	)
	err = db.WalkChain(ctx, auditColumns, func(id int64, prev, stored dal.NullString, vals []dal.NullString) error {
		rep.Checked++
		if first {
			rep.FirstProtectedID = id
			first = false
		} else if prev.String != prevHash {
			// The link is broken: this row names a predecessor that is not the
			// row before it. Something between them was removed, or one of the
			// two was rewritten.
			rep.addProblem(id, "does not follow the previous row — a row was removed or rewritten between them")
		}
		want := digest(prev.String, vals)
		if want != stored.String {
			rep.addProblem(id, "contents do not match this row's own hash — the row was edited after it was written")
		}
		prevHash = stored.String
		lastHash = stored.String
		return nil
	})
	var scanErr dal.ScanError
	var rowsErr dal.RowsError
	switch {
	case errors.As(err, &scanErr):
		return rep, fmt.Errorf("audit verify: scan: %w", scanErr.Err)
	case errors.As(err, &rowsErr):
		return rep, fmt.Errorf("audit verify: iterate: %w", rowsErr.Err)
	case err != nil:
		return rep, fmt.Errorf("audit verify: read chain: %w", err)
	}

	// The head is the only witness to rows deleted off the END of the trail:
	// with them gone, no surviving row points at them.
	head, err := db.ChainHead(ctx)
	switch {
	case err == dal.ErrNoRows:
		rep.addProblem(0, "the chain head row is missing — appends are no longer being chained")
	case err != nil:
		return rep, fmt.Errorf("audit verify: read head: %w", err)
	}
	rep.HeadMatches = head == lastHash
	if !rep.HeadMatches && rep.Checked > 0 {
		rep.addProblem(0, "the newest row does not match the chain head — rows were deleted from the end of the trail")
	}

	switch {
	case len(rep.Problems) > 0:
		rep.Status = StatusBroken
	case rep.Checked == 0:
		rep.Status = StatusEmpty
	default:
		rep.Status = StatusVerified
	}
	return rep, nil
}

func (r *Report) addProblem(id int64, reason string) {
	if len(r.Problems) >= maxProblems {
		return
	}
	r.Problems = append(r.Problems, Problem{ID: id, Reason: reason})
}

// ColumnCount is the number of columns in the digest, exported so a test can
// assert it still matches the trigger's own list without duplicating it.
func ColumnCount() int { return len(auditColumns) }
