package mysql

// Small value helpers shared by the store's writes and reads.

import (
	"os"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/model"
)

// nzInt64 maps 0 to SQL NULL (rejects_: NULL account = refused before auth).
func nzInt64(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

// nzInt maps 0 to SQL NULL so an unset status code is absent rather than 0.
func nzInt(v int) any {
	if v == 0 {
		return nil
	}
	return v
}

// archiveChecksumDisabled lets an operator archive checking only that every
// hot row's key is in cold, not that its values are the same
// (ArchiveTableToCold, step 4). The values are copied either way; the check
// compares every column of every row, which an operator who has measured its
// cost may leave out. (Until 2026-10-18 the check was CHECKSUM TABLE of both
// whole tables.)
func archiveChecksumDisabled() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("YAKU_RETENTION_SKIP_CHECKSUM")))
	return v == "1" || v == "true" || v == "yes"
}

// --- small helpers ---
func orStr(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
func orStatus(s model.Status) model.Status {
	if s == "" {
		return model.StatusQueued
	}
	return s
}
func orTransport(t, def model.Transport) model.Transport {
	if t == "" {
		return def
	}
	return t
}
func orDirection(d model.Direction) string {
	if d == "" {
		return string(model.DirectionMT)
	}
	return string(d)
}
func orSeg(n int) int {
	if n < 1 {
		return 1
	}
	return n
}
func nz(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}
func nsStr(v string) any {
	if v == "" {
		return nil
	}
	return v
}

// msOf is a time as unix milliseconds for a BIGINT column; zero = NULL.
func msOf(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UnixMilli()
}

// timeOfMS reads such a column back; 0 (NULL) = the zero time.
func timeOfMS(ms int64) time.Time {
	if ms <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

func nzTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

// gsm7Flag maps the tri-state Gsm7 string ("true"/"false"/"") to the nullable
// TINYINT column (1/0/NULL).
func gsm7Flag(s string) any {
	switch s {
	case "true":
		return 1
	case "false":
		return 0
	default:
		return nil
	}
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// trunc bounds a value to a column width. Done here rather than left to the
// database because a length error would mask the real failure being recorded.
func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// isUnknownColumn reports MySQL 1054 (ER_BAD_FIELD_ERROR) naming exactly
// `col`.
//
// The name is matched QUOTED, as MySQL writes it — "Unknown column
// 'sent_parts' in 'field list'" — because a bare substring test makes one
// column's name match another's error. "deployment_id" is inside
// "sent_deployment_id" and "stat" is inside "status", so a self-repair keyed
// on the shorter name would keep adding a column that is already there,
// retry, fail on the same error, and never touch the column actually
// missing. That cost an afternoon on 2026-09-24; the quotes are the fix.
func isUnknownColumn(err error, col string) bool {
	return err != nil && strings.Contains(err.Error(), "Unknown column") &&
		strings.Contains(err.Error(), "'"+col+"'")
}
