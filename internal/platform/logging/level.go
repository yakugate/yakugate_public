package logging

import "strings"

// Level is a log severity. The vocabulary is the PO's (P13 spec §6,
// 2026-07-27, reaffirmed 2026-09-04): debug / info / warn / error / none.
//
// Filtering is "at or above": a record is written only when its own severity
// is >= the effective level for its scope. So debug writes everything, info
// writes info+warn+error, warn writes warn+error, error writes error only,
// and none writes nothing at all.
//
// LevelNone deliberately sorts ABOVE error so that comparison alone
// suppresses every record — there is no severity a caller can pass that
// reaches a scope set to none. The DB audit trail (P13 Type 1) is a separate
// mechanism and is never suppressed by this; see the spec's "audit events are
// always written regardless of level".
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
	LevelNone
)

// String renders the level as it appears in the `level` field of a log line.
func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "debug"
	case LevelInfo:
		return "info"
	case LevelWarn:
		return "warn"
	case LevelError:
		return "error"
	case LevelNone:
		return "none"
	}
	return "info"
}

// Enabled reports whether a record of severity `rec` is written to a scope
// configured at level `l`.
func (l Level) Enabled(rec Level) bool { return rec >= l && l != LevelNone }

// ParseLevel reads a configured level, case-insensitively and tolerantly:
// the live `sms_service_config.log_level` column already holds values like
// "NONE" in upper case, and "warning" is the word the P13 spec uses in prose
// where the wire value is "warn". An empty or unrecognised value returns the
// supplied fallback rather than guessing, so a typo in one account's config
// can never silently switch that account to a noisier or quieter level than
// the system default.
func ParseLevel(s string, fallback Level) Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return LevelDebug
	case "info":
		return LevelInfo
	case "warn", "warning":
		return LevelWarn
	case "error":
		return LevelError
	case "none", "off":
		return LevelNone
	}
	return fallback
}
