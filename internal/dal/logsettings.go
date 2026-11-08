package dal

import (
	"context"
	"fmt"
	"time"

	"github.com/yakugate/yakugate/internal/platform/config"
)

// The log settings and flow captures every role keeps (internal/logcfg,
// plan of 29 Oct 2026): the per-account, per-vendor and system log_config
// columns, each account's company master number, the captures running and
// due to be purged, and the capture records carried to the portal
// (log_capture_records). How a setting is read and applied stays in logcfg.

// NamedLogConfig is one account's or vendor's name and its log_config as
// text (Valid=false when NULL).
type NamedLogConfig struct {
	Name   string
	Config NullString
}

// CaptureRow is one flow capture as RefreshCaptures reads it: SecsLeft is
// the time to its expiry on the store's own clock (a duration, so no time
// zone is assumed), Running whether it runs here, Due whether its purge time
// passed in the last two days, Marked whether its row says purged.
type CaptureRow struct {
	ID                   int64
	ScopeType            string
	ScopeID              int64
	Name                 string
	SecsLeft             int64
	Running, Due, Marked bool
}

// CaptureRecord is one record of a capture, for the portal. MessageID ""
// is written NULL.
type CaptureRecord struct {
	CaptureID int64
	Role      string
	At        time.Time
	Step      string
	MessageID string
	Line      string
}

// LogSettings is what logcfg reads and writes.
type LogSettings interface {
	// AccountLogConfigs and VendorLogConfigs read every account's
	// (username) and vendor's (cid) log_config. A row that would not scan,
	// or a read cut short, ends the read with that error.
	AccountLogConfigs(ctx context.Context) ([]NamedLogConfig, error)
	VendorLogConfigs(ctx context.Context) ([]NamedLogConfig, error)
	// AccountMasters is username -> the master number of the account's
	// company, only for companies that have one.
	AccountMasters(ctx context.Context) (map[string]string, error)
	// SystemLogConfig is the system row's log_config. ErrNoRows: no row.
	SystemLogConfig(ctx context.Context) (NullString, error)

	// Captures reads, in one statement, the flow captures running for
	// deployment (or for every deployment, on a store without the column
	// yet) and the ones whose purge time passed in the last two days.
	Captures(ctx context.Context, deployment string) ([]CaptureRow, error)
	// MarkCapturePurged marks a capture's row purged (once).
	MarkCapturePurged(ctx context.Context, id int64) error

	// AddCaptureRecords writes records in one statement.
	AddCaptureRecords(ctx context.Context, recs []CaptureRecord) error
	// TrimCaptureRecords keeps only the newest maxRecords rows of each
	// capture in ids.
	TrimCaptureRecords(ctx context.Context, ids []int64, maxRecords int) error
}

// OpenLogSettings opens the log settings on the install's SQL database (the
// config store's one pool a process).
func OpenLogSettings(cfg config.Config) (LogSettings, error) {
	e, err := OpenEngineConfig(cfg)
	if err != nil {
		return nil, err
	}
	if e.Logs == nil {
		return nil, fmt.Errorf("dal: log settings on %s: %w", SQL(cfg), ErrNotServed)
	}
	return e.Logs, nil
}
