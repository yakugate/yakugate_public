// Package logcfg keeps the logging package's per-account and per-vendor log
// settings up to date from the Config store — level, rotation size and
// retention, all held in one log_config JSON column on both
// sms_service_config and vendors (migration config/0081).
//
// It exists as its own package because the two sides of the engine that need
// it do not share a cache: P1 and P2 have refdata's in-memory cache, while
// P3/P4/P5/P6 read vendors straight from the database with their own queries.
// Rather than add a column to six different queries, every role starts this
// one refresher from cmd/yakugate and gets both maps.
//
// Reads are cheap (two small SELECTs on the same 30s cadence every other
// cache in this engine uses) and the result is pushed into logging, so a
// level lookup on the message hot path stays an in-memory map read.
package logcfg

import (
	"context"
	"encoding/json"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/logging"
)

// Interval matches the refdata cache's own refresh cadence, for consistency
// rather than because logging specifically needs that number.
const Interval = 30 * time.Second

// Refresh reads both config maps once and pushes them into logging.
//
// A failure on either query leaves the PREVIOUS settings in place rather than
// wiping them — a brief database blip must not silently switch every account
// back to the process default (and, for an account set to none, start writing
// its traffic to disk again).
func Refresh(ctx context.Context, db dal.LogSettings) error {
	acct, err := read(db.AccountLogConfigs(ctx))
	if err != nil {
		return err
	}
	vend, err := read(db.VendorLogConfigs(ctx))
	if err != nil {
		return err
	}
	logging.SetScopeConfigs(logging.ScopeAccount, acct)
	logging.SetScopeConfigs(logging.ScopeVendor, vend)

	// Each account's company master number (config/0141), so every
	// account-scoped line names the customer as the operator's books do. A
	// failed read (a store older than 0141, a blip) keeps the last set.
	if m, err := db.AccountMasters(ctx); err == nil {
		logging.SetAccountMasters(m)
	}

	// The SYSTEM level, which is also the default every unconfigured account
	// and vendor falls back to (migration config/0086). Read here rather than
	// only from YAKU_LOG_LEVEL at start-up, so an operator can turn debug on
	// during an incident without restarting every role.
	//
	// A missing or unreadable row leaves the process-wide default alone: an
	// operator who has never touched the setting must not have their env
	// value silently replaced by a compiled-in one.
	raw, err := db.SystemLogConfig(ctx)
	if err == nil && raw.Valid && raw.String != "" {
		var j logConfigJSON
		if json.Unmarshal([]byte(raw.String), &j) == nil {
			c := logging.SystemConfig()
			if j.Level != nil {
				c.Level = logging.ParseLevel(*j.Level, c.Level)
			}
			if j.MaxSizeMB != nil {
				c.MaxSizeMB = *j.MaxSizeMB
			}
			if j.RetentionDays != nil {
				c.RetentionDays = *j.RetentionDays
			}
			logging.SetSystemConfig(c)
		}
	} else if err != nil && err != dal.ErrNoRows {
		return err
	}
	return nil
}

// logConfigJSON is the stored shape of the log_config column on both
// sms_service_config and vendors (migration config/0081). Every field is a
// pointer so a key that is simply absent can be told apart from one set to
// zero — absent means "use the process default", zero means "no limit".
type logConfigJSON struct {
	Level         *string `json:"level"`
	MaxSizeMB     *int    `json:"max_size_mb"`
	RetentionDays *int    `json:"retention_days"`
}

// read turns one name/config read into settings. A row with no readable
// config is left out of the map entirely, which is what makes it fall back to
// the process-wide default (the D-069 precedence shape).
func read(rows []dal.NamedLogConfig, err error) (map[string]logging.Config, error) {
	if err != nil {
		return nil, err
	}
	out := map[string]logging.Config{}
	for _, row := range rows {
		name, raw := row.Name, row.Config
		if name == "" || !raw.Valid || raw.String == "" {
			continue
		}
		var j logConfigJSON
		if err := json.Unmarshal([]byte(raw.String), &j); err != nil {
			// Malformed config must never take a tenant's logging offline in
			// some surprising way — treat it as unconfigured and move on.
			continue
		}
		c := logging.Defaults
		if j.Level != nil {
			// A typo must not quietly move an account to a different level,
			// so a value ParseLevel does not recognise keeps the default.
			c.Level = logging.ParseLevel(*j.Level, c.Level)
		}
		if j.MaxSizeMB != nil {
			c.MaxSizeMB = *j.MaxSizeMB
		}
		if j.RetentionDays != nil {
			c.RetentionDays = *j.RetentionDays
		}
		out[name] = c
	}
	return out, nil
}

// Start does one immediate Refresh, then repeats every Interval until ctx is
// done. It never returns an error: logging config being briefly unreadable is
// not a reason for a role to fail, so problems are reported through logf and
// the last-known levels stay in force.
func Start(ctx context.Context, db dal.LogSettings, logf func(string, ...any)) {
	if db == nil {
		return
	}
	run := func() {
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := Refresh(c, db); err != nil && logf != nil {
			logf("log config not refreshed (keeping the previous settings): %v", err)
		}
	}
	run()
	go func() {
		t := time.NewTicker(Interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				run()
			}
		}
	}()
}
