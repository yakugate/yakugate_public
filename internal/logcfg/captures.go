package logcfg

// Process 13 Type 3 — keeping the running-capture set fresh.
//
// Separate cadence from the log-settings refresh, and deliberately much
// faster. The shortest capture window is ONE MINUTE: on the 30s config
// cadence an operator could lose half of it waiting for the capture to be
// noticed. Five seconds costs one small indexed query per role and makes the
// start delay a rounding error against the window.
//
// Expiry is NOT handled here. logging.Active checks expires_at against the
// clock on every call, so a capture stops exactly on time whatever this
// ticker is doing — this only has to notice STARTS.

import (
	"context"
	"os"
	"strconv"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/platform/logging"
)

// CaptureInterval is how often the running-capture set is re-read.
const CaptureInterval = 5 * time.Second

// maxCaptureRecords is how many records the PORTAL keeps per capture. The
// gateway's own file keeps everything.
//
// 20,000 is about 950 messages of history at the measured 21 records per
// message — enough to see what led up to a problem, small enough that a
// capture left running on a busy account cannot swamp the config store.
// Override with YAKU_CAPTURE_MAX_RECORDS.
var maxCaptureRecords = envInt("YAKU_CAPTURE_MAX_RECORDS", 20000)

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return n
		}
	}
	return def
}

// The DAL reads, in ONE statement (dal.LogSettings.Captures), the captures
// that are running here and the ones whose files are due to go. It used to be
// three statements on every tick of every role, one of them a walk through
// all of log_capture_records: 186,000 polls read 9.1 billion rows in 20 hours
// on the dev box while 4 rows were written (2026-10-13).
//
// The expiry comes back as SECONDS REMAINING, never as an absolute
// timestamp.
//
// log_captures.expires_at is a plain DATETIME in the database server's own
// clock (the Admin API stamps it with NOW() for exactly that reason). Our
// driver runs with parseTime=true and no loc, so it would read that value
// as if it were UTC — five hours adrift on this deployment, the same GW-39
// mismatch the Admin API's hot pool documents. A capture would then run
// hours past its window, or stop before it started, depending on which way
// the server's zone leans.
//
// A DURATION crosses the boundary safely: it is the same number in every
// zone. Adding it to our own clock gives an instant we can compare against
// time.Now() with no shared-timezone assumption anywhere.
//
// A capture may name one deployment (config/0138, D-123): only that
// deployment's roles record it; NULL = every deployment, as before. A
// store without the column yet (migrations not run) reads every capture.
//
// A capture whose purge time passed in the last two days is read whatever
// its status (see purgeDue). Flow captures only: a packet capture (type
// 'traffic') is the http-server's own (admin_pcap.go).

// RefreshCaptures reads every capture that is still running and pushes it
// into the logging package, and deletes the files of the captures past their
// purge time (purgeDue).
//
// A failed read leaves the previous set in place rather than clearing it: a
// database blip must not silently stop a capture an operator is watching, nor
// silently start one that was stopped. The clock-based expiry in
// logging.Active bounds how wrong a stale set can be.
func RefreshCaptures(ctx context.Context, db dal.LogSettings) error {
	rows, err := db.Captures(ctx, deployment.ID())
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	byKind := map[logging.ScopeKind][]logging.Capture{
		logging.ScopeAccount: {},
		logging.ScopeVendor:  {},
		logging.ScopeSystem:  {},
	}
	var due, unmarked []int64
	for _, r := range rows {
		id, scopeType, scopeID, name, secsLeft := r.ID, r.ScopeType, r.ScopeID, r.Name, r.SecsLeft
		running, isDue, mark := r.Running, r.Due, r.Marked
		if isDue {
			due = append(due, id)
			if !mark {
				unmarked = append(unmarked, id)
			}
		}
		if !running || secsLeft <= 0 {
			continue // not running, or already over by the time we read it
		}
		kind := logging.ScopeKind(scopeType)
		if kind != logging.ScopeAccount && kind != logging.ScopeVendor && kind != logging.ScopeSystem {
			continue
		}
		byKind[kind] = append(byKind[kind], logging.Capture{
			ID: id, Kind: kind, ScopeID: scopeID, Name: name,
			ExpiresAt: now.Add(time.Duration(secsLeft) * time.Second),
		})
	}
	for kind, list := range byKind {
		logging.SetCaptures(kind, list)
	}
	return purgeDue(ctx, db, due, unmarked)
}

// StartCaptures polls for running captures until ctx is done. Like Start, it
// never returns an error: a capture being briefly unreadable is not a reason
// for a role to fail.
func StartCaptures(ctx context.Context, db dal.LogSettings, logf func(string, ...any)) {
	if db == nil {
		return
	}
	run := func() {
		c, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		// One statement: the running set and the files due to go. The
		// portal's copy is kept bounded by the shipper, for the captures it
		// writes to (shipper.go).
		if err := RefreshCaptures(c, db); err != nil && logf != nil {
			logf("running captures not refreshed (keeping the previous set): %v", err)
		}
	}
	run()
	go func() {
		t := time.NewTicker(CaptureInterval)
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

// purgeDue deletes the files of every capture past its purge time (due), and
// marks the rows not yet marked (unmarked) so the work is not repeated.
//
// Runs on the same tick as the refresh. Each role purges the files IT wrote —
// one capture spans several roles, so each holds its own copy under its own
// folder — but only the role that gets there first flips the row.
//
// Every role still deletes its own files for a row another role has already
// flipped (2026-10-02): with roles on several hosts — several deployments —
// the first host to flip the row used to leave every other host's copy on
// disk for good, message text included. A capture whose purge time passed in
// the last two days is looked at whatever its status; deleting a file that is
// not there costs nothing.
func purgeDue(ctx context.Context, db dal.LogSettings, due, unmarked []int64) error {
	if len(due) == 0 {
		return nil
	}
	logging.PurgeCaptures(due)
	for _, id := range unmarked {
		if err := db.MarkCapturePurged(ctx, id); err != nil {
			return err
		}
	}
	return nil
}
