package logcfg

// Carrying a capture's records to where the portal can read them.
//
// A capture writes its complete record to a file on this gateway host. The
// portal runs elsewhere and cannot reach that file, so each record is ALSO
// inserted into log_capture_records — a table both sides already talk to.
// The portal tails it. No new network path, no list of gateway hosts, and a
// host that is down simply contributes nothing instead of breaking the view.
//
// Two rules shape everything here:
//
//   1. A message must never wait for the database. Records go onto a buffered
//      channel and a single goroutine writes them in batches. When the buffer
//      is full the record is DROPPED — the gateway file already has it, and
//      losing a line in the portal is always better than adding latency to
//      someone's SMS.
//
//   2. The table is a ring. One message produces 21 records, so a busy
//      account fills any table in minutes; the newest N per capture are kept
//      (PO ruling 2026-09-04) and the rest trimmed on the refresh tick.

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/logging"
)

type capRec struct {
	captureID int64
	role      string
	step      string
	messageID string
	at        time.Time
	line      string
}

// bufferSize is how many records may be waiting to be written. Deliberately
// generous — a burst on a captured account should survive a slow write — but
// finite, because the alternative to dropping is growing without limit.
const bufferSize = 4096

// batchSize caps one INSERT. 21 records per message means even a modest rate
// arrives in clumps; writing them one at a time would spend the whole budget
// on round trips.
const batchSize = 200

// flushEvery bounds how long a record waits when traffic is light, so a quiet
// capture still appears in the portal promptly rather than when the buffer
// happens to fill.
const flushEvery = 500 * time.Millisecond

// trimEvery is how often the captures this process wrote to are cut back to
// their newest records.
const trimEvery = 5 * time.Second

type shipper struct {
	db      dal.LogSettings
	ch      chan capRec
	dropped atomic.Int64
}

// StartShipper begins carrying capture records to the database and installs
// itself with the logging package. Stops when ctx is done.
func StartShipper(ctx context.Context, db dal.LogSettings, logf func(string, ...any)) {
	if db == nil {
		return
	}
	s := &shipper{db: db, ch: make(chan capRec, bufferSize)}
	logging.SetCaptureShipper(func(id int64, role, step, msgID string, at time.Time, line string) {
		select {
		case s.ch <- capRec{captureID: id, role: role, step: step, messageID: msgID, at: at, line: line}:
		default:
			// Full. The gateway's own file still has this record, so the cost
			// is one missing line in the portal, not a lost fact — and never
			// a message that waited.
			s.dropped.Add(1)
		}
	})
	go s.run(ctx, logf)
}

func (s *shipper) run(ctx context.Context, logf func(string, ...any)) {
	t := time.NewTicker(flushEvery)
	defer t.Stop()
	batch := make([]capRec, 0, batchSize)
	var lastReport time.Time
	// The captures this process wrote records for since the last trim. Only
	// those can have grown, so only those are trimmed — and a process that
	// ships nothing asks the database nothing.
	wrote := map[int64]bool{}
	lastTrim := time.Now()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := s.write(ctx, batch); err != nil && logf != nil {
			logf("capture records not shipped to the portal (the gateway file is unaffected): %v", err)
		}
		for _, r := range batch {
			wrote[r.captureID] = true
		}
		batch = batch[:0]
	}
	trim := func() {
		if len(wrote) == 0 || time.Since(lastTrim) < trimEvery {
			return
		}
		lastTrim = time.Now()
		ids := make([]int64, 0, len(wrote))
		for id := range wrote {
			ids = append(ids, id)
			delete(wrote, id)
		}
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		// Keep the portal's copy bounded. The gateway file is untouched by
		// this — it stays complete (PO ruling 2026-09-04).
		if err := TrimCaptureRecords(c, s.db, ids, maxCaptureRecords); err != nil && logf != nil {
			logf("capture records not trimmed: %v", err)
		}
	}
	for {
		select {
		case <-ctx.Done():
			flush()
			logging.SetCaptureShipper(nil)
			return
		case r := <-s.ch:
			batch = append(batch, r)
			if len(batch) >= batchSize {
				flush()
			}
		case <-t.C:
			flush()
			trim()
			// Say so if records are being dropped: a portal view that is
			// quietly missing lines is worse than one that admits it.
			if n := s.dropped.Swap(0); n > 0 && logf != nil && time.Since(lastReport) > 10*time.Second {
				logf("capture: %d record(s) dropped before reaching the portal — the gateway file still has them", n)
				lastReport = time.Now()
			}
		}
	}
}

func (s *shipper) write(ctx context.Context, batch []capRec) error {
	recs := make([]dal.CaptureRecord, len(batch))
	for i, r := range batch {
		recs[i] = dal.CaptureRecord{CaptureID: r.captureID, Role: r.role, At: r.at, Step: r.step,
			MessageID: r.messageID, Line: r.line}
	}
	c, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	// One INSERT for the batch. A capture that has already been purged takes
	// its rows with it (ON DELETE CASCADE), so a late batch can fail its
	// foreign key. That is expected, not an error worth shouting about — the
	// caller only logs.
	return s.db.AddCaptureRecords(c, recs)
}

// TrimCaptureRecords keeps only the newest maxRecords rows of each capture
// in ids (PO ruling 2026-09-04: keep the newest, drop the oldest).
//
// Called by the shipper for the captures it wrote to, every few seconds
// rather than per insert: trimming on every write would double the cost of
// the hot path's own shipping, and a few seconds of overshoot costs nothing.
//
// It used to find the captures itself with SELECT DISTINCT capture_id over
// the whole table, on every tick of every role (2026-10-13).
//
// For each capture the cutoff is the id of the Nth newest row; everything at
// or below it goes. One indexed lookup plus one ranged delete, rather than
// counting rows (dal.LogSettings.TrimCaptureRecords).
func TrimCaptureRecords(ctx context.Context, db dal.LogSettings, ids []int64, maxRecords int) error {
	return db.TrimCaptureRecords(ctx, ids, maxRecords)
}
