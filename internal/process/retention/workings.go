package retention

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Step runs one input -> logic -> output cycle for the "retention" process.
//
//	INPUT:  the log tables (<kind>_<company>_<YYYYMMDD>, issue 25.3) + each
//	        company's data_retention.hot_days from the config store
//	LOGIC:  the hot store keeps today and the days before it, up to the
//	        company's number (default YAKU_HOT_DAYS, 90). From 02:00 UTC each
//	        night, every table of a day older than that is copied to the cold
//	        archive, checked (every row the same) and dropped from hot.
//	        Hourly: messages whose vendor's wait for a receipt ran out leave
//	        open_msgs; long messages whose receipts never all came let go of
//	        their working rows; matched orphans' marks older than the days kept
//	        go.
//	OUTPUT: aged records live only in cold; hot shrinks
func (p *Process) Step(ctx context.Context) error {
	if p.hot == nil {
		return nil
	}
	// Gateway-wide role: one deployment runs it, the rest stand by
	// (internal/singleton). Two copies of retention against one shared store
	// would drop the same table twice, the loser erroring every pass.
	if held, err := p.lease.Held(ctx); !held {
		return err
	}
	now := time.Now().UTC()
	if !p.lastRun.IsZero() && now.Sub(p.lastRun) < p.interval {
		return nil // throttled
	}
	p.lastRun = now
	p.job.SetInterval(p.interval)
	var archived, archivedRows, refused, letGo int64
	var pruned int
	defer func() {
		p.job.Finish(now, map[string]any{
			"tables_archived": archived, "rows_archived": archivedRows,
			"tables_refused": refused, "concat_groups_pruned": pruned,
			"open_messages_let_go": letGo,
		}, nil)
	}()

	// GW-23 (PO 2026-08-28): the ledger's partition calendar + post-invoice
	// rollup — the retention role's config-store duty, also cold-independent.
	p.ledgerUpkeep(ctx, now)
	// Messages whose vendor's wait for a receipt has run out (D-136): they
	// keep their last status; a receipt that comes later is an orphan. (With
	// Redis the waiting messages are in Redis and leave with their facts.)
	if n, err := p.hot.SweepOpenMessages(ctx, 50000); err != nil {
		p.log.Printf("let go of open messages: %v", err)
		p.job.NoteError(err)
	} else {
		letGo = n
	}
	// Retire multi-part correlation rows whose receipts never all arrived. A
	// completed group lets go of its rows the moment it reaches a verdict
	// (D-040); this is the safety net for the ones that never do.
	if n, err := p.hot.PruneConcatParts(ctx, p.concatHours, 500); err != nil {
		p.log.Printf("prune concat parts: %v", err)
		p.job.NoteError(err)
	} else if n > 0 {
		pruned = int(n)
		p.log.Printf("pruned %d stale concat group(s) older than %dh", n, p.concatHours)
	}

	if p.upkeep == nil || p.coldDB == "" {
		// Nothing is ever dropped without a copy: with no cold store the
		// record tables simply stay in hot.
		if !p.saidNoCold {
			p.saidNoCold = true
			p.log.Printf("no cold store is set (YAKU_COLD_DSN): record tables are kept in the hot store, nothing is archived")
		}
		return nil
	}
	// The cold store must be the hot store's own server (2026-10-18, go-live
	// P8 A6): a month is copied inside one server, so with YAKU_COLD_DSN on
	// another server the name alone used to be made on the HOT server, months
	// were moved there, and backup — which reads cold from the DSN — never
	// saw them. Nothing is moved until the DSN names the same server.
	if why := p.coldProblem(ctx); why != "" {
		if why != p.saidCold {
			p.saidCold = why
			p.log.Printf("nothing is archived: %s", why)
		}
		p.job.NoteError(errors.New(why))
		return nil
	}
	p.saidCold = ""
	if err := p.hot.EnsureColdSchema(ctx, p.coldDB); err != nil {
		p.log.Printf("ensure cold schema %s: %v", p.coldDB, err)
		p.job.NoteError(err)
		return nil
	}
	// The marks of orphans applied after all go with the days kept: their
	// orphan rows are archived by then.
	if _, err := p.hot.PruneOrphansMatched(ctx, p.hotDays); err != nil {
		p.log.Printf("prune matched orphans: %v", err)
	}
	days := p.loadHotDays(ctx)
	tables, err := p.hot.ListRecordTables(ctx)
	if err != nil {
		p.log.Printf("list record tables: %v", err)
		p.job.NoteError(err)
		return nil
	}
	for _, t := range tables {
		keep := p.hotDays
		if d, ok := days[t.CompanyID]; ok && d > 0 {
			keep = d
		}
		if !archiveDue(t.Date, keep, now) {
			continue
		}
		res, err := p.hot.ArchiveTableToCold(ctx, p.coldDB, t.Name)
		if err != nil {
			// Verification refusing to drop (S8-05) or a plain failure: the hot
			// table is STILL THERE and is tried again next cycle. Said at full
			// volume every time.
			p.log.Printf("archive %s -> %s FAILED, hot table kept: %v", t.Name, p.coldDB, err)
			p.job.NoteError(err)
			refused++
			continue
		}
		archived++
		archivedRows += res.Rows
		p.job.Done.Add(1)
		var how []string
		if res.Refreshed > 0 {
			how = append(how, fmt.Sprintf("%d row(s) cold already had brought up to date", res.Refreshed))
		}
		if res.Renumbered {
			how = append(how, "the table was made again after an earlier move: its row ids were moved above cold's")
		}
		note := ""
		if len(how) > 0 {
			note = "; " + strings.Join(how, "; ")
		}
		p.log.Printf("archived %s (%d rows verified in cold, %d day(s) kept in hot) -> %s; hot table dropped%s",
			t.Name, res.Rows, keep, p.coldDB, note)
	}
	return nil
}

// coldProblem says why months cannot be moved to the cold store now: its
// server does not answer, or it is not the hot store's server. "" = they can.
func (p *Process) coldProblem(ctx context.Context) string {
	if p.coldSrv == nil || p.hotSrv == nil {
		return ""
	}
	c, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var cold, hot string
	var err error
	if cold, err = p.coldSrv.ServerUUID(c); err != nil {
		return fmt.Sprintf("the cold store's server (YAKU_COLD_DSN) does not answer: %v", err)
	}
	if hot, err = p.hotSrv.ServerUUID(c); err != nil {
		return fmt.Sprintf("the hot store does not answer: %v", err)
	}
	if cold != hot {
		return "YAKU_COLD_DSN names another MySQL server than the hot store's; a day is moved inside one server only, so the days stay in hot until it names the hot store's server"
	}
	return ""
}

// archiveDue says a table of the day `day` is older than the days kept
// (Saleem and the PO, 26 Oct 2026: retention in days, YAKU_HOT_DAYS). Today
// and the keep-1 days before it stay. Before 02:00 UTC the day that just
// ended still counts as today, so a day goes in the quiet of the night.
func archiveDue(day time.Time, keep int, now time.Time) bool {
	if keep < 1 {
		keep = 1
	}
	ref := now.UTC()
	if ref.Hour() < archiveHourUTC {
		ref = ref.AddDate(0, 0, -1)
	}
	oldest := time.Date(ref.Year(), ref.Month(), ref.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -(keep - 1))
	return day.UTC().Before(oldest)
}

// archiveHourUTC is when, each night, the day that just ended starts to
// count as a day gone by.
const archiveHourUTC = 2

// Days a company's records stay in hot: 1 to maxHotDays.
const maxHotDays = 730

// loadHotDays returns each company's days kept in hot
// (companies.data_retention.hot_days). A company that has only the months of
// D-136 (hot_months) keeps that many times 30 days; one that sets neither
// keeps the gateway's YAKU_HOT_DAYS.
func (p *Process) loadHotDays(ctx context.Context) map[int64]int {
	out := map[int64]int{}
	err := p.upkeep.CompanyHotDays(ctx, func(id int64, d, m int, err error) {
		if err != nil {
			return
		}
		if d <= 0 && m > 0 {
			d = m * 30
		}
		if d > maxHotDays {
			d = maxHotDays
		}
		if d > 0 {
			out[id] = d
		}
	})
	if err != nil {
		p.log.Printf("load days kept in hot: %v", err)
		return out
	}
	return out
}

// hotDaysSetting is YAKU_HOT_DAYS: the days a company's records stay in the
// hot store when it sets none (default 90: today and the 89 before it). The
// retired YAKU_HOT_MONTHS is read as that many times 30 days while
// YAKU_HOT_DAYS is unset, so an install that set it keeps about as much.
func hotDaysSetting() int {
	if n, err := strconv.Atoi(strings.TrimSpace(os.Getenv("YAKU_HOT_DAYS"))); err == nil && n >= 1 && n <= maxHotDays {
		return n
	}
	if n, err := strconv.Atoi(strings.TrimSpace(os.Getenv("YAKU_HOT_MONTHS"))); err == nil && n >= 1 && n*30 <= maxHotDays {
		return n * 30
	}
	return 90
}
