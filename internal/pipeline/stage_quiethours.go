package pipeline

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// tzCache memoizes time.LoadLocation results (chunk-5 ruling 5.2,
// 2026-08-03): LoadLocation reads the zone database from disk on EVERY call —
// paid per message on the hot path before this cache — and an invalid company
// timezone silently fell back to UTC on every single message with no trace.
// Now: one load (and one warning line for a bad tz) per distinct zone name
// per process lifetime.
var tzCache sync.Map // timezone name -> *time.Location

func tzLocation(tz string) *time.Location {
	if tz == "" {
		return time.UTC
	}
	if v, ok := tzCache.Load(tz); ok {
		return v.(*time.Location)
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		log.Printf("[pipeline] unknown company timezone %q — quiet-hours evaluated in UTC", tz)
		loc = time.UTC
	}
	tzCache.Store(tz, loc)
	return loc
}

// --- stage 3/18: quiet hours --- [SCOPE P1-04]
type quietHoursStage struct{}

func (quietHoursStage) Name() string { return "quiet-hours" }
func (quietHoursStage) Run(_ context.Context, e *Envelope) *Reject {
	start, end := e.Account.QuietStartMin, e.Account.QuietEndMin
	if start < 0 || end < 0 || start == end {
		return nil
	}
	now := time.Now().In(tzLocation(e.Account.CompanyTimezone))
	mins := now.Hour()*60 + now.Minute()
	blocked := false
	if start < end {
		blocked = mins >= start && mins < end
	} else { // window wraps midnight
		blocked = mins >= start || mins < end
	}
	if blocked {
		// Say the window (2026-10-02, "each error must contain message"): it
		// is the account's own setting, and the client needs it to know when
		// to send again. The SMPP edge matches the code by prefix.
		zone := e.Account.CompanyTimezone
		if zone == "" {
			zone = "UTC"
		}
		return reject(403, fmt.Sprintf("QUIET_HOURS: sending is paused from %s to %s (%s time); try again after %s",
			hhmm(start), hhmm(end), zone, hhmm(end)))
	}
	return nil
}

// hhmm writes minutes after midnight as HH:MM.
func hhmm(min int) string { return fmt.Sprintf("%02d:%02d", min/60, min%60) }
