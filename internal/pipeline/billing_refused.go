package pipeline

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/billing"
)

// billingRefused says in the log why the balance step refused a message.
// The client is told "INTERNAL_ERROR: balance reservation failed" (or
// "usage could not be booked") and nothing more, and until the load test of
// 30 Oct 2026 the error was written nowhere: 237,351 refusals and nobody
// could say whether the main Redis (the balance) or the box's queue Redis
// (the billing events) had failed. One line at most every
// queueRefusedSayEvery, with how many it stood for.
//
// Refusals kept quiet are said when the window ends even if no refusal
// follows (later), so the last ones of a storm are never lost from the log.
// Lines are stamped in UTC, as the roles' own lines are.
type billingRefusedLog struct {
	what  string // what the client was told
	mu    sync.Mutex
	last  time.Time
	quiet int
	armed bool // a "the rest" line is waiting for the window's end
	print func(format string, args ...any)
	later func(d time.Duration, f func()) // nil = no "the rest" line (tests)
}

// utcPrintf is log.Printf stamped in UTC.
func utcPrintf(format string, args ...any) {
	log.New(log.Writer(), log.Prefix(), log.Flags()|log.LUTC).Printf(format, args...)
}

func afterFunc(d time.Duration, f func()) { time.AfterFunc(d, f) }

var (
	balanceRefused = &billingRefusedLog{what: "INTERNAL_ERROR: balance reservation failed", print: utcPrintf, later: afterFunc}
	usageRefused   = &billingRefusedLog{what: "INTERNAL_ERROR: usage could not be booked", print: utcPrintf, later: afterFunc}
)

func (b *billingRefusedLog) say(account int64, err error) {
	b.mu.Lock()
	now := time.Now()
	if !b.last.IsZero() && now.Sub(b.last) < queueRefusedSayEvery {
		b.quiet++
		if b.later != nil && !b.armed {
			b.armed = true
			b.later(queueRefusedSayEvery-now.Sub(b.last), b.sayRest)
		}
		b.mu.Unlock()
		return
	}
	quiet := b.quiet
	b.quiet, b.last = 0, now
	b.mu.Unlock()
	store := billing.StoreOf(err)
	if store == "" {
		store = "store not named"
	}
	more := ""
	if quiet > 0 {
		more = fmt.Sprintf(" (and %d more since the last line)", quiet)
	}
	b.print("[pipeline] the balance step failed (%s) for account %d: %v — the client was told %s%s", store, account, err, b.what, more)
}

// sayRest says the refusals kept quiet since the last line when no later
// refusal said them (the end of a storm).
func (b *billingRefusedLog) sayRest() {
	b.mu.Lock()
	quiet := b.quiet
	b.armed = false
	if quiet > 0 {
		b.quiet, b.last = 0, time.Now()
	}
	b.mu.Unlock()
	if quiet > 0 {
		b.print("[pipeline] the balance step failed %d more times since the last line — the client was told %s", quiet, b.what)
	}
}
