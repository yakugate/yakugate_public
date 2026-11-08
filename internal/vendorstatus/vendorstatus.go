// Package vendorstatus caches the per-vendor status-code overrides in
// vendor_status_codes, for both the submit leg and the receipt leg.
//
// It exists because the same table now has two readers. P3 loaded it privately
// for submit_sm_resp classification; the receipt path needs the same rows for
// the `dlr` scope, and that path is reached from THREE processes (P3's
// deliver_sm, P4's status poll, and the dlr role's webhook). A second private
// loader in each would be four copies of one query, drifting apart.
//
// The receipt half is OPT-IN PER VENDOR (PO 2026-09-09). A vendor with
// dlr_code_overrides=0 — every vendor today — behaves exactly as before, so
// adding a row to the table cannot start changing outcomes until someone
// deliberately turns it on for that vendor.
package vendorstatus

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// Rule is one vendor_status_codes row as a caller needs it.
type Rule struct {
	// Action is what we DO: success, retry, permanent, throttle, hlr_retry,
	// failover, block_destination, pause_vendor.
	Action string
	// TargetStatus is what the MESSAGE BECOMES: delivered, failed, expired or
	// sent. Empty means "not stated" — the generic mapping decides the status
	// and Action only governs what happens next. The two are separate
	// questions: a retry rule with no target status would otherwise leave a
	// record reading `failed` while the message is still being retried.
	TargetStatus string
}

// Cache holds the table in memory and refreshes it on a ticker. Reads are
// per-receipt, so they never touch the database.
type Cache struct {
	db dal.VendorStatusCodes

	mu sync.RWMutex
	// rules is vendorID -> "scope:code" -> Rule. Codes are stored lower-cased
	// and stripped of a 0x prefix so "0x58", "0X58" and "58" are one key —
	// vendors write the same code three ways and an operator should not have
	// to guess which spelling we compare against.
	rules map[int64]map[string]Rule
	// dlrOptIn is vendorID -> vendors.dlr_code_overrides. Absent means off.
	dlrOptIn map[int64]bool
	// names is vendorID -> its cid, for the monitoring report of hits.
	names map[int64]string

	// hits counts, second by second over the last minute, how often each
	// rule matched (hits.go) — its own lock, taken on the receipt path.
	hitMu sync.Mutex
	hits  map[hitKey]*hitWindow
}

// New returns an empty cache bound to the config store (dal.VendorStatusCodes).
// A nil db yields a cache that answers "no override" to everything, which is
// what a non-MySQL config store gets — same degradation as every other
// config-backed feature.
func New(db dal.VendorStatusCodes) *Cache {
	return &Cache{db: db, rules: map[int64]map[string]Rule{}, dlrOptIn: map[int64]bool{}}
}

// canonical turns a code as WRITTEN into one comparable form.
//
// The same command_status is written three ways by three people: "0x256" in a
// vendor's PDF, "598" in a ticket, "0X256" in a spreadsheet. They are one
// code and must be one key.
//
// This also repairs a live defect. P3's own loader scanned `code` into a
// uint32 while the column is VARCHAR, so the only row configured today —
// vendor 16, submit, "0x256" — failed to scan and was skipped by a bare
// `continue`. The override existed in the table, was never loaded, and
// nothing said so. Parsing the text here is what makes a hex-written rule
// work at all.
//
// A code that is not a number at all (some HTTP vendors send words) keeps its
// lower-cased text, so those still match exactly.
func canonical(code string) string {
	c := strings.ToLower(strings.TrimSpace(code))
	if c == "" {
		return ""
	}
	base, digits := 10, c
	if strings.HasPrefix(c, "0x") {
		base, digits = 16, c[2:]
	}
	if n, err := strconv.ParseUint(digits, base, 64); err == nil {
		return strconv.FormatUint(n, 10) // always decimal, so 0x256 and 598 meet
	}
	return c
}

func key(scope, code string) string { return scope + ":" + canonical(code) }

// Reload replaces the cached table.
func (c *Cache) Reload(ctx context.Context) error {
	if c == nil || c.db == nil {
		return nil
	}
	rules := map[int64]map[string]Rule{}
	rows, err := c.db.StatusCodes(ctx)
	if err != nil {
		return err
	}
	for _, r := range rows {
		if rules[r.VendorID] == nil {
			rules[r.VendorID] = map[string]Rule{}
		}
		rules[r.VendorID][key(r.Scope, r.Code)] = Rule{Action: r.Action, TargetStatus: r.Target}
	}

	optIn := map[int64]bool{}
	switches, err := c.db.Switches(ctx)
	if err != nil {
		return err
	}
	names := map[int64]string{}
	for _, s := range switches {
		optIn[s.VendorID] = s.On
		if s.CID != "" {
			names[s.VendorID] = s.CID
		}
	}

	c.mu.Lock()
	c.rules, c.dlrOptIn, c.names = rules, optIn, names
	c.mu.Unlock()
	return nil
}

// Submit returns the override for a submit_sm_resp command_status. The caller
// has a number; rules may have been written in hex, and canonical() is what
// makes those meet.
//
// NOT opt-in: this is the behaviour P3 has had since the table was wired, and
// the opt-in ruling was about the receipt leg only. Changing it here would
// silently disable overrides an operator already relies on.
func (c *Cache) Submit(vendorID int64, status uint32) (Rule, bool) {
	return c.lookup(vendorID, "submit", strconv.FormatUint(uint64(status), 10))
}

// DLR returns the override for a receipt's error code — only for a vendor that
// has opted in. A vendor with the switch off gets no override no matter what
// rows exist for it.
func (c *Cache) DLR(vendorID int64, code string) (Rule, bool) {
	if c == nil {
		return Rule{}, false
	}
	c.mu.RLock()
	on := c.dlrOptIn[vendorID]
	c.mu.RUnlock()
	if !on {
		return Rule{}, false
	}
	return c.lookup(vendorID, "dlr", code)
}

func (c *Cache) lookup(vendorID int64, scope, code string) (Rule, bool) {
	if c == nil || code == "" {
		return Rule{}, false
	}
	c.mu.RLock()
	r, ok := c.rules[vendorID][key(scope, code)]
	c.mu.RUnlock()
	if ok {
		c.note(vendorID, scope, code, r.Action, time.Now())
	}
	return r, ok
}

// Start refreshes the cache now and then every interval until ctx ends. A
// failed refresh keeps the previous contents rather than emptying them: losing
// the config store must not quietly turn every override off.
func (c *Cache) Start(ctx context.Context, every time.Duration, logf func(string, ...any)) {
	if c == nil || c.db == nil {
		return
	}
	if err := c.Reload(ctx); err != nil && logf != nil {
		logf("vendor status codes: %v", err)
	}
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := c.Reload(ctx); err != nil && logf != nil {
					logf("vendor status codes refresh: %v", err)
				}
			}
		}
	}()
}
