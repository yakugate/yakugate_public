package vendorstatus

import (
	"sort"
	"time"
)

// Hits: how often a vendor answered with a code that has a row in its Status
// Codes, over the last minute (go-live test P3 D5, 2026-10-12, Saleem: "custom
// error was captured but not thrown any alert in email or monitoring").
//
// A rule did its work in silence: the message was retried, moved to another
// route or the vendor paused, and nothing told an operator that the vendor
// had begun answering with that code. Every match is counted here; the role
// reports the counts with its monitoring sample (Section), and the events
// pass turns a code that keeps being answered into a "vendor_code" event —
// which Monitoring shows and the alert emails can carry.
//
// Counted in the process that looked the rule up, per vendor, scope (submit
// or dlr), code and action. Sixty one-second slots a key; a key nobody has
// hit for a minute is dropped at the next report.

type hitKey struct {
	vendor              int64
	scope, code, action string
}

type hitWindow struct {
	sec [60]int64
	n   [60]int64
}

func (w *hitWindow) add(now int64) {
	i := now % 60
	if w.sec[i] != now {
		w.sec[i], w.n[i] = now, 0
	}
	w.n[i]++
}

func (w *hitWindow) total(now int64) (n int64) {
	for i := range w.sec {
		if now-w.sec[i] < 60 {
			n += w.n[i]
		}
	}
	return n
}

// note counts one match of a rule.
func (c *Cache) note(vendorID int64, scope, code, action string, at time.Time) {
	k := hitKey{vendor: vendorID, scope: scope, code: canonical(code), action: action}
	c.hitMu.Lock()
	if c.hits == nil {
		c.hits = map[hitKey]*hitWindow{}
	}
	w := c.hits[k]
	if w == nil {
		w = &hitWindow{}
		c.hits[k] = w
	}
	w.add(at.Unix())
	c.hitMu.Unlock()
}

// Hit is one rule that matched in the last minute.
type Hit struct {
	VendorID int64
	Vendor   string // its cid, "" when the vendor is not known here
	Scope    string // submit | dlr
	Code     string
	Action   string
	Count    int64 // matches in the last 60 seconds
}

// Hits lists the rules that matched in the last minute, busiest first.
func (c *Cache) Hits(now time.Time) []Hit {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	names := c.names
	c.mu.RUnlock()
	c.hitMu.Lock()
	out := make([]Hit, 0, len(c.hits))
	for k, w := range c.hits {
		n := w.total(now.Unix())
		if n == 0 {
			delete(c.hits, k)
			continue
		}
		out = append(out, Hit{VendorID: k.vendor, Vendor: names[k.vendor], Scope: k.scope, Code: k.code, Action: k.action, Count: n})
	}
	c.hitMu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		if out[i].VendorID != out[j].VendorID {
			return out[i].VendorID < out[j].VendorID
		}
		return out[i].Code < out[j].Code
	})
	return out
}

// Section is Hits in the form a role adds to its monitoring sample, under the
// key "vendor_codes". Nil when no rule matched in the last minute.
func (c *Cache) Section() []map[string]any {
	hits := c.Hits(time.Now())
	if len(hits) == 0 {
		return nil
	}
	out := make([]map[string]any, 0, len(hits))
	for _, h := range hits {
		out = append(out, map[string]any{
			"vendor_id": h.VendorID, "cid": h.Vendor, "scope": h.Scope, "code": h.Code,
			"action": h.Action, "hits_60s": h.Count,
		})
	}
	return out
}
