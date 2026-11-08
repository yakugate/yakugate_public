package monitor

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// Overview is the gateway at a glance, built from the snapshots every role
// pushes (Redis `status:<role>:<instance>`, or the newest rows of
// monitor_samples when Redis is off). It is the ONE summary behind the
// console's Monitoring screen and the admin listener's HTML page, so the two
// can never disagree (PO 2026-09-21: "basic monitoring in the CLI, and via the
// HTTP health endpoint in HTML"). Monitoring figures come from the roles'
// own memory, never from SQL on operational tables.
type Overview struct {
	Time    time.Time
	Source  string // "redis" | "history" | "this process"
	Roles   []RoleLine
	Traffic TrafficSum
	Queues  []QueueLine
	// Customers' queues (D-127): receipts and MO waiting per account.
	CustomerQueues []QueueLine
	// MySQL and Redis as they report themselves (D-129).
	Stores  []StoreLine
	SMPP    SMPPLine
	Billing *BillingLine // nil when no db-worker reported its billing queue
	// Host is the busiest machine (highest CPU) — an average would hide the
	// one in trouble; Hosts is every machine reporting.
	Host  *HostLine
	Hosts []HostLine
	// Deployments are the gateways reporting, when there is more than one
	// worth telling apart (2026-10-05).
	Deployments []DeploymentLine
	// Problems are the lines an operator must act on, most serious first.
	Problems []string
}

// RoleLine is one running instance.
type RoleLine struct {
	Role, Instance, Host string
	Ready                bool
	Uptime               time.Duration
	Seen                 time.Duration // since its last snapshot
	// Per process, for the live view: memory in use (RSS), goroutines, and
	// the role's own traffic per second over the last minute (accepted in
	// for the servers, sent out for the vendor roles).
	RSSBytes      int64
	Goroutines    int64
	InPerS, OutPS float64
}

// CountRate is a counter's total and its rate over the last minute.
type CountRate struct {
	Total int64
	PerS  float64
}

// TrafficSum sums every instance of each role.
type TrafficSum struct {
	Accepted, Rejected, Sent, SendFailed, Receipts, MO CountRate
}

// QueueLine is one queue (D-127: with what can be sent now, what is held,
// and its rates in and out across every process).
type QueueLine struct {
	Kind      string // mt | dlr | mo
	Vendor    string // the vendor queue's name; the account id for a customer queue
	Depth     int64
	Ready     int64
	Held      int64
	OldestSec int64
	InPerS    float64
	OutPerS   float64
}

// DeploymentLine is one deployment's roles.
type DeploymentLine struct {
	ID, Env string
	Hosts   []string
	Roles   int
	Ready   int
}

// StoreLine is one MySQL or Redis server.
type StoreLine struct {
	Kind    string // mysql | redis
	Key     string
	Name    string // address, or where the settings point
	Health  string // ok | warning | down
	Reasons []string
	PingMS  float64
	Figures string // the few numbers that matter, in words
	// Deployment is set when the store is that gateway's own: its address
	// names the machine that read it (a queue Redis at 127.0.0.1:6379 on each
	// box is two servers). Its Name then says which, once more than one
	// gateway reports.
	Deployment string
	// Raw is the whole reading (internal/storewatch), for a detail screen.
	Raw map[string]any
	at  string
}

// storeID is which readings are one server: a store's key and its address,
// or, at an address of the reading machine itself, its key and the gateway
// that read it (issue 1 of the design audit, 2026-10-26: each production box
// has its own queue Redis at 127.0.0.1:6379, and keyed by `redis_queues` alone
// the two became one line). Kept the same as the Admin API's
// services/monitorRollup.js rollupStores.
func storeID(l *StoreLine, addr, dep string) string {
	if ownMachineAddress(addr) {
		l.Deployment = dep
		return l.Key + "@" + dep
	}
	return l.Key + "|" + addr
}

// ownMachineAddress says an address is the reading machine's own: a
// loopback, a socket, or none at all.
func ownMachineAddress(a string) bool {
	f := strings.Fields(strings.ToLower(a))
	if len(f) == 0 {
		return true
	}
	s := f[0]
	if strings.HasPrefix(s, "/") || strings.HasPrefix(s, "unix:") {
		return true
	}
	host := s
	if strings.HasPrefix(s, "[") {
		if i := strings.Index(s, "]"); i > 0 {
			host = s[1:i]
		}
	} else if strings.Count(s, ":") == 1 {
		host = s[:strings.Index(s, ":")]
	}
	return host == "" || host == "localhost" || host == "::1" || host == "0.0.0.0" || strings.HasPrefix(host, "127.")
}

func firstOf(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// SMPPLine is the SMPP vendor binds.
type SMPPLine struct {
	VendorsBound, VendorsTotal, SessionsBound, SessionsTotal, SessionsDegraded int64
}

// BillingLine is the billing queue as the db-workers report it (the billing
// role was retired on 26 Oct 2026): money events waiting to be booked, the
// ones MySQL refused, how long the oldest has waited, and how many were
// booked. Added up over every deployment that reports.
type BillingLine struct {
	Waiting, Delayed int64
	OldestSec        int64
	Booked           int64
}

// HostLine is the machine a role runs on.
type HostLine struct {
	Name                    string
	CPUPct, MemPct, DiskPct float64
	Load1                   float64
}

// Queue ages beyond this are listed as a problem.
const staleQueueSec = 300

// Summarize builds the overview from raw snapshots keyed by anything (the
// Redis key, usually). now is when it was read.
func Summarize(snaps map[string]json.RawMessage, source string, now time.Time) Overview {
	o := Overview{Time: now.UTC(), Source: source}
	hosts := map[string]HostLine{}
	deps := map[string]*DeploymentLine{}
	stores := map[string]StoreLine{}    // newest reading per store
	queues := map[string]QueueLine{}    // vendor queues by QueueKey
	customers := map[string]QueueLine{} // kind|account
	keys := make([]string, 0, len(snaps))
	for k := range snaps {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		var d map[string]any
		if json.Unmarshal(snaps[k], &d) != nil {
			continue
		}
		g := obj(d, "gateway")
		role := str(g, "role")
		if role == "" || role == "console" { // a person at a terminal is not a service
			continue
		}
		rl := RoleLine{Role: role, Instance: str(g, "instance"), Ready: str(g, "status") == "ready",
			Uptime: time.Duration(num(g, "uptime_sec")) * time.Second}
		if sys := obj(d, "system"); sys != nil {
			rl.Host = str(sys, "hostname")
			rl.RSSBytes = int64(num(obj(sys, "process"), "rss_bytes"))
			rl.Goroutines = int64(num(obj(sys, "process"), "goroutines"))
			if _, seen := hosts[rl.Host]; !seen {
				h := HostLine{Name: rl.Host,
					CPUPct: num(obj(sys, "cpu"), "usage_percent"), Load1: num(obj(sys, "cpu"), "load_1"),
					MemPct: num(obj(sys, "memory"), "usage_percent")}
				if disks, ok := sys["disk"].([]any); ok {
					for _, x := range disks {
						if dm, ok := x.(map[string]any); ok && str(dm, "mount") == "/" {
							h.DiskPct = num(dm, "usage_percent")
						}
					}
				}
				hosts[rl.Host] = h
			}
		}
		if t, err := time.Parse(time.RFC3339, str(g, "time")); err == nil {
			rl.Seen = now.Sub(t)
		}
		if dp := obj(d, "deployment"); dp != nil {
			id := str(dp, "id")
			dl := deps[id]
			if dl == nil {
				dl = &DeploymentLine{ID: id, Env: str(dp, "env")}
				deps[id] = dl
			}
			dl.Roles++
			if rl.Ready {
				dl.Ready++
			}
			if rl.Host != "" && !contains(dl.Hosts, rl.Host) {
				dl.Hosts = append(dl.Hosts, rl.Host)
			}
		}
		o.Roles = append(o.Roles, rl)

		rl.InPerS = num(obj(obj(d, "sms"), "accepted"), "tps_60s")
		rl.OutPS = num(obj(obj(d, "flow"), "out"), "tps_60s")
		o.Roles[len(o.Roles)-1] = rl
		if sms := obj(d, "sms"); sms != nil {
			add(&o.Traffic.Accepted, obj(sms, "accepted"))
			add(&o.Traffic.Rejected, obj(sms, "rejected"))
		}
		if fl := obj(d, "flow"); fl != nil {
			add(&o.Traffic.Sent, obj(fl, "out"))
			add(&o.Traffic.SendFailed, obj(fl, "out_failed"))
			add(&o.Traffic.Receipts, obj(fl, "receipts"))
			add(&o.Traffic.MO, obj(fl, "mo"))
		}
		for vendor, v := range obj(d, "queues") {
			qm, ok := v.(map[string]any)
			if !ok {
				continue
			}
			name := QueueKey(vendor)
			cur := queues[name]
			if d := int64(num(qm, "depth")); d >= cur.Depth { // several http-servers report the same queue
				cur.Kind, cur.Vendor = "mt", name
				cur.Depth, cur.Ready, cur.Held = d, int64(num(qm, "ready")), int64(num(qm, "held"))
				cur.OldestSec = int64(num(qm, "oldest_age_sec"))
			}
			queues[name] = cur
		}
		// Rates in and out: every process counts its own, so they add up.
		if list, ok := d["queue_flow"].([]any); ok {
			for _, e := range list {
				fm, _ := e.(map[string]any)
				switch kind := str(fm, "kind"); kind {
				case "mt":
					name := QueueKey(str(fm, "queue"))
					cur := queues[name]
					cur.Kind, cur.Vendor = "mt", name
					cur.InPerS += num(fm, "in_tps_60s")
					cur.OutPerS += num(fm, "out_tps_60s")
					queues[name] = cur
				case "dlr", "mo":
					k := kind + "|" + str(fm, "queue")
					cur := customers[k]
					cur.Kind, cur.Vendor = kind, str(fm, "queue")
					cur.InPerS += num(fm, "in_tps_60s")
					// Handed over on a bind by an smpp-server; a push to the
					// customer's URL is added from the push counts below.
					cur.OutPerS += num(fm, "out_tps_60s")
					customers[k] = cur
				}
			}
		}
		if st := obj(d, "stores"); st != nil {
			at := str(st, "checked_at")
			dep := str(obj(d, "deployment"), "id")
			keep := func(l StoreLine, e map[string]any) {
				l.Reasons = strs(e["reasons"])
				l.Raw = e
				id := storeID(&l, firstOf(str(e, "addr"), str(e, "where")), dep)
				if cur, ok := stores[id]; !ok || at >= cur.at {
					stores[id] = l
				}
			}
			for _, e := range listOf(st["mysql"]) {
				conn := obj(e, "connections")
				keep(StoreLine{Kind: sqlKind(e), Key: str(e, "key"), Name: str(e, "addr"), Health: str(e, "health"),
					PingMS: num(e, "ping_ms"), at: at,
					Figures: fmt.Sprintf("%d/%d connections, %.0f queries/s", int64(num(conn, "current")), int64(num(conn, "max")),
						num(obj(e, "rates"), "queries"))}, e)
			}
			for _, e := range listOf(st["redis"]) {
				mem := obj(e, "memory")
				keep(StoreLine{Kind: "redis", Key: str(e, "key"), Name: str(e, "where"), Health: str(e, "health"),
					PingMS: num(e, "ping_ms"), at: at,
					Figures: fmt.Sprintf("memory %.0f%%, %.0f commands/s, %d clients", num(mem, "pct"), num(e, "ops_per_sec"),
						int64(num(e, "clients")))}, e)
			}
		}
		for _, kind := range []string{"dlr", "mo"} {
			sec := obj(d, kind)
			if list, ok := sec["backlog_accounts"].([]any); ok {
				for _, e := range list {
					bm, _ := e.(map[string]any)
					acct := fmt.Sprint(int64(num(bm, "account_id")))
					cur := customers[kind+"|"+acct]
					cur.Kind, cur.Vendor = kind, acct
					cur.Depth, cur.Ready, cur.Held = int64(num(bm, "depth")), int64(num(bm, "ready")), int64(num(bm, "held"))
					cur.OldestSec = int64(num(bm, "oldest_age_sec"))
					customers[kind+"|"+acct] = cur
				}
			}
			if list, ok := sec["client_push_accounts"].([]any); ok {
				for _, e := range list {
					pm, _ := e.(map[string]any)
					acct := fmt.Sprint(int64(num(pm, "account_id")))
					cur := customers[kind+"|"+acct]
					cur.Kind, cur.Vendor = kind, acct
					cur.OutPerS += num(obj(pm, "delivered"), "tps_60s")
					customers[kind+"|"+acct] = cur
				}
			}
		}
		if sv := obj(d, "smpp_vendor"); sv != nil {
			o.SMPP.VendorsBound += int64(num(sv, "vendors_bound"))
			o.SMPP.VendorsTotal += int64(num(sv, "vendors_total"))
			o.SMPP.SessionsBound += int64(num(sv, "sessions_bound"))
			o.SMPP.SessionsTotal += int64(num(sv, "sessions_total"))
			o.SMPP.SessionsDegraded += int64(num(sv, "sessions_degraded"))
		}
		if bq := obj(obj(d, "db_worker"), "billing_queue"); bq != nil && bq["inert"] == nil {
			if o.Billing == nil {
				o.Billing = &BillingLine{}
			}
			o.Billing.Waiting += int64(num(bq, "waiting"))
			o.Billing.Delayed += int64(num(bq, "delayed"))
			o.Billing.Booked += int64(num(obj(bq, "booked"), "total"))
			if sec := int64(num(bq, "oldest_ms")) / 1000; sec > o.Billing.OldestSec {
				o.Billing.OldestSec = sec
			}
		}
	}
	for _, q := range queues {
		o.Queues = append(o.Queues, q)
	}
	sort.Slice(o.Queues, func(i, j int) bool {
		if o.Queues[i].Depth != o.Queues[j].Depth {
			return o.Queues[i].Depth > o.Queues[j].Depth
		}
		return o.Queues[i].Vendor < o.Queues[j].Vendor
	})
	for _, q := range customers {
		o.CustomerQueues = append(o.CustomerQueues, q)
	}
	for _, s := range stores {
		if s.Deployment != "" && len(deps) > 1 {
			s.Name += " (" + s.Deployment + ")"
		}
		o.Stores = append(o.Stores, s)
	}
	for _, h := range hosts {
		o.Hosts = append(o.Hosts, h)
	}
	sort.Slice(o.Hosts, func(i, j int) bool { return o.Hosts[i].Name < o.Hosts[j].Name })
	for i := range o.Hosts {
		if o.Host == nil || o.Hosts[i].CPUPct > o.Host.CPUPct {
			h := o.Hosts[i]
			o.Host = &h
		}
	}
	if len(deps) > 1 {
		for _, d := range deps {
			sort.Strings(d.Hosts)
			o.Deployments = append(o.Deployments, *d)
		}
		sort.Slice(o.Deployments, func(i, j int) bool { return o.Deployments[i].ID < o.Deployments[j].ID })
	}
	// Two gateways' own stores share a key: their names keep the order steady
	// (the console numbers the stores and opens one by its number).
	sort.Slice(o.Stores, func(i, j int) bool {
		a, b := o.Stores[i], o.Stores[j]
		if a.Kind+a.Key != b.Kind+b.Key {
			return a.Kind+a.Key < b.Kind+b.Key
		}
		return a.Name < b.Name
	})
	sort.Slice(o.CustomerQueues, func(i, j int) bool {
		a, b := o.CustomerQueues[i], o.CustomerQueues[j]
		if a.Depth != b.Depth {
			return a.Depth > b.Depth
		}
		return a.Kind+a.Vendor < b.Kind+b.Vendor
	})
	sort.SliceStable(o.Roles, func(i, j int) bool { return o.Roles[i].Role < o.Roles[j].Role })
	o.Problems = problems(o)
	return o
}

func problems(o Overview) []string {
	var p []string
	if len(o.Roles) == 0 {
		return []string{"no role is reporting — nothing is running, or monitoring is not reaching here"}
	}
	for _, r := range o.Roles {
		if !r.Ready {
			p = append(p, fmt.Sprintf("%s (%s) is running but not ready", r.Role, r.Instance))
		}
	}
	for _, s := range o.Stores {
		name := map[string]string{"mysql": "MySQL", "mssql": "SQL Server", "redis": "Redis"}[s.Kind] + " " + s.Name
		if s.Health == "down" {
			r := ""
			if len(s.Reasons) > 0 {
				r = " — " + s.Reasons[0]
			}
			p = append(p, name+" is down"+r)
			continue
		}
		for _, r := range s.Reasons {
			p = append(p, name+": "+r)
		}
	}
	if o.SMPP.VendorsTotal > 0 && o.SMPP.VendorsBound < o.SMPP.VendorsTotal {
		p = append(p, fmt.Sprintf("SMPP vendors bound: %d of %d", o.SMPP.VendorsBound, o.SMPP.VendorsTotal))
	}
	for _, q := range o.Queues {
		if q.Depth > 0 && q.OldestSec > staleQueueSec {
			p = append(p, fmt.Sprintf("queue %s: %d waiting, the oldest for %s", q.Vendor, q.Depth, Ago(time.Duration(q.OldestSec)*time.Second)))
		}
	}
	for _, q := range o.CustomerQueues {
		if q.Depth > 0 && q.OldestSec > staleQueueSec {
			p = append(p, fmt.Sprintf("%s for account %s: %d waiting, the oldest for %s",
				map[string]string{"dlr": "receipts", "mo": "incoming messages"}[q.Kind], q.Vendor, q.Depth, Ago(time.Duration(q.OldestSec)*time.Second)))
		}
	}
	if b := o.Billing; b != nil {
		if b.Waiting > 0 && b.OldestSec >= int64(billingStuckAfter/time.Second) {
			p = append(p, fmt.Sprintf("%d money event(s) wait on the billing queue, the oldest for %s: usage and invoices lag",
				b.Waiting, Ago(time.Duration(b.OldestSec)*time.Second)))
		}
		if b.Delayed > 0 {
			p = append(p, fmt.Sprintf("%d money event(s) MySQL refused wait on the billing queue to be tried again", b.Delayed))
		}
	}
	hosts := o.Hosts
	if len(hosts) == 0 && o.Host != nil {
		hosts = []HostLine{*o.Host}
	}
	for _, h := range hosts { // every machine, not only the busiest
		if h.DiskPct >= 90 {
			p = append(p, fmt.Sprintf("disk on %s is %.0f%% full", h.Name, h.DiskPct))
		}
		if h.MemPct >= 90 {
			p = append(p, fmt.Sprintf("memory on %s is %.0f%% used", h.Name, h.MemPct))
		}
	}
	return p
}

// Ago writes a duration the way people say it: 45s, 12m, 3h 20m, 4d 2h.
func Ago(d time.Duration) string {
	s := int64(d / time.Second)
	switch {
	case s < 60:
		return fmt.Sprintf("%ds", s)
	case s < 3600:
		return fmt.Sprintf("%dm", s/60)
	case s < 86400:
		return fmt.Sprintf("%dh %dm", s/3600, s%3600/60)
	}
	return fmt.Sprintf("%dd %dh", s/86400, s%86400/3600)
}

// FromHistory reads the newest sample of every instance that reported within
// `within` from monitor_samples — the view when Redis is off. Monitoring's own
// table, so reading it is not SQL on operational data.
func FromHistory(ctx context.Context, hot dal.MonitorStatements, within time.Duration) (map[string]json.RawMessage, error) {
	rows, err := hot.Newest(ctx, time.Now().UTC().Add(-within))
	if err != nil {
		return nil, err
	}
	out := map[string]json.RawMessage{}
	for _, r := range rows {
		zr, err := gzip.NewReader(bytes.NewReader(r.Body))
		if err != nil {
			continue
		}
		raw, err := io.ReadAll(zr)
		if err != nil {
			continue
		}
		out[statusKeyFor(r.Role, r.Instance)] = raw
	}
	return out, nil
}

// ── reading a decoded snapshot ──────────────────────────────────────────────

func obj(m map[string]any, k string) map[string]any {
	if m == nil {
		return nil
	}
	v, _ := m[k].(map[string]any)
	return v
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func str(m map[string]any, k string) string {
	if m == nil {
		return ""
	}
	s, _ := m[k].(string)
	return strings.TrimSpace(s)
}

func num(m map[string]any, k string) float64 {
	if m == nil {
		return 0
	}
	f, _ := m[k].(float64)
	return f
}

// add sums a counter section ({"total":…, "tps_60s":…}) into r.
func add(r *CountRate, c map[string]any) {
	r.Total += int64(num(c, "total"))
	r.PerS += num(c, "tps_60s")
}
