package monitor

// Every queue, seen whole (Saleem 2026-10-05, D-127: "we need queues as well
// and their entire visibility").
//
// What each queue reports comes from where it is known:
//   - its contents (waiting, ready, held, taken before, oldest wait) from the
//     store, published by the role that reads it — vendor queues by the
//     http-server (`queues`), customers' receipts and MO by the dlr and mo
//     roles (`backlog_accounts`);
//   - its rates in and out from every process that moved a message
//     (`queue_flow`, internal/queueflow), added up per deployment;
//   - for customers' queues, what was handed to them: a push to the
//     customer's own URL from the dlr and mo roles' push counts
//     (`client_push_accounts`), and what an smpp-server handed over on a bind
//     from its `queue_flow` (2026-10-09 — until then a bind's hand-overs were
//     counted nowhere per account, so an SMPP customer's queue showed nothing
//     leaving it). The two never count the same message: the dlr and mo roles
//     put no way out in their flow, and an smpp-server pushes to no URL.
//
// liftQueues folds all of that into the small `queues` column (hot/0055) each
// sample carries, so a queue's history, its events and its day are read
// without opening a single snapshot.

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/queueflow"
	"github.com/yakugate/yakugate/internal/store"
)

// BacklogAccountsLimit is the most accounts a role lists per backlog.
const BacklogAccountsLimit = 200

// AccountBacklog is one account's receipts or MO waiting.
type AccountBacklog struct {
	AccountID int64 `json:"account_id"`
	store.VendorQueueStat
}

// BacklogAccounts lists a per-account backlog, largest first.
func BacklogAccounts(m map[int64]store.VendorQueueStat) []AccountBacklog {
	out := make([]AccountBacklog, 0, len(m))
	for id, st := range m {
		out = append(out, AccountBacklog{AccountID: id, VendorQueueStat: st})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Depth != out[j].Depth {
			return out[i].Depth > out[j].Depth
		}
		return out[i].AccountID < out[j].AccountID
	})
	return out
}

// QueueKey is how a vendor queue is named in monitoring: the vendor's cid in
// lower case, anything but letters, digits and _ made _ — the MySQL store's
// table name, so a queue reads the same whichever store and process report it.
func QueueKey(vendor string) string {
	b := []byte(strings.ToLower(vendor))
	for i, c := range b {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_') {
			b[i] = '_'
		}
	}
	return string(b)
}

// queuePoint is one queue in one sample (the column's short names).
type queuePoint struct {
	W  int64   `json:"w,omitempty"`  // waiting
	R  int64   `json:"r,omitempty"`  // ready now
	H  int64   `json:"h,omitempty"`  // held
	T  int64   `json:"t,omitempty"`  // taken before, still there
	A  int64   `json:"a,omitempty"`  // seconds the oldest ready one has waited
	I  float64 `json:"i,omitempty"`  // in, a second (last minute)
	O  float64 `json:"o,omitempty"`  // out, a second (last minute)
	IT int64   `json:"it,omitempty"` // in, this process since it started
	OT int64   `json:"ot,omitempty"` // out, this process since it started
	S  bool    `json:"s,omitempty"`  // stale: the queue could not be read just now; these are its last figures
}

func (q queuePoint) zero() bool { return q == queuePoint{} }

// queueDoc is the column: per kind, per queue; D lists the kinds whose
// depths the sample reports (a queue missing under one of them is empty).
type queueDoc struct {
	D   []string              `json:"d,omitempty"`
	MT  map[string]queuePoint `json:"mt,omitempty"`
	DLR map[string]queuePoint `json:"dlr,omitempty"`
	MO  map[string]queuePoint `json:"mo,omitempty"`
	// REC is the records waiting for the DB worker, one entry per list
	// ("0", "1" …; internal/recordq, 2026-10-13): waiting = still to be
	// written, held = refused by MySQL and waiting to be tried again, the
	// oldest wait = how far a record is behind its message. Its depths come
	// from the db-worker's report, its rates from every role that queued a
	// record (in) and from the worker (out).
	REC map[string]queuePoint `json:"rec,omitempty"`
	// B is how many SMPP binds each account holds on this smpp-server
	// (2026-10-06), by username. Rides this column — it is the same kind of
	// small per-name figure — so the events pass can tell when an account
	// that was bound has no bind left. D lists bindsKind when the sample is
	// an smpp-server's: an account missing from B then has none.
	B map[string]int `json:"b,omitempty"`
	// VC is how often, in the last minute, a vendor answered with a code that
	// has a row in its Status Codes (2026-10-12, vendorstatus/hits.go), keyed
	// vendorCodeKey. Rides this column like B, so the events pass can tell an
	// operator that a vendor has begun answering with such a code.
	VC map[string]int64 `json:"vc,omitempty"`
	// VS is each SMPP vendor's connections on this smpp-vendor, [bound, of],
	// by cid (2026-10-18, go-live P3 G1: the vendor_down mail said "vendor
	// connections up 4 of 6" and never which vendor). D lists vendorsKind.
	VS map[string][2]int `json:"vs,omitempty"`
	// HV is each HTTP vendor on this http-vendor (2026-10-18, go-live P4 G1:
	// a vendor whose calls failed for five minutes raised nothing). D lists
	// httpVendorsKind.
	HV map[string]hvPoint `json:"hv,omitempty"`
	// JB is what is wrong with a gateway-wide job, by name (jobBackupBad
	// …), from the billing, backup and retention roles' own sections
	// (2026-10-18, go-live P8 G2, P9 G2). D lists the job ("billing",
	// "backup", "retention") when its section was in the sample, so a name
	// missing from JB then means nothing is wrong.
	JB map[string]int64 `json:"jb,omitempty"`
	// RQ is the record queue as a role that writes through the DB worker sees
	// it (2026-10-18, go-live P14 G1/G3): the records waiting, and whether a
	// db-worker is running. D lists recordQueueKind.
	RQ *rqPoint `json:"rq,omitempty"`
}

// hvPoint is one HTTP vendor on one http-vendor process.
type hvPoint struct {
	E  int   `json:"e,omitempty"`  // 1: a failed call in the last minute (status ERRORS)
	NR int   `json:"nr,omitempty"` // 1: waiting for its session (status NOT_READY)
	A  int64 `json:"a,omitempty"`  // calls it accepted since the process started
}

// rqPoint is the record queue as one role sees it.
type rqPoint struct {
	W  int64 `json:"w,omitempty"`  // records waiting for the DB worker
	NW int   `json:"nw,omitempty"` // 1: no db-worker is running
}

// The names that mark, in queueDoc.D, what else a sample reports.
const (
	vendorsKind     = "vsessions"
	httpVendorsKind = "hvendors"
	recordQueueKind = "recq"
)

// The troubles of the gateway-wide jobs, as keys of queueDoc.JB.
const (
	jobBackupBad      = "backup_bad"      // 1: the last backup run is not whole or did not verify
	jobArchiveRefused = "archive_refused" // day tables retention could not move to cold
	// The billing queue (26 Oct 2026; the billing role is retired):
	jobBillingStuck = "billing_stuck" // the db-worker's lanes: seconds the oldest event has waited, or 1 for events MySQL refused
	jobBillingWaits = "billing_waits" // a booking role's view of the lanes: seconds the oldest event has waited
)

// billingStuckAfter is how long an event may wait on the billing queue before
// it is a trouble: the db-worker books a lane within a second when it can.
const billingStuckAfter = 2 * time.Minute

// The sections a sample's D names for the billing queue.
const (
	billingQueueKind  = "billingq"  // a db-worker's billing_queue section was there
	billingEventsKind = "billingev" // a booking role's billing_events section was there
)

// vendorCodeKey names one status-code rule of one vendor in queueDoc.VC:
// "<cid>|<scope>|<code>|<action>" (the vendor's id when its cid is not known).
func vendorCodeKey(cid string, vendorID int64, scope, code, action string) string {
	if cid == "" {
		cid = "#" + strconv.FormatInt(vendorID, 10)
	}
	return cid + "|" + scope + "|" + code + "|" + action
}

// bindsKind marks, in queueDoc.D, a sample that reports client binds.
const bindsKind = "binds"

func (d *queueDoc) kind(k string) map[string]queuePoint {
	switch k {
	case queueflow.MT:
		if d.MT == nil {
			d.MT = map[string]queuePoint{}
		}
		return d.MT
	case queueflow.DLR:
		if d.DLR == nil {
			d.DLR = map[string]queuePoint{}
		}
		return d.DLR
	case queueflow.MO:
		if d.MO == nil {
			d.MO = map[string]queuePoint{}
		}
		return d.MO
	case queueflow.REC:
		if d.REC == nil {
			d.REC = map[string]queuePoint{}
		}
		return d.REC
	}
	return nil
}

func (d *queueDoc) reports(k string) bool {
	for _, x := range d.D {
		if x == k {
			return true
		}
	}
	return false
}

// liftQueues builds the `queues` column from a decoded snapshot; nil when the
// sample says nothing about any queue.
func liftQueues(snap map[string]any) []byte {
	var d queueDoc
	depth := func(kind, name string, m map[string]any) {
		p := d.kind(kind)[name]
		p.W, p.R, p.H, p.T = toInt64(m["depth"]), toInt64(m["ready"]), toInt64(m["held"]), toInt64(m["tried"])
		p.A = toInt64(m["oldest_age_sec"])
		if p.W == 0 {
			p.A = 0
		}
		p.S, _ = m["stale"].(bool)
		d.kind(kind)[name] = p
	}
	if q, ok := snap["queues"].(map[string]any); ok {
		d.D = append(d.D, queueflow.MT)
		for vendor, e := range q {
			if m, ok := e.(map[string]any); ok {
				depth(queueflow.MT, QueueKey(vendor), m)
			}
		}
	}
	for _, kind := range []string{queueflow.DLR, queueflow.MO} {
		sec, _ := snap[kind].(map[string]any)
		if sec == nil {
			continue
		}
		if list, ok := sec["backlog_accounts"].([]any); ok {
			d.D = append(d.D, kind)
			for _, e := range list {
				if m, ok := e.(map[string]any); ok {
					depth(kind, strconv.FormatInt(toInt64(m["account_id"]), 10), m)
				}
			}
		}
		// What was handed to the customer is this queue's way out.
		if list, ok := sec["client_push_accounts"].([]any); ok {
			for _, e := range list {
				m, _ := e.(map[string]any)
				if m == nil {
					continue
				}
				name := strconv.FormatInt(toInt64(m["account_id"]), 10)
				p := d.kind(kind)[name]
				p.O += snapRate(m["delivered"])
				p.OT += snapTotal(m["delivered"])
				d.kind(kind)[name] = p
			}
		}
	}
	// The record lists, from the DB worker that reads them.
	if dw, ok := snap["db_worker"].(map[string]any); ok {
		if list, ok := dw["lanes"].([]any); ok {
			d.D = append(d.D, queueflow.REC)
			for _, e := range list {
				m, _ := e.(map[string]any)
				if m == nil {
					continue
				}
				name := strconv.FormatInt(toInt64(m["lane"]), 10)
				p := d.kind(queueflow.REC)[name]
				p.R, p.H = toInt64(m["waiting"]), toInt64(m["parked"])
				p.W = p.R + p.H
				if p.W > 0 {
					p.A = toInt64(m["oldest_ms"]) / 1000
				}
				d.kind(queueflow.REC)[name] = p
			}
		}
	}
	if list, ok := snap["queue_flow"].([]any); ok {
		for _, e := range list {
			m, _ := e.(map[string]any)
			if m == nil {
				continue
			}
			kind, _ := m["kind"].(string)
			name, _ := m["queue"].(string)
			if kind == queueflow.MT {
				name = QueueKey(name)
			}
			target := d.kind(kind)
			if target == nil || name == "" {
				continue
			}
			p := target[name]
			p.I += toFloat(m["in_tps_60s"])
			p.IT += toInt64(m["in_total"])
			// A customer's queue: this is what an smpp-server handed over on
			// a bind; a push to the customer's URL is the push count above.
			p.O += toFloat(m["out_tps_60s"])
			p.OT += toInt64(m["out_total"])
			target[name] = p
		}
	}
	for _, m := range []map[string]queuePoint{d.MT, d.DLR, d.MO, d.REC} {
		for k, p := range m {
			if p.zero() {
				delete(m, k)
			}
		}
	}
	// An smpp-server's client binds, counted per account. The list is there
	// (empty when nobody is bound) on every smpp-server report.
	if sm, ok := snap["smpp"].(map[string]any); ok {
		if list, ok := sm["binds"].([]any); ok {
			d.D = append(d.D, bindsKind)
			for _, e := range list {
				if m, ok := e.(map[string]any); ok {
					if name, _ := m["account"].(string); name != "" {
						if d.B == nil {
							d.B = map[string]int{}
						}
						d.B[name]++
					}
				}
			}
		}
	}
	// Status-code rules that matched in the last minute, from a vendor role
	// or the dlr role.
	if list, ok := snap["vendor_codes"].([]any); ok {
		for _, e := range list {
			m, ok := e.(map[string]any)
			if !ok {
				continue
			}
			n := toInt64(m["hits_60s"])
			code, _ := m["code"].(string)
			if n <= 0 || code == "" {
				continue
			}
			cid, _ := m["cid"].(string)
			scope, _ := m["scope"].(string)
			action, _ := m["action"].(string)
			if d.VC == nil {
				d.VC = map[string]int64{}
			}
			d.VC[vendorCodeKey(cid, toInt64(m["vendor_id"]), scope, code, action)] += n
		}
	}
	liftVendors(snap, &d)
	liftJobs(snap, &d)
	if len(d.D) == 0 && len(d.MT) == 0 && len(d.DLR) == 0 && len(d.MO) == 0 && len(d.REC) == 0 && len(d.VC) == 0 &&
		len(d.VS) == 0 && len(d.HV) == 0 && len(d.JB) == 0 && d.RQ == nil {
		return nil
	}
	b, err := json.Marshal(d)
	if err != nil {
		return nil
	}
	return b
}

// liftVendors carries each vendor's own state: an SMPP vendor's connections
// and an HTTP vendor's calls.
func liftVendors(snap map[string]any, d *queueDoc) {
	if sv, ok := snap["smpp_vendor"].(map[string]any); ok {
		if list, ok := sv["vendors"].([]any); ok {
			d.D = append(d.D, vendorsKind)
			for _, e := range list {
				m, _ := e.(map[string]any)
				if m == nil {
					continue
				}
				cid := vendorName(m)
				if d.VS == nil {
					d.VS = map[string][2]int{}
				}
				v := d.VS[cid]
				v[1]++
				if b, _ := m["bound"].(bool); b {
					v[0]++
				}
				d.VS[cid] = v
			}
		}
	}
	if hv, ok := snap["http_vendor"].(map[string]any); ok {
		if list, ok := hv["vendors"].([]any); ok {
			d.D = append(d.D, httpVendorsKind)
			for _, e := range list {
				m, _ := e.(map[string]any)
				if m == nil {
					continue
				}
				p := hvPoint{A: toInt64(m["accepted"])}
				switch st, _ := m["status"].(string); st {
				case "ERRORS":
					p.E = 1
				case "NOT_READY":
					p.NR = 1
				}
				if d.HV == nil {
					d.HV = map[string]hvPoint{}
				}
				d.HV[vendorName(m)] = p
			}
		}
	}
}

// vendorName is a status row's vendor: its cid, else its id.
func vendorName(m map[string]any) string {
	if cid, _ := m["cid"].(string); cid != "" {
		return cid
	}
	return "#" + strconv.FormatInt(toInt64(m["vendor_id"]), 10)
}

// liftJobs carries what is wrong with the billing queue, backup and
// retention, and the record queue as a role writing through the DB worker
// sees it.
func liftJobs(snap map[string]any, d *queueDoc) {
	note := func(name string, n int64) {
		if n <= 0 {
			return
		}
		if d.JB == nil {
			d.JB = map[string]int64{}
		}
		d.JB[name] = n
	}
	if dw, ok := snap["db_worker"].(map[string]any); ok {
		if bq, ok := dw["billing_queue"].(map[string]any); ok && bq["inert"] == nil {
			d.D = append(d.D, billingQueueKind)
			switch {
			case toInt64(bq["waiting"]) > 0 && toInt64(bq["oldest_ms"]) >= billingStuckAfter.Milliseconds():
				note(jobBillingStuck, toInt64(bq["oldest_ms"])/1000)
			case toInt64(bq["delayed"]) > 0:
				note(jobBillingStuck, 1)
			}
		}
	}
	if be, ok := snap["billing_events"].(map[string]any); ok {
		if _, read := be["waiting"]; read {
			d.D = append(d.D, billingEventsKind)
			if toInt64(be["waiting"]) > 0 && toInt64(be["oldest_ms"]) >= billingStuckAfter.Milliseconds() {
				note(jobBillingWaits, toInt64(be["oldest_ms"])/1000)
			}
		}
	}
	if b, ok := snap["backup"].(map[string]any); ok {
		if lr, _ := b["last_result"].(map[string]any); lr != nil {
			d.D = append(d.D, "backup")
			if v, ok := lr["verified"].(bool); ok && !v {
				note(jobBackupBad, 1)
			}
		}
	}
	if r, ok := snap["retention"].(map[string]any); ok {
		if lr, _ := r["last_result"].(map[string]any); lr != nil {
			d.D = append(d.D, "retention")
			note(jobArchiveRefused, toInt64(lr["tables_refused"]))
		}
	}
	if rq, ok := snap["record_queue"].(map[string]any); ok {
		d.D = append(d.D, recordQueueKind)
		p := &rqPoint{W: toInt64(rq["waiting"])}
		if running, ok := rq["db_worker_running"].(bool); ok && !running {
			p.NW = 1
		}
		d.RQ = p
	}
}

// parseQueues reads the column back; nil for NULL or anything unreadable.
func parseQueues(b []byte) *queueDoc {
	if len(b) == 0 {
		return nil
	}
	var d queueDoc
	if json.Unmarshal(b, &d) != nil {
		return nil
	}
	return &d
}

// queueFlowSection is this process's queue_flow, or nil when it moved nothing.
func queueFlowSection() []queueflow.Stat {
	if s := queueflow.Snapshot(); len(s) > 0 {
		return s
	}
	return nil
}
