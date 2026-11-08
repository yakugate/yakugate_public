// Package deployment answers two questions for the process it runs in: which
// gateway am I, and does this account or vendor belong here.
//
// A DEPLOYMENT is the set of role processes sharing one YAKU_DEPLOYMENT_ID.
// Hosts are a detail inside it — fifteen roles may sit on one box or five,
// and that is a deployment either way. The id is the identity; nothing else
// is.
//
// The engine reads the id from its own settings and NEVER reads the
// `deployments` register to decide anything. The register is the operator's
// record of what should exist, kept by the Admin API and drawn by the portal.
// A gateway with no Admin API at all still knows which deployment it is and
// still enforces every assignment (PO 2026-09-18: the gateway must run
// without the Admin API).
//
// Assignment is deliberately permissive by default. An empty list means
// EVERYWHERE, so a live gateway that has never heard of deployments keeps
// working exactly as before: every account may submit anywhere and every
// vendor is claimed by every vendor role, until somebody deliberately
// narrows it.
package deployment

import (
	"encoding/json"
	"os"
	"strings"
	"sync"
)

// Assignment is one entry of an account's or vendor's `deployments` list.
//
// The throughput fields are the per-deployment caps: an account with 1000/s
// on `lhr-1` and 200/s on `khi-1` gets exactly that on each, and the sum is
// what the portal shows as its total. They are a SECOND, narrower bucket —
// the company's contracted `max_throughput` still applies across all of them,
// so splitting an account over three deployments never triples what it bought.
//
// Keys are snake_case like every other JSON config the engine owns
// (receipt_policy, dlr_config, protocol_config), not the reference gateway's
// camelCase.
type Assignment struct {
	ID            string `json:"deployment_id"`
	MTThroughput  int    `json:"mt_throughput,omitempty"`
	MOThroughput  int    `json:"mo_throughput,omitempty"`
	DLRThroughput int    `json:"dlr_throughput,omitempty"`
}

// legacyAll was a wildcard entry meaning "every gateway", briefly offered on
// 2026-09-24 and withdrawn the same day: it could carry no throughput (there
// is no gateway to count on), so an account holding it had no rate limit of
// its own and the abuse monitor could not throttle it — only suspend it.
// Throughput is per deployment, so a deployment has to be named.
//
// Parse DROPS such an entry rather than honouring or refusing it. A row that
// still holds one therefore reads as "nothing chosen", which means everywhere
// — the same fail-open the rest of this package takes. Refusing the traffic
// of an account whose column we withdrew would be our mistake charged to the
// customer.
const legacyAll = "*"

// List is a parsed `deployments` column.
//
// An EMPTY list means every deployment. Choosing is mandatory at every door
// (the Admin API, the portal and the console all refuse a save that names
// nothing) ONCE A DEPLOYMENT IS REGISTERED — on an install that has never
// registered one there is no choice to make, and account creation must not be
// blocked by it.
//
// The engine itself stays forgiving: a row written before config/0122, a
// value this binary cannot parse, a withdrawn wildcard and a gateway that
// never named itself all fall through to "everywhere". Refusing traffic is
// the expensive failure; serving it where an operator has not yet chosen is
// visible and recoverable.
type List []Assignment

// Parse reads a `deployments` column. An empty, NULL or `[]` value is the
// "everywhere" default. Unknown keys are tolerated: this column is written by
// the Admin API and the console, and a newer portal writing a field this
// binary has not learned yet must not stop traffic.
//
// A malformed value is an error, and every caller treats an error the same
// way it treats empty — see AllowsRaw. Refusing traffic because one row's
// JSON is broken would turn a typo into an outage.
func Parse(raw string) (List, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "null" || raw == "[]" {
		return nil, nil
	}
	var l List
	if err := json.Unmarshal([]byte(raw), &l); err != nil {
		return nil, err
	}
	out := l[:0]
	for _, a := range l {
		a.ID = strings.TrimSpace(a.ID)
		if a.ID == "" || a.ID == legacyAll {
			continue // names no deployment, or the withdrawn wildcard
		}
		out = append(out, a)
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

// Allows reports whether this deployment may serve the owner of the list.
// An empty list allows everywhere.
func (l List) Allows(id string) bool {
	if len(l) == 0 {
		return true
	}
	for _, a := range l {
		if a.ID == id {
			return true
		}
	}
	return false
}

// Chosen reports whether somebody has actually made a choice. Used by the
// doors to refuse a save that names nothing; never by the engine, which must
// keep serving a row written before this existed.
func (l List) Chosen() bool { return len(l) > 0 }

// Kind names one of the three rates an assignment can carry.
type Kind string

const (
	MT  Kind = "mt"
	MO  Kind = "mo"
	DLR Kind = "dlr"
)

// Rate is the cap for this deployment, in the unit that kind is counted in
// (MT in segments/second, MO and DLR in messages/second). 0 = no cap.
//
// Throughput is ALWAYS per deployment (PO 2026-09-24), so this is the only
// place a rate comes from — the old account-level columns are retired.
func (l List) Rate(id string, k Kind) int {
	for _, a := range l {
		if a.ID != id {
			continue
		}
		return a.rate(k)
	}
	return 0
}

// Total is the sum of a rate across every deployment named, which is the
// number an operator is quoted and the one the screens show. What is enforced
// on any one gateway is that gateway's own Rate.
func (l List) Total(k Kind) int {
	n := 0
	for _, a := range l {
		n += a.rate(k)
	}
	return n
}

func (a Assignment) rate(k Kind) int {
	switch k {
	case MT:
		return a.MTThroughput
	case MO:
		return a.MOThroughput
	case DLR:
		return a.DLRThroughput
	}
	return 0
}

// IDs lists the deployments named, in order, for display and for audit.
func (l List) IDs() []string {
	out := make([]string, 0, len(l))
	for _, a := range l {
		out = append(out, a.ID)
	}
	return out
}

var (
	idOnce sync.Once
	id     string
)

// ID is this process's deployment id: YAKU_DEPLOYMENT_ID, falling back to the
// hostname so it is never empty. An unnamed deployment in a log line is worse
// than a hostname.
//
// Read once. It cannot change while the process runs, and every caller is on
// the hot path.
func ID() string {
	idOnce.Do(func() {
		id = strings.TrimSpace(os.Getenv("YAKU_DEPLOYMENT_ID"))
		if id == "" {
			h, _ := os.Hostname()
			id = h
		}
		if len(id) > MaxIDLen {
			id = id[:MaxIDLen]
		}
	})
	return id
}

var (
	ownerOnce sync.Once
	owner     string
)

// Owner is the deployment whose queues this process serves: YAKU_DEPLOYMENT_ID
// when it is SET, else "" (Saleem 2026-10-02, D-121: "each deployment should
// have separate queues, not to be shared by other gateways").
//
// Every queued message — outgoing, incoming (MO) and client receipts — carries
// an owner, and a process takes only its own and the unowned ones (queued
// before this, or by a gateway that never named itself). So a deployment's
// messages wait for it through an outage; no other gateway sends them.
//
// Deliberately NOT ID(): ID falls back to the hostname, and a container that
// comes back with a new hostname would strand every message its old one
// queued. "" keeps today's single shared queues exactly, so an install that
// never set YAKU_DEPLOYMENT_ID behaves as it always did.
func Owner() string {
	ownerOnce.Do(func() {
		owner = strings.TrimSpace(os.Getenv("YAKU_DEPLOYMENT_ID"))
		if len(owner) > MaxIDLen {
			owner = owner[:MaxIDLen]
		}
	})
	return owner
}

// OverrideOwner makes Owner answer o until the returned func restores it —
// tests only (a role reads YAKU_DEPLOYMENT_ID once, at start).
func OverrideOwner(o string) (restore func()) {
	Owner() // settle the Once first, so it cannot overwrite the override
	prev := owner
	owner = o
	return func() { owner = prev }
}

// MaxIDLen is the width of the deployment_id column on the log tables
// (hot/0046). The id is truncated rather than dropped, so an over-long name
// still identifies the gateway instead of logging nothing; the Admin API
// refuses anything longer when the register is written.
const MaxIDLen = 64

// SQLFilter renders the WHERE fragment that keeps rows assigned to this
// deployment, for the role loaders that read `vendors` directly.
//
// Written as containment on the array (`JSON_CONTAINS`), matching on the id
// alone so an entry's throughput fields never affect whether it matches. The
// NULL and empty checks come first so the common case — nothing assigned
// anywhere — short-circuits without touching a JSON function.
func SQLFilter(column, id string) (string, []any) {
	if id == "" {
		return "1=1", nil
	}
	q := "(" + column + " IS NULL OR JSON_LENGTH(" + column + ") = 0" +
		" OR JSON_CONTAINS(" + column + ", JSON_OBJECT('deployment_id', '" + legacyAll + "'))" +
		" OR JSON_CONTAINS(" + column + ", JSON_OBJECT('deployment_id', ?)))"
	return q, []any{id}
}

// AllowsRaw parses and asks in one call, and is the safe form for anything
// reading the column directly.
//
// A value this binary cannot parse allows everywhere. That is deliberate: the
// alternative is that one malformed row — a hand-edited column, a newer
// portal's field, a truncated write — stops that account's traffic on every
// deployment at once. Refusing traffic is the expensive failure here;
// serving it where an operator did not intend is recoverable and visible.
func AllowsRaw(raw, id string) bool {
	l, err := Parse(raw)
	if err != nil {
		return true
	}
	return l.Allows(id)
}
