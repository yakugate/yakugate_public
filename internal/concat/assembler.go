// Package concat implements the SMPP multipart ASSEMBLE-FIRST buffer (D-096,
// PO 2026-08-04): concatenated parts arriving over SMPP — possibly on
// different binds and different gateway instances — are collected in a shared
// DB table ("it must reside into the db"), each part acknowledged immediately
// on its durable insert (Q3 ruling: "we will acknowledge immidiately and will
// do rest later"). When the group completes, ONE assembled whole message runs
// the pipeline, so every message-level rule (spam, template, keyword routing,
// rewrites, encoding/segments) sees the full text exactly once. Egress is
// unchanged: the whole message takes the same path long HTTP submissions
// always took (SMPP egress cost-splits it with per-part concat_parts
// tracking; HTTP egress sends it as one call).
//
// Incomplete groups past the timeout are released as INDIVIDUAL parts through
// the legacy per-part path (better than discarding paid-for traffic), logged.
package concat

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/lib/smpp/pdu"
)

// Assembler is the part buffer kept in a database (dal.ConcatBuffer: the
// queue database's concat_assembly, or the main Redis). Safe for concurrent
// use across binds AND instances: group completion is decided in one step
// of the database (row locks in MySQL, one script in Redis), so exactly one
// caller assembles a finished group.
type Assembler struct {
	buf     dal.ConcatBuffer
	timeout time.Duration
	logf    func(format string, args ...any)
	// mo marks the buffer of INCOMING multi-part messages (2026-09-14). Its
	// groups are kept apart from MT's (keys starting "mo|" in MySQL, keys of
	// their own in Redis), keyed by the vendor rather than the account
	// (nobody owns an MO until it is routed, and it is routed only once
	// joined), and each reaper releases only its own kind — smpp-server's MT
	// reaper must never record an MO group as an expired MT message. The
	// buffer is opened for the same side.
	mo bool
	// scope gives the id an MO group is keyed by (SetScope).
	scope func(int64) int64
}

// CountedAssembler is an Assembler whose database also counts the groups
// that wait (dal.ConcatPending: the Redis index), for monitoring.
type CountedAssembler struct {
	*Assembler
	p dal.ConcatPending
}

// Pending reports how many groups are waiting for parts and how long the
// oldest has waited (seconds). Only the Redis buffer offers this: the MySQL
// buffer is a table, and monitoring does not query tables (PO 2026-09-11).
func (c *CountedAssembler) Pending(ctx context.Context) (groups, oldestAgeSec int64, err error) {
	return c.p.Pending(ctx)
}

// New wires the assembler over a buffer of the MT side. timeout is how long
// an incomplete group waits before it is recorded expired and dropped
// (YAKU_CONCAT_ASSEMBLY_TIMEOUT_SEC, default 60s).
func New(buf dal.ConcatBuffer, timeout time.Duration, logf func(string, ...any)) Buffer {
	return wrap(newAssembler(buf, timeout, logf, false))
}

// NewMO is New for incoming (MO) multi-part messages (2026-09-14), over a
// buffer opened for the MO side: ReleaseExpired returns only MO groups.
func NewMO(buf dal.ConcatBuffer, timeout time.Duration, logf func(string, ...any)) Buffer {
	return wrap(newAssembler(buf, timeout, logf, true))
}

func newAssembler(buf dal.ConcatBuffer, timeout time.Duration, logf func(string, ...any), mo bool) *Assembler {
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Assembler{buf: buf, timeout: timeout, logf: logf, mo: mo}
}

func wrap(a *Assembler) Buffer {
	if p, ok := a.buf.(dal.ConcatPending); ok {
		return &CountedAssembler{Assembler: a, p: p}
	}
	return a
}

// GroupKey is the SMPP-standard concat-group identity, account-scoped
// (tenants can never collide on the 8-bit ref).
func GroupKey(m *model.Message) string {
	return strconv.FormatInt(m.AccountID, 10) + "|" + m.Sender + "|" + m.Destination + "|" +
		strconv.Itoa(m.ConcatRef) + "|" + strconv.Itoa(m.ConcatTotal)
}

// MOGroupKey is an incoming message's group identity: the vendor it came
// from, the subscriber, the short code and the handset's reference. The
// "mo|" prefix keeps it apart from MT groups in a shared buffer.
func MOGroupKey(m *model.Message) string { return moKey(m, nil) }

// moKey is MOGroupKey with the vendor read through scope: a carrier's TX and
// RX rows are two vendors of one company, and its parts may come in on
// either, on any gateway (Saleem, 28 Oct 2026), so they are grouped under
// the company (vendorgroup.Groups.Scope, as receipts are since D-135). A nil
// scope keys by the vendor itself.
func moKey(m *model.Message, scope func(int64) int64) string {
	v := m.VendorID
	if scope != nil {
		v = scope(v)
	}
	return "mo|" + strconv.FormatInt(v, 10) + "|" + m.Sender + "|" + m.Destination + "|" +
		strconv.Itoa(m.ConcatRef) + "|" + strconv.Itoa(m.ConcatTotal)
}

func (a *Assembler) key(m *model.Message) string {
	if a.mo {
		return moKey(m, a.scope)
	}
	return GroupKey(m)
}

// AddPart durably buffers one part. complete=true returns the assembled whole
// message (parts concatenated in seq order, concat fields cleared) — exactly
// one caller per group ever gets it. A retransmitted duplicate part is
// idempotent (unique group+seq). An error means the part was NOT buffered —
// the caller must fall back to the legacy per-part path rather than ack a
// part that isn't durable anywhere.
func (a *Assembler) AddPart(ctx context.Context, m *model.Message) (complete bool, assembled *model.Message, err error) {
	payload, err := json.Marshal(m)
	if err != nil {
		return false, nil, fmt.Errorf("marshal part: %w", err)
	}
	parts, err := a.buf.AddPart(ctx, a.key(m), m.AccountID, m.ConcatSeq, m.ConcatTotal, string(payload), a.timeout)
	if err != nil {
		return false, nil, err
	}
	if len(parts) == 0 {
		return false, nil, nil // still waiting for more parts
	}
	return true, Assemble(parts), nil
}

// ReleaseExpired claims every group whose OLDEST part has waited past the
// timeout and returns ONE representative per group (the assembled partial,
// Status=expired) for the caller to record — choice A (PO 2026-08-09: record
// expired, drop; a partial concat can't be delivered coherently). This
// SUPERSEDES the earlier D-096 Q4 per-part release. Instance-safe the same way
// AddPart is: a group is claimed and deleted before it is returned.
func (a *Assembler) ReleaseExpired(ctx context.Context) ([]*model.Message, error) {
	var expired []*model.Message
	err := a.buf.ClaimExpired(ctx, a.timeout, func(key string, parts []*model.Message, bad error) {
		if bad != nil {
			a.logf("concat reaper: bad payload in group %s: %v", key, bad)
			return
		}
		a.logf("concat group %s incomplete after %s — recording expired, dropping %d part(s)", key, a.timeout, len(parts))
		rep := Assemble(parts)
		rep.Status = model.StatusExpired
		if a.mo {
			// An MO group reports how many parts it was waiting for; the
			// caller records "k of n parts arrived".
			rep.ConcatTotal = parts[0].ConcatTotal
		}
		expired = append(expired, rep)
	})
	return expired, err
}

// Assemble builds the whole message from a group's parts: part 1's envelope
// (sender/destination/account/priority/validity/idempotency all identical by
// construction), bodies concatenated in seq order, concat fields cleared so
// the pipeline and egress treat it as one long message (encoding stage
// recomputes Segments; SMPP egress re-splits cost-aware). Exported for tests
// — pure function, no DB.
func Assemble(parts []*model.Message) *model.Message {
	sort.Slice(parts, func(i, j int) bool { return parts[i].ConcatSeq < parts[j].ConcatSeq })
	whole := *parts[0]
	body := ""
	for _, p := range parts {
		body += p.Body
	}
	// A GSM-7 extension character a part boundary cut in two (its ESC kept
	// at the end of one part, its value opening the next) is one character
	// again, and an ESC left alone is dropped (pdu.MendSplitEscapes; Saleem,
	// 18 Oct 2026: "previously it was showing = instead of ~").
	whole.Body = pdu.MendSplitEscapes(body)
	whole.ConcatRef, whole.ConcatSeq, whole.ConcatTotal = 0, 0, 0
	// Every id the client was given, so each part can get its receipt
	// (PO 2026-09-11). Part 1's id is the whole message's own ID.
	whole.PartIDs = make([]string, len(parts))
	for i, p := range parts {
		whole.PartIDs[i] = p.ID
	}
	return &whole
}
