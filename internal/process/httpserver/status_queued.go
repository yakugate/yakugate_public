package httpserver

// GET /messages/{id} for a message that is still waiting in its vendor's
// queue (QA BUG-YK-EP-016, 2026-10-01).
//
// A message is recorded (record_index + mtlog) only when a vendor takes it, or
// when it finally fails or expires. One that is accepted and still waiting —
// a vendor paused or down, a sendAt in the future, a backlog — has no record,
// so the lookup answered 404 NOT_FOUND although the client guide (section 10),
// openapi.yaml and this handler's own comment promise QUEUED.
//
// Nothing is written at acceptance for this: the queue is read on demand, and
// only when record_index has no row for the id. The id alone does not say
// which vendor's queue holds it, so the lookup is bounded to the queues the
// message CAN be in — the vendors the account's rules name, plus the re-route
// inbox — each read by the message id's index, never scanned.

import (
	"context"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/refdata"
)

// maxStatusQueues caps the queues one lookup reads. An account's rules name a
// handful of vendors; the cap only guards against a configuration with very
// many, and the account's own rules come first so they are never the ones cut.
const maxStatusQueues = 32

// statusReader is how GET /messages/{id} reads records: dal.RecordStore in
// production, an interface so the handler can be tested without a database.
type statusReader interface {
	StatusByID(ctx context.Context, ref model.RecordRef) (dal.MsgStatus, bool, error)
	StatusByReference(ctx context.Context, accountID, companyID int64, ref string) (dal.MsgStatus, bool, error)
}

// freshStatus reads a message's status where its record is WRITTEN, when the
// reader can tell the two apart (a record store with a read instance of its
// own); otherwise it is the same read as StatusByID.
func freshStatus(ctx context.Context, rd statusReader, ref model.RecordRef) (dal.MsgStatus, bool, error) {
	if fr, ok := rd.(interface {
		StatusByIDFresh(ctx context.Context, ref model.RecordRef) (dal.MsgStatus, bool, error)
	}); ok {
		return fr.StatusByIDFresh(ctx, ref)
	}
	return rd.StatusByID(ctx, ref)
}

// readsApart says the record reader reads from another instance than the one
// the records are written to.
func readsApart(rd statusReader) bool {
	ra, ok := rd.(interface{ ReadsApart() bool })
	return ok && ra.ReadsApart()
}

// records returns the record reader, nil when there is none (non-MySQL hot
// store). statusRead is a test's double; production leaves it nil and reads
// hotRead. Written this way, not by storing hotRead in an interface field, so
// a nil record store can never become a non-nil interface.
func (p *Process) records() statusReader {
	if p.statusRead != nil {
		return p.statusRead
	}
	if p.hotRead != nil {
		return p.hotRead
	}
	return nil
}

// statusQueues names the queues a waiting message of this account can be in:
// the re-route inbox, the vendors of the account's own rules (every level,
// every network exception, disabled rules too — a message accepted before a
// rule was turned off still waits where it was put), then the vendors of rules
// for every account (retired for new traffic, but a message accepted under one
// may still be waiting). MO rules are left out: their vendor is the one a
// message ARRIVES from, not a queue.
func statusQueues(ref *refdata.Mem, accountID int64) []string {
	seen := map[string]bool{}
	var out []string
	add := func(cid string) {
		if cid != "" && !seen[cid] && len(out) < maxStatusQueues {
			seen[cid] = true
			out = append(out, cid)
		}
	}
	addRule := func(r *refdata.Route) {
		if r == nil || strings.EqualFold(r.Direction, "MO") {
			return
		}
		add(r.VendorCID)
		for _, lv := range r.Levels {
			for _, v := range lv.Vendors {
				if v != nil {
					add(v.VendorCID)
				}
			}
		}
		for _, code := range r.ExceptionCodes() {
			for _, lv := range r.Exceptions[code] {
				for _, v := range lv.Vendors {
					if v != nil {
						add(v.VendorCID)
					}
				}
			}
		}
	}
	add(model.RerouteInbox) // first, so the cap can never cut it
	if ref == nil {
		return out
	}
	own, global := ref.RoutesFor(accountID)
	for _, r := range own {
		addRule(r)
	}
	for _, r := range global {
		addRule(r)
	}
	return out
}

// findQueued looks for id in the account's queues. ok=false when it is not
// waiting anywhere this account's messages can wait, or is someone else's —
// the caller answers both exactly like an unknown id. Only an id shaped like
// one of ours is looked for: a client reference ("order-1") is never a queue
// key, and the queues are not indexed by reference.
func (p *Process) findQueued(ctx context.Context, acct *refdata.Account, id string) (model.Message, bool, error) {
	if p.queued == nil || acct == nil || !isOurID(id) {
		return model.Message{}, false, nil
	}
	// The store filters by account itself, so someone else's waiting message
	// takes the same road (and time) as an id that does not exist; the check
	// below is a second lock on the same door.
	m, ok, err := p.queued.FindQueued(ctx, strings.ToUpper(id), acct.ID, statusQueues(p.ref, acct.ID))
	if err != nil || !ok {
		return model.Message{}, false, err
	}
	if m.AccountID != acct.ID {
		return model.Message{}, false, nil
	}
	return m, true, nil
}

// isOurID reports whether id is shaped like a message id we give (a ULID).
func isOurID(id string) bool {
	_, err := ulid.ParseStrict(id)
	return err == nil
}

// queuedAnswer is the 200 body for a waiting message: the same fields a
// recorded one carries, plus what the queue knows. The vendor is never named
// (the client is not told which network carries its traffic, as on the send
// reply).
func queuedAnswer(m model.Message, now time.Time) map[string]any {
	out := map[string]any{"messageId": m.ID, "requestId": orID(m.RequestID, m.ID), "status": "QUEUED", "to": m.Destination}
	if m.IdempotencyKey != "" {
		out["reference"] = m.IdempotencyKey
	}
	if m.Sender != "" {
		out["sender"] = m.Sender
	}
	if u, err := ulid.ParseStrict(m.ID); err == nil {
		out["acceptedTime"] = ulid.Time(u.Time()).UTC().Format(time.RFC3339)
	}
	if m.Priority > 0 {
		out["priority"] = m.Priority
	}
	// When the queue will next offer it to the network: the sendAt the client
	// asked for, or the end of a wait between tries.
	if !m.NextAttemptAt.IsZero() && m.NextAttemptAt.After(now) {
		out["nextAttemptTime"] = m.NextAttemptAt.UTC().Format(time.RFC3339)
	}
	return out
}

// orID is the request id as the queue writes it: the message's own id when
// the request had none (mysql.Store.Enqueue's orStr(m.RequestID, m.ID)).
func orID(requestID, msgID string) string {
	if requestID != "" {
		return requestID
	}
	return msgID
}
