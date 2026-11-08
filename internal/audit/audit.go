// Package audit records operator actions taken against the engine's own admin
// endpoints, in the same `audit_log` table the portal writes to (PO
// 2026-09-05: unblock gets "a security record AND a database row", and every
// admin endpoint is recorded, not just the ones that change something).
//
// Why a table and not just a log file: a log answers "what happened", an audit
// row answers "who did this, to what, and when" long after the files have
// rotated away. The portal already answers that question for config changes;
// until now the engine's own admin endpoints — including one that DECRYPTS a
// customer's message body — answered it with a single printf line.
//
// Scope note, on the record: migration config/0039 carries a PO ruling from
// 2026-07-27 that audit_log is portal CRUD change-tracking and deliberately
// NOT an HTTP access log. Recording read-only endpoints (a dialect dry-run, a
// regex test) sits on the access-log side of that line. It is written here
// because the PO chose "record all four" on 2026-09-05 with that trade-off
// visible; `action` names each one, so trimming later is a WHERE clause.
//
// Nothing here may ever delay or fail a request: Record is fire-and-forget on
// its own goroutine with its own timeout, exactly like the reject log.
package audit

import (
	"context"
	"encoding/json"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// Entry is one recorded action. Only Actor and Action are required; the rest
// are written when they are known, so a caller never has to invent a value.
type Entry struct {
	// Actor is who did it. The engine's admin endpoints authenticate with a
	// status key rather than a user identity, so this is the honest name of
	// that credential ("engine-admin"), not a person — the IP is what
	// narrows it down further.
	Actor string
	// Action is the verb, dotted and stable, because it is what an auditor
	// filters on: "security.unblock", "config.cache_refresh", "data.decrypt".
	Action string
	// Entity/EntityID/TargetLabel say WHAT was acted on, in audit_log's
	// existing shape (the same three columns the abuse monitor writes).
	Entity      string
	EntityID    string
	TargetLabel string

	AccountID int64
	CompanyID int64

	// Method/Route/ResponseCode/IP place the action. audit_log gained these
	// columns for the portal; they are exactly what identifies an engine
	// admin call too.
	Method       string
	Route        string
	ResponseCode int
	IP           string

	// Summary is one human sentence. Outcome is "success" or "failure" —
	// empty means success, since that is the column's own default.
	Summary string
	Outcome string
	Reason  string

	// Detail carries whatever else matters, as JSON. Never put a credential,
	// a token or a message body in here: this table is read by more people
	// than the engine's own operators.
	Detail map[string]any
}

// Writer records entries. An implementation must never block the caller.
type Writer interface {
	Record(e Entry)
}

// Trail writes to the config store's audit_log (dal.AuditLog; it was named
// MySQL before the DAL, 29 Oct 2026).
type Trail struct {
	log dal.AuditLog
	// onErr reports a failed write. An audit row that cannot be stored is
	// worth a loud line — silently losing the trail is the one failure mode
	// this package must not have.
	onErr func(error)
}

// Log exposes the store so the trail can be VERIFIED through the same writer
// that records it (see Verify). Returns nil on a nil receiver, which is how a
// deployment with no audit connection presents itself throughout this
// package.
func (m *Trail) Log() dal.AuditLog {
	if m == nil {
		return nil
	}
	return m.log
}

// New is the trail on the config store's audit_log (dal.OpenAuditLog: the
// process's one pool for the config store). A nil *Trail is safe to use, so
// a deployment without one simply records nothing rather than failing to
// start.
func New(log dal.AuditLog, onErr func(error)) *Trail {
	return &Trail{log: log, onErr: onErr}
}

// Record stores one entry. Fire-and-forget: it returns immediately, so a slow
// or unreachable config store can never delay an operator's request or change
// its answer.
func (m *Trail) Record(e Entry) {
	if m == nil || m.log == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := m.write(ctx, e); err != nil && m.onErr != nil {
			m.onErr(err)
		}
	}()
}

func (m *Trail) write(ctx context.Context, e Entry) error {
	outcome := e.Outcome
	if outcome == "" {
		outcome = "success"
	}
	var detail string
	if len(e.Detail) > 0 {
		b, err := json.Marshal(e.Detail)
		if err != nil {
			return err
		}
		detail = string(b)
	}
	// An empty text and a zero id or code are written NULL by the DAL, so a
	// missing fact reads as missing instead of as "" or 0.
	return m.log.AppendAudit(ctx, dal.AdminAuditRow{
		Actor: e.Actor, AccountID: e.AccountID, CompanyID: e.CompanyID, Action: e.Action,
		Entity: e.Entity, EntityID: e.EntityID, Label: e.TargetLabel,
		Method: e.Method, Route: e.Route, ResponseCode: e.ResponseCode, IP: e.IP,
		Summary: e.Summary, Outcome: outcome, Reason: e.Reason, Detail: detail,
	})
}

// Close does nothing: the pool is the process's own (dbpool), shared with
// everything else that reads the config store.
func (m *Trail) Close() error { return nil }
