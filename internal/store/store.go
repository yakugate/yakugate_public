// Package store defines the backend-agnostic storage interfaces. Concrete
// engines live in mysql/ mssql/ redis/ mongo/ and are selected per role via config.
package store

import (
	"context"

	"github.com/yakugate/yakugate/internal/model"
)

// Queue is a per-vendor pending queue (pending messages only). Claim leases rows
// with a visibility timeout; Ack removes them once durably handled.
type Queue interface {
	Enqueue(ctx context.Context, m model.Message) error
	Claim(ctx context.Context, vendor string, n int) ([]model.Message, error)
	Ack(ctx context.Context, vendor string, msgIDs ...string) error
}

// VendorQueueStat is one queue's live depth + oldest pending age — the
// earliest-warning monitoring signal (PO 2026-07-27): depth AND age rising
// together means a vendor/consumer is stalled, not just busy.
//
// Since D-127 (2026-10-05) the depth is split by what can happen to a message
// next, and the age has ONE meaning on every backend:
//   - Ready: a sender may take it now;
//   - Held: not yet — a sender has it, it waits to be tried again, or it is
//     scheduled for later (Depth = Ready + Held);
//   - Tried: taken at least once and still here (being sent, or waiting to be
//     tried again);
//   - OldestAgeSec: the longest any READY message has been ready. A message
//     scheduled for tomorrow is not waiting, and one a sender holds is moving;
//     what the number catches is messages nobody is taking.
type VendorQueueStat struct {
	Depth        int64 `json:"depth"`
	OldestAgeSec int64 `json:"oldest_age_sec"`
	Ready        int64 `json:"ready"`
	Held         int64 `json:"held"`
	Tried        int64 `json:"tried,omitempty"`
	// Stale: this queue could not be read just now (slow or failed count),
	// so the figures are its last good reading — never a silent 0.
	Stale bool `json:"stale,omitempty"`
}

// QueueDepthStats is optionally implemented by a Queue backend to expose live
// per-vendor depth for /status (monitor.SectionProvider). Not every backend
// implements this yet — MySQL does; see mysql.Store.QueueDepths.
type QueueDepthStats interface {
	QueueDepths(ctx context.Context) (map[string]VendorQueueStat, error)
}

// VendorQueueCounter is optionally implemented by a Queue backend: how many
// messages are waiting for one vendor across EVERY deployment — its own, the
// shared ones and every other deployment's (D-121). QueueDepths reports only
// this deployment's view; a check that must see the whole queue (refusing to
// delete a vendor that still has messages, 2026-10-02) asks this instead.
type VendorQueueCounter interface {
	QueuedForVendor(ctx context.Context, vendor string) (int64, error)
}

// QueuedFinder is optionally implemented by a Queue backend: it finds one
// message still waiting in a vendor queue, by its id, looking ONLY in the
// queues named (an index lookup per queue, never a scan). GET /messages/{id}
// uses it for a message that was accepted but has not left its queue yet —
// such a message has no record until a vendor takes it or it fails or
// expires (QA BUG-YK-EP-016). Nothing is written at acceptance for it.
//
// Only a message of accountID is found. Another account's waiting message
// reads as not found INSIDE the store, by the same steps an unknown id takes,
// so its answer takes no less time: a store that stopped early on someone
// else's message (Redis holding it, an unknown id going on to the MySQL truth
// copy) would let its timing say "this id is waiting" (review of
// BUG-YK-EP-016).
//
// The message comes back as the queue holds it, with Vendor set to the queue
// it was found in, Attempts the times a sender has taken it, and
// NextAttemptAt the time the queue next offers it to a sender — set only when
// that is in the future (a sendAt, a retry wait, a sender holding it now).
//
// MySQL and Redis implement it; MSSQL and Mongo (Phase 2) do not, and a
// waiting message there is simply not found, as before.
type QueuedFinder interface {
	FindQueued(ctx context.Context, msgID string, accountID int64, vendors []string) (model.Message, bool, error)
}

// PendingCountStats is optionally implemented by a store to expose how many
// client_dlr/mo_queue rows are staged waiting for a client push (PO
// 2026-07-27) — MySQL does; see mysql.Store.PendingCounts.
type PendingCountStats interface {
	PendingCounts(ctx context.Context) (dlr, mo VendorQueueStat, err error)
}

// PendingByAccountStats is optionally implemented by a store: one of the two
// backlogs (kind "dlr" or "mo") per account (D-127), for this deployment's
// queues and the shared ones — the accounts with something waiting, largest
// first, at most limit of them.
type PendingByAccountStats interface {
	PendingByAccount(ctx context.Context, kind string, limit int) (map[int64]VendorQueueStat, error)
}

// HotStore holds recent records (MDRs).
type HotStore interface {
	Write(ctx context.Context, m model.Message) error
	UpdateStatus(ctx context.Context, ref model.RecordRef, s model.Status, vendorMsgID, errorCode string) error
}

// ColdStore is the long-term archive (implemented by the retention process).
type ColdStore interface {
	Archive(ctx context.Context, ms []model.Message) error
}
