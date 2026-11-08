package concat

import (
	"context"

	"github.com/yakugate/yakugate/internal/model"
)

// Buffer is the SMPP multipart assemble-first part buffer. Two backends
// implement it and the smpp-server picks the one that MIRRORS the queue
// backend (PO 2026-08-09), so the whole ingress flow — buffer, assemble,
// enqueue — stays in one store with no cross-store hop:
//
//   - *Assembler        MySQL queue  → concat_assembly table
//   - *CountedAssembler Redis queue  → one hash per group + an index ZSET
//
// (both over a dal.ConcatBuffer since 29 Oct 2026; the memory buffer is the
// third, YAKU_CONCAT_*_STORE)
//
// Both are safe across binds AND gateway instances: group completion and
// expiry are claimed atomically (row locks in MySQL, a single Lua script in
// Redis), so exactly one caller ever assembles or expires a given group.
//
// Timeout policy is CHOICE A (PO 2026-08-09), identical on both backends: an
// incomplete group past the assembly timeout is RECORDED as expired and
// dropped — never flushed part-by-part. A partial concat group cannot be
// delivered coherently to a handset (the UDH promises N parts; delivering
// fewer shows nothing or garbage), so the honest outcome is a clean expiry.
// This SUPERSEDES the earlier D-096 Q4 "release parts via the per-part path"
// ruling for both backends.
type Buffer interface {
	// AddPart durably buffers one part. The durable insert is what "accepted"
	// means for a part under assemble-first, so the caller may ack the part as
	// soon as this returns without error. complete=true returns the assembled
	// whole message (parts joined in seq order, concat fields cleared) and is
	// handed to EXACTLY ONE caller per group. A retransmitted duplicate part is
	// idempotent. A non-nil error means the part was NOT buffered anywhere — the
	// caller must fall back to the legacy per-part path rather than ack a part
	// it cannot hold.
	AddPart(ctx context.Context, m *model.Message) (complete bool, assembled *model.Message, err error)

	// ReleaseExpired claims every group whose OLDEST part has waited past the
	// timeout and returns ONE representative message per group (the assembled
	// partial, Status=expired) for the caller to record. It does NOT reprocess
	// parts (choice A). Instance-safe: a group is claimed and deleted before it
	// is returned, so concurrent reapers never double-record it.
	ReleaseExpired(ctx context.Context) ([]*model.Message, error)
}

// compile-time proof both backends satisfy the interface.
var (
	_ Buffer = (*Assembler)(nil)
	_ Buffer = (*CountedAssembler)(nil)
)
