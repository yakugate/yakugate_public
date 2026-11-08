package dal

import (
	"context"
	"fmt"
	"time"

	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// The multi-part buffer (plan of 29 Oct 2026): the parts of a long message
// gathered until they are all in (internal/concat, YAKU_CONCAT_MT_STORE /
// YAKU_CONCAT_MO_STORE). Assembling the whole and recording an expired group
// stay in concat; what is kept is here. A buffer is opened for one side: MT
// (a client's long message) or MO (a vendor's), each with its own groups.

// ConcatBuffer keeps parts by their group's key.
type ConcatBuffer interface {
	// AddPart keeps one part (its JSON payload; a part sent again is kept
	// once) and, when the group then has total parts, takes the whole group
	// in the same step: exactly one caller is given it. nil = still waiting.
	// timeout is how long an incomplete group waits (a key's life is set
	// from it where the database needs one).
	AddPart(ctx context.Context, key string, accountID int64, seq, total int, payload string, timeout time.Duration) ([]*model.Message, error)
	// ClaimExpired takes, one group at a time, up to a bounded number of
	// groups whose oldest part has waited past timeout, and hands each to
	// each (bad = a part that cannot be read: the group is dropped). It
	// stops at the first error.
	ClaimExpired(ctx context.Context, timeout time.Duration, each func(key string, parts []*model.Message, bad error)) error
}

// ConcatPending is the buffer's optional count of the groups that wait and
// how long the oldest has waited (seconds), for monitoring. Only a buffer
// that keeps an index has it: a table is not counted (PO 2026-09-11).
type ConcatPending interface {
	Pending(ctx context.Context) (groups, oldestAgeSec int64, err error)
}

// OpenConcatBuffer opens the buffer of one side on the database the setting
// names (on: redis = the main Redis; mysql = the queue database).
func OpenConcatBuffer(cfg config.Config, on config.Backend, mo bool) (ConcatBuffer, error) {
	d, ok := Lookup(on)
	if !ok {
		return nil, fmt.Errorf("dal: no %s DAL in this binary (have %v)", on, Registered())
	}
	if d.ConcatBuffer == nil {
		return nil, fmt.Errorf("dal: concat buffer on %s: %w", on, ErrNotServed)
	}
	b, err := d.ConcatBuffer(cfg, mo)
	if err != nil {
		return nil, fmt.Errorf("dal: concat buffer on %s: %w", on, err)
	}
	return b, nil
}
