package mysql

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/dal/stmt"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/platform/dbpool"
)

// The hand-off lists in MySQL, for an install with no Redis (issue 5 of the
// design audit; plan of 29 Oct 2026, step 3; config/0169, queue/0045).
//
// With Redis a receipt or MO taken in for a client on a gateway that reads
// another queue store is parked in the MAIN Redis, which every gateway sees,
// and its owner moves it into its own queue (internal/handoff). With none
// the place every gateway of the install sees is the CONFIG store: the
// lists (handoff_items), the names of the queue stores (handoff_stores) and
// the importers' turns (queue_turns) are there. A queue store's own id is
// in that queue store (qstore_id), as qstore:id is in each queue Redis.
// Gateways on one queue database read one id and park nothing: what is
// filed there under a deployment's name is read by that deployment.
//
// One difference from a Redis list: an id given out to a parker that has
// not committed yet can sit below one already read. So a trim takes off
// exactly the items its importer read (Head, remembered per list), never
// "the first n by id": an item that becomes visible below them is read next
// time instead of lost.

// HandOffStmts is every statement of the hand-off lists, by name.
var HandOffStmts = stmt.New("mysql")

const (
	handoffNowMS = "TIMESTAMPDIFF(MICROSECOND, '1970-01-01', UTC_TIMESTAMP(6)) DIV 1000"
	parkHead     = "INSERT INTO handoff_items (owner, kind, item) VALUES "
	parkRow      = "(?, ?, CONCAT('{\"p\":', " + handoffNowMS + ", ',\"x\":', ?, '}'))"
)

var (
	stHoStoreIDSet = HandOffStmts.Add("handoff.store_id_set", "INSERT IGNORE INTO qstore_id (k, store_id) VALUES (1, ?)", false)
	stHoStoreID    = HandOffStmts.Add("handoff.store_id", "SELECT store_id FROM qstore_id WHERE k = 1", false)
	stHoName       = HandOffStmts.Add("handoff.name_store", `INSERT INTO handoff_stores (deployment, store_id) VALUES (?, ?)
		ON DUPLICATE KEY UPDATE store_id = VALUES(store_id)`, false)
	stHoUnname = HandOffStmts.Add("handoff.unname_store", "DELETE FROM handoff_stores WHERE deployment = ?", false)
	stHoStores = HandOffStmts.Add("handoff.stores", "SELECT deployment, store_id FROM handoff_stores LIMIT 10000", false)
	// Each item stamped with the database's clock, all in one statement.
	stHoPark = HandOffStmts.Add("handoff.park", parkHead+parkRow, false)
	stHoHead = HandOffStmts.Add("handoff.head",
		"SELECT id, item FROM handoff_items WHERE owner = ? AND kind = ? ORDER BY id LIMIT ?", false)
	stHoNow  = HandOffStmts.Add("handoff.now", "SELECT "+handoffNowMS+" LIMIT 1", false)
	stHoTrim = HandOffStmts.Add("handoff.trim", "DELETE FROM handoff_items WHERE owner = ? AND kind = ? AND id IN (?)", false)
	// A trim with no read before it (another caller's way): the first n.
	stHoTrimFirst = HandOffStmts.Add("handoff.trim_first",
		"DELETE FROM handoff_items WHERE owner = ? AND kind = ? ORDER BY id LIMIT ?", false)
	stHoWaiting = HandOffStmts.Add("handoff.waiting", `SELECT
		(SELECT COUNT(*) FROM handoff_items WHERE owner = ? AND kind = ?),
		(SELECT item FROM handoff_items WHERE owner = ? AND kind = ? ORDER BY id LIMIT 1),
		`+handoffNowMS, false)
)

// HandOffs is the hand-off lists on the config store, with this gateway's
// queue store for its id.
type HandOffs struct {
	cfg, queue *sql.DB
	turns      turns

	mu   sync.Mutex
	read map[string][]int64 // per owner|kind: the ids the last Head read, not yet trimmed
}

var _ dal.HandOffs = (*HandOffs)(nil)

// NewHandOffs keeps the lists on cfgDB (the config store) and reads the
// queue store's id from queueDB.
func NewHandOffs(cfgDB, queueDB *sql.DB) *HandOffs {
	if cfgDB == nil || queueDB == nil {
		return nil
	}
	return &HandOffs{cfg: cfgDB, queue: queueDB, turns: turns{db: cfgDB}, read: map[string][]int64{}}
}

func handoffTurn(owner, kind string) string   { return "handoff:" + owner + ":" + kind }
func handoffStrays(owner, kind string) string { return handoffTurn(owner, kind) + ":strays" }

// QueuesHere: this gateway's queues are in its queue database, which has an
// id of its own.
func (h *HandOffs) QueuesHere() bool { return true }

// QueueStoreID gives the queue store an id if it has none and reads it.
func (h *HandOffs) QueueStoreID(ctx context.Context, newID string) (string, error) {
	if _, err := h.queue.ExecContext(ctx, stHoStoreIDSet.SQL, newID); err != nil {
		return "", err
	}
	var id string
	err := h.queue.QueryRowContext(ctx, stHoStoreID.SQL).Scan(&id)
	return id, err
}

// NameStore files me's queue store id.
func (h *HandOffs) NameStore(ctx context.Context, me, id string) error {
	_, err := h.cfg.ExecContext(ctx, stHoName.SQL, me, id)
	return err
}

// UnnameStore takes me's name out.
func (h *HandOffs) UnnameStore(ctx context.Context, me string) error {
	_, err := h.cfg.ExecContext(ctx, stHoUnname.SQL, me)
	return err
}

// Stores is every deployment's queue store id.
func (h *HandOffs) Stores(ctx context.Context) (map[string]string, error) {
	rows, err := h.cfg.QueryContext(ctx, stHoStores.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var d, id string
		if err := rows.Scan(&d, &id); err != nil {
			return nil, err
		}
		out[d] = id
	}
	return out, rows.Err()
}

// Park adds items to owner's list of kind, each stamped with the database's
// clock, in one statement: all or none.
func (h *HandOffs) Park(ctx context.Context, owner, kind string, items []string) error {
	if len(items) == 0 {
		return nil
	}
	var sb strings.Builder
	sb.WriteString(parkHead)
	args := make([]any, 0, len(items)*3)
	for i, it := range items {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(parkRow)
		args = append(args, owner, kind, it)
	}
	_, err := h.cfg.ExecContext(ctx, sb.String(), args...)
	return err
}

// TakeTurn takes the importer's turn, or keeps it: 1 while it is token's.
func (h *HandOffs) TakeTurn(ctx context.Context, owner, kind, token string, ttl time.Duration) (int, error) {
	mine, err := h.turns.take(ctx, handoffTurn(owner, kind), token, ttl, false)
	if err != nil || !mine {
		return 0, err
	}
	return 1, nil
}

// DropTurn drops the turn if it is still token's.
func (h *HandOffs) DropTurn(ctx context.Context, owner, kind, token string) error {
	return h.turns.drop(ctx, handoffTurn(owner, kind), token)
}

// Head reads up to most items at the head of owner's list of kind, and
// remembers their ids for the trims that follow.
func (h *HandOffs) Head(ctx context.Context, owner, kind string, most int) ([]string, error) {
	rows, err := h.cfg.QueryContext(ctx, stHoHead.SQL, owner, kind, most)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	var out []string
	for rows.Next() {
		var id int64
		var it string
		if err := rows.Scan(&id, &it); err != nil {
			return nil, err
		}
		ids, out = append(ids, id), append(out, it)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	h.mu.Lock()
	h.read[owner+"|"+kind] = ids
	h.mu.Unlock()
	return out, nil
}

// Now is the database's clock.
func (h *HandOffs) Now(ctx context.Context) (time.Time, error) {
	var ms int64
	if err := h.cfg.QueryRowContext(ctx, stHoNow.SQL).Scan(&ms); err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(ms), nil
}

// Trim takes the next n items the last Head read off the list while the
// turn is token's, in one transaction: 1 = done, 0 = the turn was lost.
func (h *HandOffs) Trim(ctx context.Context, owner, kind, token string, n int) (int, error) {
	if n <= 0 {
		return 1, nil
	}
	key := owner + "|" + kind
	h.mu.Lock()
	ids := h.read[key]
	h.mu.Unlock()
	take := ids
	if len(take) > n {
		take = take[:n]
	}
	done := 0
	err := rcTx(ctx, h.cfg, 8, func(tx *sql.Tx) error {
		done = 0
		mine, err := turnHeld(ctx, tx, handoffTurn(owner, kind), token)
		if err != nil || !mine {
			return err
		}
		if len(take) == 0 {
			_, err = tx.ExecContext(ctx, stHoTrimFirst.SQL, owner, kind, n)
		} else {
			args := []any{owner, kind}
			for _, id := range take {
				args = append(args, id)
			}
			_, err = tx.ExecContext(ctx, strings.Replace(stHoTrim.SQL, "IN (?)", "IN ("+placeholders(len(take))+")", 1), args...)
		}
		if err != nil {
			return err
		}
		done = 1
		return nil
	})
	if err != nil || done == 0 {
		return done, err
	}
	h.mu.Lock()
	if cur := h.read[key]; len(cur) >= len(take) && len(take) > 0 && cur[0] == take[0] {
		h.read[key] = cur[len(take):]
	}
	h.mu.Unlock()
	return 1, nil
}

// TakeStrays takes the one-time walk's lock when it is free (SET NX).
func (h *HandOffs) TakeStrays(ctx context.Context, owner, kind, token string, ttl time.Duration) (bool, error) {
	return h.turns.take(ctx, handoffStrays(owner, kind), token, ttl, true)
}

// DropStrays drops the walk's lock if it is still token's.
func (h *HandOffs) DropStrays(ctx context.Context, owner, kind, token string) error {
	return h.turns.drop(ctx, handoffStrays(owner, kind), token)
}

// Waiting reads, in one statement, how many items wait on owner's list of
// kind, the one at its head ("" = none) and the database's clock.
func (h *HandOffs) Waiting(ctx context.Context, owner, kind string) (int64, string, time.Time, error) {
	var n, ms int64
	var head sql.NullString
	if err := h.cfg.QueryRowContext(ctx, stHoWaiting.SQL, owner, kind, owner, kind).Scan(&n, &head, &ms); err != nil {
		return 0, "", time.Time{}, err
	}
	return n, head.String, time.UnixMilli(ms), nil
}

// FactsGone: without Redis no receipt facts are kept, so none is known to
// be gone — a progress receipt is never dropped for it (a parked receipt
// carries no facts key then anyway).
func (h *HandOffs) FactsGone(_ context.Context, keys []string) (map[string]bool, error) {
	return map[string]bool{}, nil
}

// errNoQueueStore: the hand-off needs the queue store for its id.
var errNoQueueStore = errors.New("no queue database (YAKU_QUEUE_DSN) to read the queue store's id from")

// With no Redis (step 3): the hand-off lists in the config store, with the
// queue store's id.
var _ = serves(func(d *dal.Driver) { d.HandOffs = openHandOffs })

// openHandOffs is the hand-off's opener with no Redis: the lists on the
// config store's pool, the id on the queue store's.
func openHandOffs(cfg config.Config) (dal.HandOffs, error) {
	if cfg.QueueDSN == "" {
		return nil, errNoQueueStore
	}
	c, err := dbpool.Get(cfg.ConfigDSN)
	if err != nil {
		return nil, err
	}
	q, err := dbpool.Get(cfg.QueueDSN)
	if err != nil {
		return nil, err
	}
	return NewHandOffs(c, q), nil
}
