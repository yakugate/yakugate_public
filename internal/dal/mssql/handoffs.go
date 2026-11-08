package mssql

// The hand-off lists on SQL Server, for an install with no Redis (mirrors
// internal/dal/mysql/handoffs.go and the hand-off half of turns.go).
//
// As on MySQL: the lists (handoff_items), the names of the queue stores
// (handoff_stores) and the importers' turns (queue_turns) are in the CONFIG
// store, which every gateway of the install sees; a queue store's own id is
// in that queue store (qstore_id). A trim takes off exactly the items its
// importer read (Head, remembered per list): an id given to a parker that
// commits late can sit below one already read, and is read next time
// instead of lost. Every time is the database's own UTC clock.

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
)

var (
	// The id is written when there is none (INSERT IGNORE's twin, the key
	// held so two first writers make one), then read: one round trip.
	stHoStoreID = QueueStmts.Add("handoff.store_id", `SET NOCOUNT ON;
		INSERT INTO qstore_id (k, store_id) SELECT 1, @p1
			WHERE NOT EXISTS (SELECT 1 FROM qstore_id WITH (UPDLOCK, HOLDLOCK) WHERE k = 1);
		SELECT store_id FROM qstore_id WHERE k = 1`, false)
	// updated_at had MySQL's ON UPDATE CURRENT_TIMESTAMP: set here.
	stHoName = AddUpsert(QueueStmts, "handoff.name_store", UpsertSpec{
		Table: "handoff_stores", Keys: []string{"deployment"},
		Set:    []string{"store_id = $store_id", "updated_at = SYSUTCDATETIME()"},
		Insert: []string{"deployment", "store_id"},
	}, false)
	stHoUnname = QueueStmts.Add("handoff.unname_store", "DELETE FROM handoff_stores WHERE deployment = @p1", false)
	stHoStores = QueueStmts.Add("handoff.stores", "SELECT TOP (10000) deployment, store_id FROM handoff_stores", false)
	// Every item stamped with the database's clock, all in one statement,
	// the ids given in the items' order (INSERT … SELECT … ORDER BY).
	stHoPark = QueueStmts.Add("handoff.park", `INSERT INTO handoff_items (owner, kind, item)
		SELECT @p1, @p2, CONCAT(N'{"p":', `+qNowMS+`, N',"x":', j.value, N'}')
		FROM OPENJSON(@p3) AS j ORDER BY CAST(j.[key] AS INT)`, false)
	stHoHead = QueueStmts.Add("handoff.head",
		"SELECT TOP (@p3) id, item FROM handoff_items WHERE owner = @p1 AND kind = @p2 ORDER BY id", false)
	stHoNow = QueueStmts.Add("handoff.now", "SELECT TOP (1) "+qNowMS, false)
	// A trim runs only while the turn is the token's, its row held to the
	// end (a takeover waits for it, as a Redis script runs whole). It
	// answers 1 when it trimmed, 0 when the turn was lost.
	stHoTrim = QueueStmts.Add("handoff.trim", `SET NOCOUNT ON; SET XACT_ABORT ON;
		DECLARE @yk_mine INT = 0;
		BEGIN TRAN;
		IF EXISTS (SELECT 1 FROM queue_turns WITH (UPDLOCK, HOLDLOCK)
			WHERE name = @p1 AND token = @p2 AND until_at >= SYSUTCDATETIME())
		BEGIN
			SET @yk_mine = 1;
			DELETE t FROM OPENJSON(@p5) WITH (v BIGINT '$') AS j INNER LOOP JOIN handoff_items AS t ON t.id = j.v
				WHERE t.owner = @p3 AND t.kind = @p4;
		END
		COMMIT;
		SELECT @yk_mine`, false)
	// A trim with no read before it: the first n.
	stHoTrimFirst = QueueStmts.Add("handoff.trim_first", `SET NOCOUNT ON; SET XACT_ABORT ON;
		DECLARE @yk_mine INT = 0;
		BEGIN TRAN;
		IF EXISTS (SELECT 1 FROM queue_turns WITH (UPDLOCK, HOLDLOCK)
			WHERE name = @p1 AND token = @p2 AND until_at >= SYSUTCDATETIME())
		BEGIN
			SET @yk_mine = 1;
			WITH yk_first AS (SELECT TOP (@p5) id FROM handoff_items WHERE owner = @p3 AND kind = @p4 ORDER BY id)
			DELETE FROM yk_first;
		END
		COMMIT;
		SELECT @yk_mine`, false)
	stHoWaiting = QueueStmts.Add("handoff.waiting", `SELECT
		(SELECT COUNT_BIG(*) FROM handoff_items WHERE owner = @p1 AND kind = @p2),
		(SELECT TOP (1) item FROM handoff_items WHERE owner = @p1 AND kind = @p2 ORDER BY id),
		`+qNowMS, false)

	// A turn (MySQL's turn.take and turn.take_free, which answer alike: a
	// turn is taken when free, run out or already the token's, and then
	// runs ttl from now), and whether it is the token's now — one round
	// trip. The key is held (UPDLOCK, HOLDLOCK) so two first takers make
	// one row. @p3 and @p4 are the ttl's seconds and microseconds.
	stHoTurnTake = QueueStmts.Add("handoff.turn_take", `SET NOCOUNT ON; SET XACT_ABORT ON;
		DECLARE @yk_until DATETIME2(6) = DATEADD(MICROSECOND, @p4, DATEADD(SECOND, @p3, SYSUTCDATETIME()));
		BEGIN TRAN;
		UPDATE queue_turns WITH (UPDLOCK, HOLDLOCK) SET token = @p2, until_at = @yk_until
			WHERE name = @p1 AND (token = @p2 OR until_at < SYSUTCDATETIME());
		IF @@ROWCOUNT = 0 AND NOT EXISTS (SELECT 1 FROM queue_turns WITH (UPDLOCK, HOLDLOCK) WHERE name = @p1)
			INSERT INTO queue_turns (name, token, until_at) VALUES (@p1, @p2, @yk_until);
		COMMIT;
		SELECT CASE WHEN token = @p2 AND until_at >= SYSUTCDATETIME() THEN 1 ELSE 0 END FROM queue_turns WHERE name = @p1`, false)
	stHoTurnDrop = QueueStmts.Add("handoff.turn_drop", "DELETE FROM queue_turns WHERE name = @p1 AND token = @p2", false)
)

// HandOffs is the hand-off lists on the config store, with this gateway's
// queue store for its id.
type HandOffs struct {
	cfg, queue *sql.DB

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
	return &HandOffs{cfg: cfgDB, queue: queueDB, read: map[string][]int64{}}
}

func hoTurn(owner, kind string) string   { return "handoff:" + owner + ":" + kind }
func hoStrays(owner, kind string) string { return hoTurn(owner, kind) + ":strays" }

// QueuesHere: this gateway's queues are in its queue database, which has an
// id of its own.
func (h *HandOffs) QueuesHere() bool { return true }

// QueueStoreID gives the queue store an id if it has none and reads it.
func (h *HandOffs) QueueStoreID(ctx context.Context, newID string) (string, error) {
	var id string
	err := Again(ctx, 4, func() error {
		return h.queue.QueryRowContext(ctx, stHoStoreID.SQL, newID).Scan(&id)
	})
	return id, err
}

// NameStore files me's queue store id.
func (h *HandOffs) NameStore(ctx context.Context, me, id string) error {
	return Again(ctx, 4, func() error {
		_, err := stHoName.Exec(ctx, h.cfg, sql.Named("deployment", me), sql.Named("store_id", id))
		return err
	})
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
	j, err := qJSON(items)
	if err != nil {
		return err
	}
	return Again(ctx, 4, func() error {
		_, err := h.cfg.ExecContext(ctx, stHoPark.SQL, owner, kind, j)
		return err
	})
}

// takeTurn takes the turn name for token for ttl when it is free, run out
// or already token's, and says whether it is token's now.
func (h *HandOffs) takeTurn(ctx context.Context, name, token string, ttl time.Duration) (bool, error) {
	us := ttl.Microseconds()
	var mine int
	err := Again(ctx, 4, func() error {
		return h.cfg.QueryRowContext(ctx, stHoTurnTake.SQL, name, token, us/1_000_000, us%1_000_000).Scan(&mine)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return mine == 1, err
}

// TakeTurn takes the importer's turn, or keeps it: 1 while it is token's.
func (h *HandOffs) TakeTurn(ctx context.Context, owner, kind, token string, ttl time.Duration) (int, error) {
	mine, err := h.takeTurn(ctx, hoTurn(owner, kind), token, ttl)
	if err != nil || !mine {
		return 0, err
	}
	return 1, nil
}

// DropTurn drops the turn if it is still token's.
func (h *HandOffs) DropTurn(ctx context.Context, owner, kind, token string) error {
	_, err := h.cfg.ExecContext(ctx, stHoTurnDrop.SQL, hoTurn(owner, kind), token)
	return err
}

// Head reads up to most items at the head of owner's list of kind, and
// remembers their ids for the trims that follow.
func (h *HandOffs) Head(ctx context.Context, owner, kind string, most int) ([]string, error) {
	rows, err := h.cfg.QueryContext(ctx, stHoHead.SQL, owner, kind, int64(most))
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
	var done int
	err := Again(ctx, 8, func() error {
		if len(take) == 0 {
			return h.cfg.QueryRowContext(ctx, stHoTrimFirst.SQL, hoTurn(owner, kind), token, owner, kind, int64(n)).Scan(&done)
		}
		j, err := qJSON(take)
		if err != nil {
			return err
		}
		return h.cfg.QueryRowContext(ctx, stHoTrim.SQL, hoTurn(owner, kind), token, owner, kind, j).Scan(&done)
	})
	if err != nil || done == 0 {
		return 0, err
	}
	h.mu.Lock()
	if cur := h.read[key]; len(cur) >= len(take) && len(take) > 0 && cur[0] == take[0] {
		h.read[key] = cur[len(take):]
	}
	h.mu.Unlock()
	return 1, nil
}

// TakeStrays takes the one-time walk's lock when it is free (MySQL's
// take_free: free, run out, or already token's).
func (h *HandOffs) TakeStrays(ctx context.Context, owner, kind, token string, ttl time.Duration) (bool, error) {
	return h.takeTurn(ctx, hoStrays(owner, kind), token, ttl)
}

// DropStrays drops the walk's lock if it is still token's.
func (h *HandOffs) DropStrays(ctx context.Context, owner, kind, token string) error {
	_, err := h.cfg.ExecContext(ctx, stHoTurnDrop.SQL, hoStrays(owner, kind), token)
	return err
}

// Waiting reads, in one statement, how many items wait on owner's list of
// kind, the one at its head ("" = none) and the database's clock.
func (h *HandOffs) Waiting(ctx context.Context, owner, kind string) (int64, string, time.Time, error) {
	var n, ms int64
	var head sql.NullString
	if err := h.cfg.QueryRowContext(ctx, stHoWaiting.SQL, owner, kind).Scan(&n, &head, &ms); err != nil {
		return 0, "", time.Time{}, err
	}
	return n, head.String, time.UnixMilli(ms), nil
}

// FactsGone: without Redis no receipt facts are kept, so none is known to
// be gone.
func (h *HandOffs) FactsGone(_ context.Context, keys []string) (map[string]bool, error) {
	return map[string]bool{}, nil
}

// errNoQueueStore: the hand-off needs the queue store for its id.
var errNoQueueStore = errors.New("no queue database (YAKU_QUEUE_DSN) to read the queue store's id from")

// openHandOffs is the hand-off's opener with no Redis: the lists on the
// config store's pool, the id on the queue store's.
func openHandOffs(cfg config.Config) (dal.HandOffs, error) {
	if cfg.QueueDSN == "" {
		return nil, errNoQueueStore
	}
	c, err := Get(cfg.ConfigDSN)
	if err != nil {
		return nil, err
	}
	q, err := Get(cfg.QueueDSN)
	if err != nil {
		return nil, err
	}
	return NewHandOffs(c, q), nil
}
