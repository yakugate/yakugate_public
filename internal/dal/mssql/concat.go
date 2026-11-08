package mssql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// The multi-part buffer in the queue database's concat_assembly (mirrors
// internal/dal/mysql/concat.go). MT and MO groups share the table; an MO
// group's key starts "mo|".

// ConcatStmts is the multi-part buffer's statements.
var ConcatStmts = newRegistry()

var _ = serves(func(d *dal.Driver) { d.ConcatBuffer = openConcatBuffer })

// openConcatBuffer opens the buffer on the queue database's one pool a
// process, when the queue DSN is a SQL Server one (the queues themselves on
// SQL Server, or in Redis beside an older SQL Server queue database).
func openConcatBuffer(cfg config.Config, mo bool) (dal.ConcatBuffer, error) {
	if !config.IsSQLServerDSN(cfg.QueueDSN) {
		return nil, fmt.Errorf("no SQL Server queue database (YAKU_QUEUE_DSN) to keep concat_assembly in")
	}
	db, err := Get(cfg.QueueDSN)
	if err != nil {
		return nil, err
	}
	return NewConcatBuffer(db, mo), nil
}

var (
	// concat.add keeps the part (a part sent again is kept once) and takes
	// the group's parts when they are total: the DELETE OUTPUTs them. MySQL's
	// insert, locking read and delete in one round trip. The parts of one
	// group are added one after the other, under an application lock named
	// for the group (held until the commit): two last parts of one group
	// never both see the group whole, and no range locks are taken, so two
	// groups never wait on each other (with range locks, parts arriving
	// together deadlocked on the index's gaps). The count is a locking read
	// (UPDLOCK), as MySQL's FOR UPDATE: it waits for a claim deleting the
	// group and then counts what is left.
	stBufAdd = ConcatStmts.Add("concat.add", `SET NOCOUNT ON; SET XACT_ABORT ON;
DECLARE @yk_lock INT, @yk_n INT, @yk_res NVARCHAR(255) = N'concat|' + @key;
BEGIN TRAN;
EXEC @yk_lock = sp_getapplock @Resource = @yk_res, @LockMode = 'Exclusive', @LockOwner = 'Transaction', @LockTimeout = 30000, @DbPrincipal = 'public';
IF @yk_lock < 0 THROW 51000, 'concat: the group''s lock was not given', 1;
IF NOT EXISTS (SELECT 1 FROM dbo.concat_assembly WHERE group_key = @key AND seq = @seq)
  INSERT INTO dbo.concat_assembly (account_id, group_key, seq, total, payload)
  VALUES (@acct, @key, @seq, @total, @payload);
SELECT @yk_n = COUNT(*) FROM dbo.concat_assembly WITH (UPDLOCK) WHERE group_key = @key;
IF @yk_n >= @total
  DELETE FROM dbo.concat_assembly OUTPUT deleted.seq, deleted.payload WHERE group_key = @key;
COMMIT;`, true)
	// concat.claim takes up to 100 groups whose oldest part has waited past
	// the timeout — a group has one when any of its parts is that old, so
	// the age index finds them — and deletes every part of each in one
	// transaction, OUTPUT to the caller. MySQL reads the keys, then claims
	// them one transaction a group.
	stBufClaimMT = ConcatStmts.Add("concat.claim_mt", concatClaimSQL("group_key NOT LIKE N'mo|%'"), false)
	stBufClaimMO = ConcatStmts.Add("concat.claim_mo", concatClaimSQL("group_key LIKE N'mo|%'"), false)
)

func concatClaimSQL(kind string) string {
	return `SET NOCOUNT ON; SET XACT_ABORT ON;
DECLARE @yk_keys TABLE (group_key NVARCHAR(191) NOT NULL PRIMARY KEY);
BEGIN TRAN;
INSERT INTO @yk_keys (group_key)
  SELECT DISTINCT TOP (100) group_key FROM dbo.concat_assembly
   WHERE received_at < DATEADD(SECOND, -@p1, SYSUTCDATETIME()) AND ` + kind + `;
DELETE c OUTPUT deleted.group_key, deleted.seq, deleted.payload
  FROM dbo.concat_assembly AS c INNER JOIN @yk_keys AS k ON k.group_key = c.group_key;
COMMIT;`
}

// ConcatBuffer buffers one side's parts in concat_assembly. Instance-safe:
// a group is taken under its locks and deleted as it is handed over.
type ConcatBuffer struct {
	db *sql.DB
	mo bool
}

var _ dal.ConcatBuffer = (*ConcatBuffer)(nil)

// NewConcatBuffer keeps one side's parts on db (the queue database).
func NewConcatBuffer(db *sql.DB, mo bool) *ConcatBuffer { return &ConcatBuffer{db: db, mo: mo} }

// concatPart is one part as a statement OUTPUTs it.
type concatPart struct {
	key     string
	seq     int
	payload string
}

// AddPart keeps one part and, when the group then has total parts, takes it
// whole: of two last parts at once, exactly one is given the group. A
// deadlock victim is run again (the part is kept once however often it is
// sent).
func (a *ConcatBuffer) AddPart(ctx context.Context, key string, accountID int64, seq, total int, payload string, _ time.Duration) ([]*model.Message, error) {
	var got []concatPart
	err := Again(ctx, 4, func() error {
		start := time.Now()
		rows, err := a.db.QueryContext(ctx, stBufAdd.SQL,
			sql.Named("acct", accountID), sql.Named("key", key), sql.Named("seq", int64(seq)),
			sql.Named("total", int64(total)), sql.Named("payload", payload))
		if err != nil {
			return err
		}
		got, err = concatScan(rows, key)
		if err == nil {
			observe(stBufAdd, start, int64(len(got)))
		}
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("buffer part: %w", err)
	}
	if len(got) == 0 {
		return nil, nil // still waiting
	}
	return concatMessages(got)
}

// ClaimExpired takes every group (at most 100 a call) whose OLDEST part has
// waited past the timeout, in one statement, and hands each to each in
// order of its parts. A group whose part cannot be read is handed over with
// that error and no parts (it is dropped, as the interface says; MySQL
// leaves it in the table and stops there).
func (a *ConcatBuffer) ClaimExpired(ctx context.Context, timeout time.Duration, each func(string, []*model.Message, error)) error {
	q := stBufClaimMT
	if a.mo {
		q = stBufClaimMO
	}
	var got []concatPart
	err := Again(ctx, 4, func() error {
		start := time.Now()
		rows, err := a.db.QueryContext(ctx, q.SQL, int32(timeout.Seconds()))
		if err != nil {
			return err
		}
		got, err = concatScan(rows, "")
		if err == nil {
			observe(q, start, int64(len(got)))
		}
		return err
	})
	if err != nil {
		return err
	}
	groups := map[string][]concatPart{}
	var keys []string
	for _, p := range got {
		if _, ok := groups[p.key]; !ok {
			keys = append(keys, p.key)
		}
		groups[p.key] = append(groups[p.key], p)
	}
	for _, k := range keys {
		parts, bad := concatMessages(groups[k])
		each(k, parts, bad)
	}
	return nil
}

// concatScan reads the parts a statement OUTPUT: (seq, payload), or (key,
// seq, payload) when key is "".
func concatScan(rows *sql.Rows, key string) ([]concatPart, error) {
	defer rows.Close()
	var out []concatPart
	for rows.Next() {
		p := concatPart{key: key}
		var err error
		if key == "" {
			err = rows.Scan(&p.key, &p.seq, &p.payload)
		} else {
			err = rows.Scan(&p.seq, &p.payload)
		}
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// concatMessages is a group's parts in seq order.
func concatMessages(parts []concatPart) ([]*model.Message, error) {
	sort.Slice(parts, func(i, j int) bool { return parts[i].seq < parts[j].seq })
	out := make([]*model.Message, 0, len(parts))
	for _, p := range parts {
		var m model.Message
		if err := json.Unmarshal([]byte(p.payload), &m); err != nil {
			return nil, err
		}
		out = append(out, &m)
	}
	return out, nil
}
