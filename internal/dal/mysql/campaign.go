package mysql

import (
	"context"
	"database/sql"
	"sort"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// CampaignWindow is the live campaign window in the config store
// (dal.CampaignWindow) for a gateway with no Redis: campaign_counts
// (config/0166), one row per signature, bucket and account — Redis's
// cw:<hash>:<bucket> with a field per account. internal/campaignwin buffers
// the counts in memory and flushes them once a second, so this is one
// statement a second a process (500 rows a statement), never one a message.
type CampaignWindow struct{ db *sql.DB }

// NewCampaignWindow keeps the window on the config store's pool.
func NewCampaignWindow(db *sql.DB) *CampaignWindow { return &CampaignWindow{db: db} }

var (
	// campaign.add is written with its rows repeated (rowsOf): an ended row
	// starts again from the new count, and every row written is kept for
	// `keep` again (Redis: EXPIRE on each bucket written). n is assigned
	// before expires_at, so its IF still sees the old end.
	stCampaignAdd = counterStmts.Add("campaign.add", `
		INSERT INTO campaign_counts (hash, bucket, account, n, expires_at) VALUES (?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		  n = IF(expires_at <= ?, VALUES(n), n + VALUES(n)),
		  expires_at = VALUES(expires_at)`, true)
	// campaign.read is written with its bucket list repeated.
	stCampaignRead = counterStmts.Add("campaign.read", `
		SELECT bucket, account, n FROM campaign_counts FORCE INDEX (PRIMARY) WHERE hash = ? AND bucket IN (?) AND expires_at > ?`, false)
)

type campaignRow struct {
	hash            string
	bucket, account int64
}

// AddCampaignCounts adds a flush: rows in key order (two processes lock the
// rows they share in the same order, so they never deadlock on them), 500 a
// statement.
func (w *CampaignWindow) AddCampaignCounts(ctx context.Context, counts []dal.CampaignCount, keep time.Duration) error {
	sum := map[campaignRow]int64{}
	for _, c := range counts {
		if c.Hash == "" || len(c.Hash) > 64 {
			continue
		}
		sum[campaignRow{c.Hash, c.Bucket, c.Account}] += int64(c.N)
	}
	keys := make([]campaignRow, 0, len(sum))
	for k := range sum {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.hash != b.hash {
			return a.hash < b.hash
		}
		if a.bucket != b.bucket {
			return a.bucket < b.bucket
		}
		return a.account < b.account
	})
	now := time.Now().Unix()
	ends := now + int64(max(keep, time.Second)/time.Second)
	head, tail := splitValues(stCampaignAdd.SQL)
	for from := 0; from < len(keys); from += rowsPerStatement {
		part := keys[from:min(from+rowsPerStatement, len(keys))]
		args := make([]any, 0, len(part)*5+1)
		for _, k := range part {
			args = append(args, k.hash, k.bucket, k.account, sum[k], ends)
		}
		args = append(args, now)
		q := head + rowsOf(5, len(part)) + tail
		if err := againIfVictimN(ctx, 4, func() error {
			_, err := execText(ctx, w.db, stCampaignAdd, q, args...)
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}

// CampaignBuckets reads one signature's buckets in one statement.
func (w *CampaignWindow) CampaignBuckets(ctx context.Context, hash string, buckets []int64) ([]map[int64]int, error) {
	out := make([]map[int64]int, len(buckets))
	at := make(map[int64][]int, len(buckets))
	for i, b := range buckets {
		out[i] = map[int64]int{}
		at[b] = append(at[b], i)
	}
	if len(buckets) == 0 {
		return out, nil
	}
	start := time.Now()
	args := make([]any, 0, len(buckets)+2)
	args = append(args, hash)
	for _, b := range buckets {
		args = append(args, b)
	}
	args = append(args, time.Now().Unix())
	q := inList(stCampaignRead.SQL, len(buckets))
	rows, err := w.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var n int64
	for rows.Next() {
		var b, acct, cnt int64
		if err := rows.Scan(&b, &acct, &cnt); err != nil {
			return nil, err
		}
		for _, i := range at[b] {
			out[i][acct] += int(cnt)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	stCampaignRead.Observe(time.Since(start), n)
	return out, nil
}

// splitValues cuts a registered multi-row INSERT at its one row of
// placeholders: the text before "VALUES (", and from the row's end on. The
// caller puts rowsOf(...) between them.
func splitValues(q string) (head, tail string) {
	i := strings.Index(q, "VALUES (")
	j := strings.Index(q[i:], ")")
	return q[:i+len("VALUES ")], q[i+j+1:]
}
