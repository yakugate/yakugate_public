package mssql

import (
	"context"
	"database/sql"
	"sort"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// CampaignWindow is the live campaign window in the config store
// (dal.CampaignWindow) for an install with no Redis: campaign_counts, one
// row per signature, bucket and account (mirrors
// internal/dal/mysql/campaign.go). internal/campaignwin flushes once a
// second, so this is one statement a second a process (500 rows a
// statement), never one a message.
type CampaignWindow struct{ db *sql.DB }

// NewCampaignWindow keeps the window on the config store's pool.
func NewCampaignWindow(db *sql.DB) *CampaignWindow { return &CampaignWindow{db: db} }

// campaignRow is a row of dbo.campaign_count_rows, the table type the flush
// is sent as (its columns in this order).
type campaignRow struct {
	Hash      []byte
	Bucket    int64
	Account   int64
	N         int64
	ExpiresAt int64
}

var (
	// campaign.add: an ended row starts again from the new count, and every
	// row written is kept for `keep` again.
	stCampaignAdd = AddUpsertBatch(CounterStmts, "campaign.add", UpsertSpec{
		Table: "dbo.campaign_counts", Keys: []string{"hash", "bucket", "account"},
		Set: []string{
			"n = CASE WHEN t.expires_at <= @now THEN $n ELSE t.n + $n END",
			"expires_at = $expires_at",
		},
		Type: "dbo.campaign_count_rows",
	}, true)
	// campaign.read is written with its bucket list at the call.
	stCampaignRead = CounterStmts.Add("campaign.read", `
		SELECT bucket, account, n FROM dbo.campaign_counts WHERE hash = @hash AND bucket IN (@in) AND expires_at > @now`, false)
)

type campaignKey struct {
	hash            string
	bucket, account int64
}

// AddCampaignCounts adds a flush, 500 rows a statement, the rows in key
// order.
func (w *CampaignWindow) AddCampaignCounts(ctx context.Context, counts []dal.CampaignCount, keep time.Duration) error {
	sum := map[campaignKey]int64{}
	for _, c := range counts {
		if c.Hash == "" || len(c.Hash) > 64 {
			continue
		}
		sum[campaignKey{c.Hash, c.Bucket, c.Account}] += int64(c.N)
	}
	keys := make([]campaignKey, 0, len(sum))
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
	for from := 0; from < len(keys); from += counterRowsPerStatement {
		part := keys[from:min(from+counterRowsPerStatement, len(keys))]
		rows := make([]campaignRow, len(part))
		for i, k := range part {
			rows[i] = campaignRow{[]byte(k.hash), k.bucket, k.account, sum[k], ends}
		}
		if err := Again(ctx, counterTries, func() error {
			return upsertRows(ctx, w.db, stCampaignAdd, rows, sql.Named("now", now))
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
	list, args := namedList("b", buckets)
	args = append(args, sql.Named("hash", []byte(hash)), sql.Named("now", time.Now().Unix()))
	rows, err := w.db.QueryContext(ctx, withList(stCampaignRead.SQL, list), args...)
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
	observe(stCampaignRead, start, n)
	return out, nil
}
