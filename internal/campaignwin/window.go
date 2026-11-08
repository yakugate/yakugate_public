// Package campaignwin keeps the live campaign window in Redis (P10 register
// "Content-hash campaign signature", ruled 2026-09-09: "live window in Redis
// alongside the record column").
//
// Every accepted message carries its text's signature (abuse.CampaignHash)
// to its record, which answers "what else went out like this" for any day the
// hot store still holds. The window answers the live question — how much of
// it went out in the last hour and the last day, and from which accounts —
// without reading a single record: identical wording pushed through several
// accounts at once shows up here within a second.
//
// Counts live in 10-minute buckets, cw:<hash>:<bucket>, a hash of account id
// to messages, kept 25 hours. The accept path only adds to a map in memory;
// a background loop flushes it once a second in one pipeline, so a slow or
// missing Redis never delays a message. Investigation only: nothing gates on
// these numbers. The buckets are kept by the DAL (dal.CampaignWindow, moved
// into internal/dal/redis on 29 Oct 2026); the window's arithmetic is here.
package campaignwin

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

const (
	bucket  = 10 * time.Minute
	keep    = 25 * time.Hour
	maxHeld = 50000 // (hash, account) pairs held between flushes; beyond it new ones are dropped
)

type pair struct {
	hash string
	acct int64
}

// Window buffers and flushes counts, and reads them back.
type Window struct {
	w    dal.CampaignWindow
	logf func(string, ...any)
	mu   sync.Mutex
	held map[pair]int
	at   map[pair]int64 // the bucket each held count belongs to
	drop int
}

// New starts the flush loop; it stops with ctx.
func New(ctx context.Context, cw dal.CampaignWindow, logf func(string, ...any)) *Window {
	w := &Window{w: cw, logf: logf, held: map[pair]int{}, at: map[pair]int64{}}
	go w.loop(ctx)
	return w
}

// Note counts one accepted message against its signature.
func (w *Window) Note(hash string, accountID int64) {
	if w == nil || hash == "" {
		return
	}
	k := pair{hash, accountID}
	b := time.Now().Unix() / int64(bucket/time.Second)
	w.mu.Lock()
	if _, ok := w.held[k]; !ok && len(w.held) >= maxHeld {
		w.drop++
		w.mu.Unlock()
		return
	}
	// A pair held across a bucket boundary is flushed into the bucket of its
	// first message: at most a second out, which the window cannot notice.
	if _, ok := w.at[k]; !ok {
		w.at[k] = b
	}
	w.held[k]++
	w.mu.Unlock()
}

func (w *Window) loop(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			w.flush(context.Background())
			return
		case <-t.C:
			w.flush(ctx)
		}
	}
}

func (w *Window) flush(ctx context.Context) {
	w.mu.Lock()
	if len(w.held) == 0 {
		w.mu.Unlock()
		return
	}
	held, at, drop := w.held, w.at, w.drop
	w.held, w.at, w.drop = map[pair]int{}, map[pair]int64{}, 0
	w.mu.Unlock()

	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	counts := make([]dal.CampaignCount, 0, len(held))
	for k, n := range held {
		counts = append(counts, dal.CampaignCount{Hash: k.hash, Bucket: at[k], Account: k.acct, N: n})
	}
	if err := w.w.AddCampaignCounts(cctx, counts, keep); err != nil && w.logf != nil {
		w.logf("campaign window: flush of %d counts lost: %v", len(held), err)
	}
	if drop > 0 && w.logf != nil {
		w.logf("campaign window: %d counts dropped (more than %d campaigns and accounts between flushes)", drop, maxHeld)
	}
}

// Account is one account's share of a campaign.
type Account struct {
	ID       int64 `json:"accountId"`
	LastHour int   `json:"lastHour"`
	LastDay  int   `json:"lastDay"`
}

// Summary is what the window knows about one signature.
type Summary struct {
	Hash     string    `json:"hash"`
	LastHour int       `json:"lastHour"` // the last six 10-minute buckets, this one included
	LastDay  int       `json:"lastDay"`  // the last 24 hours of buckets
	Accounts []Account `json:"accounts"` // busiest first
}

// Read sums the last day of buckets for one signature.
func Read(ctx context.Context, cw dal.CampaignWindow, hash string, now time.Time) (Summary, error) {
	out := Summary{Hash: hash, Accounts: []Account{}}
	if _, err := hex.DecodeString(hash); err != nil || len(hash) != 64 {
		return out, fmt.Errorf("a campaign signature is 64 hex characters")
	}
	cur := now.Unix() / int64(bucket/time.Second)
	n := int64(24 * time.Hour / bucket)
	buckets := make([]int64, 0, n)
	for i := int64(0); i < n; i++ {
		buckets = append(buckets, cur-i)
	}
	got, err := cw.CampaignBuckets(ctx, hash, buckets)
	if err != nil {
		return out, err
	}
	byAcct := map[int64]*Account{}
	for i, m := range got {
		for id, cnt := range m {
			a := byAcct[id]
			if a == nil {
				a = &Account{ID: id}
				byAcct[id] = a
			}
			a.LastDay += cnt
			out.LastDay += cnt
			if i < 6 {
				a.LastHour += cnt
				out.LastHour += cnt
			}
		}
	}
	for _, a := range byAcct {
		out.Accounts = append(out.Accounts, *a)
	}
	sort.Slice(out.Accounts, func(i, j int) bool {
		if out.Accounts[i].LastDay != out.Accounts[j].LastDay {
			return out.Accounts[i].LastDay > out.Accounts[j].LastDay
		}
		return out.Accounts[i].ID < out.Accounts[j].ID
	})
	return out, nil
}
