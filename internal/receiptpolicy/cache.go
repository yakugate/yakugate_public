package receiptpolicy

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// Cache holds every account's policy in memory and refreshes it on a ticker,
// the same shape as internal/vendorstatus: the readers are on the hot path
// (every submit answer, every receipt) and must never query per message.
//
// The processes that read it — smpp-vendor, http-vendor, the dlr role, and
// for the gateway's own outcomes the expiry and retry roles and the
// http-server's re-route mover — each keep one loader.
//
// It also holds the gateway-wide default (issue 23): read on the same
// refresh from the config store's gateway_settings, where the gateway's other
// operator defaults live (the HLR order, config/0102), and joined under every
// account's policy (Policy.Over), so For answers with both.
type Cache struct {
	db dal.ReceiptPolicies

	mu     sync.RWMutex
	byAcct map[int64]*Policy // the account's own, with the default under it
	def    *Policy           // the gateway-wide default, nil = none
	bare   *Policy           // no rule of its own, the default under it: an account with no policy
	// levels is the account's dlr_levels (config/0120): which intermediate
	// receipts reach its clients. It rides this cache rather than a second
	// one because it is the same row, read on the same refresh, by the same
	// four processes.
	levels map[int64]string
	// eachPart: accounts whose clients get each part's final receipt of a
	// long message as it comes (part_receipts = 'each', config/0172).
	eachPart map[int64]bool
	timeouts []int64 // accounts a no_receipt rule applies to, for the dlr role's sweep
}

// DefaultSetting is the gateway_settings row that holds the default policy.
// No row = no default. The Admin API and the console write it.
const DefaultSetting = "receipt_policy_default"

// ReadDefault reads and checks the gateway-wide default. nil, nil = there is
// none (no row, or a config store from before gateway_settings existed). The
// one reading of it: the roles' cache and provision (the console) both call
// this, on any store that reads a gateway setting as text
// (dal.ReceiptPolicies, dal.ConfigStore).
func ReadDefault(ctx context.Context, db dal.DefaultReceiptPolicy) (*Policy, error) {
	if db == nil {
		return nil, nil
	}
	raw, err := db.GatewaySettingText(ctx, DefaultSetting)
	switch {
	case errors.Is(err, dal.ErrNoRows):
		return nil, nil
	case err != nil && db.MissingTable(err): // config/0102 not applied yet
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("default receipt policy: %w", err)
	}
	p, err := Parse([]byte(raw.String))
	if err != nil {
		return nil, fmt.Errorf("default receipt policy: %w", err)
	}
	return p, nil
}

// LevelsFor is an account's dlr_levels setting; "" for an account that has
// never changed it, which every caller reads as the default.
func (c *Cache) LevelsFor(accountID int64) string {
	if c == nil {
		return ""
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.levels[accountID]
}

// EachPartFor says the account's clients get each part's final receipt of a
// long message as it comes (part_receipts = 'each', Saleem 4 Nov 2026);
// false = one final, when the message closes (the default).
func (c *Cache) EachPartFor(accountID int64) bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.eachPart[accountID]
}

// New returns an empty cache bound to the CONFIG store (dal.EngineConfig's
// Policies). A nil db gives a cache that answers "no policy" to everything —
// what a non-MySQL config store gets, the same degradation as every other
// config-backed feature.
func New(db dal.ReceiptPolicies) *Cache {
	return &Cache{db: db, byAcct: map[int64]*Policy{}, levels: map[int64]string{}, eachPart: map[int64]bool{}}
}

// Reload replaces the cached policies.
//
// One account's broken policy is skipped and reported; the rest still load. A
// policy that cannot be parsed means that account's clients hear exactly what
// the vendor said, which is the safe direction: the gateway never invents an
// answer it could not read the rule for.
func (c *Cache) Reload(ctx context.Context) error {
	if c == nil || c.db == nil {
		return nil
	}
	// Only the accounts that have asked for something: on a gateway where
	// nobody uses either setting this reads no rows at all.
	rows, err := c.db.ReceiptPolicyRows(ctx)
	var cut dal.RowsError
	if errors.As(err, &cut) {
		return cut.Err // a row that would not scan, or a read cut short
	}
	if err != nil {
		return fmt.Errorf("receipt policies: %w", err)
	}
	byAcct := map[int64]*Policy{}
	levels := map[int64]string{}
	eachPart := map[int64]bool{}
	var timeouts []int64
	var bad []string
	for _, row := range rows {
		id, raw, lvl := row.AccountID, row.Policy, row.Levels
		if lvl != "" {
			levels[id] = lvl
		}
		if row.PartReceipts == "each" {
			eachPart[id] = true
		}
		p, err := Parse(raw)
		if err != nil {
			bad = append(bad, fmt.Sprintf("account %d: %v", id, err))
			continue
		}
		if p == nil {
			continue
		}
		byAcct[id] = p
		if p.NoReceipt != nil {
			timeouts = append(timeouts, id)
		}
	}
	// The default, read on the same refresh. One that cannot be read or
	// parsed is no default: the gateway never invents an answer it could not
	// read the rule for, and the accounts' own policies still apply.
	def, derr := ReadDefault(ctx, c.db)
	if derr != nil {
		bad = append(bad, derr.Error())
	}
	if def != nil {
		for id, p := range byAcct {
			byAcct[id] = p.Over(def)
		}
		if def.NoReceipt != nil {
			// The default's no-receipt rule answers for every account that has
			// none of its own: the sweep looks at every account.
			all, err := c.accountIDs(ctx)
			if err != nil {
				return err
			}
			timeouts = all
		}
	}
	sort.Slice(timeouts, func(i, j int) bool { return timeouts[i] < timeouts[j] })
	c.mu.Lock()
	c.byAcct, c.def, c.bare, c.levels, c.eachPart, c.timeouts = byAcct, def, (*Policy)(nil).Over(def), levels, eachPart, timeouts
	c.mu.Unlock()
	if len(bad) > 0 {
		return fmt.Errorf("receipt policy not applied for %d account(s) or default: %v", len(bad), bad)
	}
	return nil
}

// accountIDs lists every account, for a default with a no-receipt rule.
func (c *Cache) accountIDs(ctx context.Context) ([]int64, error) {
	out, err := c.db.AccountIDs(ctx)
	var cut dal.RowsError
	if errors.As(err, &cut) {
		return nil, cut.Err // a row that would not scan, or a read cut short
	}
	if err != nil {
		return nil, fmt.Errorf("receipt policies: accounts: %w", err)
	}
	return out, nil
}

// For returns an account's policy with the gateway-wide default under it,
// the default alone for an account with none of its own, or nil when there
// is neither. Safe on a nil cache, so a process without a config store needs
// no special case.
func (c *Cache) For(accountID int64) *Policy {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if p := c.byAcct[accountID]; p != nil {
		return p
	}
	return c.bare
}

// Default is the gateway-wide default policy, or nil.
func (c *Cache) Default() *Policy {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.def
}

// SetDefault puts a default in place by hand (tests, and a process that read
// it some other way); the next Reload reads the store's again.
func (c *Cache) SetDefault(def *Policy) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, p := range c.byAcct {
		c.byAcct[id] = p.Over(def)
	}
	c.def, c.bare = def, (*Policy)(nil).Over(def)
}

// Set puts one account's own policy in place by hand (tests); the default in
// place goes under it.
func (c *Cache) Set(accountID int64, p *Policy) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if p == nil {
		delete(c.byAcct, accountID)
		return
	}
	c.byAcct[accountID] = p.Over(c.def)
}

// Any reports whether ANY account has a policy, or the default can meet a
// receipt (Policy.OnReceiptPath: a default with rows on the gateway's own
// codes only cannot). The receipt path asks this once per receipt to skip the
// told-status lookup entirely on a gateway where nobody uses the feature —
// which is every gateway until someone sets one.
func (c *Cache) Any() bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.byAcct) > 0 || c.def.OnReceiptPath()
}

// TimeoutAccounts lists the accounts with a no_receipt rule, for the dlr
// role's sweep. Returns a copy: the caller iterates it while a refresh may be
// replacing the cache.
func (c *Cache) TimeoutAccounts() []int64 {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return append([]int64(nil), c.timeouts...)
}

// Start loads now and every interval until ctx ends. A failed refresh keeps
// the previous contents: losing the config store must not silently change
// what customers are being told.
func (c *Cache) Start(ctx context.Context, every time.Duration, logf func(string, ...any)) {
	if c == nil || c.db == nil {
		return
	}
	if err := c.Reload(ctx); err != nil && logf != nil {
		logf("%v", err)
	}
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := c.Reload(ctx); err != nil && logf != nil {
					logf("%v", err)
				}
			}
		}
	}()
}
