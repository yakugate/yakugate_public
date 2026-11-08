package billing

import (
	"context"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
)

// DBReserver adapts the package-level Reserve/Refund functions to satisfy
// internal/pipeline.Reserver's method interface (structural typing — this
// package deliberately does not import internal/pipeline, keeping the
// dependency direction one-way: every ingress edge imports billing, not the
// other way around). Shared by every process that wires
// pipeline.SubmitDeps.Reserve (httpserver, smppserver). [2026-07-23]
type DBReserver struct {
	DB dal.CompanyBalances
}

// Reserve atomically deducts amount from the company's balance IF AND ONLY IF
// the balance can cover it, in ONE statement (dal.CompanyBalances
// HoldIfCovered): the WHERE guard and the deduction happen together, so the
// row lock makes this correct across any number of http-server/smpp-server
// instances with no separate coordination service. false = insufficient
// balance — not an error, the normal "account is broke" outcome.
//
// Deliberately minimal (PO decision 2026-07-23, simplified down from an
// earlier transaction+ledger version that cost 5-7 round trips per call):
// no transaction wrapper (a single UPDATE is already atomic), no
// balance_history row, no idempotency guard. companyID/accountID are passed
// in by the caller (already known from the cached Account, refdata.Mem) so
// this never has to look them up itself — one round trip, full stop.
func (r DBReserver) Reserve(ctx context.Context, accountID, companyID int64, amount float64) (bool, error) {
	// Post-Hadal there is no account-level balance: an unlinked account has
	// nothing to reserve against (and cannot be prepaid), so the reservation
	// trivially succeeds. The pipeline only calls Reserve for Prepaid()
	// accounts, which are company-linked by the loader's own resolution —
	// this branch is a safety net, not a live path. [S9-01, 2026-08-10]
	if companyID == 0 {
		return true, nil
	}
	ok, err := r.DB.HoldIfCovered(ctx, companyID, amount)
	if err != nil {
		return false, onStore(StoreSQL, "balance", err)
	}
	return ok, nil
}

// Refund reverses a Reserve() that turned out not to be needed — the message
// was reserved for but then failed a LATER pipeline step (encryption,
// durable enqueue write) and was never actually queued. Symmetric to
// Reserve: one statement, no ledger, no guard needed (adding back can't fail
// the way deducting can). [2026-07-23]
func (r DBReserver) Refund(ctx context.Context, accountID, companyID int64, amount float64) error {
	if companyID == 0 {
		return nil // nothing was reserved (see Reserve) — nothing to give back
	}
	return r.DB.GiveBack(ctx, companyID, amount)
}

// ReserveFor is Reserve plus the ledger row (PO 2026-09-17: the reservation
// IS the charge, taken at the exact price and recorded under the message's
// id so the billing role's duplicate check finds it and takes nothing
// twice). One transaction: lock the company row, refuse below zero, debit,
// append the commit row.
func (r DBReserver) ReserveFor(ctx context.Context, accountID, companyID int64, amount float64, msgID string) (bool, error) {
	ok, _, err := r.ReserveForBalance(ctx, accountID, companyID, amount, msgID)
	return ok, err
}

// ReserveForBalance is ReserveFor that also says the company's balance right
// after this hold, read under the same row lock (nil when nothing was held).
// The send reply shows it as remainingBalance: the reference cache's copy of
// the balance is minutes old, so "cached balance minus this price" showed a
// figure the customer never had (QA API-004, BILL-005).
//
// The amount is taken in whole ledger units (RoundLedger) and every figure is
// written as an exact decimal, as the billing role writes its charges: a
// float handed to the DECIMAL column is rounded by MySQL's own rule, which
// differs from Go's at a half (0.00015 is stored 0.0002, Go and the live
// counter take 0.0001), so the row, the balance and the reply could disagree
// by a unit.
func (r DBReserver) ReserveForBalance(ctx context.Context, accountID, companyID int64, amount float64, msgID string) (bool, *float64, error) {
	amt := toUnits(amount)
	if companyID == 0 || amt <= 0 {
		return true, nil, nil
	}
	// One transaction (dal.CompanyBalances HoldUnits): lock the company row,
	// refuse below zero, debit, and — under LedgerPerMessage only — append
	// the commit row. No row for the message (Saleem 2026-10-13): the
	// message itself says what was taken (model.Message.Quote, Held).
	// LedgerPerMessage keeps the old row for the length of an upgrade.
	ok, left, err := r.DB.HoldUnits(ctx, dal.Hold{Account: accountID, Company: companyID, Units: amt,
		Reference: msgID, Reason: ReasonAtAcceptance}, LedgerPerMessage())
	if err != nil {
		return false, nil, onStore(StoreSQL, "balance", err)
	}
	if !ok {
		return false, nil, nil
	}
	after := float64(left) / moneyScale
	return true, &after, nil
}

// SQLReserver is the accept path's reserver on a gateway with no Redis (plan
// of 29 Oct 2026, step 3): DBReserver, plus the billing queue's events
// (pipeline.EventBooker) booked on the SQL database's balance book. What the
// Redis reserver does, it does: the hold covers the message AND its lookup
// fee (the accept events), refused 402 when the balance would go below zero;
// a postpaid message's events move nothing; a message that never reached its
// queue is given back. Each is ONE transaction: the company's row locked,
// the balance moved, the events written (billing_events), committed — so a
// hold and its events stand or fall together (the book commits a process's
// calls together under load: dal/mysql billing_events.go). Later moments (submit,
// delivery, receipts, MO, a refusal's give-back) are billing.Moments on the
// same book.
type SQLReserver struct {
	DBReserver
	book *Booker
}

// NewSQLReserver is the SQL reserver of b (dal.OpenBilling with no Redis);
// nil when b has no balance book (its DAL keeps no billing queue): the
// caller keeps DBReserver alone, as before.
func NewSQLReserver(b *dal.Billing) *SQLReserver {
	if b == nil || b.Store == nil || b.Live != nil || b.Book == nil || b.Queue == nil {
		return nil
	}
	return &SQLReserver{DBReserver: DBReserver{DB: b.Store},
		book: NewSQLBooker(b.Book, b.Queue, deployment.Owner(), LanesFromEnv())}
}

// Booker is this reserver's booker.
func (r *SQLReserver) Booker() *Booker { return r.book }

// TakeEvents is the accept path's hold with its events: what they take from
// a prepaid balance (the message and its lookup) is taken with the events
// written, refused (false) when the balance after would be below zero.
func (r *SQLReserver) TakeEvents(ctx context.Context, companyID int64, evs []Event) (bool, *float64, error) {
	o, left, err := r.book.Take(ctx, companyID, evs)
	if err != nil || o == Short {
		return false, nil, err
	}
	if left < 0 {
		return true, nil, nil
	}
	after := float64(left) / moneyScale
	return true, &after, nil
}

// BookEvents writes events that move no balance (postpaid).
func (r *SQLReserver) BookEvents(ctx context.Context, companyID int64, evs []Event) error {
	return r.book.Book(ctx, companyID, evs)
}

// GiveBackEvents books the opposite of evs: the balance back and the
// negated events, one transaction.
func (r *SQLReserver) GiveBackEvents(ctx context.Context, companyID int64, evs []Event) error {
	return r.book.GiveBack(ctx, companyID, evs)
}

// Balance is a company's balance as it stands now (companies.balance).
func (r DBReserver) Balance(ctx context.Context, companyID int64) (float64, error) {
	return r.DB.Balance(ctx, companyID)
}

// BalanceCache answers a company's balance from MySQL at most once per
// company per TTL (YAKU_BALANCE_REPLY_CACHE_MS, 2000; 0 = every time).
//
// It is for the figure a send reply shows, not for a decision. The reply's
// "remaining balance" of a company with no live counter in Redis — a
// postpaid company, or one charged at submit or delivery — was read from
// companies.balance for EVERY accepted HTTP message: the one MySQL read left
// on the accept path (2026-10-13). Such a balance moves when the billing
// role settles a batch, seconds after the message, so a figure two seconds
// old says the same thing.
//
// The Redis key is deliberately NOT made here on a miss: the write-back
// copies every live key over MySQL, so a key made by a read would let Redis
// overwrite a balance that only MySQL had changed.
type BalanceCache struct {
	DB  dal.CompanyBalances
	TTL time.Duration

	mu sync.Mutex
	m  map[int64]cachedBalance
}

type cachedBalance struct {
	v  float64
	at time.Time
}

// NewBalanceCache reads its TTL from YAKU_BALANCE_REPLY_CACHE_MS.
func NewBalanceCache(db dal.CompanyBalances) *BalanceCache {
	ttl := 2000
	if n, err := strconv.Atoi(strings.TrimSpace(os.Getenv("YAKU_BALANCE_REPLY_CACHE_MS"))); err == nil && n >= 0 {
		ttl = n
	}
	return &BalanceCache{DB: db, TTL: time.Duration(ttl) * time.Millisecond}
}

// Balance is the company's balance, read now or within the last TTL. A read
// that fails is not kept.
func (c *BalanceCache) Balance(ctx context.Context, companyID int64) (float64, error) {
	if c.TTL > 0 {
		c.mu.Lock()
		e, ok := c.m[companyID]
		c.mu.Unlock()
		if ok && time.Since(e.at) < c.TTL {
			return e.v, nil
		}
	}
	b, err := DBReserver{DB: c.DB}.Balance(ctx, companyID)
	if err != nil || c.TTL <= 0 {
		return b, err
	}
	c.mu.Lock()
	if c.m == nil {
		c.m = map[int64]cachedBalance{}
	}
	c.m[companyID] = cachedBalance{v: b, at: time.Now()}
	c.mu.Unlock()
	return b, nil
}

// RefundFor gives a reservation back when the message never reached the
// queue, and says so in the ledger (type refund, reference refund:<id>).
func (r DBReserver) RefundFor(ctx context.Context, accountID, companyID int64, amount float64, msgID string) error {
	if companyID == 0 || amount <= 0 {
		return nil
	}
	// One transaction (dal.CompanyBalances GiveBackUnits), in units as
	// ReserveForBalance took it; the row only under LedgerPerMessage.
	return r.DB.GiveBackUnits(ctx, dal.Hold{Account: accountID, Company: companyID, Units: toUnits(amount),
		Reference: "refund:" + msgID}, LedgerPerMessage())
}

// LedgerPerMessage says whether the accept path still writes a row to
// balance_history for each message it takes money for (YAKU_LEDGER_PER_MESSAGE=1).
//
// Off by default (Saleem 2026-10-13: "we don't need to record per message"):
// the message carries what was taken (its Quote), and the billing role reads
// that. It is for the length of an upgrade only: a billing role of an EARLIER
// build knows an acceptance charge only by that row, and would charge a
// message of this build a second time. While any deployment still runs the
// earlier build, set it on the upgraded ones; remove it when all are upgraded.
func LedgerPerMessage() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("YAKU_LEDGER_PER_MESSAGE"))) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}
