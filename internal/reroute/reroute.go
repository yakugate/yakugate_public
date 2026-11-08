// Package reroute moves messages waiting for a vendor that went down to the
// next route (PO 2026-09-14, routing rules review: "move immediately if there
// is any next route, else wait and expire").
//
// A vendor is down when an operator paused or disabled it, or when it has had
// no working connection for its down_after_sec (vendorhealth). Routing already
// sends NEW messages elsewhere; this takes the ones that were queued for it
// before it went down. Each is routed again the way a new message would be —
// same rules, same order, same loss check — never back onto the vendor it is
// leaving. A message with no other route stays where it is: it goes out when
// the vendor returns, or expires as it always would.
//
// Only messages ready to send move. One held by a sender, one scheduled for
// later, one in retry backoff, and the parts of a multi-part group all stay
// put; the first three are looked at again once they are due.
//
// It runs inside the routing processes (http-server, smpp-server), which
// already hold the routing data, the currency rates and the message keys.
// Several instances may run it at once: a message is moved only while it is
// still waiting and nobody holds it (one transaction on MySQL, one script on
// Redis), so two movers never move the same message.
package reroute

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/crypto"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/pipeline"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/queuecheck"
	"github.com/yakugate/yakugate/internal/receiptpolicy"
	"github.com/yakugate/yakugate/internal/refdata"
	"github.com/yakugate/yakugate/internal/vendorhealth"
)

// Queue is the part of a queue store the mover needs (*mysql.Store and the
// Redis runtime store both have it).
type Queue interface {
	PeekMovable(ctx context.Context, vendor string, after int64, n int) ([]model.Message, int64, error)
	MoveQueued(ctx context.Context, from string, m model.Message) (bool, error)
}

// Sealer opens and seals message text (*crypto.Service).
type Sealer interface {
	Decrypt(ctx context.Context, stored string) (string, error)
	Encrypt(ctx context.Context, accountID, companyID int64, plain string) (string, error)
}

// Every is how often the mover looks when YAKU_QUEUE_REROUTE_EVERY_MS says
// nothing (a Mover a test builds): at the down vendors' queues, then at the
// inbox. Down vendors are known from memory, so a look with nothing down
// costs nothing.
const Every = 2 * time.Second

// batch and maxBatches bound one look at one queue: 1,000 messages per vendor
// per look, so a very long queue drains over several looks without holding
// the loop.
const (
	batch      = 200
	maxBatches = 5
)

// Mover moves waiting messages off down vendors.
type Mover struct {
	q    Queue
	view *vendorhealth.View
	rr   *pipeline.Rerouter
	ref  *refdata.Mem
	seal Sealer
	log  *logging.Logger
	// check is how often the mover looks and how many messages one look takes
	// from the inbox (YAKU_QUEUE_REROUTE_*, internal/queuecheck; issue 7).
	check queuecheck.Check

	mu      sync.Mutex
	cursors map[string]int64
	// waitingNow is what the last full look at each down vendor left behind;
	// lastWaitLog paces the "these wait" line to once a minute per vendor.
	waitingNow  map[string]int
	lastWaitLog map[string]time.Time

	moved atomic.Int64

	// The re-route inbox (inbox.go): messages a vendor's status code sent
	// another way. nil = not wired.
	inbox  InboxQueue
	done   Finisher
	pusher ClientPusher
	// bill books the money of a message that fails here (billing.Moments:
	// its price on its record, its failure receipt's fee). nil = not booked.
	bill *billing.Moments
	// policies is the accounts' receipt policies with the gateway-wide
	// default (issue 23): what the client of a message that fails here is
	// told. nil = the standard word. Started by Run.
	policies    *receiptpolicy.Cache
	inboxMoved  atomic.Int64
	inboxFailed atomic.Int64
	inboxBack   atomic.Int64
}

// New wires a mover. seal may be nil (then a sealed message is left where it
// is: it cannot be routed without reading its text, and is never unsealed
// into plain text).
func New(q Queue, view *vendorhealth.View, rr *pipeline.Rerouter, ref *refdata.Mem, seal Sealer, log *logging.Logger) *Mover {
	if log == nil {
		log = logging.New("reroute")
	}
	return &Mover{q: q, view: view, rr: rr, ref: ref, seal: seal, log: log, cursors: map[string]int64{},
		waitingNow: map[string]int{}, lastWaitLog: map[string]time.Time{}, check: queuecheck.For(queuecheck.Reroute)}
}

// SetBilling wires the billing queue for the messages that fail here.
func (mv *Mover) SetBilling(b *billing.Moments) {
	if mv != nil {
		mv.bill = b
	}
}

// SetPolicies wires the receipt policies (a cache Run starts). nil = the
// standard word for every failure here.
func (mv *Mover) SetPolicies(c *receiptpolicy.Cache) {
	if mv != nil {
		mv.policies = c
	}
}

// Run looks every YAKU_QUEUE_REROUTE_EVERY_MS, busy or idle, until ctx ends.
func (mv *Mover) Run(ctx context.Context) {
	if mv == nil || mv.q == nil || mv.view == nil || mv.rr == nil {
		return
	}
	mv.policies.Start(ctx, 30*time.Second, mv.log.Printf)
	every := mv.check.Every
	if every <= 0 {
		every = Every
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		mv.Pass(ctx)
		mv.Inbox(ctx)
	}
}

// Stats are the mover's running totals, for monitoring.
func (mv *Mover) Stats() map[string]any {
	if mv == nil {
		return nil
	}
	mv.mu.Lock()
	waiting := make(map[string]int, len(mv.waitingNow))
	for k, n := range mv.waitingNow {
		waiting[k] = n
	}
	mv.mu.Unlock()
	return map[string]any{"moved_total": mv.moved.Load(), "waiting_by_vendor": waiting,
		"code_rerouted_total": mv.inboxMoved.Load(), "code_failed_total": mv.inboxFailed.Load(),
		"code_returned_total": mv.inboxBack.Load()}
}

// Pass looks once at every down vendor's queue and returns how many messages
// moved.
func (mv *Mover) Pass(ctx context.Context) int {
	total := 0
	for _, v := range mv.view.Movable() {
		if ctx.Err() != nil {
			break
		}
		// Paused or disabled by the view's last read: confirm with the
		// routing data too. Right after an operator switches a vendor back on,
		// the view can still say paused for a few seconds, and moving then
		// would take its first new messages away from a vendor that is on
		// (found live 2026-09-14). Both must say off.
		if v.Switch && mv.ref != nil {
			if on, known := mv.ref.VendorSwitchedOn(v.CID); known && on {
				continue
			}
		}
		total += mv.drain(ctx, v)
	}
	return total
}

// drain moves what it can from one vendor's queue.
func (mv *Mover) drain(ctx context.Context, v vendorhealth.Vendor) int {
	mv.mu.Lock()
	cur := mv.cursors[v.CID]
	mv.mu.Unlock()
	moved, waiting := 0, 0
	to := map[string]int{}
	whyWait := map[string]int{}
	for b := 0; b < maxBatches; b++ {
		msgs, next, err := mv.q.PeekMovable(ctx, v.CID, cur, batch)
		if err != nil {
			mv.log.ForVendor(v.CID).Warnf("reroute", "could not read the queue of down vendor %s: %v", v.CID, err)
			break
		}
		if len(msgs) == 0 {
			cur = 0 // end of the queue: the next look starts from the front again
			break
		}
		for _, m := range msgs {
			dst, why := mv.one(ctx, v.CID, m)
			if dst == "" {
				waiting++
				whyWait[why]++
				continue
			}
			moved++
			to[dst]++
		}
		cur = next
		if len(msgs) < batch {
			cur = 0
			break
		}
	}
	mv.moved.Add(int64(moved))
	sayWait := false
	mv.mu.Lock()
	mv.cursors[v.CID] = cur
	if cur == 0 { // a full look ended: what is left is what waits
		mv.waitingNow[v.CID] = waiting
		if waiting > 0 && time.Since(mv.lastWaitLog[v.CID]) >= time.Minute {
			mv.lastWaitLog[v.CID], sayWait = time.Now(), true
		}
	}
	mv.mu.Unlock()
	if moved > 0 {
		mv.log.ForVendor(v.CID).Infof("reroute", "moved %d waiting message(s) off %s (%s) to the next route: %v", moved, v.CID, v.Why, to)
	}
	if sayWait {
		// Once a minute per vendor, not per message: these wait for the
		// vendor to return, or expire.
		mv.log.ForVendor(v.CID).Infof("reroute", "%d message(s) wait for %s (%s): %v", waiting, v.CID, v.Why, whyWait)
	}
	return moved
}

// one routes and moves one message. It returns the vendor it moved to, or ""
// and why it stays.
func (mv *Mover) one(ctx context.Context, from string, m model.Message) (string, string) {
	out, why, ok := mv.route(ctx, from, m)
	if !ok {
		return "", why
	}
	moved, err := mv.q.MoveQueued(ctx, from, out)
	if err != nil {
		mv.log.ForVendor(from).Warnf("reroute", "move %s %s -> %s failed: %v (left where it was)", m.ID, from, out.Vendor, err)
		return "", "move failed"
	}
	if !moved {
		return "", "taken by a sender meanwhile"
	}
	return out.Vendor, ""
}

// route finds m's route now, never on `from`, with its text opened for the
// route stage and sealed again after. ok=false says why it cannot go.
func (mv *Mover) route(ctx context.Context, from string, m model.Message) (model.Message, string, bool) {
	return mv.routeWith(ctx, from, m, mv.rr.Reroute)
}

// routeFresh is route for the start of a new round: from Main, nothing
// excluded, waiting on Main when no vendor can send now.
func (mv *Mover) routeFresh(ctx context.Context, from string, m model.Message) (model.Message, string, bool) {
	return mv.routeWith(ctx, from, m, mv.rr.NextRound)
}

func (mv *Mover) routeWith(ctx context.Context, from string, m model.Message,
	how func(context.Context, model.Message, string, string) (model.Message, string, bool)) (model.Message, string, bool) {
	sealed := crypto.IsCiphertext(m.Body)
	plain, plainOrig := m.Body, m.OrigBody
	if sealed || crypto.IsCiphertext(m.OrigBody) {
		if mv.seal == nil {
			return m, "text is sealed and this process has no key", false
		}
		var err error
		if sealed {
			if plain, err = mv.seal.Decrypt(ctx, m.Body); err != nil {
				return m, "text could not be opened", false
			}
		}
		if m.OrigBody != "" && crypto.IsCiphertext(m.OrigBody) {
			if plainOrig, err = mv.seal.Decrypt(ctx, m.OrigBody); err != nil {
				return m, "text could not be opened", false
			}
		}
	}
	m.Vendor = from
	out, why, ok := how(ctx, m, plain, plainOrig)
	if !ok {
		return m, why, false
	}
	if sealed {
		// Sealed again before it is stored: a moved message is never kept in
		// plain text, and a failure to seal leaves it where it was.
		acct, found := mv.ref.AccountByID(m.AccountID)
		if !found {
			return m, "account not found", false
		}
		var err error
		if out.Body, err = mv.seal.Encrypt(ctx, m.AccountID, acct.CompanyID, out.Body); err != nil {
			return m, "text could not be sealed again", false
		}
		if out.OrigBody != "" {
			if out.OrigBody, err = mv.seal.Encrypt(ctx, m.AccountID, acct.CompanyID, out.OrigBody); err != nil {
				return m, "text could not be sealed again", false
			}
		}
	}
	return out, "", true
}
