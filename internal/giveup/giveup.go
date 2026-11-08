// Package giveup refuses an outgoing message at the moment a vendor role
// takes it from its queue (issue 22 of the design audit, Saleem and the PO,
// 23–26 Oct 2026): one whose validity ended (VALIDITY_EXPIRED) or whose tries
// ran out (MAX_ATTEMPTS_REACHED) is not sent. It replaces the expiry role and
// the retry role's sweep, which looked for such messages every half second.
//
// A refused message:
//   - keeps its mt_ row from acceptance and is closed by the gateway's own
//     receipt (dlr_, source system) with the code (the three-table design,
//     27 Oct 2026);
//   - is not charged: what was taken for it at acceptance is given back
//     (billing.Moments.GiveBackRefused), and its receipt is free;
//   - tells its client what its account's receipt policy says for the code,
//     else the gateway default's, else EXPIRED / UNDELIV (dlr.GatewayAnswer,
//     issue 23), when the client asked for a receipt;
//   - counts against the vendor it waited for (vendorquality).
//
// A record that cannot be written puts the message back on its queue for a
// little later, so nothing is lost while the record store is away.
package giveup

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/billing"
	dlrsvc "github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/receiptpolicy"
	"github.com/yakugate/yakugate/internal/vendorquality"
)

// Recorder writes a message's record and a receipt row beside it.
type Recorder interface {
	Write(ctx context.Context, m model.Message) error
	AppendRawDLR(ctx context.Context, ref model.RecordRef, entry string) error
}

// Deps is what refusing needs; a vendor role has all of it.
type Deps struct {
	Rec      Recorder
	Push     dlrsvc.ClientPusher                        // nil = no receipt is sent
	Bill     *billing.Moments                           // nil = nothing is booked
	Policies *receiptpolicy.Cache                       // nil = the standard words
	Requeue  func(context.Context, model.Message) error // the message's queue (Enqueue)
	Logf     func(string, ...any)
}

// writeAgainAfter is how long a message whose refusal could not be written
// waits on its queue before it is taken, and refused, again.
const writeAgainAfter = 30 * time.Second

// Sort splits messages just taken: those to send, those past their validity
// and those whose tries ran out (attempt at or over maxAttempts; 0 = no cap).
// Past its validity wins over out of tries.
func Sort(ms []model.Message, now time.Time, maxAttempts int) (send, expired, exhausted []model.Message) {
	for _, m := range ms {
		switch {
		case !m.ValidUntil.IsZero() && m.ValidUntil.Before(now):
			expired = append(expired, m)
		case maxAttempts > 0 && m.Attempts >= maxAttempts:
			exhausted = append(exhausted, m)
		default:
			send = append(send, m)
		}
	}
	return send, expired, exhausted
}

// Expired refuses a message whose validity ended before it was sent.
func (d Deps) Expired(ctx context.Context, m model.Message) {
	m.Status = model.StatusExpired
	// A vendor's answer to an earlier try stays the reason when there is one.
	if m.ErrorCode == "" {
		m.ErrorCode = receiptpolicy.CodeValidityExpired
	}
	if m.ErrorText == "" {
		m.ErrorText = ExpiredText(m)
	}
	d.refuse(ctx, m, receiptpolicy.CodeValidityExpired, "expired")
}

// Exhausted refuses a message whose tries ran out.
func (d Deps) Exhausted(ctx context.Context, m model.Message) {
	m.Status = model.StatusFailed
	m.ErrorCode = receiptpolicy.CodeMaxAttempts
	m.ErrorText = fmt.Sprintf("written off after %d send attempt(s): the most this message may have", m.Attempts)
	d.refuse(ctx, m, receiptpolicy.CodeMaxAttempts, "failed")
}

func (d Deps) refuse(ctx context.Context, m model.Message, code, verdict string) {
	if d.Bill != nil {
		d.Bill.Unsent(ctx, &m) // its price on the record, from its quote
	}
	ans := dlrsvc.GatewayAnswer(d.policies(m.AccountID), code)
	if err := d.Rec.Write(ctx, m); err != nil && !isDup(err) {
		d.logf("refused %s (%s) could not be written: %v — it waits %v on its queue and is refused again then", m.ID, code, err, writeAgainAfter)
		if d.Requeue != nil {
			m.NextAttemptAt = time.Now().Add(writeAgainAfter)
			if rerr := d.Requeue(ctx, m); rerr != nil {
				d.logf("LOST: refused %s could not be written nor put back on its queue: %v", m.ID, rerr)
			}
		}
		return
	}
	// What the client was told, by a rule, is a receipt row of its own (the
	// status pull answers it first).
	if e := ans.Entry(); e != "" {
		if err := d.Rec.AppendRawDLR(ctx, m.Ref(), e); err != nil {
			d.logf("record what %s was told: %v", m.ID, err)
		}
	}
	if d.Bill != nil {
		d.Bill.GiveBackRefused(ctx, m)
	}
	vendorquality.Note(m.VendorID, m.MCC, m.MNC, verdict)
	d.logf("refused msg=%s at the take: %s (attempts=%d)", m.ID, code, m.Attempts)
	if d.Push == nil || !ans.Wanted(m, d.levels(m.AccountID)) {
		return
	}
	if err := d.Push.PushToClient(ctx, m.AccountID, ans.Receipt(m)); err != nil {
		d.logf("receipt for refused %s: %v", m.ID, err)
	}
}

func (d Deps) policies(account int64) *receiptpolicy.Policy {
	if d.Policies == nil {
		return nil
	}
	return d.Policies.For(account)
}

func (d Deps) levels(account int64) string {
	if d.Policies == nil {
		return ""
	}
	return d.Policies.LevelsFor(account)
}

func (d Deps) logf(format string, args ...any) {
	if d.Logf != nil {
		d.Logf(format, args...)
	}
}

func isDup(err error) bool { return err != nil && strings.Contains(err.Error(), "Duplicate entry") }

// ExpiredText says why an expired message failed, for its record.
func ExpiredText(m model.Message) string {
	if m.ValidUntil.IsZero() {
		return "its validity ended before it could be sent"
	}
	return "its validity ended at " + m.ValidUntil.UTC().Format("2006-01-02 15:04:05") + " UTC, before it could be sent"
}

// Again counts one more try of a message its vendor did not take now (a "try
// again" answer, a call that failed, a submit never answered) and puts it
// back on its queue after its gap — or refuses it when its tries are used up
// (maxAttempts; 0 = no cap). A take leaves no lease (issue 22), so this is
// the only way such a message is sent again.
func (d Deps) Again(ctx context.Context, m model.Message, maxAttempts int) {
	ctx = context.WithoutCancel(ctx)
	m.Attempts++
	if maxAttempts > 0 && m.Attempts >= maxAttempts {
		d.Exhausted(ctx, m)
		return
	}
	m.NextAttemptAt = time.Now().Add(RetryGap(m.Attempts))
	d.put(ctx, m)
}

// HasTriesLeft says a message a vendor refused may be sent once more
// (Saleem, 27 Oct 2026: "if it has retries then retry else fails it"): its
// vendor has a cap (maxAttempts; 0 = none set, and a refusal is then final)
// and this try was not its last.
func HasTriesLeft(m model.Message, maxAttempts int) bool {
	return maxAttempts > 0 && m.Attempts+1 < maxAttempts
}

// Later puts a message back on its queue for `after`, its attempt as it was:
// one taken that could not be sent yet (a long message whose parts are not
// all there).
func (d Deps) Later(ctx context.Context, m model.Message, after time.Duration) {
	m.NextAttemptAt = time.Now().Add(after)
	d.put(context.WithoutCancel(ctx), m)
}

func (d Deps) put(ctx context.Context, m model.Message) {
	m.QueueItem = ""
	if d.Requeue == nil {
		d.logf("LOST: %s could not be put back on its queue: no queue", m.ID)
		return
	}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := d.Requeue(cctx, m); err != nil {
		d.logf("LOST: %s could not be put back on its queue: %v", m.ID, err)
	}
}

// RetryGap is how long a message waits after its try number `attempts`: 30 s
// after the first, doubling, at most 30 minutes — the old lease's backoff
// (D-027).
func RetryGap(attempts int) time.Duration {
	n := attempts - 1
	if n < 0 {
		n = 0
	}
	if n > 10 {
		n = 10
	}
	d := 30 * time.Second << n
	if d > 30*time.Minute {
		d = 30 * time.Minute
	}
	return d
}
