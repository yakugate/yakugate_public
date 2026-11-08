package reroute

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/billing"
	dlrsvc "github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/receiptpolicy"
	"github.com/yakugate/yakugate/internal/sendplan"
	"github.com/yakugate/yakugate/internal/vendorquality"
)

// The re-route inbox (PO 2026-09-15: build the status-code actions).
//
// When an SMPP vendor answers a submit with a code mapped to failover,
// hlr_retry or pause_vendor, smpp-vendor cannot route the message itself —
// it holds no routing data — so it hands the message to this queue, marked
// with what was asked (Message.CodeReroute), and takes it off its own. The
// mover drains the inbox every look:
//
//   - failover:     routed again now, never back to the vendor that refused
//     it. No other route with a working vendor: it fails with that vendor's
//     code, and the customer gets a failed receipt (PO: the vendor already
//     said it cannot deliver it).
//   - hlr_retry:    the number's network is looked up again first (the
//     account's order without our stored answer, Rerouter.LookUpAgain). The
//     same network, or none: it fails like permanent. A new one: routed as
//     failover is.
//   - pause_vendor: the vendor was paused by smpp-vendor; the message goes to
//     the next route like everything else waiting for a paused vendor, or
//     back onto that vendor's queue to wait (and expire) when there is none.
//
// A message is sent another way by a code at most once: smpp-vendor fails a
// second failover or hlr_retry for a message that already carries one.

// InboxQueue is the queue store as the inbox needs it (*mysql.Store and the
// Redis runtime store both are).
type InboxQueue interface {
	Enqueue(ctx context.Context, m model.Message) error
	Claim(ctx context.Context, vendor string, n int) ([]model.Message, error)
	Ack(ctx context.Context, vendor string, msgIDs ...string) error
}

// Finisher writes a message's final record (*mysql.Store, the hot store).
type Finisher interface {
	Write(ctx context.Context, m model.Message) error
	AppendRawDLR(ctx context.Context, ref model.RecordRef, entry string) error
}

// ClientPusher tells the customer (dlr.QueuePusher).
type ClientPusher interface {
	PushToClient(ctx context.Context, accountID int64, r dlrsvc.Receipt) error
}

// inboxBatch bounds one look at the inbox.
const inboxBatch = 200

// inboxAgainAfter is how long a message the mover could not settle waits in
// the inbox before it is looked at again.
const inboxAgainAfter = 30 * time.Second

// SetInbox wires the re-route inbox. done and pusher may be nil: then a
// message that has to fail goes back in the inbox (looked at again after
// inboxAgainAfter) rather than being lost.
func (mv *Mover) SetInbox(q InboxQueue, done Finisher, pusher ClientPusher) {
	if mv == nil {
		return
	}
	mv.inbox, mv.done, mv.pusher = q, done, pusher
}

// Inbox is one look at the re-route inbox: it takes at most
// YAKU_QUEUE_REROUTE_PER_INTERVAL messages (issue 7; one claim of 200 before),
// inboxBatch at a time, and returns how many it settled (moved, failed or
// put back).
func (mv *Mover) Inbox(ctx context.Context) int {
	if mv == nil || mv.inbox == nil || mv.rr == nil {
		return 0
	}
	per := mv.check.PerInterval
	if per <= 0 {
		per = inboxBatch
	}
	n := 0
	for taken := 0; taken < per; {
		ask := min(inboxBatch, per-taken)
		msgs, err := mv.inbox.Claim(ctx, model.RerouteInbox, ask)
		if err != nil {
			mv.log.Printf("could not read the re-route inbox: %v", err)
			return n
		}
		taken += len(msgs)
		for _, m := range msgs {
			if mv.fromInbox(ctx, m) {
				n++
				continue
			}
			// Not settled now: back in the inbox for a moment (a take leaves
			// no lease, issue 22), as it came.
			m.NextAttemptAt = time.Now().Add(inboxAgainAfter)
			if err := mv.inbox.Enqueue(context.WithoutCancel(ctx), m); err != nil {
				mv.log.Printf("LOST: message %s could not be put back in the re-route inbox: %v", m.ID, err)
			}
		}
		if len(msgs) < ask {
			break
		}
	}
	return n
}

// answered is what the vendor answered, for the failure's sentence: its code
// and, when the vendor role sent them on, its own words (the HTTP status and
// body, the login's failure, the SMPP status in words — queue/0041). Saleem
// 2026-10-15: "from vendor_rejected i cant judge any thing".
func answered(code string, m model.Message) string {
	if m.ErrorText == "" || m.ErrorText == code {
		return code
	}
	return code + ": " + m.ErrorText
}

// fromInbox settles one message. false = it goes back in the inbox and is
// looked at again after inboxAgainAfter.
func (mv *Mover) fromInbox(ctx context.Context, m model.Message) bool {
	action, code, from := model.ParseRerouteAsk(m.CodeReroute)
	if from == "" {
		from = m.Vendor
	}
	m.Vendor = from
	lg := mv.log.ForVendor(from)

	if action == model.ActionNext {
		return mv.nextInRound(ctx, m, code, from)
	}

	if action == model.ActionHLRRetry {
		again, changed := mv.rr.LookUpAgain(ctx, m)
		if !changed {
			return mv.fail(ctx, m, code, fmt.Sprintf("%s answered %s (look the network up again); a new lookup found the same network or none", from, answered(code, m)))
		}
		lg.Infof("reroute", "message %s: network looked up again after %s's %s: %s-%s -> %s-%s (%s)",
			m.ID, from, code, m.MCC, m.MNC, again.MCC, again.MNC, again.HLRSource)
		m = again
	}

	out, why, ok := mv.route(ctx, from, m)
	if ok {
		out.CodeReroute = m.CodeReroute // it stays: a second such code fails it
		if err := mv.inbox.Enqueue(ctx, out); err != nil {
			lg.Warnf("reroute", "message %s: could not queue it for %s: %v (kept in the re-route inbox)", m.ID, out.Vendor, err)
			return false
		}
		mv.ack(ctx, m.ID)
		mv.inboxMoved.Add(1)
		lg.Infof("reroute", "message %s: %s answered %s (%s); sent via %s instead", m.ID, from, code, action, out.Vendor)
		return true
	}

	if action == model.ActionPauseVendor {
		// Waits for the vendor like everything else queued for it: the mover
		// moves it once another route has a working vendor, or it expires.
		if err := mv.inbox.Enqueue(ctx, m); err != nil {
			lg.Warnf("reroute", "message %s: could not put it back on %s: %v (kept in the re-route inbox)", m.ID, from, err)
			return false
		}
		mv.ack(ctx, m.ID)
		mv.inboxBack.Add(1)
		lg.Infof("reroute", "message %s: %s was paused by its code %s and no other route has a working vendor (%s); it waits on %s",
			m.ID, from, code, why, from)
		return true
	}
	return mv.fail(ctx, m, code, fmt.Sprintf("%s answered %s (%s) and no other route has a working vendor: %s", from, answered(code, m), action, why))
}

// nextInRound sends a message its vendor could not take now to the next
// vendor of its round: the rest of its level, then the backups, never a
// vendor already tried in this round. When the round is over it waits for the
// next one — the rule's backoff — starting again from Main, until the rule's
// retries run out; then it fails and the client gets its receipt (Saleem
// 2026-09-30: "a retry is once all tries fail to submit sms, then again goes
// to queues and comes back up to n numbers of retries along with backoff").
func (mv *Mover) nextInRound(ctx context.Context, m model.Message, code, from string) bool {
	lg := mv.log.ForVendor(from)
	tried := false
	for _, v := range m.TriedVendors {
		tried = tried || v == from
	}
	if !tried {
		m.TriedVendors = append(m.TriedVendors, from)
	}
	m.CodeReroute = "" // not a status-code action: a later failover code may still move it
	if out, _, ok := mv.route(ctx, from, m); ok {
		out.Attempts = 0
		if err := mv.inbox.Enqueue(ctx, out); err != nil {
			lg.Warnf("reroute", "message %s: could not queue it for %s: %v (kept in the re-route inbox)", m.ID, out.Vendor, err)
			return false
		}
		mv.ack(ctx, m.ID)
		mv.inboxMoved.Add(1)
		lg.Infof("reroute", "message %s: %s could not take it now (%s); next in round %d: %s (%s)",
			m.ID, from, code, m.SendRound+1, out.Vendor, sendplan.LevelName(out.RouteLevel))
		return true
	}
	retry := mv.rr.RetryFor(m)
	m.SendRound++
	if m.SendRound > retry.Rounds {
		// The vendor's answer first: it is what the reader is looking for.
		return mv.fail(ctx, m, receiptpolicy.CodeMaxAttempts, fmt.Sprintf("%s answered %s; round %d failed and its rule allows %d retries",
			from, answered(code, m), m.SendRound, retry.Rounds))
	}
	out, why, ok := mv.routeFresh(ctx, from, m)
	if !ok {
		if why == "NO_ROUTE" || why == "no other route with a working vendor" {
			return mv.fail(ctx, m, code, fmt.Sprintf("%s answered %s; round %d failed and no rule of the account catches it any more",
				from, answered(code, m), m.SendRound))
		}
		lg.Warnf("reroute", "message %s: round %d failed; could not start the next one: %s (kept in the re-route inbox)", m.ID, m.SendRound, why)
		return false
	}
	wait := retry.Wait(m.SendRound)
	out.SendRound, out.TriedVendors, out.Attempts = m.SendRound, nil, 0
	out.NextAttemptAt = time.Now().Add(wait)
	if err := mv.inbox.Enqueue(ctx, out); err != nil {
		lg.Warnf("reroute", "message %s: could not queue its next round on %s: %v (kept in the re-route inbox)", m.ID, out.Vendor, err)
		return false
	}
	mv.ack(ctx, m.ID)
	mv.inboxBack.Add(1)
	lg.Infof("reroute", "message %s: round %d failed (last: %s answered %s); round %d of %d starts in %s on %s",
		m.ID, m.SendRound, from, answered(code, m), m.SendRound+1, retry.Rounds+1, wait, out.Vendor)
	return true
}

// maxWhy is the longest failure sentence kept (dlr_.raw, mt_.error_text).
const maxWhy = 512

// fail records the message as failed with the vendor's code and tells the
// customer, as a vendor's permanent refusal is.
//
// What the customer is told (issue 23): with MAX_ATTEMPTS_REACHED (the
// rule's retry rounds ran out) the account's rule for that code, else the
// gateway default's, else UNDELIV; with a vendor's code (failover or
// hlr_retry with nowhere else to go, no route left) the account's refusal
// rules as the vendor roles apply them, else the default's, else REJECTD
// with the vendor's code. The record keeps the truth: failed, and the code.
func (mv *Mover) fail(ctx context.Context, m model.Message, code, why string) bool {
	lg := mv.log.ForVendor(m.Vendor)
	if mv.done == nil {
		lg.Warnf("reroute", "message %s should fail (%s) but this process cannot write records; kept in the re-route inbox", m.ID, why)
		return false
	}
	if r := []rune(why); len(r) > maxWhy { // a receipt row's raw and the record's words hold 512
		why = string(r[:maxWhy-1]) + "…"
	}
	// No vendor took it: its record gets its price from its quote, and a
	// message accepted before the billing queue its acceptance money.
	note := mv.bill.Unsent(ctx, &m)
	m.Status = model.StatusFailed
	m.ErrorCode = code
	m.ErrorText = why // the record keeps why (hot/0064)
	// What the client is told, decided before the write so the record files
	// a policy's answer beside the truth (told_*).
	ans := dlrsvc.GatewayAnswer(mv.policies.For(m.AccountID), code)
	ans.Stamp(&m)
	if err := mv.done.Write(ctx, m); err != nil && !strings.Contains(err.Error(), "Duplicate entry") {
		lg.Warnf("reroute", "message %s: could not write its failed record: %v (kept in the re-route inbox)", m.ID, err)
		return false
	}
	// The record is closed by the gateway's own receipt (source system) and
	// keeps why itself (the three-table design, 27 Oct 2026).
	// A policy's answer has its receipt row: what the client was told.
	if e := ans.Entry(); e != "" {
		if err := mv.done.AppendRawDLR(ctx, m.Ref(), e); err != nil {
			lg.Warnf("reroute", "message %s: could not record what its client was told: %v", m.ID, err)
		}
	}
	mv.ack(ctx, m.ID)
	mv.inboxFailed.Add(1)
	vendorquality.Note(m.VendorID, m.MCC, m.MNC, "failed")
	lg.Infof("reroute", "message %s failed: %s", m.ID, why)
	if mv.pusher != nil && ans.Wanted(m, mv.policies.LevelsFor(m.AccountID)) {
		if err := mv.pusher.PushToClient(ctx, m.AccountID, ans.Receipt(m)); err != nil {
			lg.Warnf("reroute", "message %s: failed receipt not queued: %v", m.ID, err)
		} else if ans.Final() {
			// The client's first final: its receipt fee when the message was
			// charged (billing.FinalEvents). Booked on the truth, whatever
			// word the client was told; a policy's progress word in place
			// of the failure books nothing.
			mv.bill.Final(ctx, billing.Final{MsgID: m.ID, Company: m.CompanyID, Account: m.AccountID, Vendor: m.VendorID,
				Note: note, Status: string(model.StatusFailed), ToClient: true})
		}
	}
	return true
}

func (mv *Mover) ack(ctx context.Context, id string) {
	if err := mv.inbox.Ack(ctx, model.RerouteInbox, id); err != nil {
		mv.log.Printf("message %s: could not take it out of the re-route inbox: %v", id, err)
	}
}
