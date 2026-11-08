package smppserver

// Incoming messages (MO) out to bound clients: the leg that claims and pushes
// them, and their records.

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/crypto"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/mo"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/queueflow"
	"github.com/yakugate/yakugate/lib/smpp/pdu"
)

// deliverClientMO drains staged MO for bound accounts and pushes each as a
// deliver_sm (esm_class=0, a real message: source=subscriber, dest=shortcode)
// on the owning client's live bind, then finalizes it into hot records. A job
// for an unbound account is left for the mo role's mo_url poster (stale
// claim). [SCOPE P6]
//
// 2026-07-26 rework (same as the DLR leg): tunable batch/interval, drain
// loop, and confirmation-gated finalize — the MO is finalized only after the
// client's deliver_sm_resp. A finalize FAILURE is neither acked nor sent
// again (18 Oct 2026, finishBoundMO): the MO stays on the queue, remembered
// as delivered, and its next claim writes its record. It used to be acked,
// and the record was lost.
//
// One client slow to answer holds only its own bind: since 28 Oct 2026 an MO
// goes through the bind's pump (pump.go), at most the bind's delivery window
// unanswered (the go-live test of 18 Oct, P6 E6, had a round wait at most
// 250 ms and a slow account sit out the next claims).
func (p *Process) deliverClientMO(ctx context.Context, log *logging.Logger) {
	p.initFreed()
	// Checked every YAKU_QUEUE_MO_EVERY_MS, busy or idle; one check takes at
	// most YAKU_QUEUE_MO_PER_INTERVAL (issue 7). Since 28 Oct 2026 an MO goes
	// to a bind's pump, within the bind's delivery window (pump.go), and the
	// leg claims as the binds have room — as the receipt leg does.
	chk := p.legCheck(p.moCheck)
	t := time.NewTicker(chk.Every)
	defer t.Stop()
	var answers sync.WaitGroup
	// A stopping leg waits for the answers it has out (each at most
	// respTimeout): the client has those MO, so they are recorded and acked
	// before the process goes (stopDrain).
	defer answers.Wait()
	budget := chk.PerInterval
	empty := map[int64]bool{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			budget, empty = chk.PerInterval, map[int64]bool{}
		case <-p.moFreed:
		}
		if p.queue == nil || p.hot == nil { // self-heal instead of permanent exit
			p.rebuildStores()
			continue
		}
		for budget > 0 && ctx.Err() == nil {
			handed := 0
			for _, acct := range p.binds.receiveAccounts() {
				if budget <= 0 || ctx.Err() != nil {
					break
				}
				if empty[acct] {
					continue
				}
				room := p.roomOf(acct)
				if room <= 0 {
					continue
				}
				claims := p.bindMOClaims([]int64{acct})
				if len(claims) == 0 {
					empty[acct] = true
					continue
				}
				n := min(room, budget, p.deliverBatch)
				jobs, err := p.claimMOPaced(ctx, claims, n)
				if err != nil || len(jobs) < n {
					empty[acct] = true
				}
				if err != nil {
					p.claimFailed(log, &p.moClaimSaid, "incoming messages", acct, err)
				}
				budget -= len(jobs)
				for _, m := range jobs {
					// Already on the client's bind, its record not yet written
					// (the record store was away): it is recorded, not sent a
					// second time (mosvc.Delivered).
					if p.moDelivered.Has(m.ID) {
						p.recordBoundMO(ctx, log, m)
						continue
					}
					// The account's limit of tries (config/0173): this claim
					// would be one more, so it is given up instead.
					if lim := p.moMostTries(m.AccountID); lim > 0 && m.Attempts > lim {
						p.giveUpMO(ctx, log, m, lim)
						continue
					}
					// MO has no originating bind — the subscriber sent it — so it
					// spreads across every receive-capable bind in turn (PO
					// 2026-09-05: "mo to be submitted randomly across the sessions").
					sess := p.binds.sessionAny(m.AccountID)
					if sess == nil {
						continue // not bound now; mo_url poster or a later bind takes it
					}
					// An encrypted company's MO is staged encrypted and opened
					// only here, for the wire (PO 2026-09-13). If it cannot be
					// opened it is not sent: the lease brings it back.
					body, err := p.moText(ctx, m.Body)
					if err != nil {
						log.Warn(p.acctScope(m.AccountID), "mo_decrypt_failed",
							"inbound message could not be decrypted for delivery; will retry",
							logging.Fields{"messageId": m.ID, "error": err.Error()})
						continue
					}
					m := m
					p.pumpOf(sess).push(delivery{
						seq: &p.moSeq,
						build: func(s *session, next func() uint32) [][]byte {
							return p.moPDUs(log, s, m, body, next)
						},
						// A multi-part MO is delivered whole or not at all: a
						// client that refuses or never answers any ONE part leaves
						// the whole message on the lease (a partial UDH group
						// cannot be reassembled on the handset).
						done: func(s *session, a answer) {
							// Every try is a row (mo_sub_, D-140): one the client
							// refused or never answered now, the one it took as the
							// MO's final row before it leaves the queue.
							try := bindTry(s, a)
							if !a.ok {
								p.tryLog().Note(mo.TryOf(m, try))
								return
							}
							try.TryID = dal.NewTryID()
							m.Transport = model.TransportSMPP // client delivery leg
							// Not cut by a stop: the client has the message (stopDrain).
							ctx := context.WithoutCancel(ctx)
							// The client HAS the message. Its record could not be
							// written: it is not acked and not sent again — it stays
							// on the queue, remembered as delivered with this try, and
							// its next claim writes the record and acks it
							// (recordBoundMO).
							p.moDelivered.NoteTry(m.ID, try)
							if !p.finishBoundMO(ctx, log, m, try) {
								return
							}
							s.pushedMO.Add(1)
							log.Info(p.acctScope(m.AccountID), "mo_delivered", "inbound message delivered to client",
								logging.Fields{"messageId": m.ID, "from": m.Sender, "to": m.Destination, "bindSlot": s.slot})
						},
					}, &answers)
					handed++
				}
			}
			if handed == 0 {
				break
			}
		}
	}
}

// moPDUs is an MO's deliver_sm for the bind it goes down: its body's real
// encoding (P6-B5), split into UDH parts over 254 octets (P6-B6), or one PDU
// with message_payload for a client that asked for it on a 3.4 bind (PO
// 2026-09-17).
func (p *Process) moPDUs(log *logging.Logger, sess *session, m model.Message, body string, next func() uint32) [][]byte {
	usePayload := sess.payloadTLV()
	var parts [][]byte
	var dc, esm byte
	var truncated bool
	if usePayload {
		var payload []byte
		payload, dc = moWirePayload(body, m.Encoding, sess.packedGSM7())
		parts = [][]byte{payload}
	} else {
		parts, dc, esm, truncated = moWirePartsFor(body, m.Encoding, p.moMaxParts, sess.packedGSM7())
	}
	if truncated {
		log.Warn(p.acctScope(m.AccountID), "mo_truncated", "inbound message truncated at the part cap",
			logging.Fields{"messageId": m.ID, "maxParts": p.moMaxParts, "encoding": m.Encoding})
	}
	out := make([][]byte, len(parts))
	for i, part := range parts {
		sq := next()
		if usePayload && len(part) > 254 {
			out[i] = pdu.DeliverSMWithPayloadPDU(sq, m.Sender, m.Destination, esm, dc, part)
		} else {
			out[i] = pdu.DeliverSMPDU(sq, m.Sender, m.Destination, esm, dc, part)
		}
	}
	return out
}

// finishBoundMO records an MO its client's bind confirmed and takes it off
// mo_queue. ok is false when that failed: the MO stays on the queue,
// remembered as delivered (p.moDelivered, noted by the caller before it), and
// its next claim writes the record (recordBoundMO) without sending it again.
//
// A failed record write used to ack the MO anyway — the client had it, and a
// second delivery was the worse lie — so the record was simply lost: with
// Redis queues, 59 MO in a 12 s MySQL cut were on no log and not charged
// (go-live test of 18 Oct 2026, P6 E11).
func (p *Process) finishBoundMO(ctx context.Context, log *logging.Logger, m model.Message, try dal.ClientTry) bool {
	if err := mo.Finalize(ctx, p.moRec(), p.queue, mo.Via(m, model.TransportSMPP), try); err != nil {
		log.Error(p.acctScope(m.AccountID), "mo_finalize_failed",
			"inbound message delivered to the client but its record could not be written — kept on the queue: its record is written when it is next claimed, and it is not delivered again",
			logging.Fields{"messageId": m.ID, "error": err.Error()})
		return false
	}
	p.moDelivered.Forget(m.ID)
	p.stats2.mo.Add(1)
	queueflow.Out(queueflow.MO, strconv.FormatInt(m.AccountID, 10), 1) // Monitoring: handed over on a bind
	return true
}

// recordBoundMO is the next claim of an MO a bind already confirmed: its
// record (the try that delivered it) and its ack, and no deliver_sm.
func (p *Process) recordBoundMO(ctx context.Context, log *logging.Logger, m model.Message) {
	m.Transport = model.TransportSMPP // client delivery leg
	try, ok := p.moDelivered.Try(m.ID)
	if !ok {
		try = dal.ClientTry{TryID: dal.NewTryID(), Channel: dal.TrySMPP, Result: dal.TryAccepted}
	}
	if p.finishBoundMO(ctx, log, m, try) {
		log.Info(p.acctScope(m.AccountID), "mo_recorded", "inbound message delivered earlier is now recorded",
			logging.Fields{"messageId": m.ID, "from": m.Sender, "to": m.Destination})
	}
}

// moText is an MO's text as the client must receive it: staged MO of an
// encrypted company is ciphertext, opened here. Plain text passes through.
func (p *Process) moText(ctx context.Context, body string) (string, error) {
	if !crypto.IsCiphertext(body) {
		return body, nil
	}
	if p.cry == nil {
		return "", errors.New("the message is encrypted and this process has no encryption")
	}
	return p.cry.Decrypt(ctx, body)
}

// giveUpMO takes an MO that has had its account's tries off the queue (marked
// expired, as the mo role's giving-up does) and writes its final row: no try,
// expired, MAX_ATTEMPTS_REACHED (mo.RecordOutcome).
func (p *Process) giveUpMO(ctx context.Context, log *logging.Logger, m model.Message, lim int) {
	g, ok := p.queue.(giveUpQueue)
	if !ok {
		return
	}
	if err := g.GiveUpMO(ctx, m.ID); err != nil {
		log.Warn(p.acctScope(m.AccountID), "mo_give_up_failed", "an incoming message past its account's tries could not be given up; it is tried again later",
			logging.Fields{"messageId": m.ID, "error": err.Error()})
		return
	}
	m.ErrorCode = "MAX_ATTEMPTS_REACHED"
	if err := mo.RecordOutcome(context.WithoutCancel(ctx), p.moRec(), m, model.StatusExpired); err != nil {
		log.Warn(p.acctScope(m.AccountID), "mo_give_up_record", "an incoming message given up could not be recorded as such",
			logging.Fields{"messageId": m.ID, "error": err.Error()})
	}
	log.Info(p.acctScope(m.AccountID), "mo_given_up", "incoming message given up: the account's limit of tries",
		logging.Fields{"messageId": m.ID, "attempts": m.Attempts, "limit": lim})
}
