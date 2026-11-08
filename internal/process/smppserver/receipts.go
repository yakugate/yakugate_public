package smppserver

// Receipts out to bound clients: the leg that claims and pushes them, their
// acks, and the receipt's deliver_sm.

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/queueflow"
	"github.com/yakugate/yakugate/lib/smpp/pdu"
)

// deliverClientDLRs drains the client_dlr queue and pushes each receipt as a
// deliver_sm on the owning client's live RX/TRX bind (the smsc leg, D-022).
// A job for an unbound account is left for a later attempt. [SCOPE P5]
//
// A job is acked only after the client's deliver_sm_resp confirms receipt
// (flush ≠ delivery). Answered receipts are taken off the queue together,
// every ackEvery: one at a time it was two round trips to the queue store for
// every receipt (2026-10-13).
//
// Since 28 Oct 2026 (Saleem, production tw-1: receipts piled up to 95,000 and
// went out at 1–19 a second while the client submitted on ten of its binds)
// each bind has a delivery window (pump.go): the leg claims, for each account,
// as many receipts as its binds have room for, hands each to the bind its
// message goes down (sessionForMessage), and claims again whenever a bind
// frees room — within the check's quantity (YAKU_QUEUE_DLR_*, issue 7). A
// bind slow to answer slows only itself; rounds of 200 that waited 250 ms
// and an account that sat out a whole second when one answer was late are
// gone (awaiting.go retired).
func (p *Process) deliverClientDLRs(ctx context.Context, log *logging.Logger) {
	p.initFreed()
	chk := p.legCheck(p.dlrCheck)
	t := time.NewTicker(chk.Every)
	defer t.Stop()
	ackT := time.NewTicker(ackEvery)
	defer ackT.Stop()
	var answers sync.WaitGroup // receipts handed to a bind and not yet answered
	var mu sync.Mutex
	var answered []answeredReceipt
	flush := func() {
		mu.Lock()
		got := answered
		answered = nil
		mu.Unlock()
		p.ackDelivered(ctx, log, got)
	}
	// A stopping leg waits for the answers it has out (each at most
	// respTimeout): the client has those receipts, so they are acked before
	// the process goes (stopDrain).
	defer func() {
		answers.Wait()
		flush()
	}()
	budget := chk.PerInterval
	empty := map[int64]bool{} // accounts whose queue had less than was asked, this check
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			budget, empty = chk.PerInterval, map[int64]bool{}
		case <-ackT.C:
			flush()
			continue
		case <-p.dlrFreed:
		}
		if p.queue == nil { // self-heal: a store that failed at boot retries here
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
					continue // its binds are full: a freed place wakes the leg
				}
				claims := p.bindDLRClaims([]int64{acct})
				if len(claims) == 0 {
					empty[acct] = true
					continue
				}
				n := min(room, budget, p.deliverBatch)
				jobs, err := p.claimDLRPaced(ctx, claims, n)
				if err != nil || len(jobs) < n {
					empty[acct] = true
				}
				if err != nil {
					p.claimFailed(log, &p.dlrClaimSaid, "receipts", acct, err)
				}
				budget -= len(jobs)
				var late []int64
				for _, j := range jobs {
					// The account's limit of tries (config/0173): this claim
					// would be one more, so it is given up instead.
					if lim := p.dlrMostTries(j.AccountID); lim > 0 && j.Attempts > lim {
						p.giveUpReceipt(ctx, log, j, lim)
						continue
					}
					final := receiptFinal(j)
					if !final && p.finals.told(j.ClientMsgID) {
						// A progress word after its message's final: never sent
						// (receipts.md), taken off the queue (BUG-RR-03).
						late = append(late, j.ID)
						continue
					}
					// Back down the bind that sent the message when it is live and
					// can receive; else the bind the message's id picks (one bind
					// for all of a message's receipts).
					sess := p.binds.sessionForMessage(j.AccountID, j.BindSlot, j.ClientMsgID)
					if sess == nil {
						continue // not bound now; lease expires, retried
					}
					if final {
						p.finals.note(j.ClientMsgID)
					}
					j := j
					p.pumpOf(sess).push(delivery{
						seq: &p.dlrSeq,
						build: func(s *session, next func() uint32) [][]byte {
							return [][]byte{receiptPDUFor(next(), j, s.packedGSM7())}
						},
						done: func(s *session, a answer) {
							// Every try is a row (dlr_sub_, D-140).
							p.tryLog().Note(p.receiptTry(j, s, a))
							if !a.ok {
								return // refused or no answer: the lease brings it back
							}
							mu.Lock()
							answered = append(answered, answeredReceipt{job: j, sess: s})
							mu.Unlock()
						},
					}, &answers)
					handed++
				}
				p.dropLate(ctx, log, acct, late)
			}
			if handed == 0 {
				break
			}
		}
	}
}

// receiptFinal says whether a queued receipt closes its message for the
// client: its own word when it has one (ACCEPTD and ENROUTE do not), else its
// status.
func receiptFinal(j model.ClientDLR) bool {
	if st := dlr.Stat(j.Stat); st != "" {
		return st.Final()
	}
	return model.Status(j.Status).IsTerminal()
}

// finalsTold remembers for a while the messages whose final receipt this
// process has handed to a client bind, so that a progress receipt of theirs
// claimed later is not sent: a client is never told ACCEPTD or ENROUTE after
// its message's final (receipts.md). The gateway's ACCEPTD is queued when the
// vendor's answer is recorded — with the DB worker, after the record write —
// while the vendor's final can come in on the fast path a moment later and be
// claimed first (BUG-RR-03, found by receipts-records-testing on 2 Nov 2026).
// A message's receipts go down one bind (sessionForMessage), so this
// process sees both.
type finalsTold struct {
	mu     sync.Mutex
	at     map[string]time.Time
	pruned time.Time
}

// finalsKeep is how long a final is remembered: far longer than a progress
// receipt waits behind it in a queue.
const finalsKeep = 30 * time.Minute

// finalsMax bounds the memory; past it the oldest half goes at the next note.
const finalsMax = 500_000

func (f *finalsTold) note(id string) {
	if id == "" {
		return
	}
	now := time.Now()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.at == nil {
		f.at = map[string]time.Time{}
	}
	f.at[id] = now
	if now.Sub(f.pruned) < time.Minute && len(f.at) < finalsMax {
		return
	}
	f.pruned = now
	cut := now.Add(-finalsKeep)
	if len(f.at) >= finalsMax {
		cut = now.Add(-finalsKeep / 2)
	}
	for k, t := range f.at {
		if t.Before(cut) {
			delete(f.at, k)
		}
	}
}

func (f *finalsTold) told(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.at[id]
	return ok && time.Since(t) < finalsKeep
}

// dropLate takes progress receipts that came after their message's final off
// the queue without sending them.
func (p *Process) dropLate(ctx context.Context, log *logging.Logger, acct int64, ids []int64) {
	if len(ids) == 0 {
		return
	}
	actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	err := p.receiptAcks(log).Ack(actx, p.queue, ids...)
	cancel()
	fields := logging.Fields{"receipts": len(ids)}
	if err != nil {
		fields["error"] = err.Error()
	}
	log.Info(p.acctScope(acct), "dlr_progress_after_final", "progress receipts that came after their message's final were not sent", fields)
}

// ackEvery is how often the receipts the clients answered are taken off the
// queue, together.
var ackEvery = 100 * time.Millisecond

// answeredReceipt is a receipt its client answered ESME_ROK for, and the bind.
type answeredReceipt struct {
	job  model.ClientDLR
	sess *session
}

// ackDelivered takes receipts their clients answered off the queue, in one
// call, and counts them as delivered. Not cut by a stop: the client has these
// receipts, and unacked they would be handed over again (stopDrain). An ack
// the store refuses is kept and made again until it takes it (retest of 19
// Oct 2026: the round's ack failed under load, unlogged, and 55 receipts
// reached the client twice when their lease ended).
func (p *Process) ackDelivered(ctx context.Context, log *logging.Logger, taken []answeredReceipt) {
	if len(taken) == 0 {
		return
	}
	ids := make([]int64, len(taken))
	for i, h := range taken {
		ids[i] = h.job.ID
	}
	actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	err := p.receiptAcks(log).Ack(actx, p.queue, ids...)
	cancel()
	if err != nil {
		// Not even kept (too many waiting already): the lease ends and they
		// are handed over again.
		log.Warn(logging.SystemScope, "receipt_ack", "delivered receipts could not be taken off the queue: they are handed over again when their lease ends",
			logging.Fields{"receipts": len(ids), "error": err.Error()})
		return
	}
	p.stats2.receipts.Add(int64(len(taken)))
	perAcct := map[int64]int64{}
	for _, h := range taken {
		h.sess.pushedDLR.Add(1)
		perAcct[h.job.AccountID]++
		log.Info(p.acctScope(h.job.AccountID), "dlr_delivered", "receipt delivered to client",
			logging.Fields{"messageId": h.job.ClientMsgID, "status": h.job.Status, "bindSlot": h.sess.slot})
	}
	// Monitoring: each account's receipts queue, what was handed over. The
	// dlr role counts its own (a dlr_url push); a bind's were counted nowhere
	// per account, so an SMPP customer's queue showed nothing leaving it.
	for acct, n := range perAcct {
		queueflow.Out(queueflow.DLR, strconv.FormatInt(acct, 10), n)
	}
}

// receiptAcks is the role's keeper of delivered receipts' acks, made at first
// use; what it does goes to the role's system log.
func (p *Process) receiptAcks(log *logging.Logger) *dlr.AckKeeper {
	if k := p.acks.Load(); k != nil {
		return k
	}
	p.acks.CompareAndSwap(nil, dlr.NewAckKeeper(func(failed bool, text string) {
		if failed {
			log.Error(logging.SystemScope, "receipt_ack", text, nil)
			return
		}
		log.Warn(logging.SystemScope, "receipt_ack", text, nil)
	}))
	return p.acks.Load()
}

// receiptPDU builds the deliver_sm for one client receipt (PO 2026-09-11):
// source = the handset (the original destination), destination = the original
// sender, the standard receipt text, and the receipted_message_id and
// message_state TLVs. A row written before these facts were stored carries no
// addresses and a vendor's raw text; it still goes out with "0" addresses and
// a receipt rendered here from what the row does have.
func receiptPDU(seq uint32, j model.ClientDLR) []byte { return receiptPDUFor(seq, j, false) }

// receiptPDUFor builds the receipt deliver_sm; packed sends its text packed
// into septets for a client whose account asks for that (2026-09-14). The
// receipt text is plain ASCII, so it always packs. Not packed, it is written
// data_coding 0 as SMPP defines it, the GSM 03.38 alphabet one septet an
// octet (19 Oct 2026; it was the text's own bytes): its letters, digits, ':'
// and ' ' are the same octets as in ASCII, and the '_' of an err code such as
// SMPP_45 is 0x11.
func receiptPDUFor(seq uint32, j model.ClientDLR, packed bool) []byte {
	st := model.Status(j.Status)
	// The receipt's own SMPP word when it has one (queue/0032): a vendor's
	// DELETED reaches the client as DELETED, not as the UNDELIV its status
	// would imply, and an ENROUTE as ENROUTE. Empty = derive it from the
	// status, which is every receipt the gateway decided itself.
	stat := dlr.Stat(j.Stat)
	text := j.Receipt
	if !dlr.IsReceiptTextFor(text, j.ClientMsgID) {
		text = dlr.ReceiptTextStat(j.ClientMsgID, st, stat, "", j.CreatedAt)
	}
	raw := []byte(text)
	if packed {
		if p, err := pdu.PackGSM7(text); err == nil {
			raw = p
		}
	} else if g, err := pdu.GSM7Octets(text); err == nil {
		raw = g
	}
	state := dlr.MessageState(st)
	if stat != "" {
		state = stat.State()
	}
	return pdu.DeliverReceiptPDU(seq, pdu.AddrFor(j.Dst), pdu.AddrFor(j.Src), raw,
		j.ClientMsgID, state)
}

// pushPartReceipts queues a receipt for every id the client holds for m — each
// part's id for a long message sent in parts, else just m's — when smpp-server
// itself decides the outcome: the joined message was refused after its parts
// were acked, or the parts never all arrived (PO 2026-09-11). Honours the
// client's registered_delivery like every other receipt. errCode names the
// reason (the command_status the refusal would have got).
func (p *Process) pushPartReceipts(ctx context.Context, m *model.Message, st model.Status, errCode string) {
	if p.queue == nil || !model.WantsTerminalReceipt(m.NoReceipt, m.FailureReceiptOnly, st) {
		return
	}
	ids := m.PartIDs
	if len(ids) == 0 {
		ids = []string{m.ID}
	}
	now := time.Now()
	for _, id := range ids {
		if err := p.queue.EnqueueClientDLR(ctx, model.ClientDLR{
			AccountID: m.AccountID, ClientMsgID: id, MsgID: m.ID, Status: string(st),
			Receipt:  dlr.ReceiptText(id, st, errCode, now),
			BindSlot: m.BindSlot, Src: m.Sender, Dst: m.Destination,
			// The client is bound to THIS gateway, which decided the outcome
			// itself (D-121).
			Owner: deployment.Owner(),
		}); err != nil {
			logging.New(p.Name()).Error(p.acctScope(m.AccountID), "receipt_enqueue_failed",
				"could not queue a receipt for a long message's part",
				logging.Fields{"messageId": id, "status": string(st), "error": err.Error()})
		}
	}
}

// receiptTry is one delivery of a receipt down a bind, as a try (D-140): filed
// with the message the receipt is about, the id the client was told beside it.
func (p *Process) receiptTry(j model.ClientDLR, s *session, a answer) dal.ClientTry {
	t := bindTry(s, a)
	t.Kind, t.MsgID, t.ClientMsgID = dal.TryDLR, j.MsgID, j.ClientMsgID
	if t.MsgID == "" {
		t.MsgID = j.ClientMsgID
	}
	t.AccountID, t.CompanyID, t.Attempt = j.AccountID, p.companyOf(j.AccountID), j.Attempts
	t.Status, t.Stat = j.Status, j.Stat
	if t.Stat == "" {
		t.Stat = string(dlr.StatFor(model.Status(j.Status)))
	}
	return t
}

// giveUpQueue is the queue's giving-up, which every store has (the dlr and mo
// roles use it); smpp-server asks for it only where an account's limit of
// tries applies (config/0173).
type giveUpQueue interface {
	GiveUpClientDLR(ctx context.Context, id int64) error
	GiveUpMO(ctx context.Context, msgID string) error
}

// dlrMostTries and moMostTries are an account's most tries for its receipts and its
// MO, bind and webhook together (config/0173); 0 = none here.
func (p *Process) dlrMostTries(acct int64) int {
	if p.ref != nil {
		if a, ok := p.ref.AccountByID(acct); ok {
			return a.DLRMaxAttempts
		}
	}
	return 0
}

func (p *Process) moMostTries(acct int64) int {
	if p.ref != nil {
		if a, ok := p.ref.AccountByID(acct); ok {
			return a.MOMaxAttempts
		}
	}
	return 0
}

// giveUpReceipt takes a receipt that has had its account's tries off the
// queue, kept as given up (the age sweep's mark), and writes the giving-up as
// a try row with no channel (dlr_sub_, MAX_ATTEMPTS_REACHED).
func (p *Process) giveUpReceipt(ctx context.Context, log *logging.Logger, j model.ClientDLR, lim int) {
	g, ok := p.queue.(giveUpQueue)
	if !ok {
		return
	}
	if err := g.GiveUpClientDLR(ctx, j.ID); err != nil {
		log.Warn(p.acctScope(j.AccountID), "dlr_give_up_failed", "a receipt past its account's tries could not be given up; it is tried again later",
			logging.Fields{"messageId": j.ClientMsgID, "error": err.Error()})
		return
	}
	t := p.receiptTry(j, nil, answer{})
	t.Channel, t.Session, t.CommandStatus, t.Result, t.ErrorCode = "", "", nil, dal.TryExpired, "MAX_ATTEMPTS_REACHED"
	t.At, t.TookMS = time.Now(), 0
	t.Error = "reached the account's limit of " + strconv.Itoa(lim) + " tries"
	p.tryLog().Note(t)
	log.Info(p.acctScope(j.AccountID), "dlr_given_up", "receipt given up: the account's limit of tries",
		logging.Fields{"messageId": j.ClientMsgID, "attempts": j.Attempts, "limit": lim})
}
