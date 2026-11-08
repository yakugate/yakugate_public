package redis

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/queueflow"
)

// MO (mo_queue) and client-DLR (client_dlr) queues on Redis — the "user"
// queues, moved onto the queue-Redis alongside the vendor MT queue
// (2026-07-24). Both are ACCOUNT-KEYED (claimed for the set of accounts an
// instance currently serves) with a "stale" variant (age filter so a live
// SMPP bind gets first crack and an HTTP poster only picks up leftovers) —
// full parity with the MySQL store. Key layout, per account:
//
//	cdlr:seq                counter -> DLR job ids (matches MySQL auto-increment)
//	cdlr:acct:<accountID>   ZSET  jobID scored by visible-at (lease)
//	cdlrjob:<jobID>         STR   {account_id, client_msg_id, status, receipt, at}
//	mo:acct:<accountID>     ZSET  msg_id scored by visible-at (lease)
//	mojob:<msg_id>          STR   {m: <message json>, at: <received unix>}
//
// "at" is the CREATED/RECEIVED time (for the stale filter); the ZSET score is
// visible-at (bumped +leaseSeconds on claim), a separate concern.
const moLeaseSeconds = 60

// The per-account keys carry the deployment that delivers (D-121): the plain
// key is the shared queue, "<key>@<deployment>" one deployment's own (the
// same queueName rule as the vendor queues). Job payloads keep one global
// key each — their ids are unique — and record their owner, so a re-score,
// ack or give-up finds the right zset.
func cdlrSeqKey() string { return "cdlr:seq" }
func cdlrAcctKey(a int64, owner string) string {
	return queueName("cdlr:acct:"+strconv.FormatInt(a, 10), owner)
}
func cdlrJobKey(id int64) string { return "cdlrjob:" + strconv.FormatInt(id, 10) }
func moAcctKey(a int64, owner string) string {
	return queueName("mo:acct:"+strconv.FormatInt(a, 10), owner)
}
func moJobKey(msgID string) string { return "mojob:" + msgID }

// myOwners are the owners whose receipt and MO queues this process drains:
// its own first, then the shared one.
func (s *Store) myOwners() []string {
	if s.owner == "" {
		return []string{""}
	}
	return []string{s.owner, ""}
}

// Age index (PO 2026-09-17): the per-account zsets above are scored by
// visible-at, which a reschedule moves into the future, so they cannot say
// how OLD a job is. These twins are scored by creation time and never
// re-scored, and the account sets let the give-up sweep find every zset
// without scanning the keyspace — the reason the sweep was a no-op before.
func cdlrAcctsKey(owner string) string { return queueName("cdlr:accts", owner) }
func cdlrAgeKey(a int64, owner string) string {
	return queueName("cdlr:age:"+strconv.FormatInt(a, 10), owner)
}
func moAcctsKey(owner string) string { return queueName("mo:accts", owner) }
func moAgeKey(a int64, owner string) string {
	return queueName("mo:age:"+strconv.FormatInt(a, 10), owner)
}
func acctMember(a int64) string { return strconv.FormatInt(a, 10) }

// ---- client_dlr ----

type cdlrPayload struct {
	AccountID   int64  `json:"account_id"`
	ClientMsgID string `json:"client_msg_id"`
	Status      string `json:"status"`
	// Stat is the receipt's own SMPP word (queue/0032's column on MySQL);
	// absent on older jobs, which decodes to "" = derive it from Status.
	Stat     string `json:"stat,omitempty"`
	Receipt  string `json:"receipt"`
	At       int64  `json:"at"`       // created unix (stale filter + policy age)
	Attempts int    `json:"attempts"` // deliveries tried (retry policy, P5-B02)
	// BindSlot names the bind the original submit arrived on, so the receipt
	// goes back down the same one (PO 2026-09-05). Absent on jobs written
	// before this field existed, which decodes to 0 = any live bind.
	BindSlot int `json:"bind_slot,omitempty"`
	// Src/Dst: the original message's sender and destination (queue/0028's
	// columns on MySQL). Absent on older jobs, which decodes to "".
	Src string `json:"src,omitempty"`
	Dst string `json:"dst,omitempty"`
	// Owner is the deployment that delivers it (D-121); "" = shared, and
	// every job written before separate queues.
	Owner string `json:"owner,omitempty"`
	// Done is when the receipt's state was reached, unix ms (queue/0042's
	// column on MySQL). Absent on older jobs, which decodes to 0 = unknown.
	Done int64 `json:"done,omitempty"`
	// Msg is the message the receipt is about when the client was told
	// another id (a part's; D-140). Absent = ClientMsgID.
	Msg string `json:"msg,omitempty"`
}

// doneAt is Done as a time; 0 = the zero time.
func (p cdlrPayload) doneAt() time.Time {
	if p.Done <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(p.Done).UTC()
}

// EnqueueClientDLR stores one pending receipt, visible immediately. [SCOPE P5]
func (s *Store) EnqueueClientDLR(ctx context.Context, d model.ClientDLR) error {
	id, err := s.c.Incr(ctx, cdlrSeqKey()).Result()
	if err != nil {
		return err
	}
	// A delayed receipt (the account's receipt policy holding an invented
	// answer back) is just an entry scored in the future: the zset score IS
	// the schedule here, so nothing can claim it until then. `At` moves with
	// it so the stale-claim window and the give-up age count from when the
	// receipt exists for the client, exactly as the MySQL backend does it.
	pipe := s.c.TxPipeline()
	fileClientDLR(ctx, pipe, id, d)
	_, err = pipe.Exec(ctx)
	if err == nil {
		queueflow.In(queueflow.DLR, strconv.FormatInt(d.AccountID, 10), 1) // D-127
	}
	return err
}

// fileClientDLR adds the writes that file one pending receipt under id to
// pipe. The job goes first, so a claim that sees the entry always finds it.
func fileClientDLR(ctx context.Context, pipe redis.Pipeliner, id int64, d model.ClientDLR) {
	now := time.Now().Add(d.Delay).Unix()
	var done int64
	if !d.DoneAt.IsZero() {
		done = d.DoneAt.UnixMilli()
	}
	b, _ := json.Marshal(cdlrPayload{AccountID: d.AccountID, ClientMsgID: d.ClientMsgID, Status: d.Status,
		Stat: d.Stat, Receipt: d.Receipt, At: now, BindSlot: d.BindSlot, Src: d.Src, Dst: d.Dst, Owner: d.Owner,
		Done: done, Msg: cdlrMsg(d)})
	pipe.Set(ctx, cdlrJobKey(id), b, 0)
	pipe.ZAdd(ctx, cdlrAcctKey(d.AccountID, d.Owner), redis.Z{Score: float64(now), Member: id})
	pipe.ZAdd(ctx, cdlrAgeKey(d.AccountID, d.Owner), redis.Z{Score: float64(now), Member: id})
	pipe.SAdd(ctx, cdlrAcctsKey(d.Owner), acctMember(d.AccountID))
}

// cdlrBatchChunk is how many receipts EnqueueClientDLRBatch files in one
// transaction: enough to make a batch a handful of round trips, few enough
// that no other client waits long behind one.
const cdlrBatchChunk = 250

// EnqueueClientDLRBatch stores many pending receipts, in order: one call
// reserves their ids, then each cdlrBatchChunk of them is one transaction —
// what EnqueueClientDLR does for each, without two round trips per receipt.
// An error leaves the chunks already filed in place and the rest unfiled.
func (s *Store) EnqueueClientDLRBatch(ctx context.Context, ds []model.ClientDLR) error {
	if len(ds) == 0 {
		return nil
	}
	last, err := s.c.IncrBy(ctx, cdlrSeqKey(), int64(len(ds))).Result()
	if err != nil {
		return err
	}
	first := last - int64(len(ds)) + 1
	for from := 0; from < len(ds); from += cdlrBatchChunk {
		to := min(from+cdlrBatchChunk, len(ds))
		pipe := s.c.TxPipeline()
		in := map[int64]int64{}
		for i := from; i < to; i++ {
			fileClientDLR(ctx, pipe, first+int64(i), ds[i])
			in[ds[i].AccountID]++
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
		for acct, n := range in {
			queueflow.In(queueflow.DLR, strconv.FormatInt(acct, 10), n) // D-127
		}
	}
	return nil
}

// EnqueueClientDLRsWith stores pending receipts AND runs the commands `more`
// adds, in ONE transaction: all of it happens, or none does.
//
// For a vendor's receipt once it is matched and decided (2026-10-13, Saleem:
// "once matched, checked, send to customer and db insertion queue, in both
// queues at the time"): the client's receipt goes in its queue and the
// receipt's database work on the DB worker's queue (recordq), which is in
// this same Redis. Done as two steps, a role stopped between them leaves the
// message final on its record with its client never told, or the client told
// and the record `sent` for good. In one transaction there is no between.
//
// One transaction whatever the count, so it is for a receipt or a kept chunk
// of them — not for a batch of thousands (EnqueueClientDLRBatch).
func (s *Store) EnqueueClientDLRsWith(ctx context.Context, ds []model.ClientDLR, more func(pipe redis.Pipeliner)) error {
	if len(ds) == 0 && more == nil {
		return nil
	}
	var first int64
	if len(ds) > 0 {
		last, err := s.c.IncrBy(ctx, cdlrSeqKey(), int64(len(ds))).Result()
		if err != nil {
			return err
		}
		first = last - int64(len(ds)) + 1
	}
	pipe := s.c.TxPipeline()
	in := map[int64]int64{}
	for i, d := range ds {
		fileClientDLR(ctx, pipe, first+int64(i), d)
		in[d.AccountID]++
	}
	if more != nil {
		more(pipe)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	for acct, n := range in {
		queueflow.In(queueflow.DLR, strconv.FormatInt(acct, 10), n) // D-127
	}
	return nil
}

// ClientPartIDsMany returns the saved part ids of the messages that have any,
// in one read; an ordinary message is not in the answer.
func (s *Store) ClientPartIDsMany(ctx context.Context, msgIDs []string) (map[string][]string, error) {
	out := map[string][]string{}
	for from := 0; from < len(msgIDs); from += 500 {
		to := min(from+500, len(msgIDs))
		keys := make([]string, 0, to-from)
		for _, id := range msgIDs[from:to] {
			keys = append(keys, partIDsKey(id))
		}
		vals, err := s.c.MGet(ctx, keys...).Result()
		if err != nil {
			return out, err
		}
		for i, v := range vals {
			if str, ok := v.(string); ok && str != "" {
				out[msgIDs[from+i]] = strings.Split(str, ",")
			}
		}
	}
	return out, nil
}

// partIDsKey holds the part ids of one long message an SMPP client sent in
// parts, under the joined message's id (MySQL: client_part_ids, queue/0028).
func partIDsKey(msgID string) string { return "cparts:" + msgID }

// partIDsTTL bounds a row whose message never got a final receipt — past how
// long a receipt is waited for (model.DefaultReceiptWaitHours, 48 h, unless a
// vendor's dlr_config.wait_hours says otherwise) with room. A final receipt
// drops the row at once (DropClientPartIDs).
const partIDsTTL = 7 * 24 * time.Hour

// SaveClientPartIDs records the ids a client was given for each part.
func (s *Store) SaveClientPartIDs(ctx context.Context, msgID string, partIDs []string) error {
	return s.c.Set(ctx, partIDsKey(msgID), strings.Join(partIDs, ","), partIDsTTL).Err()
}

// ClientPartIDs returns the saved part ids, or nil for an ordinary message.
func (s *Store) ClientPartIDs(ctx context.Context, msgID string) ([]string, error) {
	v, err := s.c.Get(ctx, partIDsKey(msgID)).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return strings.Split(v, ","), nil
}

// DropClientPartIDs removes the entry once every part has its final receipt.
func (s *Store) DropClientPartIDs(ctx context.Context, msgID string) error {
	return s.c.Del(ctx, partIDsKey(msgID)).Err()
}

func (s *Store) ClaimClientDLR(ctx context.Context, claims []model.DLRClaim, n int) ([]model.ClientDLR, error) {
	return s.claimClientDLR(ctx, claims, 0, n)
}

func (s *Store) ClaimClientDLRStale(ctx context.Context, claims []model.DLRClaim, minAgeSec, n int) ([]model.ClientDLR, error) {
	return s.claimClientDLR(ctx, claims, minAgeSec, n)
}

func (s *Store) claimClientDLR(ctx context.Context, claims []model.DLRClaim, minAgeSec, n int) ([]model.ClientDLR, error) {
	now := time.Now().Unix()
	maxCreated := now - int64(minAgeSec) // stale: only jobs created at/before this
	var out []model.ClientDLR
	for _, cl := range claims {
		for _, owner := range s.myOwners() { // my receipts, then the shared ones (D-121)
			acct := cl.AccountID
			if len(out) >= n {
				break
			}
			key := cdlrAcctKey(acct, owner)
			// The lease and the attempt count of everything a page claims go to
			// Redis in ONE round trip (2026-10-09). They went one job at a
			// time, two round trips each, so claiming 200 receipts was about
			// 400 round trips one after another. On a quiet Redis that is 30 ms;
			// while messages are being accepted and sent every round trip
			// queues behind 50,000 other commands a second and took 3 to 7 ms
			// on the dev box, so a claim took over a second and a client was
			// handed 120 receipts a second with 890,000 waiting — until the
			// sending stopped, when the same loop did 2,400 a second.
			pipe := s.c.Pipeline()
			pageFrom := len(out)
			err := s.walkDue(ctx, key, cdlrAgeKey(acct, owner), now, n-len(out),
				func(idStr string) string {
					id, _ := strconv.ParseInt(idStr, 10, 64)
					return cdlrJobKey(id)
				},
				func() bool { return len(out) >= n },
				func() error {
					if pipe.Len() == 0 {
						return nil
					}
					if _, err := pipe.Exec(ctx); err != nil {
						// Not leased: left due, and not handed to the caller,
						// which would otherwise send them now and again later.
						out = out[:pageFrom]
						return err
					}
					pageFrom = len(out)
					return nil
				},
				func(idStr string, b []byte) bool {
					var p cdlrPayload
					if json.Unmarshal(b, &p) != nil {
						return false
					}
					if minAgeSec > 0 && p.At > maxCreated {
						return false // not stale enough yet — leave for a live bind
					}
					// P5-B03: the attempt window this account's delivery channel owns.
					// Compared BEFORE the increment below, so it counts deliveries
					// already tried — the same point MySQL's SELECT reads it at.
					if p.Attempts > 0 && p.Attempts < cl.MinAttempts {
						return false // a live bind is mid-retry; its window, not ours.
						// Attempts=0 stays claimable: no bind ever claimed it, and the
						// staleness gate has already proven nobody is going to.
					}
					if cl.MaxAttempts > 0 && p.Attempts >= cl.MaxAttempts {
						return false // this leg's budget is spent; the other leg owns it now
					}
					// Re-lease, and count the attempt so the retry policy can see it.
					// Both writes are XX (only if still there): an ack landing between
					// our read and these writes has already removed the job, and a
					// plain ZADD put its entry back with no job behind it — the
					// orphan that stalled accounts 21 and 25.
					id, _ := strconv.ParseInt(idStr, 10, 64)
					pipe.ZAddXX(ctx, key, redis.Z{Score: float64(now + moLeaseSeconds), Member: idStr})
					// A webhook's claim under 'both' raises the count to the
					// bind's budget, so what it took stays its own and is
					// retried (mysql webhookFloors, 2026-10-15).
					p.Attempts = max(p.Attempts+1, cl.MinAttempts)
					if nb, err := json.Marshal(p); err == nil {
						pipe.SetXX(ctx, cdlrJobKey(id), nb, redis.KeepTTL)
					}
					out = append(out, model.ClientDLR{ID: id, AccountID: p.AccountID,
						ClientMsgID: p.ClientMsgID, Status: p.Status, Stat: p.Stat, Receipt: p.Receipt,
						Attempts: p.Attempts, CreatedAt: time.Unix(p.At, 0), BindSlot: p.BindSlot,
						Src: p.Src, Dst: p.Dst, Owner: p.Owner, DoneAt: p.doneAt(), MsgID: p.Msg})
					return true
				})
			if err != nil {
				return out, err
			}
		}
	}
	return out, nil
}

// walkDue hands each live due job of one account's ZSET to claim, oldest
// first, and removes the entries whose job payload is gone.
//
// It pages past what it cannot claim. The old loop read ONE page per call and
// stopped: a page of orphans (entries with no job) or of jobs this leg may not
// take returned nothing, and the bind loop, seeing nothing, waited for its next
// tick — so 356,833 orphans at the head of account 21 meant about 1,800 empty
// ticks before its first real receipt. One MGET per page instead of one GET per
// entry, and one ZREM for all of a page's orphans.
//
// claim reports whether it took the job. A taken job is re-scored past now and
// leaves the due range, so only the entries left behind move the next page's
// offset. scanned is bounded so one account's backlog cannot hold up the
// accounts after it in the same call.
//
// flush is called after each page: claim only says what it takes, and flush
// writes the page's leases in one round trip — before the next page is read,
// so a taken job has left the due range by then. An error from it ends the
// walk.
//
// An orphan leaves the age index too (2026-10-15): it used to leave the queue
// only, and its age entry stayed until the give-up sweep, a day later.
func (s *Store) walkDue(ctx context.Context, key, ageKey string, now int64, want int,
	jobKey func(member string) string, full func() bool, flush func() error,
	claim func(member string, payload []byte) bool) error {
	page := int64(want)
	if page < walkPageMin {
		page = walkPageMin
	}
	budget := want * 10
	if budget < walkScanMin {
		budget = walkScanMin
	}
	upTo := strconv.FormatInt(now, 10)
	var offset int64
	for scanned := 0; scanned < budget && !full(); {
		ids, err := s.c.ZRangeByScore(ctx, key, &redis.ZRangeBy{Min: "-inf", Max: upTo, Offset: offset, Count: page}).Result()
		if err != nil || len(ids) == 0 {
			return err
		}
		scanned += len(ids)
		keys := make([]string, len(ids))
		for i, id := range ids {
			keys[i] = jobKey(id)
		}
		vals, err := s.c.MGet(ctx, keys...).Result()
		if err != nil {
			return err
		}
		var gone []any
		var left int64
		for i, id := range ids {
			v, ok := vals[i].(string)
			if !ok {
				gone = append(gone, id)
				continue
			}
			if full() || !claim(id, []byte(v)) {
				left++
			}
		}
		if err := flush(); err != nil {
			return err
		}
		if len(gone) > 0 {
			pipe := s.c.Pipeline()
			pipe.ZRem(ctx, key, gone...)
			pipe.ZRem(ctx, ageKey, gone...)
			if _, err := pipe.Exec(ctx); err != nil {
				return err
			}
		}
		if int64(len(ids)) < page {
			return nil // reached the end of what is due
		}
		offset += left
	}
	return nil
}

// Paging for walkDue: at least this many entries per round trip, and at least
// this many looked at per account per claim before moving on.
const (
	walkPageMin = 200
	walkScanMin = 5000
)

// AckClientDLR removes delivered receipts by id. [SCOPE P5]
//
// Two round trips whatever the number of ids: the jobs are read together
// (each names its account and owner, which is where its entries are), and
// everything is removed in one pipeline. It was four round trips for each id,
// one after another (2026-10-09, with the claim above).
func (s *Store) AckClientDLR(ctx context.Context, ids ...int64) error {
	if len(ids) == 0 {
		return nil
	}
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = cdlrJobKey(id)
	}
	vals, err := s.c.MGet(ctx, keys...).Result()
	if err != nil {
		return err
	}
	pipe := s.c.Pipeline()
	for i, id := range ids {
		v, ok := vals[i].(string)
		if !ok {
			continue // gone already: acked before, or given up
		}
		var p cdlrPayload
		if json.Unmarshal([]byte(v), &p) == nil {
			member := strconv.FormatInt(id, 10)
			pipe.ZRem(ctx, cdlrAcctKey(p.AccountID, p.Owner), member)
			pipe.ZRem(ctx, cdlrAgeKey(p.AccountID, p.Owner), member)
		}
		pipe.Del(ctx, keys[i])
	}
	if pipe.Len() == 0 {
		return nil
	}
	_, err = pipe.Exec(ctx)
	return err
}

// RescheduleClientDLR delays one receipt's next attempt. In Redis the zset
// score IS the schedule, so backing off is a re-score. [P5-B02]
func (s *Store) RescheduleClientDLR(ctx context.Context, id int64, delay time.Duration) error {
	b, err := s.c.Get(ctx, cdlrJobKey(id)).Bytes()
	if err == redis.Nil {
		return nil
	} else if err != nil {
		return err
	}
	var p cdlrPayload
	if json.Unmarshal(b, &p) != nil {
		return nil
	}
	// XX: an ack between the read above and this write has removed the entry,
	// and putting it back would leave it pointing at a deleted job.
	return s.c.ZAddXX(ctx, cdlrAcctKey(p.AccountID, p.Owner), redis.Z{
		Score: float64(time.Now().Add(delay).Unix()), Member: strconv.FormatInt(id, 10)}).Err()
}

// GiveUpClientDLR abandons one receipt. The MySQL backend keeps the row with
// given_up_at set; Redis has no such column, so the job is moved to a
// per-account GIVEN-UP zset — out of the delivery path, still inspectable for
// gaveUpKeep, and then gone (see giveUpClientDLRs). [P5-B02]
func (s *Store) GiveUpClientDLR(ctx context.Context, id int64) error {
	return s.giveUpClientDLRs(ctx, []string{strconv.FormatInt(id, 10)})
}

// gaveUpKeep is how long a given-up receipt or MO stays in Redis. The comment
// above said "not immortal" from the start, but nothing ever removed one: its
// payload had no life of its own and the given-up list was never trimmed, so
// every receipt a dead client never fetched stayed in memory for good
// (2026-10-07: 2.7 million waiting for one unbound account).
const gaveUpKeep = 24 * time.Hour

// giveUpClientDLRs abandons several receipts in two round trips: their
// payloads read together, then one pipeline. A payload that has gone already
// is passed over — its index entries are the caller's to remove.
func (s *Store) giveUpClientDLRs(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = "cdlrjob:" + id
	}
	vals, err := s.c.MGet(ctx, keys...).Result()
	if err != nil {
		return err
	}
	now := time.Now()
	lists := map[string]bool{}
	pipe := s.c.Pipeline()
	for i, v := range vals {
		str, _ := v.(string)
		var p cdlrPayload
		if str == "" || json.Unmarshal([]byte(str), &p) != nil {
			continue
		}
		gk := cdlrGiveUpKey(p.AccountID)
		pipe.ZRem(ctx, cdlrAcctKey(p.AccountID, p.Owner), ids[i])
		pipe.ZRem(ctx, cdlrAgeKey(p.AccountID, p.Owner), ids[i])
		pipe.ZAdd(ctx, gk, redis.Z{Score: float64(now.Unix()), Member: ids[i]})
		pipe.Expire(ctx, keys[i], gaveUpKeep)
		lists[gk] = true
	}
	trimGaveUp(ctx, pipe, lists, now)
	if pipe.Len() == 0 {
		return nil
	}
	_, err = pipe.Exec(ctx)
	return err
}

// trimGaveUp drops what was given up more than gaveUpKeep ago from each
// given-up list touched, and lets a list nobody adds to any more expire.
func trimGaveUp(ctx context.Context, pipe redis.Pipeliner, lists map[string]bool, now time.Time) {
	before := strconv.FormatInt(now.Add(-gaveUpKeep).Unix(), 10)
	for gk := range lists {
		pipe.ZRemRangeByScore(ctx, gk, "-inf", before)
		pipe.Expire(ctx, gk, gaveUpKeep)
	}
}

// cdlrGiveUpKey holds the receipts we stopped trying to deliver for one account.
func cdlrGiveUpKey(accountID int64) string {
	return "cdlr:gaveup:" + strconv.FormatInt(accountID, 10)
}

// SweepGiveUpClientDLR is the backstop for receipts nobody is even claiming:
// every receipt created more than olderThan ago is given up, whatever its
// schedule says (PO 2026-09-17; it was a no-op on Redis before, so the time
// limit applied only to claimed jobs). The age index makes it cheap: one set
// of accounts, range reads per account, no keyspace scan. Jobs enqueued
// before the index existed have no age entry and are not swept. [P5-B02]
func (s *Store) SweepGiveUpClientDLR(ctx context.Context, olderThan time.Duration) (int64, error) {
	if olderThan <= 0 {
		return 0, nil
	}
	// My receipts and the shared ones (D-121); another deployment gives up
	// on its own, when it is back.
	var total int64
	for _, owner := range s.myOwners() {
		n, err := s.sweepAge(ctx, cdlrAcctsKey(owner),
			func(a int64) string { return cdlrAgeKey(a, owner) },
			func(a int64) string { return cdlrAcctKey(a, owner) },
			olderThan, s.giveUpClientDLRs)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// sweepPage is how many jobs one read of an age index takes, and sweepBudget
// how long one sweep may run. The sweep used to take ONE page of 1000 per
// account each time it ran — every five minutes, so 288,000 a day — while a
// client that is gone is owed receipts at the rate its traffic is sent: the
// backlog only ever grew. It now takes page after page until nothing old is
// left or its time is up (the caller runs it again five minutes later).
var sweepPage int64 = 1000

const sweepBudget = 4 * time.Minute

// sweepAge gives up every member of the age zsets scored before now-olderThan.
// giveUp does the per-job work, a page at a time. Every member swept leaves
// both its age index and its queue whatever giveUp found, so a job whose
// payload has already gone cannot be revisited forever — nor stay counted as
// waiting (the queue zset kept such entries for good).
func (s *Store) sweepAge(ctx context.Context, acctsKey string, ageKey, queueKey func(int64) string, olderThan time.Duration,
	giveUp func(context.Context, []string) error) (int64, error) {
	accts, err := s.c.SMembers(ctx, acctsKey).Result()
	if err != nil {
		return 0, err
	}
	cutoff := strconv.FormatInt(time.Now().Add(-olderThan).Unix(), 10)
	deadline := time.Now().Add(sweepBudget)
	var n int64
	for _, a := range accts {
		acct, err := strconv.ParseInt(a, 10, 64)
		if err != nil {
			continue
		}
		for {
			if ctx.Err() != nil || time.Now().After(deadline) {
				return n, ctx.Err()
			}
			members, err := s.c.ZRangeByScore(ctx, ageKey(acct), &redis.ZRangeBy{Min: "-inf", Max: cutoff, Count: sweepPage}).Result()
			if err != nil {
				return n, err
			}
			if len(members) == 0 {
				break
			}
			if err := giveUp(ctx, members); err != nil {
				return n, err
			}
			gone := make([]any, len(members))
			for i, m := range members {
				gone[i] = m
			}
			pipe := s.c.Pipeline()
			pipe.ZRem(ctx, ageKey(acct), gone...)
			pipe.ZRem(ctx, queueKey(acct), gone...)
			if _, err := pipe.Exec(ctx); err != nil {
				return n, err
			}
			n += int64(len(members))
			if int64(len(members)) < sweepPage {
				break
			}
		}
		// An account with nothing left in its queue or its age index leaves
		// the account set (2026-10-15): it stayed there for good. In one
		// script, so an enqueue that lands meanwhile keeps it; the next
		// enqueue puts it back anyway.
		s.c.Eval(ctx, `if redis.call('EXISTS', KEYS[1], KEYS[2]) == 0 then return redis.call('SREM', KEYS[3], ARGV[1]) end return 0`,
			[]string{queueKey(acct), ageKey(acct), acctsKey}, a)
	}
	return n, nil
}

// ---- mo_queue ----

type moPayload struct {
	M        model.Message `json:"m"`
	At       int64         `json:"at"`       // received unix (stale filter + policy age)
	Attempts int           `json:"attempts"` // deliveries tried (retry policy, P6-B1)
}

// StageMO durably stages an accepted MO message, visible immediately. [SCOPE P6]
func (s *Store) StageMO(ctx context.Context, m model.Message) error {
	if m.Encoding == "" {
		m.Encoding = "gsm7"
	}
	now := time.Now().Unix()
	b, err := json.Marshal(moPayload{M: m, At: now})
	if err != nil {
		return err
	}
	// The deployment that delivers it (D-121); "" = shared.
	pipe := s.c.TxPipeline()
	pipe.Set(ctx, moJobKey(m.ID), b, 0)
	pipe.ZAdd(ctx, moAcctKey(m.AccountID, m.QueueOwner), redis.Z{Score: float64(now), Member: m.ID})
	pipe.ZAdd(ctx, moAgeKey(m.AccountID, m.QueueOwner), redis.Z{Score: float64(now), Member: m.ID})
	pipe.SAdd(ctx, moAcctsKey(m.QueueOwner), acctMember(m.AccountID))
	_, err = pipe.Exec(ctx)
	if err == nil {
		queueflow.In(queueflow.MO, strconv.FormatInt(m.AccountID, 10), 1) // D-127
	}
	return err
}

func (s *Store) ClaimMO(ctx context.Context, claims []model.MOClaim, n int) ([]model.Message, error) {
	return s.claimMO(ctx, claims, 0, n)
}

func (s *Store) ClaimMOStale(ctx context.Context, claims []model.MOClaim, minAgeSec, n int) ([]model.Message, error) {
	return s.claimMO(ctx, claims, minAgeSec, n)
}

func (s *Store) claimMO(ctx context.Context, claims []model.MOClaim, minAgeSec, n int) ([]model.Message, error) {
	now := time.Now().Unix()
	maxReceived := now - int64(minAgeSec)
	var out []model.Message
	for _, cl := range claims {
		for _, owner := range s.myOwners() { // my MO, then the shared ones (D-121)
			if len(out) >= n {
				break
			}
			key := moAcctKey(cl.AccountID, owner)
			// One round trip for a page's leases, as for receipts above.
			pipe := s.c.Pipeline()
			pageFrom := len(out)
			err := s.walkDue(ctx, key, moAgeKey(cl.AccountID, owner), now, n-len(out), moJobKey,
				func() bool { return len(out) >= n },
				func() error {
					if pipe.Len() == 0 {
						return nil
					}
					if _, err := pipe.Exec(ctx); err != nil {
						out = out[:pageFrom] // not leased, so not handed out
						return err
					}
					pageFrom = len(out)
					return nil
				},
				func(msgID string, b []byte) bool {
					var p moPayload
					if json.Unmarshal(b, &p) != nil {
						return false
					}
					if minAgeSec > 0 && p.At > maxReceived {
						return false
					}
					// The attempt window the account's mo_channel gives this leg
					// (config/0146) — the receipt claim's rule, read the same way.
					if p.Attempts > 0 && p.Attempts < cl.MinAttempts {
						return false // a live bind is mid-retry; its window, not ours
					}
					if cl.MaxAttempts > 0 && p.Attempts >= cl.MaxAttempts {
						return false // this leg's budget is spent; the other leg owns it now
					}
					// re-lease, and count the attempt so the retry policy can see it
					// (XX for the same reason as the receipt claim above)
					pipe.ZAddXX(ctx, key, redis.Z{Score: float64(now + moLeaseSeconds), Member: msgID})
					p.Attempts = max(p.Attempts+1, cl.MinAttempts) // as for receipts above
					if nb, err := json.Marshal(p); err == nil {
						pipe.SetXX(ctx, moJobKey(msgID), nb, redis.KeepTTL)
					}
					m := p.M
					m.Direction = model.DirectionMO
					m.Attempts = p.Attempts
					m.CreatedAt = time.Unix(p.At, 0)
					out = append(out, m)
					return true
				})
			if err != nil {
				return out, err
			}
		}
	}
	return out, nil
}

// RescheduleMO delays one staged MO's next delivery attempt — in Redis the
// zset score IS the schedule, so a backoff is a re-score. [P6-B1]
func (s *Store) RescheduleMO(ctx context.Context, msgID string, delay time.Duration) error {
	b, err := s.c.Get(ctx, moJobKey(msgID)).Bytes()
	if err == redis.Nil {
		return nil
	} else if err != nil {
		return err
	}
	var p moPayload
	if json.Unmarshal(b, &p) != nil {
		return nil
	}
	return s.c.ZAddXX(ctx, moAcctKey(p.M.AccountID, p.M.QueueOwner), redis.Z{
		Score: float64(time.Now().Add(delay).Unix()), Member: msgID}).Err()
}

// GiveUpMO abandons one staged MO. MySQL stamps status='expired' and keeps the
// row; Redis moves the job to a per-account given-up zset — out of the delivery
// path, still inspectable for gaveUpKeep, and then gone. [P6-B1]
func (s *Store) GiveUpMO(ctx context.Context, msgID string) error {
	return s.giveUpMOs(ctx, []string{msgID})
}

// giveUpMOs is giveUpClientDLRs for staged inbound messages.
func (s *Store) giveUpMOs(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = moJobKey(id)
	}
	vals, err := s.c.MGet(ctx, keys...).Result()
	if err != nil {
		return err
	}
	now := time.Now()
	lists := map[string]bool{}
	pipe := s.c.Pipeline()
	for i, v := range vals {
		str, _ := v.(string)
		var p moPayload
		if str == "" || json.Unmarshal([]byte(str), &p) != nil {
			continue
		}
		gk := moGiveUpKey(p.M.AccountID)
		pipe.ZRem(ctx, moAcctKey(p.M.AccountID, p.M.QueueOwner), ids[i])
		pipe.ZRem(ctx, moAgeKey(p.M.AccountID, p.M.QueueOwner), ids[i])
		pipe.ZAdd(ctx, gk, redis.Z{Score: float64(now.Unix()), Member: ids[i]})
		pipe.Expire(ctx, keys[i], gaveUpKeep)
		lists[gk] = true
	}
	trimGaveUp(ctx, pipe, lists, now)
	if pipe.Len() == 0 {
		return nil
	}
	_, err = pipe.Exec(ctx)
	return err
}

func moGiveUpKey(accountID int64) string {
	return "mo:gaveup:" + strconv.FormatInt(accountID, 10)
}

// SweepGiveUpMO gives up every staged inbound message older than olderThan,
// through the same age index as the receipt sweep (PO 2026-09-17; a no-op on
// Redis before). MySQL also expires by valid_until; on Redis that check stays
// at claim time. [P6-B1]
func (s *Store) SweepGiveUpMO(ctx context.Context, olderThan time.Duration) (int64, error) {
	if olderThan <= 0 {
		return 0, nil
	}
	// My MO and the shared ones (D-121).
	var total int64
	for _, owner := range s.myOwners() {
		n, err := s.sweepAge(ctx, moAcctsKey(owner),
			func(a int64) string { return moAgeKey(a, owner) },
			func(a int64) string { return moAcctKey(a, owner) },
			olderThan, s.giveUpMOs)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// AckMO removes delivered MO staging entries by canonical msg id. [SCOPE P6]
func (s *Store) AckMO(ctx context.Context, msgIDs ...string) error {
	if len(msgIDs) == 0 {
		return nil
	}
	// Two round trips whatever the number of ids, as AckClientDLR.
	keys := make([]string, len(msgIDs))
	for i, msgID := range msgIDs {
		keys[i] = moJobKey(msgID)
	}
	vals, err := s.c.MGet(ctx, keys...).Result()
	if err != nil {
		return err
	}
	pipe := s.c.Pipeline()
	for i, msgID := range msgIDs {
		v, ok := vals[i].(string)
		if !ok {
			continue
		}
		var p moPayload
		if json.Unmarshal([]byte(v), &p) == nil {
			pipe.ZRem(ctx, moAcctKey(p.M.AccountID, p.M.QueueOwner), msgID)
			pipe.ZRem(ctx, moAgeKey(p.M.AccountID, p.M.QueueOwner), msgID)
		}
		pipe.Del(ctx, keys[i])
	}
	if pipe.Len() == 0 {
		return nil
	}
	_, err = pipe.Exec(ctx)
	return err
}

// cdlrMsg is the message a receipt job names (D-140): only when the client
// was told another id.
func cdlrMsg(d model.ClientDLR) string {
	if d.MsgID == d.ClientMsgID {
		return ""
	}
	return d.MsgID
}
