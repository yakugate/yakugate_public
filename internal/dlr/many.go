package dlr

import (
	"context"
	"time"
)

// Receipts applied many at a time (load run of 2026-10-13, Saleem: "out
// throughput is not satisfactory").
//
// ApplyMany looks for all the receipts a worker has waiting in ONE round trip
// (GETDEL for a final word, GET for a progress word, each under the id as it
// came), decides each as Apply would (decide), and files every client receipt
// with its record work together. A receipt this cannot finish at once — no
// facts under the id as it came, a group, a part of a long message, a word
// that turns final only with the sending vendor's map — goes the one-receipt
// path with what was found.

// ManyWrites is an optional RecordWriter capability: many receipts' database
// work queued in one round trip.
type ManyWrites interface {
	Receipts(ctx context.Context, ws []ReceiptWrite) error
}

// BatchPusher is an optional ClientPusher capability: many client receipts —
// each with the database work it carries, if any — filed together.
type BatchPusher interface {
	PushBatch(ctx context.Context, pushes []Push) error
}

// ReceiptIn is one receipt as a vendor sent it.
type ReceiptIn struct {
	VendorID    int64
	VendorMsgID string
	RawStatus   string
	ErrorCode   string
	Raw         string
	StatusMap   map[string]string
	// DoneAt is the vendor's delivery time when it gave one.
	DoneAt time.Time
	// Number is the handset the receipt is about, when it says one (an SMPP
	// receipt's source address): of a bulk call's messages under one id, the
	// one sent to it is closed (groups.go, rule 1).
	Number string
	// AltID is a second id the vendor gave for the message — the receipt's
	// text, when its receipted_message_id field named another — tried when
	// VendorMsgID finds nothing.
	AltID string
	// Arrived is when the receipt reached the gateway: its row says so.
	Arrived time.Time
}

// ApplyMany applies receipts that were waiting together, in order. For each
// receipt it returns what Apply returns: whether it was matched to a message
// (false = not found yet: looked for again, then an orphan) and an error.
func (s *Service) ApplyMany(ctx context.Context, in []ReceiptIn) ([]bool, []error) {
	oks := make([]bool, len(in))
	errs := make([]error, len(in))
	// Without Redis the plain finals' closes are left to the end of the
	// batch and made together (closemany.go); the rest is as before.
	var coll *closeBatch
	mc, closesMany := s.manyCloser()
	if closesMany && len(in) > 1 {
		coll = &closeBatch{}
	}
	// alone sends one receipt down Apply's own path.
	alone := func(i int) {
		r := in[i]
		if s.Fast() {
			r.DoneAt = doneTime(r.VendorID, r.VendorMsgID, r.DoneAt)
			oks[i], errs[i] = s.applyRedis(ctx, r, 1)
			return
		}
		grace := correlateGrace()
		if !r.Arrived.IsZero() {
			if grace -= time.Since(r.Arrived); grace < 0 {
				grace = 0
			}
		}
		if coll != nil {
			coll.at = i
		}
		oks[i], errs[i] = s.applyTo(ctx, receiptKey{vendorID: r.VendorID, id: r.VendorMsgID, alt: r.AltID, number: r.Number},
			r.RawStatus, r.ErrorCode, r.Raw, r.StatusMap, doneTime(r.VendorID, r.VendorMsgID, r.DoneAt), grace, true, coll)
	}
	var mf ManyFacts
	many := false
	if s.Fast() {
		mf, many = s.facts.(ManyFacts)
	}
	if !many || len(in) < 2 {
		for i := range in {
			alone(i)
		}
		if coll != nil {
			s.closeMany(ctx, mc, coll, errs)
		}
		return oks, errs
	}

	// 1. One look for every receipt, under the id as it came.
	asks := make([]FactAsk, len(in))
	for i, r := range in {
		asks[i] = FactAsk{Scope: s.scope(r.VendorID), VendorMsgID: r.VendorMsgID, Number: r.Number, Close: s.closingWord(r)}
	}
	got, err := mf.TakeMany(ctx, asks)
	if err != nil {
		// Redis cannot be read: each receipt finds that out for itself and
		// is kept to be looked for again.
		for i := range in {
			alone(i)
		}
		return oks, errs
	}

	// 2. Decide each; what is not plain goes the one-receipt path with what
	// was found.
	var ds []*done
	var of []int // ds[k] is receipt of[k]
	for i, r := range in {
		fs := got[i]
		if r.VendorMsgID == "" || len(fs) == 0 {
			alone(i)
			continue
		}
		r.DoneAt = doneTime(r.VendorID, r.VendorMsgID, r.DoneAt)
		f := fs[0]
		if len(fs) > 1 || f.Parts > 1 || f.Group != "" {
			oks[i], errs[i] = s.handleTaken(ctx, r, r.VendorMsgID, fs, asks[i].Close)
			continue
		}
		if !asks[i].Close {
			sender := f.VendorID
			if sender == 0 {
				sender = r.VendorID
			}
			if ParseStat(r.RawStatus, s.mapFor(r.StatusMap, sender, r.VendorID)).Final() {
				oks[i], errs[i] = s.handleTaken(ctx, r, r.VendorMsgID, fs, false)
				continue
			}
		}
		oks[i] = true
		ds = append(ds, s.decide(ctx, r, r.VendorMsgID, f, asks[i].Close))
		of = append(of, i)
	}

	// 3. Both queues: the client receipts with the record work they carry in
	// one filing, and the record work of the receipts no client is sent in
	// another.
	for _, d := range ds {
		if d.putBack {
			if err := s.facts.PutBack(ctx, d.scope, d.found, []Facts{d.f}); err != nil {
				noteFastTrouble("taken receipt facts could not be put back (%v): %s", err, d.f.MsgID)
			}
			d.putBack = false
		}
	}
	failed := make([]bool, len(ds))
	recordIn := make([]bool, len(ds))
	s.fileMany(ctx, len(ds), func(k int) (int64, ReceiptWrite, *Receipt) {
		return ds[k].f.AccountID, ds[k].write, ds[k].push
	}, func(k int, err error, queued bool) { failed[k], recordIn[k] = true, queued; errs[of[k]] = err })
	for k, d := range ds {
		if !failed[k] {
			s.afterFiled(ctx, d)
			continue
		}
		// Its facts were taken: nothing else will finish it. What did not
		// go is filed again, and held when that fails too (hold.go).
		if recordIn[k] && d.push != nil {
			d := d
			cause := errs[of[k]]
			if herr := s.hold("the receipt of "+d.f.MsgID, cause, func(ctx context.Context) error {
				if err := s.pusher.PushToClient(ctx, d.f.AccountID, *d.push); err != nil {
					return err
				}
				s.afterFiled(ctx, d)
				return nil
			}); herr == nil {
				errs[of[k]] = nil
			}
			continue
		}
		if err := s.fileDone(ctx, d); err == nil {
			errs[of[k]] = nil
		}
	}
	return oks, errs
}

// fileMany is file for n matched receipts at once: get(k) gives the k-th
// one's account, record work and client receipt (nil = none), fail(k, err,
// queued) notes that it could not be filed — queued: its record work is in,
// only its client receipt was refused.
//
// With a pusher that takes both in one step, every client receipt goes out
// with its record work in ONE filing. Otherwise the record work goes first,
// all of it together, then the client receipts, as file does for one.
func (s *Service) fileMany(ctx context.Context, n int, get func(k int) (int64, ReceiptWrite, *Receipt), fail func(k int, err error, queued bool)) {
	if n == 0 {
		return
	}
	both, oneStep := s.pusher.(BothPusher)
	oneStep = oneStep && both.OneStep()
	batch, canBatch := s.pusher.(BatchPusher)
	var pushes []Push // client receipts (with their record work in one-step mode)
	var pushOf []int  // pushes[j] is receipt pushOf[j]
	var writes []ReceiptWrite
	var writeOf []int
	for k := 0; k < n; k++ {
		acct, w, toClient := get(k)
		if toClient != nil && oneStep {
			wc := w
			pushes = append(pushes, Push{AccountID: acct, Receipt: *toClient, Write: &wc})
			pushOf = append(pushOf, k)
			continue
		}
		writes = append(writes, w)
		writeOf = append(writeOf, k)
		if toClient != nil {
			pushes = append(pushes, Push{AccountID: acct, Receipt: *toClient})
			pushOf = append(pushOf, k)
		}
	}
	// The record work that rides on no client receipt.
	failed := map[int]bool{}
	if len(writes) > 0 {
		var werr error
		if mw, ok := s.writer.(ManyWrites); ok {
			werr = mw.Receipts(ctx, writes)
		} else {
			for _, w := range writes {
				if werr = s.writer.Receipt(ctx, w); werr != nil {
					break
				}
			}
		}
		if werr != nil {
			for _, k := range writeOf {
				failed[k] = true
				fail(k, werr, false)
			}
		}
	}
	// The client receipts. One whose record work was not queued is not sent:
	// the client would be told of a message its record never closes.
	if len(pushes) > 0 {
		send, sendOf := pushes, pushOf
		if len(failed) > 0 {
			send, sendOf = nil, nil
			for j, p := range pushes {
				if !failed[pushOf[j]] {
					send = append(send, p)
					sendOf = append(sendOf, pushOf[j])
				}
			}
		}
		if len(send) == 0 {
			return
		}
		var perr error
		if canBatch {
			perr = batch.PushBatch(ctx, send)
		} else {
			for _, p := range send {
				if p.Write != nil {
					perr = both.PushWithWrite(ctx, p.AccountID, p.Receipt, *p.Write)
				} else {
					perr = s.pusher.PushToClient(ctx, p.AccountID, p.Receipt)
				}
				if perr != nil {
					break
				}
			}
		}
		if perr != nil {
			// In one step a client receipt carried its record work, and
			// nothing of it went; else the record work went before.
			for _, k := range sendOf {
				fail(k, perr, !oneStep)
			}
		}
	}
}
