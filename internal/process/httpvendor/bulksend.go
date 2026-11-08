package httpvendor

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/vendors/httpadapter"
)

// Bulk sending: a bulk vendor's claim topped up to a full chunk, split into
// vendor-sized chunks, and one vendor call per chunk.

// bulkGather is how long a bulk vendor's worker waits for more messages to
// fill a chunk, and bulkGatherStep how often it looks. Variables for tests.
var (
	bulkGather     = time.Second
	bulkGatherStep = 100 * time.Millisecond
)

// gatherBulk tops a bulk vendor's claim up to one full chunk (QA EP-011).
//
// The worker comes back for more the moment a message lands, so messages
// submitted a few milliseconds apart — four sends in the same second — were
// each claimed alone and each went in a call of its own; Bulk never grouped
// anything outside a single fan-out request. Now a claim smaller than the
// chunk waits up to bulkGather for the rest, claiming what arrives. A full
// queue never waits: its first claim already fills the chunk.
func (p *Process) gatherBulk(ctx context.Context, v vendorAdapter, msgs []model.Message, claim int) []model.Message {
	chunk := v.ad.BulkChunkSize()
	if !v.ad.BulkMode() || chunk < 2 || len(msgs) >= chunk || len(msgs) >= claim {
		return msgs
	}
	deadline := time.Now().Add(bulkGather)
	for len(msgs) < chunk && len(msgs) < claim && time.Now().Before(deadline) {
		t := time.NewTimer(bulkGatherStep)
		select {
		case <-ctx.Done():
			t.Stop()
			return msgs
		case <-t.C:
		}
		more, err := p.take(ctx, v, claim-len(msgs))
		if err != nil {
			break
		}
		msgs = append(msgs, stampSender(more)...)
	}
	return msgs
}

// bulkChunks splits single-part messages into vendor-sized chunks (GW-15
// slice 1, Hadal submissionMode BULK). A chunk may only carry ONE request
// body/sender — the vendor call renders the text once with the recipients
// joined — so messages group by (sender, text) first; a broadcast fan-out
// (one request, many recipients) collapses to one call per chunk, while mixed
// traffic degrades gracefully to chunks of one. textOf is the text the
// vendor will be sent (nil = the stored body).
func bulkChunks(v vendorAdapter, msgs []model.Message, textOf func(model.Message) string) [][]model.Message {
	if len(msgs) == 0 {
		return nil
	}
	if textOf == nil {
		textOf = func(m model.Message) string { return m.Body }
	}
	groups := map[string][]model.Message{}
	var order []string
	for _, m := range msgs {
		k := m.Sender + "\x00" + textOf(m)
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], m)
	}
	size := v.ad.BulkChunkSize()
	if size < 1 {
		size = 1
	}
	var chunks [][]model.Message
	for _, k := range order {
		g := groups[k]
		for start := 0; start < len(g); start += size {
			end := start + size
			if end > len(g) {
				end = len(g)
			}
			chunks = append(chunks, g[start:end])
		}
	}
	return chunks
}

// deliverChunk performs one bulk vendor call and finalizes every message in it.
//
// together, when set, is the round's filer: a call with few messages hands it
// what the vendor answered, to be filed with the round's other calls.
func (p *Process) deliverChunk(ctx context.Context, v vendorAdapter, chunk []model.Message, together *filer) {
	if len(chunk) >= fileTogetherUnder {
		together = nil // a full batch by itself: filed here, as before
	}
	plain, err := p.plainBody(ctx, chunk[0].Body)
	if err != nil {
		p.log.ForVendor(v.cid).Warnf("decrypt", "decrypt bulk lead %s: %v (the chunk is tried again later)", chunk[0].ID, err)
		p.tryAgain(ctx, v, chunk...)
		return
	}
	batch := make([]map[string]string, len(chunk))
	for i, m := range chunk {
		batch[i] = map[string]string{
			"to": m.Destination, "from": m.Sender, "sender": m.Sender,
			"text": plain, "msgid": m.ID, "mcc": m.MCC, "mnc": m.MNC,
			"priority": strconv.Itoa(m.Priority), "encoding": m.Encoding,
			"segments": strconv.Itoa(m.Segments),
			"ap1":      m.AP1, "ap2": m.AP2, "apg1": m.APG1, "apg2": m.APG2,
		}
	}
	// [SCOPE P13-CAPTURE] The bulk path was invisible: the capture calls added
	// when this feature landed sit in deliverOne only, so a vendor onboarded in
	// bulk mode produced nothing at all. Every message in the chunk may belong
	// to a different account, so each account watching gets its own record and
	// the vendor's capture gets one line for the whole chunk.
	vendCap, vendCapOn := logging.ActiveForVendor(v.cid)
	for i, m := range chunk {
		if c, on := logging.ActiveForAccountID(m.AccountID); on {
			logging.CaptureTo(c, p.log, "vendor_request", "sending to vendor (in a bulk chunk)", logging.Fields{
				"messageId": m.ID, "vendor": v.cid, "to": m.Destination, "from": m.Sender,
				"body": plain, "chunkSize": len(chunk), "chunkIndex": i + 1,
			})
		}
	}
	if vendCapOn {
		logging.CaptureTo(vendCap, p.log, "vendor_request", "sending a bulk chunk to vendor", logging.Fields{
			"vendor": v.cid, "chunkSize": len(chunk), "leadMessageId": chunk[0].ID,
		})
	}
	// Counted for monitoring exactly as deliverOne counts a single send, and
	// timed around the vendor call only. This path used to record nothing, so
	// a bulk vendor reported `vendors: []` while it delivered 44,936 messages
	// in the 2026-09-10 load run — the same omission the P13 capture comment
	// above describes, made a second time.
	n := int64(len(chunk))
	p.stats.noteSubmit(v.cid, v.ad.SubmitURL(), chunk[0].Encoding, n)
	startedAt := time.Now()
	// The call as it went is kept with each message's answer (hot/0077).
	callCtx, calls := httpadapter.WithCallRecorder(ctx)
	ids, read, sendErr := v.ad.SendBulkCall(callCtx, batch)
	answeredAt := time.Now()
	took := answeredAt.Sub(startedAt).Milliseconds()
	sent := p.sendFactsOf(ctx, calls, chunk, startedAt, answeredAt)
	answers := httpAnswers(v, chunk, ids, sendErr, sent)
	p.noteHealth(v, sendErr)
	if sendErr != nil {
		p.stats.noteResult(v.cid, false, took, sendErr.Error(), n)
	} else {
		p.stats.noteResult(v.cid, true, took, "", n)
	}
	captureBulkResult := func(msg string, extra logging.Fields) {
		for _, m := range chunk {
			if c, on := logging.ActiveForAccountID(m.AccountID); on {
				f := logging.Fields{"messageId": m.ID, "vendor": v.cid}
				for k, val := range extra {
					f[k] = val
				}
				logging.CaptureTo(c, p.log, "vendor_response", msg, f)
			}
		}
		if vendCapOn {
			f := logging.Fields{"vendor": v.cid, "chunkSize": len(chunk)}
			for k, val := range extra {
				f[k] = val
			}
			logging.CaptureTo(vendCap, p.log, "vendor_response", msg, f)
		}
	}
	if sendErr != nil {
		kind := "vendor refused the chunk"
		if !errors.Is(sendErr, httpadapter.ErrPermanent) {
			kind = "chunk failed, to be tried again"
		}
		captureBulkResult(kind, logging.Fields{"error": sendErr.Error()})
	} else {
		captureBulkResult("vendor accepted the chunk", logging.Fields{"vendorMsgIds": len(ids)})
	}
	// The vendor has answered: from here a record that cannot be written is
	// tried again until this time, never left to the lease at once.
	recordUntil := time.Now().Add(recordRetryFor)
	if sendErr != nil {
		if errors.Is(sendErr, httpadapter.ErrPermanent) {
			p.log.ForVendor(v.cid).Errorf("send", "bulk send (%d msgs): %v (permanent — failing chunk)", len(chunk), sendErr)
			refused(chunk, sendErr)
			if chunk = p.retryRefused(ctx, v, chunk, sendErr, sent); len(chunk) == 0 {
				return
			}
			if together != nil {
				together.addCall(chunk, nil, rejectionCode(sendErr), answers)
				return
			}
			// Filed in a few writes for the whole chunk (chunk.go).
			p.fileChunk(ctx, v, chunk, nil, rejectionCode(sendErr), recordUntil, answers)
			return
		}
		// "Not now": each message goes on to the next vendor of its round
		// through the re-route inbox, exactly as a single send does below
		// (Saleem 2026-09-30: retries are rounds of Main and backups). Until
		// 2026-10-06 a vendor that sends in bulk skipped this — its messages
		// just sat out their lease (30 s, then 60, 120 …) and came back to the same vendor, so the
		// rule's retry settings (rounds, first wait, backoff) were never
		// read for it and a message could wait minutes behind a vendor that
		// kept saying "later" (load test E2). What cannot be handed on (a
		// long message sent as parts, a queue that cannot take it) is tried
		// again here after its gap (handOn).
		failed(chunk, sendErr)
		// "Not now": a vendor that charges on submission charges this try.
		for _, m := range chunk {
			p.bill.VendorAnswered(ctx, &m, v.id, false)
		}
		p.writeLooseAnswers(ctx, v.cid, answers)
		left := p.handOn(ctx, v, chunk, chunk[0].ErrorCode)
		if left > 0 {
			p.log.ForVendor(v.cid).Warnf("send", "bulk send (%d msgs): %v (%d tried again after their gap)", len(chunk), sendErr, left)
		} else {
			p.log.ForVendor(v.cid).Warnf("send", "bulk send (%d msgs): %v (handed to the next vendor of their round)", len(chunk), sendErr)
		}
		return
	}
	// One id for the call files the call as one group under it; a list of
	// ids that cannot be matched files each message under our own id.
	p.noteBulkIDs(v, chunk, ids, read)
	// Every message of the chunk is filed in a few writes for all of them
	// (chunk.go): one record write, one queue acknowledgement, one push of
	// the client receipts. They used to be filed one after another — six
	// round trips a message — which made Bulk slower than single sends.
	if together != nil {
		together.addCall(chunk, ids, "", answers)
		// A call that carried one message is a single send in all but name:
		// it gets no line of its own (a thousand a second otherwise). The
		// vendor's log says once a minute that Bulk is grouping nothing
		// (noteBulkAlone).
		if len(chunk) > 1 {
			p.log.ForVendor(v.cid).Infof("send", "bulk: %d message(s) in one call", len(chunk))
		}
		return
	}
	if !p.fileChunk(ctx, v, chunk, ids, "", recordUntil, answers) {
		return
	}
	p.log.ForVendor(v.cid).Infof("send", "bulk: %d message(s) in one call", len(chunk))
}
