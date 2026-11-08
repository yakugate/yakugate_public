// What the vendor sends in deliver_sm: receipts and MO, and long MO waiting
// for their parts.

package smppvendor

import (
	"context"
	"math/big"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/oklog/ulid/v2"
	dlrsvc "github.com/yakugate/yakugate/internal/dlr"
	mosvc "github.com/yakugate/yakugate/internal/mo"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/lib/smpp/pdu"
	"github.com/yakugate/yakugate/lib/smpp/session"
)

// onDeliverSM handles an inbound deliver_sm: branch on the esm_class
// message-type bits — a receipt (bit 0x04) is a DLR (correlate + update);
// anything else is a mobile-originated message routed to the owning client —
// and answer it once what it carries is kept (deliverAnswer). due counts the
// session's deliver_sm not yet answered. [P5/P6]
func (p *Process) onDeliverSM(ctx context.Context, v vendorConn, key sessionKey, cl *session.Client, pd *pdu.PDU, due *atomic.Int64) {
	// Message-id transform and status words apply to the next receipt.
	v = p.withLive(v)
	answer := newAnswer(cl, pd.Header.SequenceNumber, due)
	b, d, err := pdu.DecodeDeliverSM(pd.Body)
	if err != nil || b == nil {
		// Nothing in it can be read, now or when sent again.
		answer.send(true)
		return
	}
	isReceipt := b.ESMClass&0x04 != 0
	// Counted on ARRIVAL, before correlation: this is "is the vendor sending
	// receipts at all". Whether each one matched a message is the DLR
	// service's question, and an unmatched receipt still proves the vendor
	// is talking.
	if isReceipt {
		p.countReceipt(key)
	} else {
		p.countMO(key)
	}
	// The database work goes to the receipt/MO workers (inbound.go), so this
	// reader returns at once to the vendor's submit_sm_resp. Detached from the
	// session's context: a shutdown must finish the write rather than cancel
	// it, and the worker answers the deliver_sm when it has.
	wctx := context.WithoutCancel(ctx)
	if isReceipt && d == nil {
		answer.send(true) // a receipt with no text to read anything from
		return
	}
	if isReceipt && d != nil {
		// The vendor's id for the message: the receipted_message_id field
		// first, the receipt text after (Saleem 2026-10-14). Some vendors
		// report the id differently in the DLR than in the original
		// submit_sm_resp (hex vs decimal, extra/missing padding) —
		// msgid_transform reconciles it back to the form we stored at
		// submit time, per vendor, whichever place it came from; the usual
		// other forms are tried too (dlr.msgIDCandidates). [SCOPE P3 item 3.2]
		id, other := d.ID()
		if id == "" {
			answer.send(true) // no id: it can match nothing, now or when sent again
			return
		}
		vmid := applyMsgIDTransform(id, v.msgidTransform)
		alt := ""
		if other != "" {
			alt = applyMsgIDTransform(other, v.msgidTransform)
		}
		raw := d.Text // the receipt's octets, or its text unpacked when the vendor packed it
		stat, errCode := d.Word(), d.Err
		arrived := time.Now()
		// The handset tells apart two messages of one company that two of
		// its vendors gave the same id (dlr groups.go).
		in := dlrsvc.ReceiptIn{
			VendorID: v.id, VendorMsgID: vmid, AltID: alt, Number: d.Number,
			RawStatus: stat, ErrorCode: errCode, Raw: raw, StatusMap: v.statusMap, Arrived: arrived,
		}
		// Keyed by vendor message id: one message's receipts stay in order.
		// Applied with the other receipts waiting on that worker
		// (inboundPool.work → applyReceipts → dlr.Service.ApplyMany).
		p.inbound.submitReceipt(vmid, inboundReceipt{cid: v.cid, in: in, answer: answer}, func() {
			// Centralized DLR application (D-023): correlate + raw_dlr + status + client push.
			// The time it sat in the worker's queue counts towards the
			// correlation grace (ReceiptIn.Arrived): a receipt that has
			// already waited is not made to wait again.
			p.applyReceipts([]inboundReceipt{{cid: v.cid, in: in, answer: answer}})
		})
		return
	}
	// MO (mobile-originated): source=subscriber MSISDN, dest=client shortcode. [P6]
	if p.mo == nil {
		answer.send(true) // this process takes no MO (no config store)
		return
	}
	// A part of a multi-part message carries a concatenation header (UDH, or
	// SAR fields). Its header is stripped — it used to be decoded into the
	// text — and the part waits in the MO assembly buffer until its group is
	// complete; the joined message is routed and delivered ONCE (2026-09-14).
	ref, total, seq, payload, isPart := b.Concat()
	enc := pdu.EncodingFromDataCoding(b.DataCoding)
	multi := isPart && total > 1 && p.moParts != nil
	msg := b.MessageBytes()
	in := mosvc.Inbound{
		VendorID: v.id, Source: b.SourceAddr, Dest: b.DestinationAddr,
		Text: moText(msg, len(msg)-len(payload), b.DataCoding, v.gsm7Packing, multi), Encoding: enc,
		Transport: model.TransportSMPP}
	if multi {
		part := model.Message{ID: ulid.Make().String(), Direction: model.DirectionMO, VendorID: v.id,
			Sender: in.Source, Destination: in.Dest, Body: in.Text, Encoding: enc,
			ConcatRef: ref, ConcatTotal: total, ConcatSeq: seq}
		p.inbound.submit(in.Source, func() {
			complete, whole, err := p.moParts.AddPart(wctx, &part)
			if err != nil {
				// Not buffered anywhere: stage the part on its own rather
				// than lose it (its text, at least, is clean now).
				p.log.Printf("mo part %d/%d from %s not buffered (%v) — staged on its own", seq, total, in.Source, err)
				in.Text = pdu.MendSplitEscapes(in.Text)
				answer.send(p.ingestMO(wctx, v, in))
				return
			}
			if !complete {
				answer.send(true) // kept in the assembly buffer
				return
			}
			// An extension character a part boundary cut in two is one
			// again (pdu.MendSplitEscapes).
			in.Text, in.Segments = pdu.MendSplitEscapes(whole.Body), total
			answer.send(p.ingestMO(wctx, v, in))
		})
		return
	}
	// Keyed by subscriber: one person's messages reach the client in order.
	p.inbound.submit(in.Source, func() { answer.send(p.ingestMO(wctx, v, in)) })
}

// applyMsgIDTransform normalizes a DLR's vendor message id per the vendor's
// msgid_transform column ("" | "none" | "hex2dec" | "dec2hex" | "trim:<n>").
// Some SMSCs report the id in a different base, or with padding trimmed/added,
// versus what they returned at submit_sm_resp time; this reconciles the DLR's
// id back to that stored form so ResolveVendorMsgID still matches. Malformed
// input (doesn't parse as the expected base) passes through unchanged rather
// than corrupting a valid id.
func applyMsgIDTransform(id, transform string) string {
	transform = strings.ToLower(strings.TrimSpace(transform))
	switch {
	case transform == "" || transform == "none":
		return id
	// Any length (2026-10-12): an id longer than 16 hex digits or 20 decimal
	// ones did not fit in 64 bits and was passed on unconverted. A 0x prefix
	// is read. dec2hex keeps writing lower case; the receipt service tries
	// the other case too (dlr.msgIDCandidates).
	case transform == "hex2dec":
		h := strings.TrimSpace(id)
		if len(h) > 2 && (h[:2] == "0x" || h[:2] == "0X") {
			h = h[2:]
		}
		if n, ok := new(big.Int).SetString(h, 16); ok && h != "" {
			return n.Text(10)
		}
	case transform == "dec2hex":
		if n, ok := new(big.Int).SetString(strings.TrimSpace(id), 10); ok && n.Sign() >= 0 {
			return n.Text(16)
		}
	case strings.HasPrefix(transform, "trim:"):
		if n, err := strconv.Atoi(strings.TrimPrefix(transform, "trim:")); err == nil && n > 0 && n < len(id) {
			return id[:len(id)-n]
		}
	}
	return id
}

// moText is an incoming message's text (one part's, when part): msg is the
// whole short_message or message_payload, its text from octet textAt (after
// a UDH). data_coding 0 is the GSM 03.38 alphabet (Saleem, 19 Oct 2026: "use
// smpp defaults"), and the vendor's gsm7_packing says how it comes, as it says
// how the vendor is sent it: "packed", 7-bit, or "unpacked", one septet in
// each octet (Saleem, 19 Oct 2026: "the setting separates packing or
// unpacking"). It was guessed from the octets until then, and a short packed
// MO whose octets are all under 0x80 was read unpacked: "Ab" as "A1". A part
// keeps a GSM-7 escape that ends it, for pdu.MendSplitEscapes once the parts
// are joined.
func moText(msg []byte, textAt int, dc byte, packing string, part bool) string {
	return pdu.ReadText(msg, textAt, dc, !onePerOctet(packing), part)
}

// ingestMO routes and stages one incoming message (joined, if it came in
// parts). kept is false when it could not be staged: its deliver_sm is
// answered so that the vendor sends it again. An MO no rule owns is kept as
// an orphan (mo.Service.Ingest) and counts as kept.
func (p *Process) ingestMO(wctx context.Context, v vendorConn, in mosvc.Inbound) (kept bool) {
	id, staged, err := p.mo.Ingest(wctx, in)
	if err != nil {
		p.log.Printf("mo ingest vendor=%d src=%s dst=%s: %v — not kept; the vendor is told to send it again", v.id, in.Source, in.Dest, err)
		return false
	}
	if !staged {
		p.log.Printf("mo unrouted vendor=%d dst=%s (no MO route)", v.id, in.Dest)
		return true
	}
	p.log.Printf("mo staged %s vendor=%d %s->%s", id, v.id, in.Source, in.Dest)
	return true
}

// reapMOParts records incoming multi-part messages whose parts did not all
// arrive within the assembly wait: dropped and recorded, never delivered in
// part (PO 2026-09-14, the same rule as MT). Instance-safe: a group is
// claimed atomically, so two smpp-vendor processes never record it twice.
func (p *Process) reapMOParts(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		groups, err := p.moParts.ReleaseExpired(ctx)
		if err != nil {
			p.log.Printf("mo assembly reaper: %v", err)
			continue
		}
		for _, g := range groups {
			in := mosvc.Inbound{VendorID: g.VendorID, Source: g.Sender, Dest: g.Destination,
				Text: pdu.MendSplitEscapes(g.Body), Encoding: g.Encoding, Transport: model.TransportSMPP}
			if err := p.mo.RecordIncomplete(ctx, p.moRec(), in, len(g.PartIDs), partsExpected(g)); err != nil {
				p.log.Printf("record incomplete mo from %s: %v", g.Sender, err)
			}
		}
	}
}

// partsExpected is how many parts an expired MO group was waiting for.
func partsExpected(g *model.Message) int {
	if g.ConcatTotal > 0 {
		return g.ConcatTotal
	}
	return len(g.PartIDs)
}
