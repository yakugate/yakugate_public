package daltest

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/model"
)

// The record store's contract (dal/recordstore.go): the key outcomes every
// DAL that keeps records must give the same way — an accepted message's
// record, a vendor's taking of it, the records written at their end, a
// receipt found and closing its message once, the messages waiting for a
// receipt by day, and the orphans. a and b are the store opened twice, as two
// processes would have it: what one writes the other reads.
//
// It runs with the waiting messages kept in the SQL database
// (dal.OpenMessagesInSQL, a gateway with no Redis) and on ids of its own: a
// company, an account and a vendor no other row has.

// RecordStoreOpts is what the DAL's harness gives the suite.
type RecordStoreOpts struct {
	// Cleanup clears what the run wrote for its company, account and vendor
	// (their day tables, their waiting and orphan rows).
	Cleanup func(company, account, vendor int64)
}

// RunRecordStore is the record store's contract.
func RunRecordStore(t *testing.T, a, b dal.RecordStore, o RecordStoreOpts) {
	ctx := context.Background()
	if !dal.OpenMessagesInSQL() {
		t.Fatal("the suite runs with the waiting messages in the SQL database: dal.SetOpenMessagesInSQL(true)")
	}
	seed := 8_000_000 + time.Now().UnixNano()%1_000_000
	company, account, vendor := seed, seed, seed
	if o.Cleanup != nil {
		t.Cleanup(func() { o.Cleanup(company, account, vendor) })
	}
	now := time.Now().UTC()
	msg := func(st model.Status, ago time.Duration) model.Message {
		id := ulid.MustNew(ulid.Timestamp(now.Add(-ago)), rand.Reader).String()
		return model.Message{ID: id, RequestID: "req-" + id, AccountID: account, CompanyID: company,
			Sender: "SENDER", Destination: "923001234567", Body: "hello", Encoding: "gsm7", Segments: 1,
			VendorID: vendor, Transport: model.TransportSMPP, Ingress: model.TransportHTTP,
			Status: st, ReceiptWaitHours: 1, Attempts: 1}
	}
	answer := func(m model.Message, vmid string) model.SubmitAnswer {
		return model.SubmitAnswer{MsgID: m.ID, AccountID: account, CompanyID: company, Part: 1, Parts: 1, Attempt: 1,
			VendorID: vendor, VendorMsgID: vmid, VendorProtocol: "smpp", HasStatus: true, Result: model.AnswerAccepted,
			SentAt: now, AnsweredAt: now}
	}
	status := func(t *testing.T, s dal.RecordStore, m model.Message) dal.MsgStatus {
		t.Helper()
		st, ok, err := s.StatusByIDFresh(ctx, m.Ref())
		if err != nil || !ok {
			t.Fatalf("status of %s: found=%v %v", m.ID, ok, err)
		}
		return st
	}
	vmid := func(n int) string { return fmt.Sprintf("V%d-%d", seed, n) }

	t.Run("an accepted message is written once and read by the other process", func(t *testing.T) {
		m := msg(model.StatusQueued, 0)
		m.IdempotencyKey = fmt.Sprintf("ref-%d", seed)
		for i := 0; i < 2; i++ {
			if err := a.WriteAccepted(ctx, []model.Message{m}); err != nil {
				t.Fatalf("write %d: %v", i+1, err)
			}
		}
		st := status(t, b, m)
		if st.MsgID != m.ID || st.Status != "queued" || st.AccountID != account || st.RequestID != m.RequestID {
			t.Fatalf("status %+v, want queued, account %d, request %s", st, account, m.RequestID)
		}
		if old, ok, err := b.StatusByID(ctx, m.Ref()); err != nil || !ok || old.Status != "queued" {
			t.Fatalf("status (may be a moment old): %+v %v %v", old, ok, err)
		}
		byRef, ok, err := b.StatusByReference(ctx, account, company, m.IdempotencyKey)
		if err != nil || !ok || byRef.MsgID != m.ID || byRef.Reference != m.IdempotencyKey {
			t.Fatalf("by the client's reference: %+v %v %v", byRef, ok, err)
		}
		if _, ok, err := b.StatusByID(ctx, model.RecordRef{MsgID: ulid.Make().String(), CompanyID: company}); err != nil || ok {
			t.Fatalf("an unknown message: found=%v %v", ok, err)
		}
	})

	sent := msg(model.StatusSent, 0)
	t.Run("a vendor's taking makes it sent, written once", func(t *testing.T) {
		if err := a.WriteAccepted(ctx, []model.Message{sent}); err != nil {
			t.Fatal(err)
		}
		rec := dal.TerminalRecord{Msg: sent, VendorMsgID: vmid(1), AckAt: now, Answers: []model.SubmitAnswer{answer(sent, vmid(1))}}
		for i := 0; i < 2; i++ {
			if err := a.WriteBatch(ctx, []dal.TerminalRecord{rec}); err != nil {
				t.Fatalf("write %d: %v", i+1, err)
			}
		}
		if st := status(t, b, sent); st.Status != "sent" || st.VendorMsgID != vmid(1) {
			t.Fatalf("status %+v, want sent with the vendor's id %s", st, vmid(1))
		}
	})

	t.Run("records written at their end read as their end", func(t *testing.T) {
		done := msg(model.StatusDelivered, 0)
		refused := msg(model.StatusFailed, 0)
		recs := []dal.TerminalRecord{
			{Msg: done, VendorMsgID: vmid(2), AckAt: now, Answers: []model.SubmitAnswer{answer(done, vmid(2))}},
			{Msg: refused, ErrorCode: "SMPP_45"},
		}
		for i := 0; i < 2; i++ {
			if err := a.WriteTerminalBatch(ctx, recs); err != nil {
				t.Fatalf("write %d: %v", i+1, err)
			}
		}
		if st := status(t, b, done); st.Status != "delivered" {
			t.Fatalf("delivered record reads %+v", st)
		}
		if st := status(t, b, refused); st.Status != "failed" {
			t.Fatalf("refused record reads %+v", st)
		}
	})

	t.Run("a receipt finds its message and closes it once", func(t *testing.T) {
		id, acct, _, _, _, _, _, _, ok, err := b.ResolveVendorMsgID(ctx, vendor, vmid(1))
		if err != nil || !ok || id != sent.ID || acct != account {
			t.Fatalf("resolve: %s %d ok=%v %v, want %s", id, acct, ok, err, sent.ID)
		}
		match, err := b.ResolveReceipt(ctx, []int64{vendor}, vmid(1), "")
		if err != nil || len(match.MsgIDs) != 1 || match.MsgIDs[0] != sent.ID {
			t.Fatalf("resolve receipt: %+v %v", match, err)
		}
		waiting, err := b.SentAwaitingDLRPage(ctx, vendor, "", 100)
		if err != nil || !hasPending(waiting, sent.ID) {
			t.Fatalf("waiting for a poll: %+v %v", waiting, err)
		}
		entry := `{"stat":"DELIVRD","raw":"id:` + vmid(1) + ` stat:DELIVRD"}`
		mine, err := a.CloseWithReceipt(ctx, sent.Ref(), model.StatusDelivered, vmid(1), "", entry, true)
		if err != nil || !mine {
			t.Fatalf("the first receipt closes it: mine=%v %v", mine, err)
		}
		mine, err = b.CloseWithReceipt(ctx, sent.Ref(), model.StatusDelivered, vmid(1), "", entry, true)
		if err != nil || mine {
			t.Fatalf("a second receipt only records: mine=%v %v", mine, err)
		}
		if st := status(t, a, sent); st.Status != "delivered" {
			t.Fatalf("closed message reads %+v", st)
		}
		if _, _, _, _, _, _, _, _, ok, err := b.ResolveVendorMsgID(ctx, vendor, vmid(1)); err != nil || ok {
			t.Fatalf("a closed message still matches: ok=%v %v", ok, err)
		}
		if waiting, err := b.SentAwaitingDLRPage(ctx, vendor, "", 100); err != nil || hasPending(waiting, sent.ID) {
			t.Fatalf("a closed message still waits for a poll: %+v %v", waiting, err)
		}
		_, err = a.CloseWithReceipt(ctx, model.RecordRef{MsgID: ulid.Make().String(), CompanyID: company}, model.StatusDelivered, "x", "", entry, true)
		if !errors.Is(err, dal.ErrNoRows) {
			t.Fatalf("a receipt for no record: %v, want ErrNoRows", err)
		}
		if dal.RecordAway(err) {
			t.Fatal("a receipt for no record reads as the database away: its task would hold the lane")
		}
	})

	t.Run("the messages waiting for a receipt, by day", func(t *testing.T) {
		days := dal.AwaitDays(time.Hour)
		late := msg(model.StatusSent, 5*time.Minute)
		if err := a.WriteAccepted(ctx, []model.Message{late}); err != nil {
			t.Fatal(err)
		}
		rec := dal.TerminalRecord{Msg: late, VendorMsgID: vmid(3), AckAt: now, Answers: []model.SubmitAnswer{answer(late, vmid(3))}}
		if err := a.WriteBatch(ctx, []dal.TerminalRecord{rec}); err != nil {
			t.Fatal(err)
		}
		rows, next, err := b.AwaitingReceiptInDays(ctx, company, account, time.Minute, dal.AwaitCursor{}, 100, days)
		if err != nil {
			t.Fatal(err)
		}
		var got *dal.AwaitingReceipt
		for i := range rows {
			if rows[i].MsgID == late.ID {
				got = &rows[i]
			}
		}
		if got == nil || got.VendorID != vendor || got.VendorMsgID != vmid(3) || got.CompanyID != company {
			t.Fatalf("waiting by day: %+v, want %s taken by %d as %s", rows, late.ID, vendor, vmid(3))
		}
		if next.MsgID == "" {
			t.Fatal("no cursor after a page with rows")
		}
		if again, _, err := b.AwaitingReceiptInDays(ctx, company, account, time.Minute, next, 100, days); err != nil || hasAwaiting(again, late.ID) {
			t.Fatalf("read on from the cursor: %+v %v", again, err)
		}
		polls, err := b.SentAwaitingDLRInDays(ctx, vendor, "", 100, days)
		if err != nil || !hasPending(polls, late.ID) {
			t.Fatalf("waiting for a poll by day: %+v %v", polls, err)
		}
		// Sent just now by the clock of open_msgs: younger than a minute.
		if young, _, err := b.AwaitingReceiptOlderAfter(ctx, account, time.Minute, dal.AwaitCursor{}, 100); err != nil || hasAwaiting(young, late.ID) {
			t.Fatalf("a message sent a moment ago is listed as older than a minute: %+v %v", young, err)
		}
		entry := `{"stat":"UNDELIV"}`
		if mine, err := a.CloseWithReceipt(ctx, late.Ref(), model.StatusFailed, vmid(3), "", entry, true); err != nil || !mine {
			t.Fatalf("close: mine=%v %v", mine, err)
		}
		if rows, _, err := b.AwaitingReceiptInDays(ctx, company, account, time.Minute, dal.AwaitCursor{}, 100, days); err != nil || hasAwaiting(rows, late.ID) {
			t.Fatalf("a closed message still waits by day: %+v %v", rows, err)
		}
		if polls, err := b.SentAwaitingDLRInDays(ctx, vendor, "", 100, days); err != nil || hasPending(polls, late.ID) {
			t.Fatalf("a closed message still waits for a poll by day: %+v %v", polls, err)
		}
	})

	t.Run("an orphan is offered until it is matched", func(t *testing.T) {
		since := time.Now().Add(-time.Minute)
		lost := fmt.Sprintf("O%d-1", seed)
		if err := a.WriteOrphanDLR(ctx, vendor, lost, "DELIVRD", "", "id:"+lost, "no message"); err != nil {
			t.Fatal(err)
		}
		mine := orphansOf(t, b, since, vendor, lost)
		if len(mine) != 1 || mine[0].Status != "DELIVRD" || mine[0].Table == "" || mine[0].ID == 0 {
			t.Fatalf("filed orphan: %+v", mine)
		}
		if err := a.MarkOrphanMatched(ctx, mine[0].Table, mine[0].ID, sent.ID); err != nil {
			t.Fatal(err)
		}
		if left := orphansOf(t, b, since, vendor, lost); len(left) != 0 {
			t.Fatalf("a matched orphan is offered again: %+v", left)
		}
		twice := fmt.Sprintf("O%d-2", seed)
		row := dal.OrphanRow{Kind: "dlr", VendorID: vendor, VendorMsgID: twice, Status: "UNDELIV", Raw: "id:" + twice,
			Reason: "no message", At: time.Now().UTC().Truncate(time.Millisecond)}
		for i := 0; i < 2; i++ {
			if err := a.WriteOrphans(ctx, []dal.OrphanRow{row}, true); err != nil {
				t.Fatalf("write %d: %v", i+1, err)
			}
		}
		if got := orphansOf(t, b, since, vendor, twice); len(got) != 1 {
			t.Fatalf("an orphan task seen twice filed %d rows, want 1", len(got))
		}
	})

	t.Run("the days a wait spans", func(t *testing.T) {
		for _, c := range []struct {
			wait time.Duration
			want int
		}{{0, 4}, {time.Hour, 2}, {25 * time.Hour, 3}} {
			if got := dal.AwaitDays(c.wait); got != c.want {
				t.Errorf("AwaitDays(%s) = %d, want %d", c.wait, got, c.want)
			}
		}
	})

	t.Run("what a failed write says", func(t *testing.T) {
		if dal.RecordAway(nil) || dal.RecordAway(dal.ErrNoRows) || dal.RecordAway(fmt.Errorf("close: %w", dal.ErrNoRows)) {
			t.Fatal("no error, or no record yet, read as the database away")
		}
		if !dal.RecordAway(errors.New("connection refused")) {
			t.Fatal("an error no DAL knows must read as away")
		}
		if dal.RecordDuplicate(errors.New("Duplicate entry")) {
			t.Fatal("a plain error read as a duplicate key")
		}
	})
}

func hasPending(rows []dal.PendingDLR, id string) bool {
	for _, r := range rows {
		if r.MsgID == id {
			return true
		}
	}
	return false
}

func hasAwaiting(rows []dal.AwaitingReceipt, id string) bool {
	for _, r := range rows {
		if r.MsgID == id {
			return true
		}
	}
	return false
}

// orphansOf is the receipt orphans of vendor with the vendor id given that
// are still offered for a match.
func orphansOf(t *testing.T, s dal.RecordStore, since time.Time, vendor int64, vendorMsgID string) []model.OrphanDLR {
	t.Helper()
	all, err := s.PendingOrphanDLRs(context.Background(), since, 1000)
	if err != nil {
		t.Fatal(err)
	}
	var out []model.OrphanDLR
	for _, o := range all {
		if o.VendorID == vendor && o.VendorMsgID == vendorMsgID {
			out = append(out, o)
		}
	}
	return out
}
