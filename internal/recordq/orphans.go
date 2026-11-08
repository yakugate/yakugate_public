package recordq

import (
	"context"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/yakugate/yakugate/internal/dal"
)

// What a role used to write to the record store itself — an orphan, a
// filed receipt applied after all, a lookup with no message — is a task in
// worker mode (Saleem, 28 Oct 2026: "every db work is always queue no matter
// from any role of the gateway"). The methods carry the record store's own
// names, so the client stands in for the store where a role wrote them.
// A task the queue does not take is held and put again (kept.go).

// WriteOrphanMO queues an incoming message no route owns.
func (c *Client) WriteOrphanMO(ctx context.Context, vendorID int64, source, destination, body, enc, mcc, mnc, reason string) error {
	return c.orphan(ctx, ulid.Make().String(), dal.OrphanRow{Kind: "mo", VendorID: vendorID, Source: source,
		Destination: destination, Body: body, Encoding: enc, MCC: mcc, MNC: mnc, Reason: reason, At: time.Now()})
}

// WriteOrphanDLR queues a receipt that matched no message. It rides the lane
// of its vendor's message id, so copies of one receipt keep their order.
func (c *Client) WriteOrphanDLR(ctx context.Context, vendorID int64, vendorMsgID, status, errorCode, raw, reason string) error {
	id := vendorMsgID
	if id == "" {
		id = ulid.Make().String()
	}
	return c.orphan(ctx, id, dal.OrphanRow{Kind: "dlr", VendorID: vendorID, VendorMsgID: vendorMsgID, Status: status,
		ErrorCode: errorCode, Raw: raw, Reason: reason, At: time.Now()})
}

func (c *Client) orphan(ctx context.Context, id string, r dal.OrphanRow) error {
	t, err := task(KindOrphan, id, r)
	if err != nil {
		return err
	}
	return c.putOrKeep(ctx, t)
}

// MarkOrphanMatched queues the note that a filed receipt was applied to msgID.
func (c *Client) MarkOrphanMatched(ctx context.Context, table string, id int64, msgID string) error {
	t, err := task(KindOrphanMatched, msgID, dal.OrphanMatch{Table: table, ID: id, MsgID: msgID})
	if err != nil {
		return err
	}
	return c.putOrKeep(ctx, t)
}

// WriteLookup queues a lookup that has no message.
func (c *Client) WriteLookup(ctx context.Context, r dal.LookupRow) error {
	if r.At.IsZero() {
		r.At = time.Now()
	}
	t, err := task(KindLookup, ulid.Make().String(), r)
	if err != nil {
		return err
	}
	return c.putOrKeep(ctx, t)
}
