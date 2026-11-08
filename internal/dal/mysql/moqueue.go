package mysql

// The MO queue: an incoming message staged for its client, claimed, put
// off, given up and acked.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/queueflow"
)

// StageMO durably writes an accepted MO (model.Message with Direction=MO) into the
// shared mo_queue: sender=subscriber MSISDN, destination=client shortcode.
// mo_queue carries no username since the Hadal reconciliation — the account_id
// resolves the owner; consumers re-derive the username from config when they
// need a per-tenant table name. [SCOPE P6]
func (s *Store) StageMO(ctx context.Context, m model.Message) error {
	// A lock victim is staged again at once (queuelocks.go).
	_, err := s.writeOne(ctx,
		`INSERT INTO mo_queue
		 (msg_id,trace_id,vendor_id,account_id,route_id,sender,destination,body,encoding,segments,mcc,mnc,
		  queue_owner,company_id,priced,status,priority,next_attempt_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'queued',9, NOW())`,
		m.ID, orStr(m.TraceID, m.ID), m.VendorID, m.AccountID, nz(m.RouteID), m.Sender, m.Destination, m.Body,
		orStr(m.Encoding, "gsm7"), orSeg(m.Segments), nsStr(m.MCC), nsStr(m.MNC),
		nsStr(m.QueueOwner),  // the deployment that delivers it (queue/0037, D-121)
		nz(m.CompanyID),      // whose record table it is (queue/0039, D-136)
		pricedText(m.Priced)) // its price, for a record its delivery writes (queue/0048)
	if err == nil {
		queueflow.In(queueflow.MO, strconv.FormatInt(m.AccountID, 10), 1) // D-127
	}
	return err
}

// pricedText is an MO's price as mo_queue keeps it (queue/0048): JSON, NULL
// for none. A string, not bytes: JSON goes into the text of the statement.
func pricedText(p *model.Priced) any {
	if p == nil {
		return nil
	}
	b, err := json.Marshal(p)
	if err != nil {
		return nil
	}
	return string(b)
}

// pricedOf reads it back; nil for none or for a value that does not read.
func pricedOf(s string) *model.Priced {
	if s == "" {
		return nil
	}
	var p model.Priced
	if json.Unmarshal([]byte(s), &p) != nil {
		return nil
	}
	return &p
}

// ClaimMO leases up to n staged MO messages for the given accounts (the caller
// holds those binds), bumping next_attempt_at for a 60s lease. Each claim
// carries the attempt window its account's mo_channel gives the bind
// (config/0146). [SCOPE P6]
func (s *Store) ClaimMO(ctx context.Context, claims []model.MOClaim, n int) ([]model.Message, error) {
	return s.claimMO(ctx, claims, 0, n)
}

// ClaimMOStale leases MO older than minAgeSec — used by the mo role's mo_url
// poster so a live SMPP bind gets first crack. [SCOPE P6]
func (s *Store) ClaimMOStale(ctx context.Context, claims []model.MOClaim, minAgeSec, n int) ([]model.Message, error) {
	return s.claimMO(ctx, claims, minAgeSec, n)
}

func (s *Store) claimMO(ctx context.Context, claims []model.MOClaim, minAgeSec, n int) ([]model.Message, error) {
	if len(claims) == 0 {
		return nil, nil
	}
	age := ""
	if minAgeSec > 0 {
		age = fmt.Sprintf(" AND received_at < DATE_SUB(NOW(), INTERVAL %d SECOND)", minAgeSec)
	}
	// The account and attempt-window half is the receipts' own: mo_queue
	// counts its attempts the same way client_dlr does.
	scope, args := dlrClaimScope(claims)
	// status='queued' already excludes rows the retry policy abandoned — those
	// are stamped 'expired', kept as the record of what the client never got.
	own, ownArgs := s.ownerCond() // my MO and the shared ones (D-121)
	// By idx_mo_claim (queue/0047, stMOClaim): one account's queued rows in
	// id order, read until the page is full.
	q := fmt.Sprintf(moClaimHead+`
		WHERE status='queued' AND %s AND (next_attempt_at IS NULL OR next_attempt_at<=NOW())%s%s
		ORDER BY id LIMIT ?`, scope, age, own)
	args = append(append(args, ownArgs...), n)
	// Read and leased in ONE transaction, the rows locked as they are read
	// (FOR UPDATE SKIP LOCKED, READ COMMITTED: queuelocks.go), as the client
	// receipts' claim is since 18 Oct. It was a read and then a lease: under
	// mo_channel = both the smpp-server's bind and the mo role's webhook
	// could read the same MO in the same moment and each push it (retest of
	// 19 Oct 2026, rig 62: 43 and 179 MO handed to the client twice in two
	// loaded runs).
	mins := webhookFloors(claims)
	var out []model.Message
	err := s.queueTx(ctx, func(tx *sql.Tx) error {
		out = nil
		start := time.Now()
		rows, err := tx.QueryContext(ctx, q+" FOR UPDATE SKIP LOCKED", args...)
		if err != nil {
			return err
		}
		defer func() { stMOClaim.Observe(time.Since(start), int64(len(out))) }()
		var mids []any
		var accts []int64
		for rows.Next() {
			var m model.Message
			var priced string
			if err := rows.Scan(&m.ID, &m.VendorID, &m.AccountID, &m.RouteID, &m.Sender, &m.Destination,
				&m.Body, &m.Encoding, &m.Segments, &m.MCC, &m.MNC, &m.Attempts, &m.CreatedAt, &m.QueueOwner,
				&m.CompanyID, &priced); err != nil {
				rows.Close()
				return err
			}
			m.Priced = pricedOf(priced)
			m.Direction = model.DirectionMO
			// The try this claim is (D-140, 5 Nov 2026): the count the lease
			// below stores — one more, raised to the account's floor — as Redis
			// answers it. It answered the count before the claim, so a limit of
			// N meant N+1 tries here and N on Redis.
			m.Attempts = max(m.Attempts+1, mins[m.AccountID])
			out = append(out, m)
			mids = append(mids, m.ID)
			accts = append(accts, m.AccountID)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		return leaseClaimedIn(ctx, tx, stMOLease, "mo_queue", "msg_id", mids, accts, mins)
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RescheduleMO pushes one staged MO's next attempt out by the retry policy's
// delay after a failed client delivery. [P6-B1]
func (s *Store) RescheduleMO(ctx context.Context, msgID string, delay time.Duration) error {
	_, err := s.execQueue(ctx,
		"UPDATE mo_queue SET next_attempt_at=DATE_ADD(NOW(), INTERVAL ? SECOND) WHERE msg_id=?",
		int(delay.Seconds()), msgID)
	return err
}

// GiveUpMO marks one staged MO abandoned. status='expired' is a value the enum
// has always carried and nothing ever set; the row is KEPT as the record that
// this client never received this message, and 'expired' also excludes it from
// every claim. [P6-B1]
func (s *Store) GiveUpMO(ctx context.Context, msgID string) error {
	_, err := s.execQueue(ctx,
		"UPDATE mo_queue SET status='expired' WHERE msg_id=? AND status='queued'", msgID)
	return err
}

// SweepGiveUpMO abandons staged MO nobody is even trying — an account with no
// mo_url and no live bind is never claimed, so without this backstop its rows
// would age forever and the time limit would never fire. Uses the GLOBAL policy
// window; there is no endpoint here whose override could apply. Also honours
// each row's own valid_until, a column that has existed unread since the table
// was created. Returns how many were abandoned. [P6-B1]
func (s *Store) SweepGiveUpMO(ctx context.Context, olderThan time.Duration) (int64, error) {
	var total int64
	// Mine and the shared ones (D-121).
	own, ownArgs := s.ownerCond()
	if olderThan > 0 {
		res, err := s.execQueue(ctx, // READ COMMITTED: a range UPDATE locks no gap (queuelocks.go)
			"UPDATE mo_queue SET status='expired' WHERE status='queued' AND received_at < DATE_SUB(NOW(), INTERVAL ? SECOND)"+own,
			append([]any{int(olderThan.Seconds())}, ownArgs...)...)
		if err != nil {
			return 0, err
		}
		n, _ := res.RowsAffected()
		total += n
	}
	res, err := s.execQueue(ctx,
		"UPDATE mo_queue SET status='expired' WHERE status='queued' AND valid_until IS NOT NULL AND valid_until <= NOW()"+own, ownArgs...)
	if err != nil {
		return total, err
	}
	n, _ := res.RowsAffected()
	return total + n, nil
}

// AckMO removes delivered MO staging rows by canonical msg id. [SCOPE P6]
func (s *Store) AckMO(ctx context.Context, msgIDs ...string) error {
	if len(msgIDs) == 0 {
		return nil
	}
	del := fmt.Sprintf(`DELETE FROM mo_queue WHERE msg_id IN (%s)`, placeholders(len(msgIDs)))
	args := make([]any, len(msgIDs))
	for i, v := range msgIDs {
		args[i] = v
	}
	start := time.Now()
	_, err := s.execQueue(ctx, del, args...) // a lock victim is made again (queuelocks.go)
	stMOAck.Observe(time.Since(start), int64(len(msgIDs)))
	return err
}
