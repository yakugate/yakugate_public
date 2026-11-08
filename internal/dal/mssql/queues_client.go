package mssql

// The client queues on SQL Server: the receipts waiting for a client
// (client_dlr), the incoming messages (mo_queue), the SMPP part ids
// (client_part_ids) and what Monitoring reads of them — mirrors the client
// receipt, MO and part-id halves of internal/dal/mysql/mysql.go.

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/queueflow"
	"github.com/yakugate/yakugate/internal/store"
)

// qMS is a time as unix milliseconds for a BIGINT column; zero = 0 (NULL).
func qMS(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

// qTimeOfMS reads such a column back; 0 (NULL) = the zero time.
func qTimeOfMS(ms int64) time.Time {
	if ms <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

// EnqueueClientDLR queues a client-facing receipt. A delayed receipt (the
// receipt policy spreading invented answers out) is a row nobody can claim
// yet: created_at moves with next_attempt_at, as on MySQL. [SCOPE P5]
func (s *QueueStore) EnqueueClientDLR(ctx context.Context, d model.ClientDLR) error {
	start := time.Now()
	err := Again(ctx, enqueueTries, func() error {
		_, err := s.db.ExecContext(ctx, stQCDLREnqueue.SQL,
			d.AccountID, Varchar(d.ClientMsgID), qText(d.Status), qText(d.Stat), qText(d.Receipt), int64(d.BindSlot),
			qText(d.Src), qText(d.Dst), qText(d.Owner), qMS(d.DoneAt), int64(d.Delay.Seconds()), Varchar(qCDLRMsgID(d)))
		return err
	})
	observe(stQCDLREnqueue, start, 1)
	if err == nil {
		queueflow.In(queueflow.DLR, strconv.FormatInt(d.AccountID, 10), 1) // D-127
	}
	return err
}

// qCDLRRow is one receipt of a batch, as qCDLRBatch's OPENJSON reads it.
type qCDLRRow struct {
	A   int64  `json:"a"`
	M   string `json:"m"`
	S   string `json:"s"`
	T   string `json:"t"`
	R   string `json:"r"`
	B   int    `json:"b"`
	Src string `json:"src"`
	Dst string `json:"dst"`
	O   string `json:"o"`
	D   int64  `json:"d"`
	G   string `json:"g,omitempty"` // msg_id (queue/0050)
	W   int    `json:"w"`
	I   int    `json:"i"`
}

// EnqueueClientDLRBatch queues many receipts, in order, 500 a statement
// (as MySQL's multi-row INSERTs): each chunk is one JSON parameter read by
// OPENJSON, so a chunk is one statement and one plan whatever its size. An
// error leaves the chunks already written in place and the rest unwritten.
func (s *QueueStore) EnqueueClientDLRBatch(ctx context.Context, ds []model.ClientDLR) error {
	for from := 0; from < len(ds); from += 500 {
		chunk := ds[from:min(from+500, len(ds))]
		rows := make([]qCDLRRow, len(chunk))
		in := map[int64]int64{}
		for i, d := range chunk {
			rows[i] = qCDLRRow{A: d.AccountID, M: d.ClientMsgID, S: d.Status, T: d.Stat, R: d.Receipt, B: d.BindSlot,
				Src: d.Src, Dst: d.Dst, O: d.Owner, D: qMS(d.DoneAt), G: qCDLRMsgID(d), W: int(d.Delay.Seconds()), I: i}
			in[d.AccountID]++
		}
		j, err := qJSON(rows)
		if err != nil {
			return err
		}
		start := time.Now()
		err = Again(ctx, enqueueTries, func() error {
			_, err := s.db.ExecContext(ctx, stQCDLRBatch.SQL, j)
			return err
		})
		observe(stQCDLRBatch, start, int64(len(chunk)))
		if err != nil {
			return err
		}
		for acct, n := range in {
			queueflow.In(queueflow.DLR, strconv.FormatInt(acct, 10), n) // D-127
		}
	}
	return nil
}

// ClaimClientDLR leases up to n due receipts of the given accounts for 60 s,
// counting the attempt. [SCOPE P5]
func (s *QueueStore) ClaimClientDLR(ctx context.Context, claims []model.DLRClaim, n int) ([]model.ClientDLR, error) {
	return s.claimClientDLR(ctx, claims, 0, n)
}

// ClaimClientDLRStale is ClaimClientDLR for receipts older than minAgeSec
// (the webhook poster, so a live bind gets first go). [SCOPE P5]
func (s *QueueStore) ClaimClientDLRStale(ctx context.Context, claims []model.DLRClaim, minAgeSec, n int) ([]model.ClientDLR, error) {
	return s.claimClientDLR(ctx, claims, minAgeSec, n)
}

// qClaimScope is MySQL's dlrClaimScope: the accounts and their attempt
// windows, accounts with the same bounds in one IN list. attempts = 0 stays
// claimable under a floor (an account with no live bind never counts up).
func qClaimScope(a *qArgs, claims []model.DLRClaim) string {
	type bounds struct{ min, max int }
	var order []bounds
	byBounds := map[bounds][]int64{}
	for _, c := range claims {
		b := bounds{c.MinAttempts, c.MaxAttempts}
		if _, seen := byBounds[b]; !seen {
			order = append(order, b)
		}
		byBounds[b] = append(byBounds[b], c.AccountID)
	}
	terms := make([]string, 0, len(order))
	for _, b := range order {
		ps := make([]string, len(byBounds[b]))
		for i, id := range byBounds[b] {
			ps[i] = a.add(id)
		}
		t := "account_id IN (" + strings.Join(ps, ",") + ")"
		if b.min > 0 {
			t += " AND (attempts = 0 OR attempts >= " + a.add(int64(b.min)) + ")"
		}
		if b.max > 0 {
			t += " AND attempts < " + a.add(int64(b.max))
		}
		terms = append(terms, "("+t+")")
	}
	return "(" + strings.Join(terms, " OR ") + ")"
}

// qFloors is, per account, the count a webhook's claim raises its take to
// (MySQL's webhookFloors): the bind's budget under 'both'.
func qFloors(claims []model.DLRClaim) map[int64]int {
	out := map[int64]int{}
	for _, c := range claims {
		if c.MinAttempts > 0 {
			out[c.AccountID] = c.MinAttempts
		}
	}
	return out
}

// qAttemptsSet is the lease's new count: one more, raised to the account's
// floor where it has one (MySQL's GREATEST(attempts+1, floor), one UPDATE
// per floor there; one CASE here, in the claim's own statement).
func qAttemptsSet(a *qArgs, floors map[int64]int) string {
	if len(floors) == 0 {
		return "attempts + 1"
	}
	byFloor := map[int][]int64{}
	for acct, f := range floors {
		byFloor[f] = append(byFloor[f], acct)
	}
	fs := make([]int, 0, len(byFloor))
	for f := range byFloor {
		fs = append(fs, f)
	}
	sort.Ints(fs)
	var b strings.Builder
	b.WriteString("CASE")
	for _, f := range fs {
		accts := byFloor[f]
		sort.Slice(accts, func(i, j int) bool { return accts[i] < accts[j] })
		ps := make([]string, len(accts))
		for i, id := range accts {
			ps[i] = a.add(id)
		}
		fp := a.add(int64(f))
		b.WriteString(" WHEN account_id IN (" + strings.Join(ps, ",") + ") THEN IIF(attempts + 1 > " + fp + ", attempts + 1, " + fp + ")")
	}
	b.WriteString(" ELSE attempts + 1 END")
	return b.String()
}

// qClaimText builds a claim's statement: n first (@p1), then the scope, the
// age, the owner, the lease's count.
func (s *QueueStore) qClaimText(text, ageCol string, a *qArgs, claims []model.DLRClaim, floors map[int64]int, minAgeSec int) string {
	scope := qClaimScope(a, claims)
	age := ""
	if minAgeSec > 0 {
		age = " AND " + ageCol + " < DATEADD(SECOND, -" + a.add(int64(minAgeSec)) + ", SYSUTCDATETIME())"
	}
	set := qAttemptsSet(a, floors)
	return strings.NewReplacer("{scope}", scope, "{age}", age, "{own}", s.ownCond(), "{attempts}", set).Replace(text)
}

// claimClientDLR takes and leases in ONE statement: the due rows read in id
// order past the ones another taker holds (READPAST), each one's attempt
// counted and its lease set as it is taken. So two takers never hand out one
// receipt (MySQL's read and lease in one READ COMMITTED transaction). The
// count handed back is the one read, raised to the webhook floor, as MySQL
// answers it.
func (s *QueueStore) claimClientDLR(ctx context.Context, claims []model.DLRClaim, minAgeSec, n int) ([]model.ClientDLR, error) {
	if len(claims) == 0 || n <= 0 {
		return nil, nil
	}
	a := &qArgs{}
	a.add(int64(n))
	floors := qFloors(claims)
	q := s.qClaimText(qCDLRClaim, "created_at", a, claims, floors, minAgeSec)
	args := append(a.vals, s.ownArgs()...)
	var out []model.ClientDLR
	start := time.Now()
	err := Again(ctx, 8, func() error {
		out = nil
		rows, err := s.db.QueryContext(ctx, q, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var c model.ClientDLR
			var doneMS int64
			if err := rows.Scan(&c.ID, &c.AccountID, &c.ClientMsgID, &c.Status, &c.Stat, &c.Receipt,
				&c.Attempts, &c.CreatedAt, &c.BindSlot, &c.Src, &c.Dst, &c.Owner, &doneMS, &c.MsgID); err != nil {
				return err
			}
			c.ClientMsgID = strings.TrimRight(c.ClientMsgID, " ")
			c.MsgID = strings.TrimRight(c.MsgID, " ")
			c.DoneAt = qTimeOfMS(doneMS)
			c.Attempts = max(c.Attempts, floors[c.AccountID])
			out = append(out, c)
		}
		return rows.Err()
	})
	observe(stQCDLRClaim, start, int64(len(out)))
	if err != nil {
		return nil, err
	}
	// OUTPUT keeps no order: in id order, as MySQL hands them out.
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// AckClientDLR removes delivered receipts: one by its key, or a list in one
// statement. A lock victim is made again. [SCOPE P5]
func (s *QueueStore) AckClientDLR(ctx context.Context, ids ...int64) error {
	if len(ids) == 0 {
		return nil
	}
	start := time.Now()
	defer func() { observe(stQCDLRAckMany, start, int64(len(ids))) }()
	if len(ids) == 1 {
		return Again(ctx, enqueueTries, func() error {
			_, err := s.db.ExecContext(ctx, stQCDLRAckOne.SQL, ids[0])
			return err
		})
	}
	j, err := qJSON(ids)
	if err != nil {
		return err
	}
	return Again(ctx, 8, func() error {
		_, err := s.db.ExecContext(ctx, stQCDLRAckMany.SQL, j)
		return err
	})
}

// RescheduleClientDLR pushes one receipt's next attempt out by delay. [P5-B02]
func (s *QueueStore) RescheduleClientDLR(ctx context.Context, id int64, delay time.Duration) error {
	return Again(ctx, enqueueTries, func() error {
		_, err := s.db.ExecContext(ctx, stQCDLRResched.SQL, int64(delay.Seconds()), id)
		return err
	})
}

// GiveUpClientDLR marks one receipt abandoned; the row is kept and never
// claimed again (PO ruling 2026-08-31). [P5-B02]
func (s *QueueStore) GiveUpClientDLR(ctx context.Context, id int64) error {
	return Again(ctx, enqueueTries, func() error {
		_, err := s.db.ExecContext(ctx, stQCDLRGiveUp.SQL, id)
		return err
	})
}

// SweepGiveUpClientDLR abandons this deployment's (and the shared) receipts
// older than olderThan that nobody is trying. [P5-B02]
func (s *QueueStore) SweepGiveUpClientDLR(ctx context.Context, olderThan time.Duration) (int64, error) {
	if olderThan <= 0 {
		return 0, nil
	}
	return s.execCount(ctx, qWithOwn(stQCDLRSweep.SQL, s.ownCond()), append([]any{int64(olderThan.Seconds())}, s.ownArgs()...)...)
}

// execCount runs one change, again when it is a lock victim, and answers the
// rows it changed.
func (s *QueueStore) execCount(ctx context.Context, q string, args ...any) (int64, error) {
	var n int64
	err := Again(ctx, 8, func() error {
		res, err := s.db.ExecContext(ctx, q, args...)
		if err != nil {
			return err
		}
		n, err = res.RowsAffected()
		return err
	})
	return n, err
}

// StageMO writes an accepted MO into mo_queue, due at once. [SCOPE P6]
func (s *QueueStore) StageMO(ctx context.Context, m model.Message) error {
	trace := m.TraceID
	if trace == "" {
		trace = m.ID
	}
	enc := m.Encoding
	if enc == "" {
		enc = "gsm7"
	}
	seg := int64(m.Segments)
	if seg < 1 {
		seg = 1
	}
	priced := ""
	if m.Priced != nil {
		if b, err := json.Marshal(m.Priced); err == nil {
			priced = string(b)
		}
	}
	start := time.Now()
	err := Again(ctx, enqueueTries, func() error {
		_, err := s.db.ExecContext(ctx, stQMOStage.SQL,
			Varchar(m.ID), Varchar(trace), m.VendorID, m.AccountID, m.RouteID, qText(m.Sender), qText(m.Destination), qText(m.Body),
			qText(enc), seg, qText(m.MCC), qText(m.MNC),
			qText(m.QueueOwner), // the deployment that delivers it (queue/0037, D-121)
			m.CompanyID,         // whose record table it is (queue/0039, D-136)
			qText(priced))       // its price, for a record its delivery writes (queue/0048)
		return err
	})
	observe(stQMOStage, start, 1)
	if err == nil {
		queueflow.In(queueflow.MO, strconv.FormatInt(m.AccountID, 10), 1) // D-127
	}
	return err
}

// qPricedOf reads an MO's price back; nil for none or one that does not read.
func qPricedOf(s string) *model.Priced {
	if s == "" {
		return nil
	}
	var p model.Priced
	if json.Unmarshal([]byte(s), &p) != nil {
		return nil
	}
	return &p
}

// ClaimMO leases up to n staged MO of the given accounts for 60 s. [SCOPE P6]
func (s *QueueStore) ClaimMO(ctx context.Context, claims []model.MOClaim, n int) ([]model.Message, error) {
	return s.claimMO(ctx, claims, 0, n)
}

// ClaimMOStale is ClaimMO for MO older than minAgeSec. [SCOPE P6]
func (s *QueueStore) ClaimMOStale(ctx context.Context, claims []model.MOClaim, minAgeSec, n int) ([]model.Message, error) {
	return s.claimMO(ctx, claims, minAgeSec, n)
}

// claimMO takes and leases in ONE statement, as claimClientDLR does.
func (s *QueueStore) claimMO(ctx context.Context, claims []model.MOClaim, minAgeSec, n int) ([]model.Message, error) {
	if len(claims) == 0 || n <= 0 {
		return nil, nil
	}
	a := &qArgs{}
	a.add(int64(n))
	floors := qFloors(claims)
	q := s.qClaimText(qMOClaim, "received_at", a, claims, floors, minAgeSec)
	args := append(a.vals, s.ownArgs()...)
	var out []model.Message
	var rowIDs []int64
	start := time.Now()
	err := Again(ctx, 8, func() error {
		out, rowIDs = nil, nil
		rows, err := s.db.QueryContext(ctx, q, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var m model.Message
			var priced string
			var seg, rowID int64
			if err := rows.Scan(&m.ID, &m.VendorID, &m.AccountID, &m.RouteID, &m.Sender, &m.Destination,
				&m.Body, &m.Encoding, &seg, &m.MCC, &m.MNC, &m.Attempts, &m.CreatedAt, &m.QueueOwner,
				&m.CompanyID, &priced, &rowID); err != nil {
				return err
			}
			rowIDs = append(rowIDs, rowID)
			m.ID = strings.TrimRight(m.ID, " ")
			m.Segments = int(seg)
			m.Priced = qPricedOf(priced)
			m.Direction = model.DirectionMO
			m.Attempts = max(m.Attempts, floors[m.AccountID])
			out = append(out, m)
		}
		return rows.Err()
	})
	observe(stQMOClaim, start, int64(len(out)))
	if err != nil {
		return nil, err
	}
	// OUTPUT keeps no order: in row id order, as MySQL hands them out.
	order := make([]int, len(out))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool { return rowIDs[order[a]] < rowIDs[order[b]] })
	sorted := make([]model.Message, len(out))
	for i, k := range order {
		sorted[i] = out[k]
	}
	return sorted, nil
}

// RescheduleMO pushes one staged MO's next attempt out by delay. [P6-B1]
func (s *QueueStore) RescheduleMO(ctx context.Context, msgID string, delay time.Duration) error {
	_, err := s.execCount(ctx, stQMOResched.SQL, int64(delay.Seconds()), Varchar(msgID))
	return err
}

// GiveUpMO marks one staged MO expired: kept, never claimed again. [P6-B1]
func (s *QueueStore) GiveUpMO(ctx context.Context, msgID string) error {
	_, err := s.execCount(ctx, stQMOGiveUp.SQL, Varchar(msgID))
	return err
}

// SweepGiveUpMO abandons this deployment's (and the shared) staged MO older
// than olderThan, and those past their own valid_until. [P6-B1]
func (s *QueueStore) SweepGiveUpMO(ctx context.Context, olderThan time.Duration) (int64, error) {
	var total int64
	if olderThan > 0 {
		n, err := s.execCount(ctx, qWithOwn(stQMOSweep.SQL, s.ownCond()), append([]any{int64(olderThan.Seconds())}, s.ownArgs()...)...)
		if err != nil {
			return 0, err
		}
		total += n
	}
	n, err := s.execCount(ctx, qWithOwn(stQMOExpire.SQL, s.ownCond()), s.ownArgs()...)
	return total + n, err
}

// AckMO removes delivered MO by message id, the list in one statement.
// [SCOPE P6]
func (s *QueueStore) AckMO(ctx context.Context, msgIDs ...string) error {
	if len(msgIDs) == 0 {
		return nil
	}
	j, err := qJSON(msgIDs)
	if err != nil {
		return err
	}
	start := time.Now()
	err = Again(ctx, 8, func() error {
		_, err := s.db.ExecContext(ctx, stQMOAck.SQL, j)
		return err
	})
	observe(stQMOAck, start, int64(len(msgIDs)))
	return err
}

// SaveClientPartIDs records the ids an SMPP client was given for the parts
// of one long message (queue/0028): an upsert, never MERGE.
func (s *QueueStore) SaveClientPartIDs(ctx context.Context, msgID string, partIDs []string) error {
	return Again(ctx, enqueueTries, func() error {
		_, err := stQPartsSave.Exec(ctx, s.db, sql.Named("msg_id", Varchar(msgID)), sql.Named("part_ids", qText(strings.Join(partIDs, ","))))
		return err
	})
}

// ClientPartIDs returns the part ids saved for msgID; nil when none.
func (s *QueueStore) ClientPartIDs(ctx context.Context, msgID string) ([]string, error) {
	var ids string
	err := s.db.QueryRowContext(ctx, stQPartsOne.SQL, Varchar(msgID)).Scan(&ids)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return strings.Split(ids, ","), nil
}

// ClientPartIDsMany returns the part ids of the messages that have any, one
// read per 500.
func (s *QueueStore) ClientPartIDsMany(ctx context.Context, msgIDs []string) (map[string][]string, error) {
	out := map[string][]string{}
	for from := 0; from < len(msgIDs); from += 500 {
		j, err := qJSON(msgIDs[from:min(from+500, len(msgIDs))])
		if err != nil {
			return out, err
		}
		rows, err := s.db.QueryContext(ctx, stQPartsMany.SQL, j)
		if err != nil {
			return out, err
		}
		for rows.Next() {
			var id, ids string
			if err := rows.Scan(&id, &ids); err == nil && ids != "" {
				out[strings.TrimRight(id, " ")] = strings.Split(ids, ",")
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return out, err
		}
	}
	return out, nil
}

// DropClientPartIDs removes the row once every part has its final receipt.
func (s *QueueStore) DropClientPartIDs(ctx context.Context, msgID string) error {
	_, err := s.db.ExecContext(ctx, stQPartsDrop.SQL, Varchar(msgID))
	return err
}

// PendingCounts is how many receipts and MO wait for a client, and each
// one's oldest age, in one round trip. [SCOPE MONITOR]
func (s *QueueStore) PendingCounts(ctx context.Context) (dlr, mo store.VendorQueueStat, err error) {
	var d, m int64
	var dAge, mAge sql.NullInt64
	own := s.ownCond()
	q := strings.NewReplacer("{own}", own, "{own2}", own).Replace(stQPendingCounts.SQL)
	if err = s.db.QueryRowContext(ctx, q, s.ownArgs()...).Scan(&d, &dAge, &m, &mAge); err != nil {
		return dlr, mo, err
	}
	dlr = store.VendorQueueStat{Depth: d}
	if dAge.Valid && dAge.Int64 > 0 {
		dlr.OldestAgeSec = dAge.Int64
	}
	mo = store.VendorQueueStat{Depth: m}
	if mAge.Valid && mAge.Int64 > 0 {
		mo.OldestAgeSec = mAge.Int64
	}
	return dlr, mo, nil
}

// PendingByAccount is PendingCounts per account, largest first (D-127).
func (s *QueueStore) PendingByAccount(ctx context.Context, kind string, limit int) (map[int64]store.VendorQueueStat, error) {
	if limit <= 0 {
		limit = 200
	}
	st := stQPendingDLR
	if kind == "mo" {
		st = stQPendingMO
	}
	rows, err := s.db.QueryContext(ctx, qWithOwn(st.SQL, s.ownCond()), append([]any{int64(limit)}, s.ownArgs()...)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]store.VendorQueueStat{}
	for rows.Next() {
		var acct int64
		var depth, ready, tried, age sql.NullInt64
		if err := rows.Scan(&acct, &depth, &ready, &tried, &age); err != nil {
			return nil, err
		}
		st := store.VendorQueueStat{Depth: depth.Int64, Ready: ready.Int64, Held: depth.Int64 - ready.Int64, Tried: tried.Int64}
		if age.Valid && age.Int64 > 0 {
			st.OldestAgeSec = age.Int64
		}
		out[acct] = st
	}
	return out, rows.Err()
}

// qCDLRMsgID is the msg_id a receipt row keeps (queue/0050): the message it
// is about, only when the client was told another id (a part's).
func qCDLRMsgID(d model.ClientDLR) string {
	if d.MsgID == d.ClientMsgID {
		return ""
	}
	return d.MsgID
}
