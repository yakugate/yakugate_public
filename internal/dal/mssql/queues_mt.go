package mssql

// The outgoing (MT) queues on SQL Server (mirrors internal/dal/mysql/
// mtqueue.go, and reroute.go, queued.go and handover.go for the moves and
// looks). A vendor's queue is its table queue_<cid>; a row is in the
// sub-queue of its owner and short code ('' = common) and ready once pick_at
// has come. A take deletes what it takes: nothing is leased, nothing acked.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/queueflow"
	"github.com/yakugate/yakugate/internal/queues"
)

// qSub is one (owner, short code) of a vendor's table: own = this
// deployment's rows, else the shared ones.
type qSub struct {
	own bool
	sc  string
}

// Claim takes up to n messages of a vendor, its rate split fairly between
// its short codes.
func (s *QueueStore) Claim(ctx context.Context, vendor string, n int) ([]model.Message, error) {
	return s.ClaimSplit(ctx, vendor, n, queues.Fair)
}

// ClaimSplit takes up to n messages of a vendor from this process's rows, as
// MySQL's: how many each (owner, short code) has ready is counted (up to n),
// n is split between them by mode (queues.Split), and each gives its share in
// priority order, oldest first. Two round trips: the count (a plain read,
// which waits on nobody), then one batch that takes every share in one
// transaction — each share one DELETE of the rows read past the ones other
// takers hold. MySQL took 1 + 2 a sub-queue + 1 statements in a transaction.
func (s *QueueStore) ClaimSplit(ctx context.Context, vendor string, n int, mode queues.Mode) ([]model.Message, error) {
	if n <= 0 {
		return nil, nil
	}
	t := qTable(vendor)
	subs, waits, err := s.countReady(ctx, t, n)
	if err != nil || len(subs) == 0 {
		return nil, err
	}
	share := queues.Split(n, waits, mode, queues.Turn(vendor))
	var b strings.Builder
	b.WriteString("SET NOCOUNT ON; SET XACT_ABORT ON; BEGIN TRAN;\n")
	args := []any{}
	takes := 0
	for i, sq := range subs {
		if share[i] <= 0 {
			continue
		}
		takes++
		own := "queue_owner IS NULL"
		if sq.own {
			own = "queue_owner = @own"
		}
		nName, scName := fmt.Sprintf("n%d", takes), fmt.Sprintf("sc%d", takes)
		b.WriteString(strings.NewReplacer("{n}", "@"+nName, "{own}", own, "{sc}", "@"+scName).Replace(qOn(qTakeOne, t)))
		b.WriteString(qTakeOutput + ";\n")
		args = append(args, sql.Named(nName, int64(share[i])), sql.Named(scName, sq.sc))
	}
	if takes == 0 {
		return nil, nil
	}
	b.WriteString("COMMIT;")
	args = append(args, sql.Named("own", s.owner))
	var out []model.Message
	start := time.Now()
	err = Again(ctx, 8, func() error {
		out = nil
		got, err := s.readTake(ctx, b.String(), vendor, true, args...)
		out = got
		return err
	})
	observe(stQTake, start, int64(len(out)))
	if err != nil {
		if MissingTable(err) {
			return nil, nil
		}
		return nil, err
	}
	queueflow.Out(queueflow.MT, vendor, int64(len(out))) // monitoring: the queue's rate out (D-127)
	return out, nil
}

// readTake runs a take's batch and reads every result set it answers. What
// it took is handed out only when the whole batch committed: an error rolls
// all of it back (XACT_ABORT), so nothing read before it is lost. ordered:
// each row ends with its pick time, and each result set (one sub-queue) is
// put in the take's order — priority, pick time, id — as MySQL hands it out.
func (s *QueueStore) readTake(ctx context.Context, batch, vendor string, ordered bool, args ...any) ([]model.Message, error) {
	rows, err := s.db.QueryContext(ctx, batch, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type took struct {
		m    model.Message
		id   int64
		pick time.Time
	}
	var out []model.Message
	for {
		var set []took
		for rows.Next() {
			var tk took
			var pick sql.NullTime
			var err error
			if ordered {
				tk.m, tk.id, err = qScanRow(rows.Scan, vendor, &pick)
			} else {
				tk.m, tk.id, err = qScanRow(rows.Scan, vendor)
			}
			if err != nil {
				return nil, err
			}
			tk.pick = pick.Time
			set = append(set, tk)
		}
		if ordered {
			sort.Slice(set, func(i, j int) bool {
				a, b := set[i], set[j]
				if a.m.Priority != b.m.Priority {
					return a.m.Priority < b.m.Priority
				}
				if !a.pick.Equal(b.pick) {
					return a.pick.Before(b.pick)
				}
				return a.id < b.id
			})
		}
		for _, tk := range set {
			out = append(out, tk.m)
		}
		if !rows.NextResultSet() {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// countReady is how much each (owner, short code) of a vendor's table has
// ready, counted up to n, by priority too: own rows first, then the shared
// ones, each owner's short codes in the index's order. One round trip. A
// table not made yet is an empty queue.
func (s *QueueStore) countReady(ctx context.Context, t string, n int) ([]qSub, []queues.Waiting, error) {
	start := time.Now()
	rows, err := s.db.QueryContext(ctx, qOn(stQReady.SQL, t), sql.Named("own", s.owner), sql.Named("n", int64(n)))
	if err != nil {
		observe(stQReady, start, 0)
		if MissingTable(err) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	defer rows.Close()
	var subs []qSub
	var waits []queues.Waiting
	var total int64
	for rows.Next() {
		var own bool
		var sc string
		var prio int
		var c int64
		if err := rows.Scan(&own, &sc, &prio, &c); err != nil {
			return nil, nil, err
		}
		if len(subs) == 0 || subs[len(subs)-1] != (qSub{own: own, sc: sc}) {
			subs = append(subs, qSub{own: own, sc: sc})
			waits = append(waits, queues.Waiting{})
		}
		w := &waits[len(waits)-1]
		w.Ready += c
		w.ByPrio[queues.Priority(prio)] += c
		total += c
	}
	observe(stQReady, start, total)
	if err := rows.Err(); err != nil {
		if MissingTable(err) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	return subs, waits, nil
}

// Ack is nothing for an outgoing message: a take removed it.
func (s *QueueStore) Ack(ctx context.Context, vendor string, msgIDs ...string) error { return nil }

// Return puts messages taken but not sent back on their queues, ready at
// once and with the attempt they had. A vendor's messages go in as one
// multi-row INSERT per 33 (SQL Server takes 2,100 parameters a statement).
func (s *QueueStore) Return(ctx context.Context, ms ...model.Message) error {
	byTable := map[string][]model.Message{}
	var order []string
	for _, m := range ms {
		m.NextAttemptAt = time.Time{}
		t := qTable(m.Vendor)
		if _, ok := byTable[t]; !ok {
			order = append(order, t)
		}
		byTable[t] = append(byTable[t], m)
	}
	per := 2000 / qEnqueueArgsPerRow
	for _, t := range order {
		if err := s.ensureTable(ctx, t); err != nil {
			return err
		}
		group := byTable[t]
		for from := 0; from < len(group); from += per {
			chunk := group[from:min(from+per, len(group))]
			tuples := make([]string, len(chunk))
			args := make([]any, 0, len(chunk)*qEnqueueArgsPerRow)
			for i, m := range chunk {
				tuples[i] = "(" + qEnqueueValues(len(args)+1) + ")"
				args = append(args, qEnqueueArgs(m)...)
			}
			q := "INSERT INTO " + quoteIdent(t) + " (" + qEnqueueColumnList() + ") VALUES " + strings.Join(tuples, ",")
			start := time.Now()
			err := Again(ctx, enqueueTries, func() error {
				_, err := s.db.ExecContext(ctx, q, args...)
				return err
			})
			observe(stQEnqueue, start, int64(len(chunk)))
			if err != nil {
				return s.forgetIfMissing(err, t)
			}
			queueflow.In(queueflow.MT, chunk[0].Vendor, int64(len(chunk)))
		}
	}
	return nil
}

// FetchConcatGroup takes the still-queued parts of one long message, in part
// order (D-036), deleted as a take's rows are: one statement.
func (s *QueueStore) FetchConcatGroup(ctx context.Context, vendor string, accountID int64,
	sender, dest string, ref, total int) ([]model.Message, error) {
	t := qTable(vendor)
	args := append([]any{accountID, qText(sender), qText(dest), int64(ref), int64(total)}, s.ownArgs()...)
	var out []model.Message
	start := time.Now()
	err := Again(ctx, 8, func() error {
		got, err := s.readTake(ctx, qOn(qWithOwn(stQConcatGroup.SQL, s.ownCond()), t), vendor, false, args...)
		out = got
		return err
	})
	observe(stQConcatGroup, start, int64(len(out)))
	if err != nil {
		if MissingTable(err) {
			return nil, nil
		}
		return nil, err
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].ConcatSeq < out[j].ConcatSeq })
	queueflow.Out(queueflow.MT, vendor, int64(len(out)))
	return out, nil
}

// PeekMovable returns up to n messages of one vendor's queue that are ready
// now, after the row id `after`, with the last id seen. Nothing is locked or
// changed; parts of a long message are left out (MySQL's reroute.go).
func (s *QueueStore) PeekMovable(ctx context.Context, vendor string, after int64, n int) ([]model.Message, int64, error) {
	t := qTable(vendor)
	rows, err := s.db.QueryContext(ctx, qOn(qWithOwn(stQPeek.SQL, s.ownCond()), t),
		append([]any{int64(n), after}, s.ownArgs()...)...)
	if err != nil {
		if MissingTable(err) {
			return nil, after, nil
		}
		return nil, after, err
	}
	defer rows.Close()
	var out []model.Message
	last := after
	for rows.Next() {
		m, id, err := qScanRow(rows.Scan, vendor)
		if err != nil {
			return out, last, err
		}
		out = append(out, m)
		last = id
	}
	return out, last, rows.Err()
}

// MoveQueued moves one waiting message from vendor `from` to m.Vendor's
// queue in ONE transaction (one round trip): taken only while it still waits
// and no sender holds it, copied with the new route's fields, the vendor it
// leaves added to tried_vendors, deleted from the old queue. false = a
// sender took it, or it left meanwhile; nothing changed.
func (s *QueueStore) MoveQueued(ctx context.Context, from string, m model.Message) (bool, error) {
	src, dst := qTable(from), qTable(m.Vendor)
	if src == dst {
		return false, nil
	}
	if err := s.ensureTable(ctx, dst); err != nil {
		return false, err
	}
	enc := m.Encoding
	if enc == "" {
		enc = "gsm7"
	}
	q := strings.NewReplacer("{src}", quoteIdent(src), "{dst}", quoteIdent(dst)).Replace(stQMove.SQL)
	var moved int
	start := time.Now()
	err := Again(ctx, 8, func() error {
		return s.db.QueryRowContext(ctx, q, Varchar(m.ID), m.VendorID, m.RouteID, qText(m.Sender), qText(m.Destination),
			qText(m.Body), qText(enc), int64(m.Segments), qText(string(m.Transport)),
			qText(m.OrigSender), qText(m.OrigDestination), qText(m.OrigBody),
			qText(from),                      // tried_vendors: which vendor it was moved off
			int64(m.RouteLevel)).Scan(&moved) // the level of its rule it moves to
	})
	observe(stQMove, start, int64(moved))
	if err != nil {
		return false, s.forgetIfMissing(err, dst)
	}
	if moved == 0 {
		return false, nil
	}
	queueflow.Out(queueflow.MT, from, 1) // it left one vendor's queue for another's (D-127)
	queueflow.In(queueflow.MT, m.Vendor, 1)
	return true, nil
}

// FindQueued looks for accountID's message msgID in the named vendors'
// queues (MySQL's queued.go): which of the tables are there (asked only for
// tables this store does not know), then ONE statement over all of them, so
// a message moved between two queues at that moment is found in one.
func (s *QueueStore) FindQueued(ctx context.Context, msgID string, accountID int64, vendors []string) (model.Message, bool, error) {
	if msgID == "" || len(vendors) == 0 {
		return model.Message{}, false, nil
	}
	cidOf := map[string]string{}
	var tables []string
	for _, v := range vendors {
		if v == "" {
			continue
		}
		t := qTable(v)
		if _, dup := cidOf[t]; dup {
			continue
		}
		cidOf[t] = v
		tables = append(tables, t)
	}
	if len(tables) == 0 {
		return model.Message{}, false, nil
	}
	var unknown []string
	for _, t := range tables {
		if _, ok := s.ensured.Load(t); !ok {
			unknown = append(unknown, t)
		}
	}
	if len(unknown) > 0 {
		a := &qArgs{}
		ps := make([]string, len(unknown))
		for i, t := range unknown {
			ps[i] = a.add(t)
		}
		rows, err := s.db.QueryContext(ctx, strings.Replace(stQTablesIn.SQL, "IN (@p1)", "IN ("+strings.Join(ps, ",")+")", 1), a.vals...)
		if err != nil {
			return model.Message{}, false, err
		}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				rows.Close()
				return model.Message{}, false, err
			}
			s.ensured.Store(strings.ToLower(name), true)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return model.Message{}, false, err
		}
	}
	var present []string
	for _, t := range tables {
		if _, ok := s.ensured.Load(t); ok {
			present = append(present, t)
		}
	}
	if len(present) == 0 {
		return model.Message{}, false, nil
	}
	m, ok, err := s.findQueuedIn(ctx, msgID, accountID, present, cidOf)
	if err != nil && MissingTable(err) {
		// A table dropped since it was seen: one at a time, forgetting it.
		for _, t := range present {
			m, ok, err = s.findQueuedIn(ctx, msgID, accountID, []string{t}, cidOf)
			if err != nil && MissingTable(err) {
				s.ensured.Delete(t)
				continue
			}
			if err != nil || ok {
				return m, ok, err
			}
		}
		return model.Message{}, false, nil
	}
	return m, ok, err
}

// findQueuedIn reads msgID from the given tables in one statement. q_wait is
// the seconds until the queue offers it, on the database's clock.
func (s *QueueStore) findQueuedIn(ctx context.Context, msgID string, accountID int64, tables []string, cidOf map[string]string) (model.Message, bool, error) {
	a := &qArgs{}
	parts := make([]string, len(tables))
	for i, t := range tables {
		v, id, acct := a.add(qText(cidOf[t])), a.add(Varchar(msgID)), a.add(accountID)
		parts[i] = strings.NewReplacer("@p1", v, "@p2", id, "@p3", acct).Replace(qOn(stQFindOne.SQL, t))
	}
	rows, err := s.db.QueryContext(ctx, strings.Join(parts, "\nUNION ALL\n"), a.vals...)
	if err != nil {
		return model.Message{}, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return model.Message{}, false, rows.Err()
	}
	var cid string
	var wait int64
	m, _, err := qScanRow(rows.Scan, "", &cid, &wait)
	if err != nil {
		return model.Message{}, false, err
	}
	m.Vendor = cid
	if wait > 0 {
		m.NextAttemptAt = time.Now().Add(time.Duration(wait) * time.Second)
	}
	return m, true, nil
}

// HandInQueued adds an outgoing message moved in from another store unless
// the vendor's queue already holds it (false): one statement, the check and
// the insert together. The table is made when the vendor has none yet, and
// made again when it was dropped since.
func (s *QueueStore) HandInQueued(ctx context.Context, m model.Message) (bool, error) {
	t := qTable(m.Vendor)
	for try := 0; ; try++ {
		if err := s.ensureTable(ctx, t); err != nil {
			return false, err
		}
		var n int64
		start := time.Now()
		err := Again(ctx, enqueueTries, func() error {
			res, err := s.db.ExecContext(ctx, qOn(stQHandIn.SQL, t), qEnqueueArgs(m)...)
			if err == nil {
				n, err = res.RowsAffected()
			}
			return err
		})
		observe(stQHandIn, start, n)
		if err != nil && MissingTable(err) && try == 0 {
			s.ensured.Delete(t)
			continue
		}
		if err != nil {
			return false, err
		}
		if n > 0 {
			queueflow.In(queueflow.MT, m.Vendor, 1)
		}
		return n > 0, nil
	}
}

// errNoJSON is a value the JSON writer refused (never: plain values).
var errNoJSON = errors.New("mssql: could not write a JSON list")

// qJSON is a list as one JSON parameter (qIDsBigint, qIDsMsg).
func qJSON(v any) (any, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errNoJSON, err)
	}
	return qText(string(b)), nil
}
