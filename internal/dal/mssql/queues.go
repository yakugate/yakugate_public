package mssql

// The queue family on SQL Server (dal.Queues; DAL plan, step 4): the vendor
// queues (queue_<cid>, made from queue_template), the client receipts
// (client_dlr), the incoming messages (mo_queue), the SMPP part ids
// (client_part_ids) — made like the MySQL queue store
// (internal/dal/mysql/mysql.go, mtqueue.go, queued.go, reroute.go,
// handover.go, queuelocks.go), each method handing out the same items, once
// each, with the same attempts, lanes, priorities and delays.
//
// How it is written for SQL Server:
//   - A take is ONE statement: the rows read WITH (ROWLOCK, UPDLOCK,
//     READPAST) — MySQL's FOR UPDATE SKIP LOCKED — and deleted (a vendor
//     queue) or leased (receipts, MO) in the same statement, which OUTPUTs
//     them. MySQL needed a transaction of a read and a write.
//   - The database runs READ_COMMITTED_SNAPSHOT: a plain read never waits on
//     a writer, a locking read (UPDLOCK) sees the latest committed row. So
//     MySQL's READ COMMITTED transactions (queuelocks.go) are not needed.
//   - A statement SQL Server refuses as a deadlock victim (1205) or a lock
//     wait is run again, as MySQL's lock victims are (Again).
//   - Every time is UTC (SYSUTCDATETIME()); MySQL's NOW() was the session's.
//   - All of the SQL is in queues_stmts.go, so the memory-optimized tables
//     of a later pass replace one file.
//
// A memo of the vendor tables made (ensured), the attempt caps and the last
// depth readings are kept per store, as on MySQL.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/queueflow"
	"github.com/yakugate/yakugate/internal/store"
)

// The queue family and the hand-off lists, registered without touching
// mssql.go's init (each area sets its own openers).
var _ = serves(func(d *dal.Driver) {
	d.Queues = openQueues
	d.HandOffs = openHandOffs
})

var _ dal.Queues = (*QueueStore)(nil)

// QueueStore is the queue family on one SQL Server queue database.
type QueueStore struct {
	db    *sql.DB
	clone *Cloner
	// owner is the deployment whose queues this process serves
	// (deployment.Owner, D-121): its rows and the shared ones (queue_owner
	// NULL). "" = the shared ones only.
	owner string
	// ensured: the vendor tables known to be there (made by this store or
	// seen), so an enqueue does not ask each time.
	ensured sync.Map

	// depthLast is each queue's last good depth reading (QueueDepths).
	depthMu   sync.Mutex
	depthLast map[string]store.VendorQueueStat

	// The most times a message may be sent (attemptcap).
	capMu     sync.RWMutex
	capGlobal int
	capVendor map[string]int
}

// openQueues opens the queue family on the queue store's one pool a process.
func openQueues(cfg config.Config) (dal.Queues, error) {
	if cfg.QueueDSN == "" {
		return nil, fmt.Errorf("no queue database (YAKU_QUEUE_DSN)")
	}
	return NewQueueStore(cfg.QueueDSN)
}

// NewQueueStore is the queue family on the pool for dsn (Get).
func NewQueueStore(dsn string) (*QueueStore, error) {
	db, err := Get(dsn)
	if err != nil {
		return nil, err
	}
	return &QueueStore{db: db, clone: NewCloner(db), owner: deployment.Owner()}, nil
}

// Close does nothing: the pool is the process's (Get).
func (s *QueueStore) Close() error { return nil }

// SetAttemptCap sets the most times a message is sent: global for every
// queue, perVendor (by vendor cid) where a vendor has its own. 0 = no cap.
func (s *QueueStore) SetAttemptCap(global int, perVendor map[string]int) {
	s.capMu.Lock()
	s.capGlobal, s.capVendor = global, perVendor
	s.capMu.Unlock()
}

// MaxAttempts is the most times a vendor's message may be sent (0 = no cap).
func (s *QueueStore) MaxAttempts(vendor string) int {
	s.capMu.RLock()
	defer s.capMu.RUnlock()
	if v, ok := s.capVendor[vendor]; ok && v > 0 {
		return v
	}
	return s.capGlobal
}

// SetQueueOwner points the store at another deployment's queues (tests).
func (s *QueueStore) SetQueueOwner(owner string) { s.owner = owner }

// ownCond is the claim-side owner filter (MySQL's ownerCond): my rows and
// the shared ones; only the shared ones when I name no deployment. It names
// @own, which ownArgs gives.
func (s *QueueStore) ownCond() string {
	if s.owner == "" {
		return " AND queue_owner IS NULL"
	}
	return " AND (queue_owner = @own OR queue_owner IS NULL)"
}

// ownArgs is the @own parameter ownCond names (none when it names none). It
// goes after the positional ones, which keep their numbers.
func (s *QueueStore) ownArgs() []any {
	if s.owner == "" {
		return nil
	}
	return []any{sql.Named("own", s.owner)}
}

var qIdentRe = regexp.MustCompile(`[^a-zA-Z0-9_]`)

// qTable is a vendor's queue table, as MySQL names it.
func qTable(vendor string) string {
	return "queue_" + strings.ToLower(qIdentRe.ReplaceAllString(vendor, "_"))
}

// ensureTable makes a vendor's queue table from queue_template once (GW-01:
// a vendor added after the seed has none until its first message).
func (s *QueueStore) ensureTable(ctx context.Context, t string) error {
	if _, ok := s.ensured.Load(t); ok {
		return nil
	}
	if err := s.clone.CreateLike(ctx, "queue_template", t); err != nil {
		return err
	}
	s.ensured.Store(t, true)
	return nil
}

// forgetIfMissing drops a table from the memo when a write found it gone (a
// vendor deleted and made again under a running role), so the next write
// makes it again.
func (s *QueueStore) forgetIfMissing(err error, t string) error {
	if err != nil && MissingTable(err) {
		s.ensured.Delete(t)
	}
	return err
}

// qArgs numbers positional parameters as a statement is built: add gives
// the next @pN.
type qArgs struct{ vals []any }

func (a *qArgs) add(v any) string {
	a.vals = append(a.vals, v)
	return "@p" + strconv.Itoa(len(a.vals))
}

// enqueueTries bounds the tries of a write a client or a vendor waits on, as
// on MySQL (queuelocks.go).
const enqueueTries = 4

// Enqueue inserts a message into its vendor's queue. [SCOPE P1-17]
func (s *QueueStore) Enqueue(ctx context.Context, m model.Message) error {
	t := qTable(m.Vendor)
	if err := s.ensureTable(ctx, t); err != nil {
		return err
	}
	start := time.Now()
	err := Again(ctx, enqueueTries, func() error {
		_, err := s.db.ExecContext(ctx, qOn(stQEnqueue.SQL, t), qEnqueueArgs(m)...)
		return err
	})
	observe(stQEnqueue, start, 1)
	if err == nil {
		queueflow.In(queueflow.MT, m.Vendor, 1) // monitoring: the queue's rate in (D-127)
	}
	return s.forgetIfMissing(err, t)
}

// qQueueErrorTextMax is the size of a queue row's error_text.
const qQueueErrorTextMax = 512

// qEnqueueArgs are one row's values in qEnqueueCols' order, then the pick
// time and next_attempt_at (MySQL's enqueueArgs).
func qEnqueueArgs(m model.Message) []any {
	or := func(v, def string) string {
		if v == "" {
			return def
		}
		return v
	}
	b := func(v bool) int64 {
		if v {
			return 1
		}
		return 0
	}
	gsm7 := int64(-1)
	switch m.Gsm7 {
	case "true":
		gsm7 = 1
	case "false":
		gsm7 = 0
	}
	status := string(m.Status)
	if status == "" {
		status = string(model.StatusQueued)
	}
	ingress := string(m.Ingress)
	if ingress == "" {
		ingress = string(model.TransportHTTP)
	}
	tried := ""
	if len(m.TriedVendors) > 0 {
		j, _ := json.Marshal(m.TriedVendors)
		tried = string(j)
	}
	clientSender := ""
	if m.SenderBy != "" {
		clientSender = m.ClientSender
	}
	errText := strings.TrimSpace(m.ErrorText)
	if r := []rune(errText); len(r) > qQueueErrorTextMax {
		errText = string(r[:qQueueErrorTextMax])
	}
	vals := []any{
		Varchar(m.ID), Varchar(or(m.TraceID, m.ID)), qText(or(m.RequestID, m.ID)), m.AccountID, qText(m.Username),
		m.VendorID, m.RouteID, qText(m.Sender), qText(m.Destination), qText(m.Body),
		qText(or(m.Encoding, "gsm7")), int64(m.Segments),
		int64(m.ConcatRef), int64(m.ConcatTotal), int64(m.ConcatSeq), gsm7,
		m.TemplateID, qText(m.Channel), qText(m.AP1), qText(m.AP2), qText(m.APG1), qText(m.APG2),
		qText(string(m.Transport)), qText(ingress), qText(m.MCC), qText(m.MNC), qText(status), int64(m.Priority),
		qText(m.IdempotencyKey), Time(m.ValidUntil), int64(m.Attempts),
		b(m.NoReceipt), b(m.FailureReceiptOnly), b(m.WantIntermediateReceipt), int64(m.BindSlot), b(m.MaskBody),
		qText(m.SpamFlag), qText(m.OrigSender), qText(m.OrigDestination), qText(m.OrigBody),
		qText(m.CampaignID), qText(m.CampaignHash), qText(m.CodeReroute),
		qText(m.DeploymentID), qText(m.SenderBy), qText(clientSender),
		int64(m.SendRound), int64(m.RouteLevel), qText(tried),
		qText(m.BillingModel), qText(m.QueueOwner), qText(m.Quote), m.CompanyID,
		qText(m.HLRSource), b(m.HLRPorted), qText(errText), qText(m.IPAddress), qText(m.ShortCode),
	}
	// When it may be taken: until NextAttemptAt (a scheduled send, a retry
	// after its gap), else at once; and next_attempt_at, kept as MySQL
	// keeps it (NULL, or the seconds to NextAttemptAt).
	var pick int64
	var has, secs int64
	if !m.NextAttemptAt.IsZero() {
		if d := time.Until(m.NextAttemptAt); d > 0 {
			pick = d.Microseconds()
		}
		has, secs = 1, int64(time.Until(m.NextAttemptAt)/time.Second)
	}
	return append(vals, pick/1_000_000, pick%1_000_000, has, secs)
}

// qScanRow reads one row shaped like qSelectCols (MySQL's scanQueueRow),
// with its row id; extra are the targets of columns a caller appended.
func qScanRow(scan func(dest ...any) error, vendor string, extra ...any) (model.Message, int64, error) {
	var (
		m                            model.Message
		id                           int64
		req, trace                   sql.NullString
		uname                        sql.NullString
		enc, transp                  sql.NullString
		ingr                         sql.NullString
		mcc, mnc                     sql.NullString
		seg                          sql.NullInt64
		att                          sql.NullInt64
		chn                          sql.NullString
		ap1, ap2                     sql.NullString
		apg1, apg2                   sql.NullString
		prio                         sql.NullInt64
		route, templateID, company   int64
		ref, total, seq              int64
		noRcpt, failOnly, wantInterm int64
		hlrPorted                    int64
		bindSlot                     int64
		clientRef                    string
		maskBody                     int64
		spamFlag                     string
		gsm7                         sql.NullInt64
		validUntil                   sql.NullTime
		tried                        string
		sendRound, routeLevel        int64
	)
	targets := []any{&id, &m.ID, &req, &trace, &m.AccountID, &uname, &route, &m.Sender, &m.Destination, &m.Body, &enc, &seg,
		&ref, &total, &seq, &transp, &ingr, &mcc, &mnc, &att,
		&chn, &ap1, &ap2, &apg1, &apg2, &prio,
		&noRcpt, &failOnly, &wantInterm, &bindSlot, &clientRef, &maskBody, &spamFlag,
		&gsm7, &m.OrigSender, &m.OrigDestination, &m.OrigBody, &validUntil,
		&m.CampaignID, &m.CampaignHash, &m.CodeReroute, &templateID,
		&m.DeploymentID, &m.SenderBy, &m.ClientSender,
		&sendRound, &routeLevel, &tried,
		&m.BillingModel, &m.QueueOwner, &m.Quote, &company,
		&m.HLRSource, &hlrPorted, &m.ErrorText, &m.IPAddress, &m.ShortCode,
	}
	if err := scan(append(targets, extra...)...); err != nil {
		return model.Message{}, 0, err
	}
	m.ID = strings.TrimRight(m.ID, " ")
	m.CampaignHash = strings.TrimRight(m.CampaignHash, " ")
	m.RouteID, m.TemplateID, m.CompanyID = route, templateID, company
	m.ConcatRef, m.ConcatTotal, m.ConcatSeq = int(ref), int(total), int(seq)
	m.SendRound, m.RouteLevel = int(sendRound), int(routeLevel)
	m.IdempotencyKey = clientRef
	if tried != "" {
		_ = json.Unmarshal([]byte(tried), &m.TriedVendors)
	}
	m.RequestID, m.TraceID = req.String, strings.TrimRight(trace.String, " ")
	m.MaskBody = maskBody != 0
	m.SpamFlag = spamFlag
	if gsm7.Valid {
		m.Gsm7 = strconv.FormatBool(gsm7.Int64 != 0)
	}
	if validUntil.Valid {
		m.ValidUntil = validUntil.Time
	}
	m.NoReceipt, m.FailureReceiptOnly, m.WantIntermediateReceipt = noRcpt != 0, failOnly != 0, wantInterm != 0
	m.HLRPorted = hlrPorted != 0
	m.BindSlot = int(bindSlot)
	m.Username = uname.String
	m.Channel = chn.String
	m.AP1, m.AP2 = ap1.String, ap2.String
	m.APG1, m.APG2 = apg1.String, apg2.String
	m.Priority = int(prio.Int64)
	m.Ingress = model.Transport(ingr.String)
	m.Vendor = vendor
	m.Encoding = enc.String
	m.Transport = model.Transport(transp.String)
	m.Segments = int(seg.Int64)
	m.MCC, m.MNC = mcc.String, mnc.String
	m.Attempts = int(att.Int64)
	return m, id, nil
}

// listQueueTables is the vendor queue tables (not the template).
func (s *QueueStore) listQueueTables(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, stQTables.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// QueueDepths is every vendor queue's figures: this deployment's rows and
// the shared ones, read side by side, each in one round trip with its own
// time limit; a queue that cannot be read reports its last good figures
// marked Stale (MySQL's QueueDepths, its reasons kept).
func (s *QueueStore) QueueDepths(ctx context.Context) (map[string]store.VendorQueueStat, error) {
	tables, err := s.listQueueTables(ctx)
	if err != nil {
		return nil, err
	}
	type result struct {
		vendor string
		stat   store.VendorQueueStat
		err    error
	}
	results := make(chan result, len(tables))
	sem := make(chan struct{}, 4)
	for _, t := range tables {
		t := t
		sem <- struct{}{}
		go func() {
			defer func() { <-sem }()
			c, cancel := context.WithTimeout(ctx, qDepthReadTimeout)
			defer cancel()
			st, err := s.queueStat(c, t)
			results <- result{strings.TrimPrefix(t, "queue_"), st, err}
		}()
	}
	out := make(map[string]store.VendorQueueStat, len(tables))
	s.depthMu.Lock()
	defer s.depthMu.Unlock()
	if s.depthLast == nil {
		s.depthLast = map[string]store.VendorQueueStat{}
	}
	for range tables {
		r := <-results
		last, known := s.depthLast[r.vendor]
		if r.err != nil {
			if known {
				last.Stale = true
				out[r.vendor] = last
			}
			continue
		}
		if r.stat.Tried < 0 { // not counted this time (a large queue)
			r.stat.Tried = 0
			if known {
				r.stat.Tried = last.Tried
			}
		}
		s.depthLast[r.vendor] = r.stat
		out[r.vendor] = r.stat
	}
	return out, nil
}

// qDepthReadTimeout bounds one queue's reading.
const qDepthReadTimeout = 2500 * time.Millisecond

// qTriedReadMax is the largest queue whose "tried" count is read every time.
const qTriedReadMax = 20000

// queueStat reads one queue table's figures (MySQL's queueStat, three reads
// there, one round trip here).
func (s *QueueStore) queueStat(ctx context.Context, t string) (store.VendorQueueStat, error) {
	var st store.VendorQueueStat
	var age sql.NullInt64
	start := time.Now()
	err := s.db.QueryRowContext(ctx, qOn(qWithOwn(stQStat.SQL, s.ownCond()), t),
		append([]any{int64(qTriedReadMax)}, s.ownArgs()...)...).Scan(&st.Depth, &st.Ready, &age, &st.Tried)
	observe(stQStat, start, 1)
	if err != nil {
		return st, err
	}
	st.Held = st.Depth - st.Ready
	if st.Ready > 0 && age.Valid && age.Int64 > 0 {
		st.OldestAgeSec = age.Int64
	}
	return st, nil
}

// QueuedForVendor counts every row of a vendor's queue table, whichever
// deployment owns it. A table that is not there holds nothing.
func (s *QueueStore) QueuedForVendor(ctx context.Context, vendor string) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, qOn(stQCount.SQL, qTable(vendor))).Scan(&n)
	if err != nil && MissingTable(err) {
		return 0, nil
	}
	return n, err
}
