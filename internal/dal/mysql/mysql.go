package mysql

import (
	"context"
	"database/sql"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"

	_ "github.com/go-sql-driver/mysql"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/platform/dbpool"
	"github.com/yakugate/yakugate/internal/store"
)

// Store is the MySQL backend (Queue + HotStore).
type Store struct {
	db *sql.DB
	// rd is where this database's reads go when they may be a moment old
	// (dbpool.Reader, 2026-10-13: a read instance beside the write
	// instance). Only the few reads that say so use it (old()); everything
	// else, and every read that must see what was just written, uses db.
	// nil, or no read instance set: old() is db.
	rd      *dbpool.Read
	ensured sync.Map // per-tenant-month record tables already created
	// noSummaryCounts: the summary tables lack the message-count columns of
	// hot/0060 (found on a rebuild); the summaries are then written without
	// them (withSummaryCounts).
	noSummaryCounts atomic.Bool
	// owner is the deployment whose queues this process serves
	// (deployment.Owner, D-121): it takes queue rows owned by it and the
	// shared ones (queue_owner NULL). "" = today's single shared queues, and
	// every claim reads exactly as it always did.
	owner string

	// depthLast is each queue's last good depth reading, kept so a read that
	// is slow or fails reports the last figures marked stale instead of
	// dropping the queue (which read as 0 on every screen).
	depthMu   sync.Mutex
	depthLast map[string]store.VendorQueueStat

	// The most times a message may be claimed (attemptcap): Claim passes over
	// one that has had its tries and leaves it due for the retry role.
	capMu     sync.RWMutex
	capGlobal int            // 0 = no cap, as before
	capVendor map[string]int // by vendor cid; wins over capGlobal

	// The database this store is on, for the names of its leases (TryLease).
	leaseMu      sync.Mutex
	leaseDB      string
	leaseDBKnown bool
}

// SetAttemptCap sets the most times Claim hands a message out: global for
// every queue, perVendor (by vendor cid) where a vendor has its own. A
// message at its cap stays due and unclaimed — the retry role writes it off.
// 0 = no cap. [attemptcap.Setter]
func (s *Store) SetAttemptCap(global int, perVendor map[string]int) {
	s.capMu.Lock()
	s.capGlobal, s.capVendor = global, perVendor
	s.capMu.Unlock()
}

// SetQueueOwner points the store at another deployment's queues — tests only;
// a role always serves deployment.Owner().
func (s *Store) SetQueueOwner(owner string) { s.owner = owner }

// QueueOwner is the deployment whose queues this store serves ("" = shared).
func (s *Store) QueueOwner() string { return s.owner }

// ownerCond is the claim-side filter for a queue table, mo_queue,
// client_dlr and record_index: my rows and the shared ones. A process that
// names no deployment takes only the shared ones — every row of an install
// that never set YAKU_DEPLOYMENT_ID, so nothing changes there — and never a
// deployment's, the same as the Redis store, which cannot even see them.
func (s *Store) ownerCond() (string, []any) {
	if s.owner == "" {
		return " AND queue_owner IS NULL", nil
	}
	return " AND (queue_owner = ? OR queue_owner IS NULL)", []any{s.owner}
}

// ensureCloneTable clones templateName into table once (lazy, cached in
// s.ensured — shared across all four traffic-log families plus the legacy
// records_template, keyed by the actual table name so no family collides).
func (s *Store) ensureCloneTable(ctx context.Context, table, templateName string) error {
	if _, ok := s.ensured.Load(table); ok {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS "+table+" LIKE "+templateName); err != nil {
		return err
	}
	s.ensured.Store(table, true)
	return nil
}

// forgetIfMissing drops a partition from the "already created" memo when a
// write found it gone (dropped under a running role — a test clean-up, an
// operator, a restore), so the caller's next attempt creates it again
// instead of failing until the role is restarted (found 2026-09-17: the
// blackhole role could not record a vendor's messages for that reason).
func (s *Store) forgetIfMissing(err error, tables ...string) error {
	if err != nil && isNoSuchTable(err) {
		for _, t := range tables {
			s.ensured.Delete(t)
		}
	}
	return err
}

// New is a store on this process's ONE pool for dsn (dbpool): every store,
// loader and helper a role builds on the same database shares its
// connections. Each call used to open its own pool of 50 with 25 kept idle —
// 31 of them across the 13 roles (production, 2026-10-06: 600+ connections).
func New(dsn string) (*Store, error) {
	db, err := dbpool.Get(dsn)
	if err != nil {
		return nil, err
	}
	rd, _ := dbpool.Reader(dsn)
	return &Store{db: db, rd: rd, owner: deployment.Owner()}, nil
}

// old is the pool for a read that may be a moment old: the read instance's
// when one is set and answering, else the pool everything is written
// through. A read node runs a little behind the write node — never use this
// for a read that must see a write made a moment ago.
func (s *Store) old() *sql.DB {
	if s.rd != nil {
		if d := s.rd.DB(); d != nil {
			return d
		}
	}
	return s.db
}

// ReadsApart says this store has a read instance of its own, so a read made
// through it may miss what was written a moment ago.
func (s *Store) ReadsApart() bool { return s != nil && s.rd.Separate() }

// Close does nothing: the pool is the process's, shared with every other
// user of the same database, and lives until the process ends.
func (s *Store) Close() error { return nil }

// DB exposes the handle so a caller can check the store is actually
// REACHABLE. New() only calls sql.Open, which does not connect — it succeeds
// with a wrong password, a dead host or an empty DSN — so without this a
// process cannot tell "configured" from "working" until its first query fails.
func (s *Store) DB() *sql.DB {
	if s == nil {
		return nil
	}
	return s.db
}

var identRe = regexp.MustCompile(`[^a-zA-Z0-9_]`)

func queueTable(vendor string) string {
	return "queue_" + strings.ToLower(identRe.ReplaceAllString(vendor, "_"))
}

func placeholders(n int) string { return strings.TrimRight(strings.Repeat("?,", n), ",") }

// MsgStatus is the current-status view of one message (dal.MsgStatus).
type MsgStatus = dal.MsgStatus

// DBNow returns the SQL session clock as a DATETIME string — captured BEFORE a
// sweep's dirty scan so the next watermark can never skip concurrent writes.
func (s *Store) DBNow(ctx context.Context) (string, error) {
	var now string
	err := s.db.QueryRowContext(ctx, `SELECT DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s')`).Scan(&now)
	return now, err
}

// rawDLREntry mirrors internal/dlr.entry's wire shape (Direction/VendorMsgID/
// Status/ErrorCode/Raw/At) — decoded here rather than imported, since
// internal/store/mysql must not depend on internal/dlr.
type rawDLREntry struct {
	VendorMsgID string `json:"vendor_msg_id"`
	Status      string `json:"status"`
	// Stat is the receipt's own SMPP word, one of the eight (hot/0045).
	Stat      string `json:"stat"`
	ErrorCode string `json:"error_code"`
	Raw       string `json:"raw"`
	// At is when the receipt reached us (RFC 3339); "" = now.
	At string `json:"at"`
	// DoneAt is the vendor's delivery time (RFC 3339), "" = not given.
	DoneAt string `json:"done_at"`
	// Part is which part of a long message the receipt is about, 0 = the
	// whole message or not known.
	Part int `json:"part,omitempty"`
	// Source is who made it (hot/0072): "vendor", "system"; "" = vendor, but
	// a `policy:` row is system.
	Source string `json:"source,omitempty"`
	// FinalStatus is the status the receipt closes its message with when a
	// per-vendor rule renamed the outcome; "" = Status.
	FinalStatus string `json:"final_status,omitempty"`
	// RecordedOnly: the message was final already, the client was not sent
	// this receipt, and its row keeps no price — it is evidence, not a charge
	// (go-live test 18 Oct 2026).
	RecordedOnly bool `json:"recorded_only,omitempty"`
	// What this receipt booked, as rate version ids (issue 16): the receipt
	// fee's selling rate and the vendor's receipt cost; 0 = not booked. FX is
	// the fee's conversion into the company's currency, 0 = none.
	SellRateID int64   `json:"sell_rate,omitempty"`
	BuyRateID  int64   `json:"buy_rate,omitempty"`
	FX         float64 `json:"fx,omitempty"`
	// Charge is what an older build put here (billing.ChargeMark JSON). Read
	// by nothing now.
	Charge string `json:"charge,omitempty"`
}

// statForStatus is the SMPP `stat:` word for one of the gateway's five
// statuses — internal/dlr.StatFor, spelled out again here for the same reason
// rawDLREntry is: internal/store/mysql must not depend on internal/dlr, which
// depends on it. Five lines duplicated beats a cycle; the receipt package's
// own tests pin the mapping.
func statForStatus(status string) string {
	switch status {
	case "delivered":
		return "DELIVRD"
	case "failed":
		return "UNDELIV"
	case "expired":
		return "EXPIRED"
	case "sent":
		return "ACCEPTD"
	case "rejected":
		return "REJECTD"
	case "queued":
		return "ENROUTE"
	}
	return "UNKNOWN"
}

// Archive is handled by the retention process (hot->cold); no-op here.
func (s *Store) Archive(ctx context.Context, ms []model.Message) error { return nil }

// The rows of the record store's writes are the DAL's (moved 30 Oct 2026,
// unchanged): internal/dal recordstore.go.
type (
	TerminalRecord = dal.TerminalRecord
	RecordRating   = dal.RecordRating
	RatedOutcome   = dal.RatedOutcome
)
