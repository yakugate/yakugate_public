package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/golang-sql/civil"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// Vendor quality's per-minute counts in the hot store when Redis is off
// (mirrors the vendor quality part of internal/dal/mysql/traffic.go):
// vendor_quality_minute, one row per minute, vendor and network — a small
// counter table, not a query over messages. Minutes and cutoffs are
// computed here, never with the database's clock.

// QualityStmts is vendor quality's statements.
var QualityStmts = newRegistry()

var _ = serves(func(d *dal.Driver) { d.VendorQuality = openVendorQuality })

// openVendorQuality opens the counts on the hot store's one pool a process.
func openVendorQuality(cfg config.Config) (dal.VendorQuality, error) {
	db, err := Get(cfg.HotDSN)
	if err != nil {
		return nil, err
	}
	return NewVendorQuality(db), nil
}

// qualityRow is a row of dbo.vendor_quality_rows (hot store), the table
// type an Add is sent as.
type qualityRow struct {
	Minute    civil.DateTime
	VendorID  int64
	MCC       string
	MNC       string
	Delivered int64
	Judged    int64
}

var (
	stQualityAdd = AddUpsertBatch(QualityStmts, "vendor_quality.add", UpsertSpec{
		Table: "dbo.vendor_quality_minute", Keys: []string{"minute", "vendor_id", "mcc", "mnc"},
		Set:  []string{"delivered = t.delivered + $delivered", "judged = t.judged + $judged"},
		Type: "dbo.vendor_quality_rows",
	}, false)
	stQualityPrune = QualityStmts.Add("vendor_quality.prune",
		`DELETE FROM dbo.vendor_quality_minute WHERE minute < @p1`, false)
	stQualityLoad = QualityStmts.Add("vendor_quality.load", `
		SELECT vendor_id, mcc, mnc, SUM(delivered), SUM(judged)
		  FROM dbo.vendor_quality_minute WHERE minute >= @p1
		 GROUP BY vendor_id, mcc, mnc`, false)
)

// VendorQuality adds to vendor_quality_minute when Redis is off.
type VendorQuality struct {
	db        *sql.DB
	mu        sync.Mutex
	lastPrune time.Time
}

var _ dal.VendorQuality = (*VendorQuality)(nil)

// NewVendorQuality wraps a hot-store pool. nil db = nil store.
func NewVendorQuality(db *sql.DB) *VendorQuality {
	if db == nil {
		return nil
	}
	return &VendorQuality{db: db}
}

type qualityKey struct {
	minute time.Time
	key    dal.QualityKey
}

// Add adds the buckets, 200 rows a statement, and once an hour drops what is
// older than a day. Two buckets of one minute and key in one call are added
// together first (MySQL's multi-row upsert adds them one after the other).
func (s *VendorQuality) Add(ctx context.Context, bs []dal.QualityBucket) error {
	const per = 200
	at := make(map[qualityKey]int, len(bs)) // a key → its row
	rows := make([]qualityRow, 0, len(bs))
	for _, b := range bs {
		m := b.Minute.UTC()
		k := qualityKey{m, b.Key}
		if i, ok := at[k]; ok {
			rows[i].Delivered += b.Counts.Delivered
			rows[i].Judged += b.Counts.Judged
			continue
		}
		at[k] = len(rows)
		rows = append(rows, qualityRow{Time(m), b.Key.VendorID, b.Key.MCC, b.Key.MNC, b.Counts.Delivered, b.Counts.Judged})
	}
	for start := 0; start < len(rows); start += per {
		if err := upsertRows(ctx, s.db, stQualityAdd, rows[start:min(start+per, len(rows))]); err != nil {
			return fmt.Errorf("vendor_quality_minute: %w", err)
		}
	}
	s.mu.Lock()
	prune := time.Since(s.lastPrune) >= time.Hour
	if prune {
		s.lastPrune = time.Now()
	}
	s.mu.Unlock()
	if prune {
		// A day is far more than the one-hour window any reader uses; the
		// rest is only kept so the table never grows past a day of rows.
		_, _ = counterExec(ctx, s.db, stQualityPrune, Time(time.Now().UTC().Add(-24*time.Hour)))
	}
	return nil
}

// Load sums the window.
func (s *VendorQuality) Load(ctx context.Context, window time.Duration) (map[dal.QualityKey]dal.QualityCounts, error) {
	start := time.Now()
	rows, err := s.db.QueryContext(ctx, stQualityLoad.SQL, Time(time.Now().UTC().Add(-window).Truncate(time.Minute)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[dal.QualityKey]dal.QualityCounts{}
	for rows.Next() {
		var k dal.QualityKey
		var c dal.QualityCounts
		if err := rows.Scan(&k.VendorID, &k.MCC, &k.MNC, &c.Delivered, &c.Judged); err != nil {
			return nil, err
		}
		out[k] = c
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	observe(stQualityLoad, start, int64(len(out)))
	return out, nil
}
