package hlr

import (
	"context"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// FromShards is an HLR store's shard tables read whole (dal.HLRShards — on
// SQL Server, internal/dal/mssql/hlr.go, which was this package's
// store_mssql.go until 29 Oct 2026) as a DeltaSource: the ordinary in-memory
// Cache sits on top exactly as it does for MySQL. nil gives nil.
func FromShards(s dal.HLRShards) DeltaSource {
	if s == nil {
		return nil
	}
	return shards{s}
}

type shards struct{ s dal.HLRShards }

func (s shards) Lookup(msisdn string) (Entry, bool) {
	r, ok := s.s.Lookup(msisdn)
	if !ok {
		return Entry{}, false
	}
	return entryOf(r), true
}

func (s shards) Prepare(ctx context.Context) error { return s.s.Prepare(ctx) }

func (s shards) Count(ctx context.Context) (int64, error) { return s.s.Count(ctx) }

func (s shards) LoadSince(ctx context.Context, since time.Time) ([]Entry, time.Time, error) {
	rows, maxTS, err := s.s.LoadSince(ctx, since)
	if err != nil {
		return nil, maxTS, err
	}
	out := make([]Entry, len(rows))
	for i, r := range rows {
		out[i] = entryOf(r)
	}
	return out, maxTS, nil
}

// Close releases the store's connection.
func (s shards) Close() error { return s.s.Close() }

// entryOf is a shard row as an Entry.
func entryOf(r dal.HLRRow) Entry {
	return Entry{MSISDN: r.MSISDN, MCC: r.MCC, MNC: r.MNC, OperatorName: r.OperatorName, CCC: r.CCC,
		Ported: r.Ported, Deleted: r.Deleted, CheckedAt: r.CheckedAt}
}
