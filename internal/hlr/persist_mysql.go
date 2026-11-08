package hlr

// Persister writes a dipped entry into its MSISDN shard table
// (hlr_<first4>, in the HLR store — YAKU_HLR_DSN, else the queue database) so the result survives a restart and is
// shared across instances — the DB half of the dip's "updates DB and cache
// both" (P1 scope finalize 2026-08-09). Best-effort by design: a persist
// failure is logged, never propagated, because the in-memory cache already
// holds the entry for immediate use and the number will simply be re-dipped
// later if the row didn't land.
//
// The write itself is the DAL's (dal.HLRShardWriter, opened with
// dal.OpenHLRShardWriter); it was this file's own SQL until 29 Oct 2026, when
// the type was MySQLPersister.

import (
	"context"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// Persister satisfies Writer.
type Persister struct {
	w    dal.HLRShardWriter
	logf func(string, ...any)
}

// NewPersister wraps the HLR store's shard writer.
func NewPersister(w dal.HLRShardWriter, logf func(string, ...any)) *Persister {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Persister{w: w, logf: logf}
}

// Put upserts the entry into hlr_<Shard(msisdn)>. The shard tables are cloned
// from hlr_template at provisioning; a missing shard table is logged and
// skipped rather than erroring the dip.
func (p *Persister) Put(e Entry) {
	if e.MSISDN == "" {
		return
	}
	shard := Shard(e.MSISDN)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// The shard is derived from the number, never user input; the DAL builds
	// the table name from it and passes every value as a parameter.
	if err := p.w.PutShard(ctx, shard, dal.HLRRow{MSISDN: e.MSISDN, MCC: e.MCC, MNC: e.MNC, Ported: e.Ported,
		OperatorName: e.OperatorName, CCC: e.CCC}); err != nil {
		p.logf("[hlr-dip] persist %s to %s failed: %v (cache still holds it)", e.MSISDN, "hlr_"+shard, err)
	}
}
