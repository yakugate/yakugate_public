package bootstrap

// Filling a separate HLR store (Saleem 2026-10-02, D-124).
//
// The HLR shards, HTTP vendor sessions and JWT revocations lived in the queue
// database from the start. Once YAKU_HLR_DSN names a database of their own,
// that database is created with the hlr/ migration set and what the queue
// database held is copied in — once, in the background, so a large HLR does
// not hold a role's start. Rows already in the new database win (INSERT
// IGNORE): a lookup kept or a session made since the switch is newer than the
// copy. The queue database's tables are left as they are.
//
// The copy itself is the DAL's since 30 Oct 2026 (dal.Schema.CopyHLRFrom);
// when it runs is decided here.

import (
	"context"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/platform/logging"
)

// startHLRCopy fills a separate HLR store from the queue database, in the
// background. Nothing to do when the HLR store is the queue database, or when
// there is no queue database of the same kind (MySQL, or SQL Server) to copy
// from.
func startHLRCopy(cfg config.Config, log *logging.Logger) {
	hb, hd := cfg.HLRStore()
	if !cfg.HLRSeparate() || hd == "" || !builds(hb) || !queueDatabaseIs(cfg, hb) {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Hour)
		defer cancel()
		n, err := copyHLR(ctx, hb, cfg.QueueDSN, hd, log)
		switch {
		case err != nil:
			log.Printf("HLR store: copy from the queue database FAILED (%v) — tried again at the next start; lookups not yet copied miss until then", err)
		case n > 0:
			log.Printf("HLR store: copied %d row(s) of HLR data and vendor sessions from the queue database", n)
		}
	}()
}

// CopyHLRFromQueue copies the HLR shards, vendor sessions and JWT revocations
// from the queue database (queueDSN) into the HLR store (hlrDSN), once: a
// finished copy leaves a marker, and one process copies at a time. Returns
// the rows inserted. Both are MySQL databases (startHLRCopy's rule).
func CopyHLRFromQueue(ctx context.Context, queueDSN, hlrDSN string, log *logging.Logger) (int64, error) {
	return copyHLR(ctx, config.BackendMySQL, queueDSN, hlrDSN, log)
}

// queueDatabaseIs reports whether the queue store has a database of kind b
// to copy the HLR data from.
func queueDatabaseIs(cfg config.Config, b config.Backend) bool {
	return cfg.QueueStoreSQL() == b
}

// copyHLR is CopyHLRFromQueue on either database (both stores on it).
func copyHLR(ctx context.Context, on config.Backend, queueDSN, hlrDSN string, log *logging.Logger) (int64, error) {
	sch, err := dal.OpenSchema(config.Config{}, on, "hlr", hlrDSN)
	if err != nil {
		return 0, err
	}
	defer sch.Close()
	var p dal.Printer
	if log != nil {
		p = log // a nil logger stays a nil Printer: the DAL then writes nothing
	}
	return sch.CopyHLRFrom(ctx, queueDSN, p)
}
