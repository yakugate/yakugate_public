package mssql

import (
	"context"
	"database/sql"
	"sync"

	"github.com/yakugate/yakugate/internal/dal"
)

// A live lookup's answer written to its number's shard table, on SQL
// Server, made like internal/dal/mysql/hlrshard.go. last_check is set by the
// write itself (SQL Server has no ON UPDATE), so a lookup that confirms the
// same network still restarts the number's cache time.

// hlrShardSpec is the upsert into one shard table.
func hlrShardSpec(table string) UpsertSpec {
	return UpsertSpec{
		Table: table, Keys: []string{"msisdn"},
		Set: []string{"mcc = $mcc", "mnc = $mnc", "ported = $ported",
			// a name or ccc the answer did not give keeps the one stored (6 Nov 2026)
			"operator_name = COALESCE($operator_name, operator_name)", "ccc = COALESCE($ccc, ccc)",
			"is_deleted = 0", "last_check = SYSUTCDATETIME()"},
		Insert: []string{"msisdn", "mcc", "mnc", "ported", "operator_name", "ccc"},
	}
}

var (
	stHLRShardPut = AddUpsert(EngineStmts, "hlr_shard.put", hlrShardSpec("hlr_9230"), false)
	// A memory-optimized shard takes no locking hint: its upsert is an
	// UPDATE and, when it found no row, an INSERT, each its own statement
	// (the engine's optimistic rows; a lost race is the duplicate key, met
	// by updating again).
	stHLRShardPutMemOpt = addEngine("hlr_shard.put_memory_optimized", hlrShardPutMemOptSQL("hlr_9230"))
	stHLRShardKind      = addEngine("hlr_shard.kind",
		`SELECT is_memory_optimized FROM sys.tables WHERE object_id = OBJECT_ID(?)`)
)

func hlrShardPutMemOptSQL(table string) string {
	t := quoteIdent(table)
	return `SET NOCOUNT ON;
UPDATE ` + t + ` SET mcc = @mcc, mnc = @mnc, ported = @ported, operator_name = COALESCE(@operator_name, operator_name), ccc = COALESCE(@ccc, ccc), is_deleted = 0, last_check = SYSUTCDATETIME() WHERE msisdn = @msisdn;
IF @@ROWCOUNT = 0 INSERT INTO ` + t + ` (msisdn, mcc, mnc, ported, operator_name, ccc) VALUES (@msisdn, @mcc, @mnc, @ported, @operator_name, @ccc);`
}

// openHLRShardWriter opens the writer on the HLR store's one pool a process.
func openHLRShardWriter(dsn string) (dal.HLRShardWriter, error) {
	db, err := Get(dsn)
	if err != nil {
		return nil, err
	}
	return NewHLRShardWriter(db), nil
}

// HLRShardWriter is dal.HLRShardWriter on the HLR store.
type HLRShardWriter struct {
	db *sql.DB
	// memOpt says, by table, whether a shard is memory-optimized (asked once).
	memOpt sync.Map
}

// NewHLRShardWriter wraps the HLR store's handle.
func NewHLRShardWriter(db *sql.DB) *HLRShardWriter { return &HLRShardWriter{db: db} }

// PutShard upserts the entry into hlr_<shard>. The table name is derived from
// a validated (digit) shard, never user input; values are always parameters.
// A shard that is not there is an error (208), as on MySQL.
func (w *HLRShardWriter) PutShard(ctx context.Context, shard string, r dal.HLRRow) error {
	table := "hlr_" + shard
	args := []any{sql.Named("msisdn", Varchar(r.MSISDN)), sql.Named("mcc", r.MCC), sql.Named("mnc", r.MNC), sql.Named("ported", cfgBool(r.Ported)),
		sql.Named("operator_name", nullTrim(r.OperatorName)), sql.Named("ccc", nullTrim(r.CCC))}
	mem, err := w.memoryOptimized(ctx, table)
	if err != nil {
		return err
	}
	if !mem {
		_ = stHLRShardPut
		var found int // the Upsert shape, built for this table
		return w.db.QueryRowContext(ctx, UpsertSQL(hlrShardSpec(table)), args...).Scan(&found)
	}
	_ = stHLRShardPutMemOpt
	q := hlrShardPutMemOptSQL(table)
	_, err = w.db.ExecContext(ctx, q, args...)
	if IsDuplicate(err) || IsRetryable(err) {
		_, err = w.db.ExecContext(ctx, q, args...) // the row came in between: it is there now
	}
	return err
}

// memoryOptimized says the shard is a memory-optimized table. A table that
// is not there reads as a disk table (its write then fails with 208).
func (w *HLRShardWriter) memoryOptimized(ctx context.Context, table string) (bool, error) {
	if v, ok := w.memOpt.Load(table); ok {
		return v.(bool), nil
	}
	var mem bool
	err := w.db.QueryRowContext(ctx, stHLRShardKind.SQL, quoteIdent(table)).Scan(&mem)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	w.memOpt.Store(table, mem)
	return mem, nil
}

var _ dal.HLRShardWriter = (*HLRShardWriter)(nil)
