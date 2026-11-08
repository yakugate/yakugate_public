package mysql

import (
	"context"
	"database/sql"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/dbpool"
)

// A live lookup's answer written to its number's shard table (moved here
// unchanged on 29 Oct 2026 from internal/hlr/persist_mysql.go).

func init() {
	EngineStmts.Add("hlr_shard.put", hlrShardPutSQL("hlr_9230"), false)
}

// A live lookup's write to the HLR store.
var _ = serves(func(d *dal.Driver) { d.HLRShardWriter = openHLRShardWriter })

// openHLRShardWriter opens the writer on the HLR store's one pool a process.
func openHLRShardWriter(dsn string) (dal.HLRShardWriter, error) {
	db, err := dbpool.Get(dsn)
	if err != nil {
		return nil, err
	}
	return NewHLRShardWriter(db), nil
}

// HLRShardWriter is dal.HLRShardWriter on the HLR store.
type HLRShardWriter struct{ db *sql.DB }

// NewHLRShardWriter wraps the HLR store's handle (YAKU_HLR_DSN, else the
// queue database).
func NewHLRShardWriter(db *sql.DB) *HLRShardWriter { return &HLRShardWriter{db: db} }

// hlrShardPutSQL is the upsert into one shard table. last_check is set
// explicitly: its ON UPDATE only fires when a value changes, and a live lookup
// that confirms the same network must still restart the number's cache time
// limit (2026-09-14).
func hlrShardPutSQL(table string) string {
	return "INSERT INTO " + table + " (msisdn, mcc, mnc, ported, operator_name, ccc) VALUES (?, ?, ?, ?, ?, ?) " +
		"ON DUPLICATE KEY UPDATE mcc=VALUES(mcc), mnc=VALUES(mnc), ported=VALUES(ported), " +
		// a name or ccc the answer did not give keeps the one stored
		"operator_name=COALESCE(VALUES(operator_name), operator_name), ccc=COALESCE(VALUES(ccc), ccc), " +
		"is_deleted=0, last_check=CURRENT_TIMESTAMP"
}

// PutShard upserts the entry into hlr_<shard>. The table name is derived from
// a validated (digit) shard, never user input, so string-building it is
// safe; values are always parameterized.
func (w *HLRShardWriter) PutShard(ctx context.Context, shard string, r dal.HLRRow) error {
	_, err := w.db.ExecContext(ctx, hlrShardPutSQL("hlr_"+shard), r.MSISDN, r.MCC, r.MNC, boolInt(r.Ported),
		nullStr(r.OperatorName), nullStr(r.CCC)) // "" = not given: NULL keeps the stored one
	return err
}

var _ dal.HLRShardWriter = (*HLRShardWriter)(nil)
