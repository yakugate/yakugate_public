// The SQL implementation of Loader + hlr.Store, aligned to schema v2.0. The
// statements are the DAL's (internal/dal/mysql/reference.go, since 29 Oct
// 2026); how each row becomes an account, a route or a rule is here.
package refdata

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// rowsErr says err was met while reading a list's rows (dal.RowsError), not
// by the statement itself.
func rowsErr(err error) bool {
	var re dal.RowsError
	return errors.As(err, &re)
}

// MySQL loads reference data from the CONFIG store (db); auth revocations,
// HLR shards and JWT revocations live in the HLR store (qdb): YAKU_HLR_DSN,
// else the queue database where they always were (D-124).
type MySQL struct {
	// ref is the config store's rows; hlr is the HLR store, nil when it
	// could not be opened (see NewMySQL). Which pool each read uses — the
	// read instance beside the write instance (dbpool.Reader, 2026-10-13),
	// the write instance after a forced refresh — is the DAL's.
	ref dal.ReferenceData
	hlr dal.HLRNumbers

	// depthLast is each queue's last good depth, used when a read fails or
	// runs out of time — a missing queue reads as empty, and Least busy would
	// then pick the busiest vendor (production load test, 2026-10-05).
	depthMu   sync.Mutex
	depthLast map[string]QueueDepth

	// futureSeen is each table's newest stamp already reported as being
	// ahead of the store's clock (noteFutureStamp).
	futureMu   sync.Mutex
	futureSeen map[string]string
}

// NewMySQL opens both the config-store and queue-store connections.
func NewMySQL(configDSN, queueDSN string) (*MySQL, error) {
	return NewSQL(config.BackendMySQL, configDSN, queueDSN)
}

// For is the loader of an install: on the DAL of its SQL database (the
// config store's, dal.SQL), MySQL or SQL Server alike, the HLR half on the
// HLR store.
func For(cfg config.Config) (*MySQL, error) {
	return NewSQL(dal.SQL(cfg), cfg.ConfigDSN, cfg.HLRDSNOf())
}

// NewSQL is NewMySQL on the DAL of database on.
func NewSQL(on config.Backend, configDSN, queueDSN string) (*MySQL, error) {
	d, ok := dal.Lookup(on)
	if !ok || d.ReferenceData == nil || d.HLRNumbers == nil {
		return nil, fmt.Errorf("refdata: no %s DAL in this binary (have %v)", on, dal.Registered())
	}
	ref, err := d.ReferenceData(configDSN) // the process's one pool per database
	if err != nil {
		return nil, err
	}
	// The QUEUE connection is OPTIONAL, and that is the whole point of this
	// function returning successfully without it.
	//
	// Most of what this loader reads is in the config store; only JWT
	// revocations and the HLR shards live in the HLR store (YAKU_HLR_DSN,
	// else the queue database — D-124), passed in here. A deployment is
	// perfectly entitled to run config on MySQL and its queue on Mongo or SQL
	// Server, and until 2026-09-09 that combination failed HERE: sql.Open
	// rejects "mongodb://..." at DSN-parse time, the error bubbled out, and the
	// whole loader was discarded — so the account cache stayed empty and EVERY
	// request was refused, over a connection that only two of eleven reads
	// need. Found by pointing the queue store at a real MongoDB.
	//
	// Degrade instead: keep the config half working and say loudly which two
	// things are not available.
	h, err := d.HLRNumbers(queueDSN)
	if err != nil {
		log.Printf("[refdata] the queue store is NOT %s (%v) — reference data from the config store loads normally, "+
			"but JWT REVOCATIONS CANNOT BE READ (a revoked token will be accepted until its own expiry) and HLR lookups "+
			"from this loader will miss", on, err)
		h = nil
	}
	return &MySQL{ref: ref, hlr: h}, nil
}

func b(i int) bool { return i != 0 }

// bare is a list's error as the old loops returned it: a row's error
// without the dal.RowsError mark, the statement's own as it is.
func bare(err error) error {
	var re dal.RowsError
	if errors.As(err, &re) {
		return re.Err
	}
	return err
}

// isMissingTable reports ER_NO_SUCH_TABLE — the "optional table not
// provisioned yet" signal a few additive features treat as empty-not-error.
func (l *MySQL) isMissingTable(err error) bool { return l.ref.MissingTable(err) }

var _ = time.Now // keep time import if unused paths trimmed
