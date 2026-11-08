package monitor

// MySQL and Redis are read by ONE process of each deployment (2026-10-26,
// issue 1 of the design audit). Until then only the http-server read them, so
// a deployment that ran none reported no stores at all, and one that ran three
// read every store three times a push.
//
// Now any role may read them: on every push a process takes or keeps its
// deployment's turn, and only the holder adds `stores` to its snapshot. The
// turn is a key in the main Redis (this version needs MySQL and Redis both:
// Saleem, 2026-10-26, so there is no turn without Redis). It is held for
// storeTurnHolds push intervals and renewed at every push, so when the holder
// stops another role takes it a few intervals later.
// The first reading after a move has no rates (they are worked out between
// two reads), as after a restart.

import (
	"context"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/storewatch"
)

// storeTurnHolds is how many push intervals a turn lasts without a renewal.
const storeTurnHolds = 3

// StoreReader is what reads the stores: a *storewatch.Watcher in a role.
type StoreReader interface {
	Empty() bool
	Section(ctx context.Context) *storewatch.Section
	Close()
}

var (
	storeReaderMu    sync.Mutex
	newStoreReaderFn func() StoreReader
)

// SetStoreReader says how this process reads the stores when it has the turn
// (cmd/yakugate: storewatch.New(cfg, …)). Nothing is built until it is
// needed; unset, the process never takes the turn.
func SetStoreReader(build func() StoreReader) {
	storeReaderMu.Lock()
	defer storeReaderMu.Unlock()
	newStoreReaderFn = build
}

func storeReaderBuilder() func() StoreReader {
	storeReaderMu.Lock()
	defer storeReaderMu.Unlock()
	return newStoreReaderFn
}

// storeTurn is one process's part in its deployment's turn.
type storeTurn struct {
	scope string // the deployment, or the host when none is set
	me    string // role/instance
	hold  time.Duration
	rdb   dal.LiveStatus
	build func() StoreReader

	reader StoreReader
}

func newStoreTurn(role, instance string, every time.Duration, rdb dal.LiveStatus, build func() StoreReader) *storeTurn {
	if build == nil || rdb == nil {
		return nil
	}
	d := DeploymentInfo()
	scope := d.ID
	if scope == "" {
		scope = "host:" + d.Host
	}
	return &storeTurn{scope: scope, me: role + "/" + instance, hold: storeTurnHolds * every,
		rdb: rdb, build: build}
}

// section is the stores' reading when this process has the turn, else nil.
func (t *storeTurn) section(ctx context.Context) *storewatch.Section {
	if t == nil || !t.take(ctx) {
		return nil
	}
	if t.reader == nil {
		t.reader = t.build()
	}
	if t.reader == nil || t.reader.Empty() {
		return nil
	}
	return t.reader.Section(ctx)
}

// take takes the turn when it is free, or keeps it when it is ours (the
// live view's TakeStoreTurn: one script, storewatch:turn@<scope>). An error
// is "not mine": a Redis that cannot be asked must not make two readers.
func (t *storeTurn) take(ctx context.Context) bool {
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	ok, err := t.rdb.TakeStoreTurn(c, t.scope, t.me, t.hold)
	return err == nil && ok
}

// close gives the reader's connections back; the turn runs out by itself.
func (t *storeTurn) close() {
	if t != nil && t.reader != nil {
		t.reader.Close()
	}
}
