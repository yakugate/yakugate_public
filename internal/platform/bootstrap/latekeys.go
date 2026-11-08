package bootstrap

import (
	"context"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/platform/logging"
)

// Keys added AFTER start (2026-10-08).
//
// The clone keys (dal.Schema.SyncClones) are added to the tables cloned from
// a template while a role starts, under the migration lock — right for a key
// the gateway cannot work correctly without. It is wrong for a key that only
// makes a screen fast: the three search keys of the Rejected list (hot/0058),
// added that way to a month of 6.7 million refusals, held every role's start
// for four and a half minutes on the dev box. The same holds for the
// receive-time key of an incoming-message month (hot/0060).
//
// Those keys are added in the background instead (dal.Schema.AddLateKeys:
// which keys, and how, is the DAL's since 30 Oct 2026): one process does it
// (an advisory lock, not waited for), in ONE pass over each table, with
// writes to the table allowed throughout. Nothing waits for it and nothing
// depends on it — until it is done the screen is as slow as it was. A process
// stopped half way simply leaves the table without the keys; the next start
// tries again.

// startLateCloneKeys adds the late keys in the background. Called at every
// role's start; the lock lets one process do the work.
func startLateCloneKeys(cfg config.Config, log *logging.Logger) {
	if !builds(cfg.HotBackend) || cfg.HotDSN == "" {
		return
	}
	lateKeys.Add(1)
	go func() {
		defer lateKeys.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Hour)
		defer cancel()
		if err := lateCloneKeysNow(ctx, cfg, log); err != nil {
			log.Printf("search keys: %v — tried again at the next start; until then the Rejected list and the incoming-message summary are slow, nothing else", err)
		}
	}()
}

// lateKeys counts the background jobs startLateCloneKeys has running.
var lateKeys sync.WaitGroup

// WaitLateKeys returns once the keys being added in the background are in (or
// at once, when another process is adding them or there are none to add). A
// role never calls it. The `migrate` command does: run by hand it should
// finish the whole job before it exits, not be cut off half way.
func WaitLateKeys() { lateKeys.Wait() }

// lateCloneKeysNow adds the late keys of the hot store, and returns when done.
func lateCloneKeysNow(ctx context.Context, cfg config.Config, log *logging.Logger) error {
	sch, err := dal.OpenSchema(cfg, cfg.HotBackend, "hot", cfg.HotDSN)
	if err != nil {
		return err
	}
	defer sch.Close()
	return sch.AddLateKeys(ctx, log)
}
