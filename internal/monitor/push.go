package monitor

import (
	"context"
	"encoding/json"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// Redis status publication — the cluster view. Every role pushes its own
// StatusSnapshot to `status:<role>:<instance>` on an interval; the key
// carries a TTL of 3× the interval so a dead instance disappears from the
// cluster view by itself instead of showing stale numbers forever.
//
// The keys are found through a LIST of them, never by walking the key space
// (2026-10-08). ClusterStatus used to SCAN for `status:*`, which reads every
// key Redis holds: with five million receipts waiting in the same Redis that
// took six seconds and more, so the console's Monitor, the health page and
// /status/cluster ran out of time and showed nothing — on exactly the day
// somebody needs them. Every push now also files its key in a list of them
// (scored by the time of the push), and a reader takes the keys from there
// (the live view, dal.LiveStatus: Publish and Statuses).

func statusKeyFor(role, instance string) string { return "status:" + role + ":" + instance }

// publishStatus writes one snapshot and lists its key.
func publishStatus(ctx context.Context, live dal.LiveStatus, key string, snap []byte, every time.Duration) error {
	return live.Publish(ctx, key, snap, every)
}

// StartRedisPush publishes the snapshot every `every` until ctx is
// cancelled. Push failures are logged and retried next tick — monitoring
// must never take a role down.
func StartRedisPush(ctx context.Context, rdb dal.LiveStatus, role, instance string,
	start time.Time, w any, every time.Duration, logf func(string, ...any)) {
	StartPush(ctx, rdb, nil, role, instance, start, w, every, logf)
}

// StartPush is StartRedisPush plus the durable history sink.
//
// One timer, two sinks, on purpose. The snapshot is built ONCE per tick and
// handed to both: building it twice would double the /proc reads and, worse,
// could give the live view and the history two different pictures of the same
// instant, which is precisely the kind of discrepancy that wastes an hour
// during an incident.
//
// Either sink may be nil. A deployment without Redis still gets history; one
// without a hot store still gets the live view.
func StartPush(ctx context.Context, rdb dal.LiveStatus, history *SQLSink,
	role, instance string, start time.Time, w any, every time.Duration, logf func(string, ...any)) {
	if every < MinPushInterval {
		every = MinPushInterval
	}
	t := time.NewTicker(every)
	defer t.Stop()
	if history != nil {
		history.pushSec = int(every / time.Second)
		activeSink.Store(history)
		go history.runMaintenance(ctx, role, logf)
		go history.runEvents(ctx, logf)
	}
	var histFails int64
	var lastHistLog time.Time
	// MySQL and Redis: read by the one process of the deployment that holds
	// the turn (storeturn.go).
	turn := newStoreTurn(role, instance, every, rdb, storeReaderBuilder())
	defer turn.close()
	push := func() {
		snap := StatusSnapshot(role, instance, start, w)
		if st := turn.section(ctx); st != nil {
			snap["stores"] = st
		}
		if rdb != nil {
			b, err := json.Marshal(snap)
			if err == nil {
				err = publishStatus(ctx, rdb, statusKeyFor(role, instance), b, every)
			}
			if err != nil && logf != nil {
				logf("status push failed: %v", err)
			}
		}
		if history != nil {
			// Bounded by the sample interval: a slow database must not hold up
			// the next sample, and a monitoring write must never queue behind
			// itself into a backlog.
			c, cancel := context.WithTimeout(ctx, every)
			err := history.PublishRaw(c, role, instance, snap)
			cancel()
			if err != nil {
				// Rate-limited to once a minute. A failing history sink is
				// worth knowing about; twelve complaints a minute in the same
				// log an operator is reading to diagnose an incident is not.
				histFails++
				if logf != nil && time.Since(lastHistLog) >= time.Minute {
					lastHistLog = time.Now()
					logf("monitor history write failed (%d since start, sampling continues): %v", histFails, err)
				}
			}
		}
	}
	push()
	// Asked to report now (the portal's Live, reportnow.go): looked at once a
	// second, answered once per ask. The ask standing when this process
	// starts is not answered — it has just reported.
	//
	// With Redis the ask is read there (one GET a second) and the hot store
	// only every askStoreEvery-th second, for an ask written by something
	// that has no Redis (2026-10-13: every role read monitor_state once a
	// second, 13 statements a second for a gateway at rest).
	var askC <-chan time.Time
	inStore := history != nil && history.h != nil
	if rdb != nil {
		askRedis.Store(&liveRef{rdb})
	}
	// The same ask is written to both places, so each place remembers what it
	// last showed: an ask is new once, wherever it is seen first.
	var lastRedis, lastStore string
	newAsk := func(store bool) (fresh string) {
		if rdb != nil {
			if v := readAskRedis(ctx, rdb); v != "" && v != lastRedis {
				lastRedis, fresh = v, v
			}
		}
		if inStore && store {
			if v := history.readAsk(ctx); v != "" && v != lastStore {
				lastStore = v
				if v != lastRedis {
					fresh = v
				}
			}
		}
		return fresh
	}
	if inStore || rdb != nil {
		newAsk(true) // the ask standing at start is not answered
		ask := time.NewTicker(time.Second)
		defer ask.Stop()
		askC = ask.C
	}
	for tick := 1; ; tick++ {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			push()
		case <-askC:
			if v := newAsk(rdb == nil || tick%askStoreEvery == 0); v != "" && askedOfMe(v) {
				push()
			}
		}
	}
}

// ClusterStatus reads every live instance's pushed snapshot — the
// gateway-wide view, by status key (the live view's Statuses: the listed
// keys, and the key space walked only when nothing is listed).
func ClusterStatus(ctx context.Context, rdb dal.LiveStatus) (map[string]json.RawMessage, error) {
	return rdb.Statuses(ctx)
}

// ClusterFromHistory is the cluster view of a gateway with no Redis (plan of
// 29 Oct 2026, step 3): each instance's newest report in monitor_samples
// made within three push intervals — the life a Redis status key has, so an
// instance that stopped drops out as it does there — by status key. The
// history is the one SetOverviewSources was given (every role's own
// monitoring history handle); errNoSource when there is none.
func ClusterFromHistory(ctx context.Context) (map[string]json.RawMessage, error) {
	if pageHistory == nil {
		return nil, errNoSource
	}
	every := MinPushInterval
	if s := activeSink.Load(); s != nil && s.pushSec > 0 {
		every = time.Duration(s.pushSec) * time.Second
	}
	return FromHistory(ctx, pageHistory, 3*every)
}
