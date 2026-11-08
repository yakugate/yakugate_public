package monitor

// Reports on demand — the portal's Live (Saleem 2026-10-05: "if live then each
// request must fetch fresh data from the gateway at that point of time").
//
// Normally the screen reads the reports the roles save every interval. Live
// asks for new ones: the http-server writes the ask into the hot store
// (monitor_state `report_now`), every role looks at it once a second, and a
// role that sees a new ask for its deployment (or for every deployment)
// reports at once. The Admin API waits for those reports and shows them.
//
// Through the shared store rather than a call to each role, because the roles
// of one deployment may run on several hosts and need no address of their own;
// and the ask carries its own time, so a role compares asks, never clocks. A
// role started after an ask does not answer it (it has just reported anyway).

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

const reportNowKey = "report_now"

// With Redis the ask is also written there (the live view's Ask, key
// mon:report_now), and read there by the roles.
const (
	// askRedisKeep is how long an ask stays in Redis: long past any wait.
	askRedisKeep = 10 * time.Minute
	// askStoreEvery: with Redis, the hot store is still looked at every this
	// many seconds, for an ask written by something that has no Redis.
	askStoreEvery = 5
)

// liveRef holds the live view an ask is written to.
type liveRef struct{ dal.LiveStatus }

// askRedis is the Redis the ask is written to, when this process has one
// (set by StartPush in a role, by UseRedisForAsks in the console).
var askRedis atomic.Pointer[liveRef]

// UseRedisForAsks gives a process that does not report itself (the console)
// the Redis to write its asks to, so the roles see them within a second.
func UseRedisForAsks(c dal.LiveStatus) {
	if c != nil {
		askRedis.Store(&liveRef{c})
	}
}

// readAskRedis is the current ask in Redis ("" when none or unreadable).
func readAskRedis(ctx context.Context, c dal.LiveStatus) string {
	cc, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	v, err := c.Ask(cc)
	if err != nil {
		return ""
	}
	return v
}

// activeSink is this process's history sink, for AskForReports.
var activeSink atomic.Pointer[SQLSink]

// ErrNoHistory: this process keeps no monitoring history to ask through.
var ErrNoHistory = errors.New("this process keeps no monitoring history (no hot store)")

type reportAsk struct {
	At         string `json:"at"`
	Deployment string `json:"deployment,omitempty"` // "" = every deployment
	Nonce      string `json:"nonce"`
}

// AskForReports asks every role of a deployment ("" = every deployment) to
// report now. It returns when the ask was written.
func AskForReports(ctx context.Context, deploymentID string) (time.Time, error) {
	s := activeSink.Load()
	if s == nil || s.h == nil {
		return time.Time{}, ErrNoHistory
	}
	return AskForReportsOn(ctx, s.h, deploymentID)
}

// AskForReportsOn writes the ask through the hot store's history (the
// console's Live, which keeps no history sink of its own).
func AskForReportsOn(ctx context.Context, db dal.MonitorStatements, deploymentID string) (time.Time, error) {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	at := time.Now().UTC()
	val, _ := json.Marshal(reportAsk{At: at.Format(time.RFC3339Nano), Deployment: deploymentID, Nonce: hex.EncodeToString(b)})
	err := db.SetState(ctx, reportNowKey, string(val))
	if c := askRedis.Load(); c != nil && err == nil {
		// Best effort: a role that misses it in Redis reads the store's copy
		// a few seconds later.
		_ = c.SetAsk(ctx, string(val), askRedisKeep)
	}
	return at, err
}

// readAsk is the current ask as stored ("" when none or unreadable).
func (s *SQLSink) readAsk(ctx context.Context) string {
	c, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	v, err := s.h.State(c, reportNowKey)
	if err != nil {
		return ""
	}
	return v
}

// askedOfMe says whether an ask (as stored) is for this process's deployment.
func askedOfMe(raw string) bool {
	var a reportAsk
	if json.Unmarshal([]byte(raw), &a) != nil {
		return false
	}
	return a.Deployment == "" || a.Deployment == DeploymentInfo().ID
}
