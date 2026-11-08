package monitor

import "time"

// Three-state liveness, taken from the Hadal reference and tightened.
//
// Hadal labels every user, queue and connector running / dormant / inactive,
// and that is the single most useful idea in its whole payload: it separates
// "broken" from "quiet". A gateway at 3am with no traffic is not a gateway in
// trouble, and a binary up/down cannot say so — which is how monitoring
// systems train people to ignore them.
//
//	running   traffic within the recent window: working, and provably so
//	dormant   connected and healthy, but nothing has moved lately
//	inactive  nothing has moved for a long time, or it was never up
//
// The distinction that matters operationally is dormant vs inactive at 3am. A
// dormant vendor bind is fine. An inactive one that should be carrying traffic
// is an outage nobody has noticed yet.
type Liveness string

const (
	Running  Liveness = "running"
	Dormant  Liveness = "dormant"
	Inactive Liveness = "inactive"
)

// LivenessWindows tune the thresholds. Defaults suit a 5-second sample.
type LivenessWindows struct {
	// Running is how recently activity must have happened to count as running.
	Running time.Duration
	// Dormant is the outer edge: past this, inactive.
	Dormant time.Duration
}

// DefaultLiveness is deliberately generous on the running edge. At a 5-second
// sample rate a 60-second window means a role handling one message a minute
// still reads as running, which is true and is what an operator expects. Too
// tight and a low-volume account flickers between states every sample, which
// is worse than no signal because people learn to ignore flapping.
var DefaultLiveness = LivenessWindows{Running: 60 * time.Second, Dormant: 15 * time.Minute}

// State classifies from the last activity time. A zero lastActivity means
// nothing has EVER happened, which is inactive rather than dormant — those
// differ: dormant implies it worked once.
func (w LivenessWindows) State(lastActivity, now time.Time) Liveness {
	if lastActivity.IsZero() {
		return Inactive
	}
	age := now.Sub(lastActivity)
	switch {
	case age <= w.Running:
		return Running
	case age <= w.Dormant:
		return Dormant
	}
	return Inactive
}

// State classifies using the default windows.
func State(lastActivity, now time.Time) Liveness { return DefaultLiveness.State(lastActivity, now) }

// Rate is a success ratio that can say "I don't know".
//
// The Hadal reference reports successRate "0.00%" for a connector that has
// handled nothing at all, which is indistinguishable on a dashboard from a
// connector where every single message failed. Those two want opposite
// reactions — one is idle, the other is an outage — and Yakugate's own P3
// status had inherited the same bug.
//
// So Known is explicit. A reader that ignores it and prints Percent gets 0,
// exactly as before; a reader that honours it can print "—".
type Rate struct {
	Known   bool    `json:"known"`
	Percent float64 `json:"percent"`
	OK      int64   `json:"ok"`
	Failed  int64   `json:"failed"`
}

// SuccessRate builds a Rate from counts.
func SuccessRate(ok, failed int64) Rate {
	total := ok + failed
	if total <= 0 {
		return Rate{OK: ok, Failed: failed} // Known stays false: no data, not zero success
	}
	return Rate{Known: true, Percent: float64(ok) / float64(total) * 100, OK: ok, Failed: failed}
}
