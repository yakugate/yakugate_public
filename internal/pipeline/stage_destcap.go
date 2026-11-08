package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"time"
)

// --- stage 15/19: per-destination frequency cap --- [SCOPE P1-13D]
//
// How many messages this account may send to the SAME number inside a fixed
// window (accounts.dest_cap_hourly / dest_cap_daily, migration config/0096).
//
// Why this is not dedupe's job: dedupe's fingerprint is sender+body+
// destination, so it fires only on the identical message sent twice and
// replays the original id. Ten DIFFERENT messages to one number have ten
// different fingerprints and pass it untouched. Bombing one subscriber with
// unique text is exactly what this stage sees and dedupe cannot.
//
// It sits after quota and before HLR on purpose: no point paying for an HLR
// dip (which can be a live network call) on a message the cap will refuse.
//
// Mechanically it is the quota stage's twin — period-stamped keys read here,
// incremented by the enqueue stage only on a durable write (chunk-6 ruling
// 6.1) — with two differences: the unit is MESSAGES, not segments, and the
// key carries the destination. The same known race applies: two submits
// arriving together can both read 4-of-5 and both enqueue, so this is a close
// ceiling, not a hard one.
type destCapStage struct {
	usage UsageCounter     // nil = no counter wired, cap cannot be enforced
	now   func() time.Time // injectable for tests
}

func (destCapStage) Name() string { return "destcap" }

func (s destCapStage) Run(_ context.Context, e *Envelope) *Reject {
	// Off is the default and off is FREE — return before hashing anything or
	// touching a counter, so an account that does not use this pays nothing.
	if s.usage == nil || e.Account == nil ||
		(e.Account.DestCapHourly <= 0 && e.Account.DestCapDaily <= 0) {
		return nil
	}
	nowFn := s.now
	if nowFn == nil {
		nowFn = time.Now
	}
	// UTC for the same reason quota is UTC: an accounting window must roll at
	// one instant for everybody. Quiet-hours is the tenant-local-time feature.
	now := nowFn().UTC()

	// The destination is HASHED into the key rather than stored raw. These
	// keys live in Redis, and a subscriber's number is the one field this
	// engine works hardest to keep out of places it does not belong (the log
	// redactor exists for the same reason). 16 hex characters is ample here:
	// a collision would merely let two numbers share a counter.
	scope := "dc:" + strconv.FormatInt(e.Account.ID, 10) + ":" + destKey(e.Msg.Destination)

	if lim := e.Account.DestCapHourly; lim > 0 {
		key := scope + ":h" + now.Format("2006010215")
		if s.usage.Get(key)+1 > lim {
			return rejectAfter(429, "DESTINATION_CAP: hourly limit to this number reached", secsUntil(now, now.Truncate(time.Hour).Add(time.Hour)))
		}
		e.DestCapKeys = append(e.DestCapKeys, key)
	}
	if lim := e.Account.DestCapDaily; lim > 0 {
		key := scope + ":d" + now.Format("20060102")
		if s.usage.Get(key)+1 > lim {
			next := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, time.UTC)
			return rejectAfter(429, "DESTINATION_CAP: daily limit to this number reached", secsUntil(now, next))
		}
		e.DestCapKeys = append(e.DestCapKeys, key)
	}
	return nil
}

// destKey is the short, non-reversible stand-in for a destination used in
// counter keys. Truncated because these keys are numerous and their only job
// is to separate one subscriber's tally from another's.
func destKey(dest string) string {
	sum := sha256.Sum256([]byte(dest))
	return hex.EncodeToString(sum[:8])
}

// secsUntil rounds UP: the window has not rolled until the boundary has
// passed, so truncating would send the caller back a fraction early into the
// same refusal. Always at least 1 — "retry in 0 seconds" is not advice.
func secsUntil(now, next time.Time) int {
	secs := int((next.Sub(now) + time.Second - 1) / time.Second)
	if secs < 1 {
		secs = 1
	}
	return secs
}
