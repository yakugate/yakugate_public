package httpadapter

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"time"
)

// RetrySpec is an endpoint row's Retry (JSON) for a vendor call (2026-10-01,
// QA EP-018: the screen offered it and the engine never read it).
//
// It re-sends the SAME call at once, before the message is handed on, and
// only when re-sending cannot deliver a message twice:
//   - the vendor ANSWERED with one of the call's Retryable Status Codes (it
//     said "not now", so it did not take the message), or
//   - the connection could not be opened at all (nothing reached it).
//
// A timeout or a dropped connection after the request went out is NOT
// retried here: the vendor may have taken the message, and the routing
// plan's retry rounds decide what happens next, as before.
type RetrySpec struct {
	Max        int     `json:"max"`          // extra tries after the first (capped at maxRetries)
	BackoffMS  int     `json:"backoff_ms"`   // wait before the first retry (default 1000)
	Multiplier float64 `json:"multiplier"`   // growth per retry (default 2; 1 = flat)
	MaxDelayMS int     `json:"max_delay_ms"` // ceiling on one wait (default and cap 30 s)
}

// Bounds: the send worker waits through every retry, so a typo such as
// {"max": 300} must not stop a vendor's whole queue for an hour.
const (
	maxRetries    = 5
	maxRetryDelay = 30 * time.Second
)

// retryWindow bounds the whole in-call retry: no re-send is started that
// could end (its wait plus the call's timeout) more than this long after the
// first try. A claimed message is leased for 30 s; a send still retrying past
// that can be claimed and sent again by another http-vendor process.
var retryWindow = 25 * time.Second

// ParseRetry reads a row's retry column. It takes the screen's own example
// {"max": 3, "backoff_ms": 1000} and the push-policy spelling
// {"max_attempts": 4, "base_delay_sec": 1, "multiplier": 2, "max_delay_sec": 10}
// (max_attempts counts the first try). nil = no retry configured.
func ParseRetry(m map[string]any) *RetrySpec {
	if len(m) == 0 {
		return nil
	}
	num := func(keys ...string) (float64, bool) {
		for _, k := range keys {
			switch v := m[k].(type) {
			case float64:
				return v, true
			case int:
				return float64(v), true
			case string:
				if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
					return f, true
				}
			}
		}
		return 0, false
	}
	r := &RetrySpec{}
	if v, ok := num("max", "retries"); ok {
		r.Max = int(v)
	} else if v, ok := num("max_attempts"); ok {
		r.Max = int(v) - 1
	}
	if r.Max <= 0 {
		return nil
	}
	if r.Max > maxRetries {
		r.Max = maxRetries
	}
	if v, ok := num("backoff_ms", "base_delay_ms"); ok {
		r.BackoffMS = int(v)
	} else if v, ok := num("base_delay_sec"); ok {
		r.BackoffMS = int(v * 1000)
	}
	if v, ok := num("multiplier"); ok {
		r.Multiplier = v
	}
	if v, ok := num("max_delay_ms"); ok {
		r.MaxDelayMS = int(v)
	} else if v, ok := num("max_delay_sec"); ok {
		r.MaxDelayMS = int(v * 1000)
	}
	return r
}

// tries is how many times the call may be sent in all.
func (r *RetrySpec) tries() int {
	if r == nil || r.Max <= 0 {
		return 1
	}
	return 1 + min(r.Max, maxRetries)
}

// delay is the wait before retry number n (0 = the first retry).
func (r *RetrySpec) delay(n int) time.Duration {
	d := time.Duration(r.BackoffMS) * time.Millisecond
	if r.BackoffMS <= 0 {
		d = time.Second
	}
	mult := r.Multiplier
	if mult <= 0 {
		mult = 2
	}
	ceiling := maxRetryDelay
	if r.MaxDelayMS > 0 && time.Duration(r.MaxDelayMS)*time.Millisecond < ceiling {
		ceiling = time.Duration(r.MaxDelayMS) * time.Millisecond
	}
	for i := 0; i < n && d < ceiling; i++ {
		d = time.Duration(float64(d) * mult)
	}
	return min(d, ceiling)
}

// retryNow says whether one try's outcome may be sent again at once: the
// vendor answered a retryable code, or nothing reached it.
func retryNow(call Call, code int, err error) bool {
	if err != nil {
		return notConnected(err)
	}
	for _, c := range call.RetryCodes {
		if code == c {
			return true
		}
	}
	return false
}

// notConnected reports a request that never reached the vendor: the
// connection (or the name lookup) failed before anything was written.
func notConnected(err error) bool {
	var op *net.OpError
	return errors.As(err, &op) && op.Op == "dial"
}

// sleepCtx waits d, or less when ctx ends; false = ctx ended.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
