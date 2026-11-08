package supervisor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// Starting a gateway in the background and knowing it came up (2026-10-01):
// shared by `yakugate --background` and the console's Gateway → Processes,
// so the two starts judge "already running", "up" and "died" the same way.

// ErrNotUp: the started gateway is alive but has not answered in time.
var ErrNotUp = errors.New("the gateway has not answered")

// Probe says whether a gateway answers on c.Addr. (true, nil): one answers
// and takes this key. (true, err): something answers but refuses this key, or
// is not a gateway — either way the port is taken, so a start would fail.
// (false, nil): nothing answers. Running cannot tell the first two cases from
// the third, which made a gateway with another key look absent.
func (c *Client) Probe(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+c.Addr+"/roles", nil)
	if err != nil {
		return false, err
	}
	if c.Key != "" {
		req.Header.Set(KeyHeader, c.Key)
	}
	res, err := c.HTTP.Do(req)
	if err != nil {
		return false, nil
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 1<<16))
	res.Body.Close()
	switch {
	case res.StatusCode == http.StatusUnauthorized || res.StatusCode == http.StatusForbidden:
		return true, fmt.Errorf("a gateway answers on %s but refuses this admin key (YAKU_ADMIN_KEY)", c.Addr)
	case res.StatusCode >= 300:
		return true, fmt.Errorf("something answers on %s, but not as a gateway (%s)", c.Addr, res.Status)
	}
	return true, nil
}

// WaitUp waits for a gateway this process just started (pid) to answer on
// c.Addr, calling tick between tries (nil = none). nil once it answers AND
// the process is still ours — a start that lost the port to another gateway
// must not be reported as up. A process that ends is reported with the end
// of its log. After timeout it returns ErrNotUp and leaves the process alone;
// the caller decides (StopStarted).
//
// The pid is this process's child until this process ends, so a dead one
// lingers as a zombie that kill(pid, 0) calls alive; exited (start_unix.go)
// sees the exit with Wait4 and reaps it.
func WaitUp(ctx context.Context, c *Client, pid int, logFile string, timeout time.Duration, tick func()) error {
	deadline := time.Now().Add(timeout)
	for {
		answers, perr := c.Probe(ctx)
		if done, how := exited(pid); done {
			msg := fmt.Sprintf("the gateway stopped as it started (%s)", how)
			if answers {
				msg += "; another gateway answers on " + c.Addr
			}
			return fmt.Errorf("%s — the end of %s:\n%s", msg, logFile, LogTail(logFile, 10))
		}
		if answers {
			// Ours, alive, and the port answers. A refused key here is this
			// start's own key, so it is a real problem, not a race.
			return perr
		}
		if time.Now().After(deadline) {
			return ErrNotUp
		}
		if tick != nil {
			tick()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// LogTail is the last n lines of a log, read from its end only — the gateway
// log is appended to by every role and never rotated, so it can be large.
func LogTail(path string, n int) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	const window = 64 << 10
	if st, err := f.Stat(); err == nil && st.Size() > window {
		_, _ = f.Seek(st.Size()-window, io.SeekStart)
	}
	b, _ := io.ReadAll(io.LimitReader(f, window))
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return "  " + strings.Join(lines, "\n  ") + "\n"
}
