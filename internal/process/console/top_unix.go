//go:build !windows

package console

import (
	"time"

	"golang.org/x/sys/unix"
)

// nextWithin waits up to d for a key; got=false when none came.
func (k *ttyKeys) nextWithin(d time.Duration) (string, bool, error) {
	if k.pending == "" {
		fds := []unix.PollFd{{Fd: int32(k.f.Fd()), Events: unix.POLLIN}}
		n, err := unix.Poll(fds, int(d/time.Millisecond))
		if err == unix.EINTR {
			return "", false, nil // a resize signal: just redraw
		}
		if err != nil {
			return "", false, err
		}
		if n == 0 {
			return "", false, nil
		}
	}
	key, err := k.next()
	return key, err == nil, err
}
