//go:build windows

package console

import (
	"time"

	"golang.org/x/sys/windows"
)

// nextWithin waits up to d for a key; got=false when none came. The console
// input handle is signalled while input is waiting. That includes events that
// are not keys (focus, mouse, resize), after which the read below waits for
// a real key — the auto refresh then resumes with the next key.
func (k *ttyKeys) nextWithin(d time.Duration) (string, bool, error) {
	if k.pending == "" {
		ev, err := windows.WaitForSingleObject(windows.Handle(k.f.Fd()), uint32(d/time.Millisecond))
		if err != nil {
			return "", false, err
		}
		if ev == uint32(windows.WAIT_TIMEOUT) {
			return "", false, nil
		}
	}
	key, err := k.next()
	return key, err == nil, err
}
