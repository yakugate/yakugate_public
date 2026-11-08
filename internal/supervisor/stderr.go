package supervisor

import (
	"bytes"
	"io"
	"sync"

	"github.com/yakugate/yakugate/internal/singleton"
)

// roleStderr is a role's error output on its way to the gateway's own. It is
// read line by line so the role's marker lines (internal/singleton) can be
// taken out: they are the role telling the supervisor "I stay" (HERE) or
// "another deployment runs me" (ELSEWHERE, just before ExitElsewhere). Every
// other line passes through unchanged. The markers are not passed on — the
// supervisor logs what they mean once, rather than a role every minute.
type roleStderr struct {
	out io.Writer

	mu       sync.Mutex
	buf      []byte
	here     chan struct{} // closed at the first HERE line
	hereSeen bool
	away     *singleton.Marker // the last ELSEWHERE line
}

// maxPending is how much of a line without an end is held back before it is
// passed on as it is. A marker is a short line; a long one is never one.
const maxPending = 64 << 10

var markerStart = []byte("YAKU-")

func newRoleStderr(out io.Writer) *roleStderr {
	return &roleStderr{out: out, here: make(chan struct{})}
}

func (w *roleStderr) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			break
		}
		w.line(w.buf[:i+1])
		w.buf = w.buf[i+1:]
	}
	if len(w.buf) > maxPending {
		w.pass(w.buf)
		w.buf = nil
	}
	// Always the whole of p: a gateway that cannot write its own stderr must
	// not break the role's.
	return len(p), nil
}

// flush hands on a last line that never got its end. Called once the role
// has exited and its output is all read.
func (w *roleStderr) flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.buf) > 0 {
		w.line(w.buf)
		w.buf = nil
	}
}

// line is one line, with its end when it had one. Caller holds the mutex.
func (w *roleStderr) line(b []byte) {
	if bytes.HasPrefix(b, markerStart) {
		if m, ok := singleton.ParseMarker(string(b)); ok {
			if m.Elsewhere {
				w.away = &m
			} else if !w.hereSeen {
				w.hereSeen = true
				close(w.here)
			}
			return
		}
	}
	w.pass(b)
}

func (w *roleStderr) pass(b []byte) {
	if w.out != nil {
		_, _ = w.out.Write(b)
	}
}

// holder is where the role said it runs, in words; "another deployment" when
// it exited ExitElsewhere without saying.
func (w *roleStderr) holder() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.away == nil {
		return singleton.Holder{}.String()
	}
	return w.away.Holder.String()
}

// why is why the role said it is not here: singleton.WhyHome (the register
// lists it on another deployment), singleton.WhyNoList (the register cannot
// be read), or "" (another session holds its lease).
func (w *roleStderr) why() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.away == nil {
		return ""
	}
	return w.away.Why
}
