package logging

// Process 13 Type 3 — time-boxed flow capture.
//
// A capture is a diagnostic mode an operator turns on for ONE account or ONE
// vendor, for 1, 5 or 15 minutes. While it runs, every process that touches
// that entity's traffic writes the full flow to the capture's own file,
// INCLUDING the message text — see migration config/0084 for why that
// carve-out is acceptable and what keeps it so.
//
// The hot-path rule: Active() is called per message, so it is an in-memory
// map read guarded by an RWMutex, refreshed from the config store on a
// ticker (internal/logcfg). It never queries anything.

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// systemCaptureKey names the one capture that can run against the system
// scope. Fixed, because a system capture has no tenant to key on.
const systemCaptureKey = "system"

// Capture is one running capture session.
type Capture struct {
	ID   int64
	Kind ScopeKind // ScopeAccount or ScopeVendor
	Name string    // username or vendor cid, already sanitized for comparison
	// ScopeID is the account's or vendor's numeric id. The vendor-facing
	// processes (P3/P4/P5) carry a message's AccountID but not its username,
	// so a capture has to be findable both ways.
	ScopeID int64
	// ExpiresAt is evaluated in Go on every check rather than trusted from
	// the last refresh: a 1-minute capture would otherwise run for as long as
	// the refresh interval past its window.
	ExpiresAt time.Time
}

// CaptureRecordFn receives every capture record so it can be shipped
// somewhere the portal can read (internal/logcfg installs the database
// shipper). Called on the message hot path, so an implementation MUST return
// immediately and never block — the shipper buffers and drops rather than
// waiting.
type CaptureRecordFn func(captureID int64, role, step, messageID string, at time.Time, line string)

var (
	shipper   CaptureRecordFn
	shipperMu sync.RWMutex
)

// SetCaptureShipper installs the sink that carries records off this host. nil
// (the default) keeps captures file-only.
func SetCaptureShipper(f CaptureRecordFn) {
	shipperMu.Lock()
	shipper = f
	shipperMu.Unlock()
}

var (
	capMu      sync.RWMutex
	captures   = map[ScopeKind]map[string]Capture{}
	capturesID = map[ScopeKind]map[int64]Capture{}
	capSink    *captureSink
	// anyCapture is the hot-path fast path. Captures are rare — none is
	// running almost all of the time — and Active() would otherwise sanitize
	// the scope name (lower-case + a regex replace, both allocating) on every
	// message just to look up an empty map. One atomic load instead.
	anyCapture atomic.Bool
)

// SetCaptures replaces the set of running captures for one kind of scope.
// Called by the refresher; wholesale replacement is what makes a stopped
// capture actually stop.
func SetCaptures(kind ScopeKind, list []Capture) {
	m := make(map[string]Capture, len(list))
	byID := make(map[int64]Capture, len(list))
	for _, c := range list {
		if kind == ScopeSystem {
			c.Name = systemCaptureKey
		} else {
			c.Name = sanitize(c.Name)
		}
		m[c.Name] = c
		if c.ScopeID > 0 {
			byID[c.ScopeID] = c
		}
	}
	capMu.Lock()
	captures[kind] = m
	capturesID[kind] = byID
	n := 0
	for _, byName := range captures {
		n += len(byName)
	}
	capMu.Unlock()
	anyCapture.Store(n > 0)
}

// Active reports the capture running for a scope right now, if any.
//
// Expiry is checked here, against the wall clock, so a capture stops on time
// even though the refresher only notices minutes later.
func Active(sc Scope) (Capture, bool) {
	if !anyCapture.Load() {
		return Capture{}, false
	}
	// A SYSTEM capture has no tenant to name, so it is stored under a fixed
	// key. It collects what the engine says about itself — start-up, cache
	// refreshes, security events — which belongs to no account or vendor and
	// was therefore invisible to a capture until now.
	name := sanitize(sc.Name)
	if sc.Kind == ScopeSystem {
		name = systemCaptureKey
	}
	capMu.RLock()
	c, ok := captures[sc.Kind][name]
	capMu.RUnlock()
	if !ok || !time.Now().UTC().Before(c.ExpiresAt) {
		return Capture{}, false
	}
	return c, true
}

// ActiveForAccountID is Active for a caller that has the account's id but not
// its username — every process past the queue (P3/P4/P5) is in that position.
func ActiveForAccountID(id int64) (Capture, bool) {
	if !anyCapture.Load() || id <= 0 {
		return Capture{}, false
	}
	capMu.RLock()
	c, ok := capturesID[ScopeAccount][id]
	capMu.RUnlock()
	if !ok || !time.Now().UTC().Before(c.ExpiresAt) {
		return Capture{}, false
	}
	return c, true
}

// ActiveForVendor is Active for a vendor cid.
func ActiveForVendor(cid string) (Capture, bool) { return Active(VendorScope(cid)) }

// CaptureTo writes one step to an already-resolved capture. Used where a
// caller has looked the capture up itself — typically because the same event
// belongs to TWO captures at once (an operator watching the account and
// another watching the vendor both want the vendor exchange).
func CaptureTo(c Capture, l *Logger, step, msg string, f Fields) {
	if c.ID == 0 || !time.Now().UTC().Before(c.ExpiresAt) {
		return
	}
	now := time.Now().UTC()
	if f == nil {
		f = Fields{}
	}
	f["captureId"] = c.ID
	component := "engine"
	if l != nil {
		component = l.component
	}
	// The capture's OWN scope, not SystemScope. Until 2026-09-08 every capture
	// record rendered `"scope":"system"` no matter whose traffic it was: the
	// scope was passed as a FIELD, and render drops that key as reserved
	// because it emits its own. So the one thing the line most needed to say —
	// which account this belongs to — was the one thing silently replaced.
	line := render(now, LevelDebug, component, Scope{Kind: c.Kind, Name: c.Name}, step, msg, f)
	capMu.RLock()
	s := capSink
	capMu.RUnlock()
	s.write(c.ID, now, line)
	ship(c.ID, component, step, f, now, line)
}

// Capturing is the cheap yes/no a hot path asks before building any capture
// record. Nothing else in this package should be called when it returns false.
func Capturing(sc Scope) bool {
	_, ok := Active(sc)
	return ok
}

// CaptureRecord writes one step of a flow to the capture's file. It is a
// no-op when no capture is running for that scope, so a caller may call it
// unconditionally — but a caller that must BUILD expensive fields (a whole
// request body, a rendered response) should ask Capturing first.
//
// step names the point in the flow: "submit", "stage", "vendor_request",
// "vendor_response", "receipt", "client_push".
func CaptureRecord(l *Logger, sc Scope, step, msg string, f Fields) {
	c, ok := Active(sc)
	if !ok {
		return
	}
	now := time.Now().UTC()
	if f == nil {
		f = Fields{}
	}
	f["captureId"] = c.ID

	component := "engine"
	if l != nil {
		component = l.component
	}
	// Rendered with the same writer as every other record, so a capture file
	// is read by the same tools — one line of JSON per step — and with the
	// record's real scope, so it names the account it belongs to.
	line := render(now, LevelDebug, component, sc, step, msg, f)

	capMu.RLock()
	s := capSink
	capMu.RUnlock()
	s.write(c.ID, now, line)
	ship(c.ID, component, step, f, now, line)
}

// ship hands one record to the installed shipper, if any. The file write
// above already happened, so a shipper that drops costs the portal a line and
// never the record itself — the gateway file stays complete (PO ruling
// 2026-09-04).
func ship(id int64, role, step string, f Fields, at time.Time, line string) {
	shipperMu.RLock()
	fn := shipper
	shipperMu.RUnlock()
	if fn == nil {
		return
	}
	msgID := ""
	if f != nil {
		if v, ok := f["messageId"]; ok {
			msgID = fmt.Sprint(v)
		}
	}
	fn(id, role, step, msgID, at, line)
}

// captureSink owns the open capture files. Separate from the daily sink
// because a capture is keyed by session id, not by day: it must not roll at
// midnight, and it must not be swept by retention — a capture's files are
// deleted on their own schedule (purge_at, 24h after the window closes).
type captureSink struct {
	dir string

	mu     sync.Mutex
	open   map[int64]*os.File
	failed map[int64]bool
}

func newCaptureSink(dir string) *captureSink {
	return &captureSink{dir: dir, open: map[int64]*os.File{}, failed: map[int64]bool{}}
}

func (s *captureSink) write(id int64, _ time.Time, line string) {
	if s == nil || s.dir == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed[id] {
		return
	}
	f, ok := s.open[id]
	if !ok {
		if err := os.MkdirAll(s.dir, 0o750); err != nil {
			s.failed[id] = true
			return
		}
		// 0640, tighter than the daily logs' 0644: a capture file holds
		// message text and subscriber numbers in the clear.
		h, err := os.OpenFile(filepath.Join(s.dir, captureFileName(id)),
			os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o640)
		if err != nil {
			s.failed[id] = true
			return
		}
		s.open[id], f = h, h
	}
	_, _ = f.WriteString(line + "\n")
}

func captureFileName(id int64) string {
	return "capture_" + itoa(id) + ".log"
}

// closeCapture releases a finished capture's handle so the file can be purged.
func (s *captureSink) closeCapture(id int64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if f, ok := s.open[id]; ok {
		_ = f.Close()
		delete(s.open, id)
	}
	delete(s.failed, id)
}

func (s *captureSink) Close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range s.open {
		_ = f.Close()
	}
	s.open = map[int64]*os.File{}
}

// itoa avoids pulling strconv in for one call.
func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// PurgeCaptures deletes the files of captures whose purge time has passed.
//
// A capture holds message text and subscriber numbers in the clear, so its
// files are removed on their own schedule (24h after the window closes, PO
// ruling 2026-09-04) rather than waiting for the daily logs' retention. The
// caller supplies the ids that are due; this owns only the files.
func PurgeCaptures(due []int64) (removed int) {
	capMu.RLock()
	s := capSink
	capMu.RUnlock()
	if s == nil || s.dir == "" {
		return 0
	}
	for _, id := range due {
		// Close first: a handle still open for a capture that has just ended
		// would otherwise keep the file alive after the unlink.
		s.closeCapture(id)
		if err := os.Remove(filepath.Join(s.dir, captureFileName(id))); err == nil {
			removed++
		}
	}
	return removed
}

// CaptureDir is where this role writes capture files. Empty when file logging
// is off.
func CaptureDir() string {
	capMu.RLock()
	defer capMu.RUnlock()
	if capSink == nil {
		return ""
	}
	return capSink.dir
}

// captureSystemLine files one already-rendered system log line into a running
// system capture — both to the capture's file and to the portal.
//
// Separate from CaptureRecord because the line is already built: the ordinary
// log path renders it for the daily file, and rendering it twice would be
// both wasteful and a chance for the two copies to disagree.
func captureSystemLine(c Capture, component, typ string, f Fields, at time.Time, line string) {
	capMu.RLock()
	s := capSink
	capMu.RUnlock()
	s.write(c.ID, at, line)
	ship(c.ID, component, typ, f, at, line)
}
