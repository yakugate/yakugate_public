// Package logging is the engine's log writer (Process 13, Types 2 and 3).
//
// Two things happen to every record:
//
//   - It is rendered as ONE line of JSON and appended to a daily file — the
//     system file, or the owning account's or vendor's own file (PO
//     2026-09-04: "one line per record", "each account and vendor having
//     separate log file each day").
//   - If it belongs to the system, it is ALSO printed to the console in the
//     old plain tagged format, so `make run` looks exactly as it did before.
//     Per-account and per-vendor records go to their files only, so tenant
//     traffic can never drown the console (PO ruling, 2026-09-04).
//
// A record is written only when its severity clears the effective level for
// its scope — see level.go. Levels come from the account's or vendor's own
// config row when set, and from the process-wide default otherwise.
//
// Nothing here may ever stop traffic: an unwritable disk costs one warning on
// stderr and is then ignored, and every call is safe on a nil or unconfigured
// logger.
package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Fields carries whatever else a record needs. Keys are rendered after the
// fixed fields below, in name order, so two records of the same kind always
// read the same way down the file.
type Fields map[string]any

// Reserved field names. A caller putting one of these in Fields would emit a
// duplicate JSON key, so they are dropped at render time rather than trusted.
var reserved = map[string]bool{
	"time": true, "level": true, "type": true, "component": true,
	"scope": true, "requestId": true, "messageId": true, "msg": true,
	// Written by render itself, beside the account it belongs to.
	"master_number": true,
}

// isReserved also excludes the scope's OWN name field, but only for the scope
// that actually emits it. "account" collides on an account-scoped record and
// is dropped there; on a SYSTEM record nothing emits it, so a caller may use
// it to say which account an event was about — which is exactly what the
// security log's system copy does.
func isReserved(k string, kind ScopeKind) bool {
	if reserved[k] {
		return true
	}
	return (k == "account" && kind == ScopeAccount) || (k == "vendor" && kind == ScopeVendor)
}

// Package-wide state. Loggers are created ad hoc all over the engine
// (logging.New("http-server") appears inline in dozens of places), so the file
// sink and the level config belong to the package, not to one Logger.
// Config is one scope's complete logging settings — the shape of the
// log_config JSON column on both sms_service_config and vendors (migration
// config/0081). One object, so there is one thing to read, write and put on a
// form (PO 2026-09-04: "do one thing, save all log info into log config").
type Config struct {
	Level Level
	// MaxSizeMB starts a new part file once today's file passes this size.
	// 0 means no size limit — the file simply runs to the end of the day.
	//
	// There is deliberately NO cap on how many parts a day may have (PO
	// 2026-09-04). A per-day cap could only be enforced by deleting the
	// EARLIEST parts of a day that is still being written — throwing away
	// this morning's records while the day is in progress. Disk use is
	// bounded by RetentionDays instead, so it stays proportional to real
	// traffic rather than to an arbitrary ceiling.
	MaxSizeMB int
	// RetentionDays deletes files older than this. 0 means keep forever.
	RetentionDays int
}

// Defaults are what a scope gets when nothing is configured for it. They
// match the DEFAULT on both log_config columns, so the database and the
// engine cannot drift apart.
var Defaults = Config{Level: LevelInfo, MaxSizeMB: 100, RetentionDays: 30}

var (
	mu     sync.RWMutex
	files  *sink
	sysCfg = Defaults
	// scoped holds each account's and vendor's own settings, pushed in from
	// the config store (see internal/logcfg) and refreshed on a ticker. A
	// lookup is an in-memory map read — this is called on the message hot
	// path and must never become a query.
	scoped = map[ScopeKind]map[string]Config{}
	// masters holds each account's company master number (config/0141), by
	// sanitized username, pushed in with the settings above. Every
	// account-scoped line carries it, so a log file names the customer the
	// way the operator's own books do.
	masters = map[string]string{}
)

// Configure points file logging at dir/role and sets the process-wide default
// level. Called once per role at start-up (see cmd/yakugate). An empty dir
// leaves file logging off, which is what every test and one-off tool gets.
//
// Each role writes into its own subfolder (PO ruling 2026-09-04), so no two
// processes ever append to the same file. Tracing one message across roles
// then means reading that account's file under each role's folder — every
// line names the component that wrote it.
func Configure(dir, role string, def Config) {
	mu.Lock()
	defer mu.Unlock()
	if files != nil {
		files.Close()
	}
	if dir == "" {
		files = nil
		capMu.Lock()
		capSink = nil
		capMu.Unlock()
	} else {
		if role != "" {
			dir = filepath.Join(dir, sanitizeRole(role))
		}
		files = newSink(dir)
		// Capture files live in their own folder under the role's: they are
		// keyed by session id rather than by day, are not swept by the daily
		// retention, and hold message text in the clear (config/0084).
		cs := newCaptureSink(filepath.Join(dir, "captures"))
		capMu.Lock()
		capSink = cs
		capMu.Unlock()
	}
	sysCfg = def
}

// SystemConfig reports the process-wide settings — the level a scope with no
// configuration of its own falls back to.
func SystemConfig() Config {
	mu.RLock()
	defer mu.RUnlock()
	return sysCfg
}

// SetSystemConfig replaces the process-wide settings at runtime. Used by the
// config refresher so an operator can change the system level from the portal
// without restarting a role (migration config/0086); Configure sets the same
// value at start-up from YAKU_LOG_LEVEL.
func SetSystemConfig(c Config) {
	mu.Lock()
	sysCfg = c
	mu.Unlock()
}

// SetScopeConfigs replaces every known setting for one kind of scope at once
// — all accounts, or all vendors. Replacing wholesale (rather than merging)
// is what makes a setting actually go away when it is cleared in the portal.
//
// Safe to call at any time: config is only readable a moment after the first
// log lines are written, so until the first push everything follows the
// process-wide default.
func SetScopeConfigs(kind ScopeKind, cfgs map[string]Config) {
	norm := make(map[string]Config, len(cfgs))
	for k, v := range cfgs {
		norm[sanitize(k)] = v
	}
	mu.Lock()
	scoped[kind] = norm
	mu.Unlock()
}

// SetAccountMasters replaces the master numbers of every account's company at
// once (username -> number; an account whose company has none is left out).
// Replaced wholesale, as SetScopeConfigs is, so a cleared number goes away.
func SetAccountMasters(m map[string]string) {
	norm := make(map[string]string, len(m))
	for k, v := range m {
		if v != "" {
			norm[sanitize(k)] = v
		}
	}
	mu.Lock()
	masters = norm
	mu.Unlock()
}

// masterOf is the master number of an account's company, "" for none.
func masterOf(username string) string {
	mu.RLock()
	defer mu.RUnlock()
	if len(masters) == 0 {
		return ""
	}
	return masters[sanitize(username)]
}

// Close releases the open files on shutdown.
func Close() {
	mu.Lock()
	if files != nil {
		files.Close()
	}
	mu.Unlock()
	capMu.Lock()
	capSink.Close()
	capMu.Unlock()
}

// levelFor resolves the effective level for a scope: the account's or
// vendor's own setting when it has one, else the process-wide default. This
// is the D-069 precedence shape (accounts.dedupe_ttl_sec -> env default),
// reused rather than reinvented.
func configFor(sc Scope) Config {
	mu.RLock()
	defer mu.RUnlock()
	if sc.Kind != ScopeSystem {
		// Names are sanitized on both sides, so "Demo" and "demo" resolve to
		// the same settings as they do to the same file.
		if c, ok := scoped[sc.Kind][sanitize(sc.Name)]; ok {
			return c
		}
	}
	return sysCfg
}

// Logger tags every record with the component that wrote it.
//
// The embedded *log.Logger is what the ~235 existing call sites have always
// used. It stays so that Println and friends keep working; Printf below
// shadows it to also reach the file.
type Logger struct {
	*log.Logger
	component string
}

// New returns a logger tagged with the component name.
func New(component string) *Logger {
	return &Logger{
		Logger:    log.New(os.Stdout, "["+component+"] ", log.LstdFlags|log.LUTC),
		component: component,
	}
}

// Printf keeps the old plain-text call sites working unchanged. Such a line
// has no scope and no severity of its own, so it is treated as a system
// record at info — printed to the console exactly as before, and stored in
// the system file as JSON with the formatted text as its msg.
func (l *Logger) Printf(format string, v ...any) {
	l.write(LevelInfo, SystemScope, "", fmt.Sprintf(format, v...), nil)
}

// Debug/Info/Warn/Error write one structured record. typ names the kind of
// event ("submit", "auth_fail", "dlr_push") so a file can be filtered by it;
// msg is the human sentence; f carries the rest.
func (l *Logger) Debug(sc Scope, typ, msg string, f Fields) { l.write(LevelDebug, sc, typ, msg, f) }
func (l *Logger) Info(sc Scope, typ, msg string, f Fields)  { l.write(LevelInfo, sc, typ, msg, f) }
func (l *Logger) Warn(sc Scope, typ, msg string, f Fields)  { l.write(LevelWarn, sc, typ, msg, f) }
func (l *Logger) Error(sc Scope, typ, msg string, f Fields) { l.write(LevelError, sc, typ, msg, f) }

func (l *Logger) write(lv Level, sc Scope, typ, msg string, f Fields) {
	if l == nil {
		return
	}
	if !configFor(sc).Level.Enabled(lv) {
		return
	}
	now := time.Now().UTC()

	// [1.9.2] Subscriber numbers and one-time codes are taken out here, before
	// anything is printed or written — this is the single point every log line
	// passes through, Printf included. Flow captures do NOT come through here
	// and keep their message-text carve-out; see redact.go.
	msg = redactMessage(msg)
	f = redactFields(f)

	// Console: system records only, in the format this engine has always
	// printed. Tenant records would otherwise bury it.
	if sc.Kind == ScopeSystem && l.Logger != nil {
		l.Logger.Output(2, msg)
	}

	mu.RLock()
	s := files
	mu.RUnlock()

	// A SYSTEM capture collects what the engine says about itself. Those
	// records go through this ordinary log path, not through CaptureRecord,
	// so the capture has to be fed from here or a system capture would
	// produce an empty file.
	sysCap, sysCapturing := Capture{}, false
	if sc.Kind == ScopeSystem {
		sysCap, sysCapturing = Active(SystemScope)
	}
	if s == nil && !sysCapturing {
		return
	}
	line := render(now, lv, l.component, sc, typ, msg, f)
	if s != nil {
		s.write(sc, now, configFor(sc), line)
	}
	if sysCapturing {
		captureSystemLine(sysCap, l.component, typ, f, now, line)
	}
}

// render builds one line of JSON by hand so the fixed fields keep a stable,
// readable order (a map would sort "account" ahead of "time"). Values still
// go through encoding/json, so quotes, newlines and non-ASCII text in a
// message body cannot break the line.
func render(now time.Time, lv Level, component string, sc Scope, typ, msg string, f Fields) string {
	var b bytes.Buffer
	b.WriteByte('{')
	put := func(k, v string) {
		if v == "" {
			return
		}
		if b.Len() > 1 {
			b.WriteByte(',')
		}
		b.WriteString(`"` + k + `":`)
		writeJSON(&b, v)
	}
	put("time", now.Format("2006-01-02T15:04:05.000Z"))
	put("level", lv.String())
	put("type", typ)
	put("component", component)
	put("scope", string(sc.Kind))
	switch sc.Kind {
	case ScopeAccount:
		put("account", sc.Name)
		// The company's master number, when it has one (config/0141).
		put("master_number", masterOf(sc.Name))
	case ScopeVendor:
		put("vendor", sc.Name)
	case ScopeSystem:
		// A system record ABOUT an account (the security log's system copy)
		// names it in Fields; it carries the master number too.
		if a, ok := f["account"].(string); ok && a != "" {
			put("master_number", masterOf(a))
		}
	}
	// requestId and messageId are the correlation keys (PO 2026-09-04:
	// "request id would be enough or also include message id" — both are
	// emitted when known, so one line can be traced either way). They are
	// promoted out of Fields to keep them in a fixed position.
	if f != nil {
		for _, k := range []string{"requestId", "messageId"} {
			if v, ok := f[k]; ok {
				put(k, fmt.Sprint(v))
			}
		}
	}
	put("msg", msg)

	if len(f) > 0 {
		keys := make([]string, 0, len(f))
		for k := range f {
			if !isReserved(k, sc.Kind) {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		for _, k := range keys {
			if b.Len() > 1 {
				b.WriteByte(',')
			}
			b.WriteString(`"` + k + `":`)
			writeJSON(&b, f[k])
		}
	}
	b.WriteByte('}')
	return b.String()
}

// writeJSON appends v as JSON, falling back to its printed form if it cannot
// be marshalled (a channel, a func, a cyclic struct). A bad field value must
// cost that one value, never the whole record.
//
// Strings take their own path rather than going through encoding/json. Go's
// marshaller escapes <, > and & into \u003c, \u003e and \u0026 for safe
// embedding in HTML — correct JSON, but it turns an ordinary log message like
// "mysql -> redis" into something nobody wants to read, and these files are
// meant to be read (PO 2026-09-04: "human readable json"). It is also the hot
// path: most fields in most records are strings.
func writeJSON(b *bytes.Buffer, v any) {
	if s, ok := v.(string); ok {
		writeString(b, s)
		return
	}
	raw, err := json.Marshal(v)
	if err != nil {
		writeString(b, fmt.Sprint(v))
		return
	}
	b.Write(raw)
}

const hexDigits = "0123456789abcdef"

// writeString appends s as a quoted JSON string, escaping only what the
// format actually requires: the quote, the backslash, and control characters.
// Everything else — including non-ASCII text like Arabic or Urdu message
// bodies — is written through as UTF-8.
func writeString(b *bytes.Buffer, s string) {
	b.WriteByte('"')
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 0x20 && c != '"' && c != '\\' {
			continue
		}
		b.WriteString(s[start:i])
		switch c {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			b.WriteString(`\u00`)
			b.WriteByte(hexDigits[c>>4])
			b.WriteByte(hexDigits[c&0xF])
		}
		start = i + 1
	}
	b.WriteString(s[start:])
	b.WriteByte('"')
}

// EffectiveConfig reports the settings a scope is actually writing under,
// after its own configuration and the process-wide default have been
// resolved. Exposed for diagnostics and the status page.
func EffectiveConfig(sc Scope) Config { return configFor(sc) }

// EffectiveLevel is the level half of EffectiveConfig.
func EffectiveLevel(sc Scope) Level { return configFor(sc).Level }

// StartSweep deletes log files past their retention. It runs once immediately
// and then every `every` until ctx is done.
//
// Each file is judged by its OWN account's or vendor's retention, not one
// global number, so an account keeping 90 days and one keeping 7 can share a
// directory. A file whose owner has no configuration follows the
// process-wide default.
func StartSweep(ctx context.Context, every time.Duration) {
	if every <= 0 {
		every = time.Hour
	}
	run := func() {
		mu.RLock()
		s := files
		mu.RUnlock()
		s.sweep(time.Now().UTC(), configFor)
	}
	run()
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				run()
			}
		}
	}()
}
