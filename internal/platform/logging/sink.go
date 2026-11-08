package logging

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Scope names which log file a record belongs in (PO 2026-09-04: "each
// account and vendor having separate log file each day and one system level
// log file each day").
type Scope struct {
	Kind ScopeKind
	// Name is the account username or the vendor cid. Empty for ScopeSystem.
	Name string
}

// ScopeKind is the owner of a log file.
type ScopeKind string

const (
	ScopeSystem  ScopeKind = "system"
	ScopeAccount ScopeKind = "account"
	ScopeVendor  ScopeKind = "vendor"
)

// SystemScope is the process-wide file every non-tenant record lands in.
var SystemScope = Scope{Kind: ScopeSystem}

// AccountScope / VendorScope name a tenant's own daily file.
func AccountScope(username string) Scope { return Scope{Kind: ScopeAccount, Name: username} }
func VendorScope(cid string) Scope       { return Scope{Kind: ScopeVendor, Name: cid} }

// unsafeName matches anything that must not appear in a filename. Mirrors
// store/mysql.SanitizeUser's rule (which builds per-account, per-day TABLE
// names from the same usernames) so one account's files and tables carry the
// same recognisable identifier.
var unsafeName = regexp.MustCompile(`[^A-Za-z0-9_]+`)

// sanitizeRole cleans a role name for use as a directory. Unlike account and
// vendor names it KEEPS the hyphen, because every role in this engine has one
// ("http-server", "smpp-vendor", "abuse-monitor") and a folder called
// http_server would not match the role anyone types or reads in a log.
func sanitizeRole(s string) string {
	out := strings.ToLower(unsafeRole.ReplaceAllString(s, "_"))
	if out == "" {
		out = "unnamed"
	}
	return out
}

var unsafeRole = regexp.MustCompile(`[^A-Za-z0-9_-]+`)

func sanitize(s string) string {
	out := strings.ToLower(unsafeName.ReplaceAllString(s, "_"))
	if out == "" {
		out = "unnamed"
	}
	return out
}

// fileName is the scope's file for a given day:
//
//	system_04_09_2026.log
//	account_demo_04_09_2026.log
//	vendor_falconbh_04_09_2026.log
//
// The DD_MM_YYYY stamp matches the hot store's own daily per-account tables
// (mtlog_<user>_<DD_MM_YYYY>), so a day's logs and a day's records sort and
// read the same way.
func (s Scope) fileName(day time.Time) string { return s.partName(day, 0) }

// partName is the day's file, split by part. Part 0 is the plain name; a size
// limit rolls into part 1, 2 and so on within the same day:
//
//	account_demo_04_09_2026.log
//	account_demo_04_09_2026.1.log
func (s Scope) partName(day time.Time, part int) string {
	stamp := day.Format("02_01_2006")
	base := "system_" + stamp
	if s.Kind != ScopeSystem {
		base = fmt.Sprintf("%s_%s_%s", s.Kind, sanitize(s.Name), stamp)
	}
	if part > 0 {
		return fmt.Sprintf("%s.%d.log", base, part)
	}
	return base + ".log"
}

// namePattern pulls a log file apart again, so the retention sweep knows
// which scope and which day a file on disk belongs to without keeping any
// index of its own. Anything that does not match is left strictly alone —
// the log directory may hold files this engine did not write.
var namePattern = regexp.MustCompile(`^(system|account|vendor)_(?:(.+)_)?(\d{2})_(\d{2})_(\d{4})(?:\.(\d+))?\.log$`)

func parseFileName(f string) (sc Scope, day time.Time, part int, ok bool) {
	m := namePattern.FindStringSubmatch(f)
	if m == nil {
		return Scope{}, time.Time{}, 0, false
	}
	kind := ScopeKind(m[1])
	if (kind == ScopeSystem) != (m[2] == "") {
		return Scope{}, time.Time{}, 0, false // system carries no name; the others must
	}
	d, err := time.Parse("02_01_2006", m[3]+"_"+m[4]+"_"+m[5])
	if err != nil {
		return Scope{}, time.Time{}, 0, false
	}
	if m[6] != "" {
		if part, err = strconv.Atoi(m[6]); err != nil {
			return Scope{}, time.Time{}, 0, false
		}
	}
	return Scope{Kind: kind, Name: m[2]}, d, part, true
}

// sink owns the open log files and rolls them at the UTC day boundary.
//
// Files are opened lazily on first write and kept open — an account that
// sends nothing today never has a file created for it, so the directory
// reflects real activity rather than the size of the account table.
type sink struct {
	dir string

	mu   sync.Mutex
	open map[string]*os.File // fileName -> handle
	size map[string]int64    // fileName -> bytes written, for the size limit
	part map[string]int      // scope key -> the part currently being written today
	day  string              // the UTC day currently open; a change rolls everything

	// failed remembers a file we could not write, so a broken log path
	// degrades to one warning on stderr rather than a line of noise per
	// record. Logging must never be the reason traffic stops.
	failed map[string]bool

	// dead is set when the log DIRECTORY itself is unusable. That fails for
	// every scope at once, so it is remembered separately — otherwise a busy
	// process would print the same permission error once per account.
	dead bool
}

func newSink(dir string) *sink {
	return &sink{
		dir:    dir,
		open:   map[string]*os.File{},
		size:   map[string]int64{},
		part:   map[string]int{},
		failed: map[string]bool{},
	}
}

// write appends one already-rendered line (no trailing newline) to the
// scope's file for now's UTC day. Errors are swallowed after one warning:
// the caller is on a message hot path and must not care whether a disk is
// full.
func (s *sink) write(sc Scope, now time.Time, cfg Config, line string) {
	if s == nil || s.dir == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.dead {
		return
	}

	// Day roll: close yesterday's handles so a long-running process starts
	// writing today's files without a restart.
	if d := now.Format("2006-01-02"); d != s.day {
		for _, f := range s.open {
			_ = f.Close()
		}
		s.open = map[string]*os.File{}
		s.size = map[string]int64{}
		s.part = map[string]int{}
		s.failed = map[string]bool{}
		s.dead = false // a new day is a fresh chance: the path may be fixed by now
		s.day = d
	}

	// Size limit: once the current part passes it, start the next one. The
	// running byte count is kept in memory, seeded from the file's real size
	// when it is opened, so a restart mid-day does not lose the position.
	key := string(sc.Kind) + "/" + sanitize(sc.Name)
	name := sc.partName(now, s.part[key])
	if cfg.MaxSizeMB > 0 && s.size[name] >= int64(cfg.MaxSizeMB)<<20 {
		if f, ok := s.open[name]; ok {
			_ = f.Close()
			delete(s.open, name)
		}
		s.part[key]++
		name = sc.partName(now, s.part[key])
	}

	f, ok := s.open[name]
	if !ok {
		if s.failed[name] {
			return
		}
		if err := os.MkdirAll(s.dir, 0o755); err != nil {
			s.dead = true
			fmt.Fprintf(os.Stderr, "[logging] cannot use log dir %s: %v (file logging is OFF for this process; set YAKU_LOG_DIR to a writable path)\n", s.dir, err)
			return
		}
		h, err := os.OpenFile(filepath.Join(s.dir, name), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			s.failed[name] = true
			fmt.Fprintf(os.Stderr, "[logging] cannot open %s: %v (file logging disabled for it)\n", name, err)
			return
		}
		s.open[name], f = h, h
	}
	n, err := f.WriteString(line + "\n")
	s.size[name] += int64(n)
	if err != nil && !s.failed[name] {
		s.failed[name] = true
		fmt.Fprintf(os.Stderr, "[logging] write to %s failed: %v (file logging disabled for it)\n", name, err)
	}
}

// Close releases every open handle. Called on graceful shutdown.
func (s *sink) Close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range s.open {
		_ = f.Close()
	}
	s.open = map[string]*os.File{}
}

// sweep deletes log files that have outlived their scope's retention.
//
// cfgOf answers what settings a scope is under; it is the same resolution the
// write path uses, so a file is judged by its OWN owner's retention rather
// than one global number (PO 2026-09-04: each account and vendor configures
// its own). Files this engine did not write are never touched.
//
// Only whole days are removed. A day still being written is never trimmed,
// however many parts it has grown — see Config.MaxSizeMB.
func (s *sink) sweep(now time.Time, cfgOf func(Scope) Config) (removed int) {
	if s == nil || s.dir == "" {
		return 0
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return 0
	}
	today := now.UTC().Truncate(24 * time.Hour)

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		sc, day, _, ok := parseFileName(e.Name())
		if !ok {
			continue
		}
		cfg := cfgOf(sc)
		if cfg.RetentionDays <= 0 || !day.Before(today.AddDate(0, 0, -cfg.RetentionDays)) {
			continue
		}
		name := e.Name()
		s.mu.Lock()
		if f, open := s.open[name]; open {
			_ = f.Close()
			delete(s.open, name)
			delete(s.size, name)
		}
		s.mu.Unlock()
		if os.Remove(filepath.Join(s.dir, name)) == nil {
			removed++
		}
	}
	return removed
}

// FileNameForTest exposes the day's file name to tests in other packages.
func (s Scope) FileNameForTest(day time.Time) string { return s.fileName(day) }
