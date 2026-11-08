package config

import (
	"bufio"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Reading and changing yakugate.config from the command line (`yakugate config`,
// 2026-09-21). [SCOPE P14-SETTINGS]
//
// THE RULES THE EDITOR KEEPS
//
//   - It changes only the lines it is asked to change. Every other line —
//     comments, blank lines, order, the settings it was not asked about —
//     stays byte for byte, because operators keep notes in this file.
//   - What it writes must read back the same through parseLine, the loader's
//     own reading of a line. If it does not, nothing is written.
//   - A name the gateway does not read is refused, so a typo cannot land in the
//     file and be silently ignored (the loader accepts any name). A name
//     already in the file, even commented out, is accepted; --new accepts any.
//   - The write is all or nothing: a temp file beside the old one, then a
//     rename. The file before the change is kept as <file>.bak, with the same
//     permissions, since it holds the same passwords.
//   - Nothing running changes. Every role reads the file once, at start.

// Setting is one NAME=value line in effect in the file.
type Setting struct {
	Name, Value string
	Line        int  // 1-based line number
	Shadowed    bool // a later duplicate the loader ignores (first one wins)
}

// FindFile is the settings file LoadFile would read, without reading it.
// explicit (from --config) is returned as an absolute path even when it does
// not exist, so `config set` can create it. "" means no file was found.
func FindFile(explicit string) string {
	if explicit != "" {
		if abs, err := filepath.Abs(explicit); err == nil {
			return abs
		}
		return explicit
	}
	for _, p := range searchPaths() {
		if isFile(p) {
			return p
		}
	}
	if isFile(LegacyFileName) {
		if abs, err := filepath.Abs(LegacyFileName); err == nil {
			return abs
		}
		return LegacyFileName
	}
	return ""
}

// ReadSettings lists the settings in a file in the order they appear. A
// line the gateway would refuse is an error, exactly as at start-up.
func ReadSettings(path string) ([]Setting, error) {
	lines, err := readLines(path)
	if err != nil {
		return nil, err
	}
	var out []Setting
	seen := map[string]bool{}
	var bad []string
	for i, raw := range lines {
		k, v, ok, problem := parseLine(raw, i+1)
		if problem != "" {
			bad = append(bad, problem)
			continue
		}
		if !ok {
			continue
		}
		out = append(out, Setting{Name: k, Value: v, Line: i + 1, Shadowed: seen[k]})
		seen[k] = true
	}
	if len(bad) > 0 {
		return out, fmt.Errorf("config file %s: %s", path, strings.Join(bad, "; "))
	}
	return out, nil
}

// Change is one requested edit: Value set, or Unset.
type Change struct {
	Name, Value string
	Unset       bool
}

// Outcome says what happened to one Change, for the operator.
type Outcome struct {
	Name, Old, New string
	Line           int    // where the value now lives (0 for an unset)
	How            string // "changed", "added", "uncommented", "unset", "unchanged", "not set"
}

// EditOptions tune ApplyChanges.
type EditOptions struct {
	AllowNew bool      // accept a name the gateway does not read
	Now      time.Time // for the note on an added line; zero = time.Now()
}

// ApplyChanges makes every change or none. The file is created when it does
// not exist (only a set can do that).
func ApplyChanges(path string, changes []Change, opt EditOptions) ([]Outcome, error) {
	if len(changes) == 0 {
		return nil, fmt.Errorf("nothing to change")
	}
	var lines []string
	var mode os.FileMode = 0o600
	exists := isFile(path)
	if exists {
		var err error
		if lines, err = readLines(path); err != nil {
			return nil, err
		}
		if info, err := os.Stat(path); err == nil {
			mode = info.Mode().Perm()
		}
		if _, err := ReadSettings(path); err != nil {
			// Editing around a line the gateway refuses would hide it.
			return nil, fmt.Errorf("%v — fix that line first", err)
		}
	}
	inFile := namesMentioned(lines)
	now := opt.Now
	if now.IsZero() {
		now = time.Now()
	}

	seen := map[string]bool{}
	var outs []Outcome
	for _, c := range changes {
		if !nameRE.MatchString(c.Name) {
			return nil, fmt.Errorf("%q is not a setting name: letters, digits and _, not starting with a digit", c.Name)
		}
		if seen[c.Name] {
			return nil, fmt.Errorf("%s is named twice", c.Name)
		}
		seen[c.Name] = true
		if !c.Unset && !opt.AllowNew && !Known(c.Name) && !inFile[c.Name] {
			msg := fmt.Sprintf("the gateway does not read a setting called %s", c.Name)
			if s := Suggest(c.Name); s != "" {
				msg += fmt.Sprintf(" — did you mean %s?", s)
			}
			return nil, fmt.Errorf("%s (add --new to write it anyway)", msg)
		}
		var out Outcome
		var err error
		if c.Unset {
			lines, out = unsetLine(lines, c.Name, now)
		} else {
			if err = CheckValue(c.Name, c.Value); err != nil {
				return nil, err
			}
			lines, out, err = setLine(lines, c.Name, c.Value, now)
			if err != nil {
				return nil, err
			}
		}
		outs = append(outs, out)
	}

	changed := false
	for _, o := range outs {
		if o.How != "unchanged" && o.How != "not set" {
			changed = true
		}
	}
	if !changed {
		return outs, nil
	}

	// Read the result back the way the gateway will, before it replaces
	// anything.
	body := strings.Join(lines, "\n")
	if !strings.HasSuffix(body, "\n") {
		body += "\n"
	}
	if err := verify(body, changes); err != nil {
		return nil, err
	}
	if exists {
		if err := copyFile(path, path+".bak", mode); err != nil {
			return nil, fmt.Errorf("keep a copy of the old file: %w", err)
		}
	}
	if err := writeAtomic(path, []byte(body), mode); err != nil {
		return nil, err
	}
	return outs, nil
}

// setLine points the first live NAME= line at the new value. With no live
// line, a commented-out one ("# NAME=…", as yakugate.config.example ships them)
// is replaced where it stands, so the setting keeps the explanation above
// it; with neither, the line goes at the end.
func setLine(lines []string, name, value string, now time.Time) ([]string, Outcome, error) {
	text, err := formatLine(name, value)
	if err != nil {
		return lines, Outcome{}, err
	}
	out := Outcome{Name: name, New: value}
	for i, raw := range lines {
		k, v, ok, _ := parseLine(raw, i+1)
		if ok && k == name {
			out.Old, out.Line = v, i+1
			if v == value {
				out.How = "unchanged"
				return lines, out, nil
			}
			lines[i] = text
			out.How = "changed"
			return lines, out, nil
		}
	}
	for i := len(lines) - 1; i >= 0; i-- {
		if commentedName(lines[i]) == name {
			lines[i] = text
			out.Line, out.How = i+1, "uncommented"
			return lines, out, nil
		}
	}
	if n := len(lines); n > 0 && strings.TrimSpace(lines[n-1]) != "" {
		lines = append(lines, "")
	}
	lines = append(lines, "# added by `yakugate config set`, "+now.UTC().Format("2006-01-02"), text)
	out.Line, out.How = len(lines), "added"
	return lines, out, nil
}

// unsetLine comments out every live NAME= line (duplicates too: an unset
// setting must not come back from a line further down). Commenting rather
// than deleting keeps the old value there to restore by hand.
func unsetLine(lines []string, name string, now time.Time) ([]string, Outcome) {
	out := Outcome{Name: name, How: "not set"}
	for i, raw := range lines {
		k, v, ok, _ := parseLine(raw, i+1)
		if ok && k == name {
			if out.How == "not set" {
				out.Old = v
			}
			lines[i] = "# " + strings.TrimSpace(raw) + "   # unset " + now.UTC().Format("2006-01-02")
			out.How = "unset"
		}
	}
	return lines, out
}

// formatLine writes NAME=value so parseLine reads back exactly value. Quotes
// are added only when the value needs them.
func formatLine(name, value string) (string, error) {
	if strings.ContainsAny(value, "\r\n") {
		return "", fmt.Errorf("%s: a value cannot span lines", name)
	}
	if value != strings.Trim(value, "\"'") {
		return "", fmt.Errorf("%s: a value cannot begin or end with a quote mark — the file format strips them", name)
	}
	needsQuotes := value != strings.TrimSpace(value) ||
		strings.Contains(value, " #") || strings.Contains(value, "\t#") || strings.HasPrefix(value, "#")
	if needsQuotes {
		return name + `="` + value + `"`, nil
	}
	return name + "=" + value, nil
}

// verify reads body as the gateway would and checks each change landed.
func verify(body string, changes []Change) error {
	got := map[string]string{}
	have := map[string]bool{}
	for i, raw := range strings.Split(body, "\n") {
		k, v, ok, problem := parseLine(raw, i+1)
		if problem != "" {
			return fmt.Errorf("the new file would not load (%s) — nothing written", problem)
		}
		if ok && !have[k] {
			got[k], have[k] = v, true
		}
	}
	for _, c := range changes {
		switch {
		case c.Unset && have[c.Name]:
			return fmt.Errorf("%s would still be set — nothing written", c.Name)
		case !c.Unset && got[c.Name] != c.Value:
			return fmt.Errorf("%s would read back as %q, not %q — nothing written", c.Name, got[c.Name], c.Value)
		}
	}
	return nil
}

var commentedRE = regexp.MustCompile(`^\s*#\s*([A-Za-z_][A-Za-z0-9_]*)\s*=`)

// commentedName is NAME for a "# NAME=…" line, else "".
func commentedName(line string) string {
	if m := commentedRE.FindStringSubmatch(line); m != nil {
		return m[1]
	}
	return ""
}

// namesMentioned is every name in the file, live or commented out.
func namesMentioned(lines []string) map[string]bool {
	m := map[string]bool{}
	for i, raw := range lines {
		if k, _, ok, _ := parseLine(raw, i+1); ok {
			m[k] = true
		} else if c := commentedName(raw); c != "" {
			m[c] = true
		}
	}
	return m
}

// ── values ───────────────────────────────────────────────────────────────────

var backends = map[string][]string{
	"YAKU_QUEUE_BACKEND":  {"mysql", "redis", "mssql"},
	"YAKU_CONFIG_BACKEND": {"mysql", "mssql", "mongo"},
	"YAKU_HOT_BACKEND":    {"mysql", "mssql", "mongo"},
	"YAKU_COLD_BACKEND":   {"mysql", "mssql"},
	// The HLR store (D-124): its own database, of the install's one SQL
	// type. Empty is allowed too: mysql, as the loader reads it.
	"YAKU_HLR_BACKEND": {"mysql", "mssql"},
}

// sqlDSNs are the store DSNs a SQL Server DSN may stand in: one in the
// sqlserver:// form must be one the driver can read.
var sqlDSNs = map[string]bool{
	"YAKU_CONFIG_DSN": true, "YAKU_HOT_DSN": true, "YAKU_QUEUE_DSN": true, "YAKU_HLR_DSN": true, "YAKU_COLD_DSN": true,
	"YAKU_CONFIG_READ_DSN": true, "YAKU_HOT_READ_DSN": true, "YAKU_QUEUE_READ_DSN": true, "YAKU_HLR_READ_DSN": true, "YAKU_COLD_READ_DSN": true,
}

// offAddrs are the listen addresses that may be "off": the port is not opened
// at all. YAKU_SMPP_ADDR=off runs smpp-server on its TLS port only (full
// go-live test 18 Oct 2026, P2 A5).
var offAddrs = map[string]bool{"YAKU_SMPP_ADDR": true}

// CheckValue refuses a value the gateway would stumble on, where the name
// says what shape it must have. Anything else is taken as given: the role
// that reads it is the judge, and says so at start.
func CheckValue(name, value string) error {
	v := strings.TrimSpace(value)
	if name == "YAKU_HLR_BACKEND" && v == "" {
		return nil
	}
	if allowed, ok := backends[name]; ok {
		for _, a := range allowed {
			if strings.EqualFold(v, a) {
				return nil
			}
		}
		return fmt.Errorf("%s: %q is not a backend here; one of %s", name, value, strings.Join(allowed, ", "))
	}
	if v == "" {
		return nil // empty = the default, for every setting
	}
	for _, suf := range []string{"_SEC", "_MS", "_MB", "_DAYS", "_MIN", "_COUNT", "_ATTEMPTS", "_THRESHOLD", "_WORKERS", "_SIZE"} {
		if strings.HasSuffix(name, suf) {
			if _, err := strconv.Atoi(v); err != nil {
				return fmt.Errorf("%s: %q is not a whole number", name, value)
			}
			return nil
		}
	}
	if sqlDSNs[name] && IsSQLServerDSN(v) {
		u, err := url.Parse(v)
		if err != nil || u.Host == "" {
			return fmt.Errorf("%s: a SQL Server DSN is written sqlserver://USER:PASSWORD@HOST:1433?database=NAME (a # in the password is %%23)", name)
		}
		return nil
	}
	if offAddrs[name] && strings.EqualFold(v, "off") {
		return nil
	}
	if strings.HasSuffix(name, "_ADDR") && !strings.Contains(v, ":") {
		return fmt.Errorf("%s: %q needs a port, e.g. :8080 or 127.0.0.1:8080", name, value)
	}
	return nil
}

// Secret reports whether a setting's value is kept off the screen unless
// asked for. *_TLS_KEY is a file path, not a key, and is shown.
func Secret(name string) bool {
	if strings.HasSuffix(name, "_TLS_KEY") {
		return false
	}
	for _, w := range []string{"PASSWORD", "SECRET", "TOKEN", "PEPPER"} {
		if strings.Contains(name, w) && !strings.HasSuffix(name, "_REF") {
			return true
		}
	}
	return strings.HasSuffix(name, "_KEY")
}

var credsInURL = regexp.MustCompile(`^([^:/@\s]*:)([^@]+)(@)`)
var credsInScheme = regexp.MustCompile(`(://[^:/@\s]*:)([^@]+)(@)`)

// Shown is a value as the screen shows it: a secret setting hidden, and the
// password inside a DSN or URL hidden.
func Shown(name, value string) string {
	if value == "" {
		return ""
	}
	if Secret(name) {
		return "•••••• (hidden)"
	}
	if credsInScheme.MatchString(value) {
		return credsInScheme.ReplaceAllString(value, "${1}••••••${3}")
	}
	return credsInURL.ReplaceAllString(value, "${1}••••••${3}")
}

// ── names ────────────────────────────────────────────────────────────────────

// Known reports whether the gateway reads a setting of this name (one a
// group file registers: known_names.go).
func Known(name string) bool {
	_, ok := registeredAt[name]
	return ok
}

// KnownNames is every setting the gateway reads, sorted.
func KnownNames() []string {
	out := make([]string, 0, len(registered))
	for _, s := range registered {
		out = append(out, s.name)
	}
	sort.Strings(out)
	return out
}

// suggestNames is every known name in the order Suggest tries them: those of
// suggestOrder first, then the rest by name.
func suggestNames() []string {
	out := make([]string, 0, len(registered))
	first := make(map[string]bool, len(suggestOrder))
	for _, n := range suggestOrder {
		if Known(n) && !first[n] {
			first[n] = true
			out = append(out, n)
		}
	}
	for _, n := range KnownNames() {
		if !first[n] {
			out = append(out, n)
		}
	}
	return out
}

// Suggest is the known name closest to a mistyped one, or "". Of two equally
// close, the one first in suggestNames.
func Suggest(name string) string {
	best, bestD := "", 4 // more than 3 edits away is not a typo
	up := strings.ToUpper(name)
	for _, k := range suggestNames() {
		if k == up {
			return k
		}
		if d := distance(up, k); d < bestD {
			best, bestD = k, d
		}
	}
	return best
}

// distance is the Levenshtein edit distance.
func distance(a, b string) int {
	prev := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		cur := make([]int, len(b)+1)
		cur[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev = cur
	}
	return prev[len(b)]
}

// ── files ────────────────────────────────────────────────────────────────────

func readLines(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("config file %s: %w", path, err)
	}
	defer f.Close()
	var lines []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("config file %s: %w", path, err)
	}
	return lines, nil
}

func copyFile(from, to string, mode os.FileMode) error {
	b, err := os.ReadFile(from)
	if err != nil {
		return err
	}
	return writeAtomic(to, b, mode)
}

// writeAtomic replaces path in one step: a reader sees the old file or the
// new one, never half of either.
func writeAtomic(path string, b []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*")
	if err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	name := tmp.Name()
	defer os.Remove(name) // gone already after a successful rename
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := os.Chmod(name, mode); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := os.Rename(name, path); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}
