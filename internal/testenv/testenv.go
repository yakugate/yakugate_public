// Package testenv hands a test the scratch store it is pointed at, and nothing
// else. On 2026-10-30 a full `go test` from a laptop reached the dev box's
// Redis, because five tests named it when YAKU_TEST_REDIS_ADDR was unset.
//
// A store setting that is unset skips the test, naming the setting; there is
// no default address. A setting that names a server other than this machine
// skips it too, unless YAKU_TEST_ALLOW_REMOTE=1, so a stale variable pointing
// at the dev box is caught. A value that yakugate.config put into the
// environment (config.LoadFile) counts as unset: tests never find their
// stores in the gateway's settings file.
package testenv

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/yakugate/yakugate/internal/platform/config"
)

// AllowRemote is the setting that lets a test use a store on another machine.
// It is a test's setting, not the gateway's, so it is not in the gateway's
// register (config's TestKnownNamesCoverSource reads the double-quoted
// names of the engine's source; this one and the one in hint are raw).
const AllowRemote = `YAKU_TEST_ALLOW_REMOTE`

// Get returns the value of the store setting name, or skips t: when it is
// unset, came from the settings file, or names a server that is not this
// machine.
func Get(t testing.TB, name string) string {
	t.Helper()
	v, why := Lookup(name)
	if why != "" {
		t.Skip(why)
	}
	return v
}

// Optional is Get for a store a test can do without: "" when the setting is
// unset (or came from the settings file), and still a skip when it names a
// server that is not this machine.
func Optional(t testing.TB, name string) string {
	t.Helper()
	v := os.Getenv(name)
	if strings.TrimSpace(v) == "" || fromFile(name, v) {
		return ""
	}
	return Get(t, name)
}

// First is Get for the first of names that is set, skipping t when none is.
func First(t testing.TB, names ...string) string {
	t.Helper()
	for _, n := range names {
		if v := Optional(t, n); v != "" {
			return v
		}
	}
	t.Skipf("%s not set (%s)", strings.Join(names, " or "), hint(names[0]))
	return ""
}

// Lookup is Get for code with no test at hand (a TestMain, a harness's
// setup): the value, or why a test may not use it.
func Lookup(name string) (value, why string) {
	v := os.Getenv(name)
	if strings.TrimSpace(v) == "" || fromFile(name, v) {
		return "", fmt.Sprintf("%s not set (%s)", name, hint(name))
	}
	if why := Refuse(v); why != "" {
		return "", name + ": " + why
	}
	return v, ""
}

// Check skips t when value, read from the setting name, may not be used: for
// a value a test already holds (config.Load, a harness's own reading).
func Check(t testing.TB, name, value string) string {
	t.Helper()
	if strings.TrimSpace(value) == "" {
		t.Skipf("%s not set (%s)", name, hint(name))
	}
	if why := Refuse(value); why != "" {
		t.Skip(name + ": " + why)
	}
	return value
}

// Refuse says why a test may not use value — a host:port, a list of them, a
// DSN or a URL — or "" when it may: every server it names is this machine,
// or YAKU_TEST_ALLOW_REMOTE=1. The value itself is never repeated (a DSN
// holds a password); only the host is.
func Refuse(value string) string {
	if os.Getenv(AllowRemote) == "1" {
		return ""
	}
	hosts, err := Hosts(value)
	if err != nil {
		return fmt.Sprintf("refused: cannot tell which server it names (%v); set %s=1 to use it anyway", err, AllowRemote)
	}
	for _, h := range hosts {
		if !Loopback(h) {
			return fmt.Sprintf("refused: %q is not this machine — tests use a scratch store on 127.0.0.1 (set %s=1 to allow it)", h, AllowRemote)
		}
	}
	return ""
}

// Hosts lists the servers value names: a host:port (or a comma-separated
// list), a MySQL DSN, an ADO-style SQL Server string, or a URL (sqlserver://,
// redis://, mongodb://, http://).
func Hosts(value string) ([]string, error) {
	v := strings.TrimSpace(value)
	switch {
	case strings.Contains(v, "://"):
		u, err := url.Parse(v)
		if err != nil {
			return nil, err
		}
		if u.Host == "" {
			return nil, fmt.Errorf("no host")
		}
		var out []string
		for _, hp := range strings.Split(u.Host, ",") {
			out = append(out, hostOf(hp))
		}
		return out, nil
	case isADO(v):
		for _, part := range strings.Split(v, ";") {
			k, val, _ := strings.Cut(part, "=")
			switch strings.ToLower(strings.TrimSpace(k)) {
			case "server", "data source", "address", "addr", "network address":
				val = strings.TrimPrefix(strings.TrimSpace(val), "tcp:")
				val, _, _ = strings.Cut(val, ",") // host,port
				val, _, _ = strings.Cut(val, `\`) // host\instance
				return []string{hostOf(val)}, nil
			}
		}
		return nil, fmt.Errorf("no server")
	case strings.Contains(v, "@") || strings.Contains(v, "("):
		h, err := mysqlHost(v)
		if err != nil {
			return nil, err
		}
		return []string{h}, nil
	default:
		var out []string
		for _, hp := range strings.Split(v, ",") {
			out = append(out, hostOf(hp))
		}
		return out, nil
	}
}

// mysqlHost is the server of a MySQL DSN, user:pass@tcp(host:port)/db?opts
// (or a login with no database). Read here, not with the driver: only the
// DAL imports a database client (internal/dal's imports test).
func mysqlHost(v string) (string, error) {
	part := v
	if c := strings.LastIndex(v, ")"); c >= 0 {
		part = v[:c+1]
	} else if s := strings.LastIndex(v, "/"); s >= 0 {
		part = v[:s] // user:pass@/db: no address
	}
	if a := strings.LastIndex(part, "@"); a >= 0 {
		part = part[a+1:]
	}
	if part == "" {
		return "127.0.0.1", nil // the driver's own default, 127.0.0.1:3306
	}
	if strings.HasPrefix(part, "unix(") {
		return "localhost", nil
	}
	o := strings.Index(part, "(")
	if o < 0 || !strings.HasSuffix(part, ")") {
		return "", fmt.Errorf("not a MySQL DSN")
	}
	if addr := part[o+1 : len(part)-1]; addr != "" {
		return hostOf(addr), nil
	}
	return "127.0.0.1", nil
}

// isADO reports an ADO-style SQL Server string (server=host;user id=...).
func isADO(v string) bool {
	l := strings.ToLower(v)
	if strings.Contains(l, "@tcp(") || strings.Contains(l, "@unix(") {
		return false
	}
	return strings.Contains(l, "server=") || strings.Contains(l, "data source=") || strings.Contains(l, "address=")
}

func hostOf(hp string) string {
	hp = strings.TrimSpace(hp)
	if h, _, err := net.SplitHostPort(hp); err == nil {
		return h
	}
	return strings.Trim(hp, "[]")
}

// Loopback reports whether host is this machine: localhost, 127.0.0.0/8 or ::1.
func Loopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// fromFile reports whether the settings file, not the environment the test
// was started with, gave name its value.
func fromFile(name, v string) bool {
	f := config.LoadedFile()
	if f == "" {
		return false
	}
	for _, kv := range config.OriginalEnviron() {
		if strings.HasPrefix(kv, name+"=") {
			return false
		}
	}
	m, err := config.ReadFile(f)
	return err == nil && m[name] == v
}

func hint(name string) string {
	switch {
	case strings.Contains(name, "REDIS"):
		return "a scratch Redis, e.g. 127.0.0.1:16399"
	case strings.Contains(name, "MSSQL"):
		return "a scratch SQL Server, e.g. sqlserver://sa:PASSWORD@127.0.0.1:11433?database=NAME"
	case strings.Contains(name, "MONGO"):
		return "a scratch MongoDB, e.g. mongodb://127.0.0.1:27017/NAME"
	case strings.Contains(name, "HTTP"), strings.Contains(name, "ADMIN_ADDR"):
		return "a test gateway, e.g. http://127.0.0.1:18080"
	case strings.Contains(name, "SMPP"):
		return "a test gateway's SMPP port, e.g. 127.0.0.1:12775"
	case name == `YAKU_TEST_MYSQL`:
		return "a scratch MySQL login, no database, e.g. root:PASSWORD@tcp(127.0.0.1:13306)"
	}
	return "a scratch MySQL, e.g. root:PASSWORD@tcp(127.0.0.1:13306)/NAME?parseTime=true"
}
