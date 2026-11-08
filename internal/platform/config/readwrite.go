package config

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"strings"

	mysqldrv "github.com/go-sql-driver/mysql"

	"github.com/yakugate/yakugate/internal/secrets"
)

// A read DSN beside each store's own DSN.
//
// Saleem, 2026-10-13: "for database/mysql I need two separate instances, one
// read, the other write, both configurable from env and config file — I need
// to configure a MySQL cluster with a router with separate read and write."
// MySQL Router in front of an InnoDB Cluster gives two addresses: one that
// always reaches the node that takes writes, one that spreads reads over the
// other nodes.
//
// The settings, one per store, written like the store's own DSN:
//
//	YAKU_CONFIG_READ_DSN, YAKU_HOT_READ_DSN, YAKU_QUEUE_READ_DSN,
//	YAKU_HLR_READ_DSN, YAKU_COLD_READ_DSN
//
// A store's own DSN (YAKU_HOT_DSN …) is where it is WRITTEN. Its read DSN is
// a whole DSN — address, login, database, options — for the reads that may
// be a moment old. Unset = the store is read where it is written.
//
// The first build of that day also had two one-line settings, YAKU_WRITE_DSN
// and YAKU_READ_DSN (an address and a login applied to every store). Saleem,
// the same day: the stores' own DSNs already are the write side ("why
// requiring write separately?"), and "introduce read for each separately
// like main ones". Both were taken out before they were ever released: one
// way, the same shape as the main DSNs.
//
// Which reads go to the read DSN is not decided here (dbpool.Reader and its
// callers): a read node runs a little behind the write node, so only reads
// that may be a moment old are sent there.

// readSlots are the per-store read settings, beside the fields each fills.
func (c *Config) readSlots() []struct {
	name      string
	write, rd *string
	backend   Backend
} {
	return []struct {
		name      string
		write, rd *string
		backend   Backend
	}{
		{"YAKU_CONFIG_READ_DSN", &c.ConfigDSN, &c.ConfigReadDSN, c.ConfigBackend},
		{"YAKU_HOT_READ_DSN", &c.HotDSN, &c.HotReadDSN, c.HotBackend},
		{"YAKU_QUEUE_READ_DSN", &c.QueueDSN, &c.QueueReadDSN, sqlKind(c.QueueBackend)},
		{"YAKU_HLR_READ_DSN", &c.HLRDSN, &c.HLRReadDSN, sqlKind(c.HLRBackend)},
		{"YAKU_COLD_READ_DSN", &c.ColdDSN, &c.ColdReadDSN, c.ColdBackend},
	}
}

// sqlKind is the SQL database a queue or HLR setting names for its DSN:
// SQL Server when it says mssql, else MySQL (a redis queue setting with a
// MySQL queue DSN beside it still reads that DSN as MySQL, as before).
func sqlKind(b Backend) Backend {
	if Backend(strings.ToLower(strings.TrimSpace(string(b)))) == BackendMSSQL {
		return BackendMSSQL
	}
	return BackendMySQL
}

// applyReadDSNs reads each store's read DSN. Called by Load after the DSN
// refs are resolved and BEFORE the session time zone is added, so a read DSN
// gets the same session rule as its store's.
//
// A SQL Server store (step 4 of the DAL plan) takes a read DSN under the same
// names: an Availability Group listener reached with
// ApplicationIntent=ReadOnly, which is added when the DSN does not say it —
// with it, the listener sends the session to a readable secondary. Both DSNs
// are then in the sqlserver:// form.
func (c *Config) applyReadDSNs() {
	ctx := context.Background()
	mysqlStore := func(dsn string, b Backend) bool {
		if b != BackendMySQL || strings.TrimSpace(dsn) == "" {
			return false
		}
		_, err := mysqldrv.ParseDSN(strings.TrimSpace(dsn))
		return err == nil && !strings.Contains(dsn, "://")
	}
	for _, s := range c.readSlots() {
		own := strings.TrimSpace(env(s.name, ""))
		if secrets.IsRef(own) {
			if v, err := Secrets.ResolveString(ctx, own); err == nil {
				own = v
			} else {
				log.Printf("[secrets] %s is a ref that could not be resolved: %v", s.name, err)
				own = ""
			}
		}
		*s.rd = ""
		switch {
		case own == "":
		case sqlKind(s.backend) == BackendMSSQL && s.backend != "":
			if !IsSQLServerDSN(*s.write) {
				log.Printf("[config] %s is set, but that store's own DSN is not a sqlserver:// DSN: it is not used", s.name)
				continue
			}
			rd, err := readIntent(own)
			if err != nil {
				log.Printf("[config] %s is not a sqlserver:// DSN (%v): that store is read where it is written", s.name, err)
			} else if !sameSQLServer(rd, *s.write) {
				*s.rd = rd
			}
		case !mysqlStore(*s.write, s.backend):
			log.Printf("[config] %s is set, but that store is not a MySQL store here: it is not used", s.name)
		default:
			if _, err := mysqldrv.ParseDSN(own); err != nil {
				log.Printf("[config] %s is not a MySQL DSN (%v): that store is read where it is written", s.name, err)
			} else if !sameServer(own, *s.write) { // a read DSN that is the store's own DSN is no second address
				*s.rd = own
			}
		}
	}
}

// IsSQLServerDSN says a DSN is in the sqlserver:// form, the one the
// settings take for a SQL Server store.
func IsSQLServerDSN(dsn string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(dsn)), "sqlserver://")
}

// readIntent is a sqlserver:// read DSN with ApplicationIntent=ReadOnly,
// unless it names an intent itself.
func readIntent(dsn string) (string, error) {
	if !IsSQLServerDSN(dsn) {
		return "", fmt.Errorf("it does not start with sqlserver://")
	}
	u, err := url.Parse(strings.TrimSpace(dsn))
	if err != nil {
		return "", err
	}
	if u.Host == "" {
		return "", fmt.Errorf("no server in it")
	}
	q := u.Query()
	for k := range q {
		if strings.EqualFold(k, "applicationintent") {
			return dsn, nil
		}
	}
	q.Set("ApplicationIntent", "ReadOnly")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// sameSQLServer says two sqlserver:// DSNs reach the same server and
// database with the same login and the same intent — the same session,
// so no second address. The same listener with ReadOnly is a second one.
func sameSQLServer(a, b string) bool {
	x, errA := url.Parse(strings.TrimSpace(a))
	y, errB := url.Parse(strings.TrimSpace(b))
	if errA != nil || errB != nil {
		return strings.TrimSpace(a) == strings.TrimSpace(b)
	}
	param := func(u *url.URL, name string) string {
		for k, v := range u.Query() {
			if strings.EqualFold(k, name) && len(v) > 0 {
				return strings.ToLower(v[0])
			}
		}
		return ""
	}
	return strings.EqualFold(x.Host, y.Host) && x.User.Username() == y.User.Username() &&
		strings.EqualFold(x.Path, y.Path) && param(x, "database") == param(y, "database") &&
		param(x, "applicationintent") == param(y, "applicationintent")
}

// sameServer says two DSNs reach the same address with the same login and
// name the same database.
func sameServer(a, b string) bool {
	x, errA := mysqldrv.ParseDSN(a)
	y, errB := mysqldrv.ParseDSN(b)
	if errA != nil || errB != nil {
		return strings.TrimSpace(a) == strings.TrimSpace(b)
	}
	return x.Net == y.Net && x.Addr == y.Addr && x.User == y.User && x.DBName == y.DBName
}

// Readers maps each MySQL store's DSN — the one it is written through, as
// every caller already holds it — to the DSN its reads may use. A store with
// no read instance is not in the map. For dbpool.SetReaders.
func (c Config) Readers() map[string]string {
	out := map[string]string{}
	add := func(write, read string) {
		if strings.TrimSpace(write) != "" && strings.TrimSpace(read) != "" {
			out[write] = read
		}
	}
	add(c.ConfigDSN, c.ConfigReadDSN)
	add(c.HotDSN, c.HotReadDSN)
	add(c.QueueDSN, c.QueueReadDSN)
	add(c.HLRDSN, c.HLRReadDSN)
	add(c.ColdDSN, c.ColdReadDSN)
	return out
}

// ReadInstances says, for the log line at start, where each store is read
// from when that is not where it is written: "hot → 10.0.0.5:6447". Never a
// login.
func (c Config) ReadInstances() []string {
	var out []string
	for _, s := range []struct{ name, write, read string }{
		{"config", c.ConfigDSN, c.ConfigReadDSN}, {"hot", c.HotDSN, c.HotReadDSN},
		{"queue", c.QueueDSN, c.QueueReadDSN}, {"hlr", c.HLRDSN, c.HLRReadDSN}, {"cold", c.ColdDSN, c.ColdReadDSN},
	} {
		if s.read == "" {
			continue
		}
		if IsSQLServerDSN(s.read) {
			if u, err := url.Parse(strings.TrimSpace(s.read)); err == nil {
				out = append(out, s.name+" → "+u.Host)
			}
			continue
		}
		if r, err := mysqldrv.ParseDSN(s.read); err == nil {
			out = append(out, s.name+" → "+r.Addr)
		}
	}
	return out
}
