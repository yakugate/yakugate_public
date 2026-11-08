package console

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"errors"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/bootstrap"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// Database setup (Saleem 2026-10-02: "at least open the console and force
// them to update the settings"). A console that cannot reach its databases
// used to print one driver error and exit — on a fresh download, with the
// example settings still in place, before the operator had seen anything.
// Now it opens on this screen instead: what is wrong, in words; the MySQL
// connection asked field by field and tested before it is saved; the
// databases built if they do not exist yet; and the console's menus only
// once it all works. Leaving is always possible; skipping is not.
//
// The server may be MySQL or SQL Server (DAL plan, step 4): the first
// question is which, and each is reached through its own DAL's setup
// (dal.SetupOf). One type for the whole install (dal.CheckOneSQL).

// storeNames are the databases a gateway uses, by the setting that names each.
var storeNames = []struct{ setting, suffix string }{
	{"YAKU_CONFIG_DSN", "config"},
	{"YAKU_QUEUE_DSN", "queue"},
	{"YAKU_HOT_DSN", "hot"},
	{"YAKU_COLD_DSN", "cold"},
}

// dbForm is what the setup screen asks for.
type dbForm struct {
	kind                           config.Backend // mysql or mssql
	host, port, user, pass, prefix string
	base                           string // the current DSN, whose other parameters are kept; "" = the defaults
}

// setupStores runs when the console cannot reach its config store. true =
// the stores work now and the console may go on; false = the operator left.
func (p *Process) setupStores(ctx context.Context, sc *bufio.Scanner, cause error) bool {
	path := p.settingsPath()
	p.errf("\n  The console cannot reach the gateway's databases.\n")
	p.printf("  %s\n", plainStoreError(cause, p.cfg.ConfigDSN))
	if path == "" {
		wd, _ := os.Getwd()
		path = filepath.Join(wd, config.FileName)
		p.printf("\n  No settings file was found; one will be made: %s\n", path)
	} else {
		p.printf("\n  Settings file: %s\n", path)
	}
	// The environment as it was before the settings file was read: loading
	// the file copies its values into this process's environment.
	orig := config.OriginalEnviron()
	for _, s := range storeNames {
		if envOf(orig, s.setting) != "" {
			p.warnf("  %s is set in the environment, which wins over the file: unset it, or what is saved here has no effect.\n", s.setting)
		}
	}
	p.printf("\n  Give the database connection the gateway should use (MySQL or SQL Server). The console opens once it works.\n")
	p.printf("  Enter keeps the value in [brackets]; 0 at the first question leaves the console.\n")

	f := formFrom(p.cfg.ConfigDSN)
	for {
		if !p.askDBForm(sc, &f) {
			return false
		}
		p.printf("\n  Connecting to %s as %s ... ", net.JoinHostPort(f.host, f.port), f.user)
		if err := pingServer(ctx, f); err != nil {
			p.errf("refused.\n")
			p.printf("  %s\n\n", plainStoreError(err, f.dsn("")))
			if !p.askYes(sc, "  Try again", true) || p.ended {
				return false
			}
			continue
		}
		p.okf("connected.\n")

		missing, err := missingDatabases(ctx, f)
		if err != nil {
			p.errf("  Could not list the databases: %v\n\n", err)
			if !p.askYes(sc, "  Try again", true) || p.ended {
				return false
			}
			continue
		}
		if len(missing) > 0 {
			p.printf("\n  These databases do not exist yet: %s.\n", strings.Join(missing, ", "))
			p.printf("  The gateway builds them itself; the console needs them now.\n")
			if !p.askYes(sc, "  Create them now", true) || p.ended {
				p.printf("  The console needs its databases. Run `yakugate --config %s migrate` later, or try again.\n\n", path)
				if !p.askYes(sc, "  Try again", true) || p.ended {
					return false
				}
				continue
			}
			p.printf("  Creating the databases and their tables (this takes a minute) ... ")
			var progress bytes.Buffer
			if err := bootstrap.EnsureTo(ctx, f.applyTo(p.cfg), &progress); err != nil {
				p.errf("failed.\n  %s\n", plainStoreError(err, f.dsn("")))
				if tail := lastLines(progress.String(), 5); tail != "" {
					p.printf("  The last steps it took:\n%s\n", tail)
				}
				p.printf("\n")
				if !p.askYes(sc, "  Try again", true) || p.ended {
					return false
				}
				continue
			}
			if still, _ := missingDatabases(ctx, f); len(still) > 0 {
				p.errf("nothing was built.\n")
				p.printf("  YAKU_AUTO_MIGRATE is off in the settings, so the gateway builds no databases. Turn it on, or build them by hand.\n\n")
				if !p.askYes(sc, "  Try again", true) || p.ended {
					return false
				}
				continue
			}
			p.okf("done.\n")
		}

		changes := f.changes(p.cfg)
		p.printf("\n  This will be saved to %s:\n", path)
		for _, c := range changes {
			p.printf("    %s=%s\n", c.Name, config.Shown(c.Name, c.Value))
		}
		if !p.askYes(sc, "  Save it", true) || p.ended {
			if !p.askYes(sc, "  Change the answers instead", true) || p.ended {
				return false
			}
			continue
		}
		if _, err := config.ApplyChanges(path, changes, config.EditOptions{}); err != nil {
			p.errf("  Not saved: %v\n\n", err)
			continue
		}
		p.okf("  Saved. The file before the change is %s.bak.\n", path)

		p.cfg = f.applyTo(p.cfg)
		p.settingsFile = path
		p.followSettings(path)
		p.close()
		if err := p.connect(); err == nil {
			err = p.store.Ping(ctx)
			if err == nil {
				p.warnf("  A gateway already running reads its settings at start: restart its roles for them to use these.\n\n")
				return true
			}
		}
		p.errf("  Saved, but the console still cannot reach its databases.\n\n")
	}
}

// askDBForm asks the connection field by field. false = the operator left.
func (p *Process) askDBForm(sc *bufio.Scanner, f *dbForm) bool {
	for {
		kind, ok := p.ask(sc, fmt.Sprintf("  Database type, mysql or mssql (SQL Server) [%s]", f.kind))
		if !ok || kind == "0" {
			return false
		}
		if kind == "" {
			break
		}
		k, known := kindNamed(kind)
		if !known {
			p.errf("  Type mysql or mssql.\n")
			continue
		}
		f.setKind(k)
		break
	}
	host, ok := p.ask(sc, fmt.Sprintf("  %s host [%s]", kindWords(f.kind), f.host))
	if !ok {
		return false
	}
	if host != "" {
		f.host = host
	}
	for {
		port, ok := p.ask(sc, fmt.Sprintf("  Port [%s]", f.port))
		if !ok {
			return false
		}
		if port == "" {
			break
		}
		if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
			p.errf("  A port is a number from 1 to 65535.\n")
			continue
		}
		f.port = port
		break
	}
	user, ok := p.ask(sc, fmt.Sprintf("  User [%s]", f.user))
	if !ok {
		return false
	}
	if user != "" {
		f.user = user
	}
	keep := "Enter keeps the current one"
	if f.pass == "" {
		keep = "Enter for none"
	}
	pass, ok := p.readSecret(sc, "  Password ("+keep+")")
	if !ok {
		return false
	}
	if pass != "" {
		f.pass = pass
	}
	for {
		prefix, ok := p.ask(sc, fmt.Sprintf("  Database names start with [%s] (%s_config, %s_queue, %s_hot, %s_cold)",
			f.prefix, f.prefix, f.prefix, f.prefix, f.prefix))
		if !ok {
			return false
		}
		if prefix == "" {
			break
		}
		if !validDBPrefix(prefix) {
			p.errf("  Letters, digits and _ only, up to 50 characters.\n")
			continue
		}
		f.prefix = prefix
		break
	}
	return true
}

// The two server types the screen offers, and each one's usual port.
var defaultPorts = map[config.Backend]string{config.BackendMySQL: "3306", config.BackendMSSQL: "1433"}

// kindNamed reads an answer to the type question.
func kindNamed(s string) (config.Backend, bool) {
	switch strings.ToLower(strings.ReplaceAll(strings.TrimSpace(s), " ", "")) {
	case "mysql", "1":
		return config.BackendMySQL, true
	case "mssql", "sqlserver", "2":
		return config.BackendMSSQL, true
	}
	return "", false
}

// kindWords is a server type as the screen says it.
func kindWords(k config.Backend) string {
	if k == config.BackendMSSQL {
		return "SQL Server"
	}
	return "MySQL"
}

// setKind changes the server type: the other type's DSN parameters no
// longer apply, and its usual port gives way to this one's.
func (f *dbForm) setKind(k config.Backend) {
	if k == f.kind {
		return
	}
	if f.port == defaultPorts[f.kind] {
		f.port = defaultPorts[k]
	}
	f.kind, f.base = k, ""
}

// formFrom starts the form from the current config DSN, keeping its extra
// parameters; a missing or unreadable one starts from the usual local server.
// A sqlserver:// DSN starts it on SQL Server.
func formFrom(dsn string) dbForm {
	f := dbForm{kind: config.BackendMySQL, host: "127.0.0.1", port: "3306", user: "yakugate", prefix: "yakugate"}
	dsn = strings.TrimSpace(dsn)
	if config.IsSQLServerDSN(dsn) {
		f.kind, f.port = config.BackendMSSQL, "1433"
	}
	addr, user, pass, dbName, ok := serverSetup(f.kind).ReadDSN(dsn)
	if !ok || dsn == "" {
		return f
	}
	f.base = dsn
	if h, port, err := net.SplitHostPort(addr); err == nil {
		f.host, f.port = h, port
	}
	if user != "" {
		f.user = user
	}
	f.pass = pass
	if p, ok := strings.CutSuffix(dbName, "_config"); ok && p != "" {
		f.prefix = p
	}
	return f
}

// dsn is the connection to one database ("" = the server, no database).
func (f dbForm) dsn(db string) string {
	return serverSetup(f.kind).BuildDSN(f.base, f.host, f.port, f.user, f.pass, db)
}

// changes are the four DSN settings this form makes, and a store's type
// line where the settings now in force say the other type (a MySQL install
// saved as MySQL writes no type line, as before). Queues kept in Redis stay
// there.
func (f dbForm) changes(cur config.Config) []config.Change {
	out := make([]config.Change, 0, len(storeNames)+3)
	for _, s := range storeNames {
		out = append(out, config.Change{Name: s.setting, Value: f.dsn(f.prefix + "_" + s.suffix)})
	}
	for _, b := range []struct {
		name string
		now  config.Backend
	}{
		{"YAKU_CONFIG_BACKEND", cur.ConfigBackend}, {"YAKU_HOT_BACKEND", cur.HotBackend}, {"YAKU_QUEUE_BACKEND", cur.QueueBackend},
	} {
		now := config.Backend(strings.ToLower(strings.TrimSpace(string(b.now))))
		if now == "" {
			now = config.BackendMySQL
		}
		if now != f.kind && now != config.BackendRedis {
			out = append(out, config.Change{Name: b.name, Value: string(f.kind)})
		}
	}
	return out
}

// applyTo is the settings with this form's four stores on its server type
// (queues kept in Redis stay there).
func (f dbForm) applyTo(c config.Config) config.Config {
	c.ConfigBackend, c.ConfigDSN = f.kind, f.dsn(f.prefix+"_config")
	if c.QueueBackend != config.BackendRedis {
		c.QueueBackend = f.kind
	}
	c.QueueDSN = f.dsn(f.prefix + "_queue")
	c.HotBackend, c.HotDSN = f.kind, f.dsn(f.prefix+"_hot")
	c.ColdDSN = f.dsn(f.prefix + "_cold")
	return c
}

func validDBPrefix(s string) bool {
	if s == "" || len(s) > 50 {
		return false
	}
	for _, r := range s {
		if !(r == '_' || r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z') {
			return false
		}
	}
	return true
}

// serverSetup is the setup screen's server of a type: its DAL's setup.
func serverSetup(kind config.Backend) dal.ServerSetup {
	if s, err := dal.SetupOf(kind); err == nil {
		return s
	}
	panic("console: the " + string(kind) + " DAL is not linked in")
}

// kindOf is the server type a DSN is written for.
func kindOf(dsn string) config.Backend {
	if config.IsSQLServerDSN(dsn) {
		return config.BackendMSSQL
	}
	return config.BackendMySQL
}

// pingServer logs in to the server itself, before any database exists.
func pingServer(ctx context.Context, f dbForm) error {
	return serverSetup(f.kind).Ping(ctx, f.dsn(""))
}

// missingDatabases lists the form's databases the server does not have yet.
func missingDatabases(ctx context.Context, f dbForm) ([]string, error) {
	var names []string
	for _, s := range storeNames[:3] { // cold is an archive the retention role fills; not needed here
		names = append(names, f.prefix+"_"+s.suffix)
	}
	return serverSetup(f.kind).MissingDatabases(ctx, f.dsn(""), names)
}

// plainStoreError says what a connection failure means and what to do — the
// driver's own words come last, for whoever needs them.
func plainStoreError(err error, dsn string) string {
	if err == nil {
		return ""
	}
	user, addr, db := "", "", ""
	kind := kindOf(dsn)
	if a, u, _, d, ok := serverSetup(kind).ReadDSN(dsn); ok {
		user, addr, db = u, a, d
	}
	code, msg, refused := serverSetup(kind).Refusal(err)
	var ne net.Error
	switch {
	case strings.TrimSpace(dsn) == "":
		return "No database is set in the settings (YAKU_CONFIG_DSN is empty)."
	case kind == config.BackendMSSQL:
		return plainSQLServerError(err, refused, code, msg, user, addr, db)
	case refused && code == 1698:
		return fmt.Sprintf("MySQL refused user %s. On Ubuntu and Debian, MySQL's root logs in only through the server's own root account,\n"+
			"  never with a password. Give the gateway its own user (run `sudo mysql`, then):\n"+
			"    CREATE USER 'yakugate'@'localhost' IDENTIFIED BY 'a-strong-password';\n"+
			"    GRANT ALL PRIVILEGES ON `yakugate\\_%%`.* TO 'yakugate'@'localhost';\n"+
			"  (MySQL: %s)", user, msg)
	case refused && code == 1045:
		return fmt.Sprintf("MySQL refused the user name or password for %s. (MySQL: %s)", user, msg)
	case refused && code == 1044:
		return fmt.Sprintf("User %s may not use database %s. Grant it, e.g. GRANT ALL PRIVILEGES ON `yakugate\\_%%`.* TO '%s'@'localhost'; (MySQL: %s)",
			user, db, user, msg)
	case refused && code == 1049:
		return fmt.Sprintf("Database %s does not exist yet. (MySQL: %s)", db, msg)
	case errors.As(err, &ne) || strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "no such host"):
		return fmt.Sprintf("Nothing answers at %s: is MySQL running there, and is that the right host and port? (%v)", addr, err)
	}
	return err.Error()
}

// plainSQLServerError is plainStoreError for a SQL Server.
func plainSQLServerError(err error, refused bool, code int, msg, user, addr, db string) string {
	var ne net.Error
	switch {
	case refused && code == 18456:
		return fmt.Sprintf("SQL Server refused the login %s (a wrong name or password, or a server that takes Windows logins only:\n"+
			"  turn on \"SQL Server and Windows Authentication mode\"). (SQL Server: %s)", user, msg)
	case refused && code == 18470:
		return fmt.Sprintf("The login %s is disabled on that SQL Server. (SQL Server: %s)", user, msg)
	case refused && code == 4060:
		return fmt.Sprintf("Database %s does not exist yet, or login %s may not open it. (SQL Server: %s)", db, user, msg)
	case refused && code == 262:
		return fmt.Sprintf("Login %s may not create databases: give it dbcreator, or create them by hand. (SQL Server: %s)", user, msg)
	case errors.As(err, &ne) || strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "no such host"):
		return fmt.Sprintf("Nothing answers at %s: is SQL Server running there, listening on TCP, and is that the right host and port? (%v)", addr, err)
	}
	return err.Error()
}

// followSettings keeps the console password with the settings file. The
// password file lives beside the settings file the console loaded, else in
// ~/.config/yakugate — so when this screen makes a new settings file, the
// next start would look beside it, find nothing, and take the default
// password again. The current file is copied there first (never over one
// already there), and this session uses it from now on.
func (p *Process) followSettings(settings string) {
	next := filepath.Join(filepath.Dir(settings), AuthFileName)
	if p.authFile == "" || next == p.authFile {
		return
	}
	if _, err := os.Stat(next); errors.Is(err, os.ErrNotExist) {
		if b, err := os.ReadFile(p.authFile); err == nil {
			if err := os.WriteFile(next, b, 0o600); err != nil {
				p.warnf("  The console password could not be copied beside the settings (%v); the default applies there.\n", err)
				return
			}
		}
	}
	p.authFile = next
}

// lastLines is the end of a log, indented for the screen.
func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) == 1 && lines[0] == "" {
		return ""
	}
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	for i, l := range lines {
		lines[i] = "    " + l
	}
	return strings.Join(lines, "\n")
}
