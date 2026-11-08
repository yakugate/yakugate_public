package mysql

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"time"

	mysqldrv "github.com/go-sql-driver/mysql"

	"github.com/yakugate/yakugate/internal/dal"
)

// What the console opens for itself on MySQL (internal/dal console.go),
// moved here unchanged on 30 Oct 2026 from internal/process/console
// (monitor_view.go's hotDB, setup.go's server login and connection
// strings).

var stMissingDatabase = RoleStmts.Add("setup.database_exists",
	"SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?", false)

// monitorReader is the history on a handle the console opened and closes.
type monitorReader struct {
	*MonitorHistory
	db *sql.DB
}

func (m monitorReader) Close() error { return m.db.Close() }

// The console's own monitoring history handle and its database setup
// screen.
var _ = serves(func(d *dal.Driver) {
	d.MonitorReader = openMonitorReader
	d.ServerSetup = func() dal.ServerSetup { return serverSetup{} }
})

// openMonitorReader opens the hot store at dsn on a handle of its own (a
// plain sql.Open, as the console did).
func openMonitorReader(dsn string) (dal.MonitorReader, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return monitorReader{MonitorHistory: NewMonitorHistory(db), db: db}, nil
}

// serverSetup is the console's setup screen on a MySQL server.
type serverSetup struct{}

var _ dal.ServerSetup = serverSetup{}

func (serverSetup) ReadDSN(dsn string) (addr, user, password, database string, ok bool) {
	c, err := mysqldrv.ParseDSN(dsn)
	if err != nil {
		return "", "", "", "", false
	}
	return c.Addr, c.User, c.Passwd, c.DBName, true
}

func (serverSetup) BuildDSN(base, host, port, user, password, database string) string {
	c, err := mysqldrv.ParseDSN(base)
	if err != nil || base == "" {
		c = mysqldrv.NewConfig()
		c.ParseTime = true
	}
	c.Net = "tcp"
	c.Addr = net.JoinHostPort(host, port)
	c.User, c.Passwd, c.DBName = user, password, database
	c.ParseTime = true
	return c.FormatDSN()
}

// Ping logs in to the server itself, before any database exists.
func (serverSetup) Ping(ctx context.Context, dsn string) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	c, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return db.PingContext(c)
}

func (serverSetup) MissingDatabases(ctx context.Context, dsn string, names []string) ([]string, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	var missing []string
	for _, name := range names {
		var n int
		if err := db.QueryRowContext(ctx, stMissingDatabase.SQL, name).Scan(&n); err != nil {
			return nil, err
		}
		if n == 0 {
			missing = append(missing, name)
		}
	}
	return missing, nil
}

func (serverSetup) Refusal(err error) (int, string, bool) {
	var me *mysqldrv.MySQLError
	if errors.As(err, &me) {
		return int(me.Number), me.Message, true
	}
	return 0, "", false
}
