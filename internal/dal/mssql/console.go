package mssql

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	mssqldrv "github.com/microsoft/go-mssqldb"
	"github.com/microsoft/go-mssqldb/msdsn"

	"github.com/yakugate/yakugate/internal/dal"
)

// The console's database setup screen on a SQL Server (dal.ServerSetup,
// internal/dal/console.go): the settings' connection strings read and
// written in the sqlserver:// form, a login to the server itself (the
// master database) before the gateway's databases exist, the ones it
// lacks, and what a refusal says.

var stSetupDatabaseExists = EngineStmts.Add("setup.database_exists",
	"SELECT COUNT(*) FROM sys.databases WHERE name = @p1", false)

// serverSetup is dal.ServerSetup on SQL Server.
type serverSetup struct{}

var _ dal.ServerSetup = serverSetup{}

// ReadDSN reads any form the driver takes (sqlserver://, ADO, ODBC).
func (serverSetup) ReadDSN(dsn string) (addr, user, password, database string, ok bool) {
	if strings.TrimSpace(dsn) == "" {
		return "", "", "", "", false
	}
	c, err := msdsn.Parse(dsn)
	if err != nil {
		return "", "", "", "", false
	}
	port := c.Port
	if port == 0 {
		port = 1433
	}
	return net.JoinHostPort(c.Host, strconv.FormatUint(port, 10)), c.User, c.Password, c.Database, true
}

// BuildDSN writes sqlserver://user:password@host:port?database=…, keeping
// base's other parameters when base is a sqlserver:// DSN ("" = none; a
// database of "" = the server's default, for a login before any database
// exists).
func (serverSetup) BuildDSN(base, host, port, user, password, database string) string {
	q := url.Values{}
	if u, err := url.Parse(strings.TrimSpace(base)); err == nil && strings.EqualFold(u.Scheme, "sqlserver") {
		q = u.Query()
	}
	for k := range q {
		if strings.EqualFold(k, "database") {
			delete(q, k)
		}
	}
	if database != "" {
		q.Set("database", database)
	}
	u := url.URL{Scheme: "sqlserver", User: url.UserPassword(user, password), Host: net.JoinHostPort(host, port), RawQuery: q.Encode()}
	return u.String()
}

// Ping logs in to the server, within five seconds.
func (serverSetup) Ping(ctx context.Context, dsn string) error {
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	c, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return db.PingContext(c)
}

// MissingDatabases asks sys.databases for each name, on one connection.
func (serverSetup) MissingDatabases(ctx context.Context, dsn string, names []string) ([]string, error) {
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	var missing []string
	for _, name := range names {
		var n int
		if err := db.QueryRowContext(ctx, stSetupDatabaseExists.SQL, name).Scan(&n); err != nil {
			return nil, err
		}
		if n == 0 {
			missing = append(missing, name)
		}
	}
	return missing, nil
}

// Refusal is SQL Server's own error number and words (18456 login failed,
// 4060 cannot open the database, …).
func (serverSetup) Refusal(err error) (int, string, bool) {
	var me mssqldrv.Error
	if errors.As(err, &me) {
		return int(me.Number), me.Message, true
	}
	var mp *mssqldrv.Error
	if errors.As(err, &mp) && mp != nil {
		return int(mp.Number), mp.Message, true
	}
	return 0, "", false
}
