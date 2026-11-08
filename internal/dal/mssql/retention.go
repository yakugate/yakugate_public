package mssql

import (
	"context"
	"database/sql"
	"net/url"
	"strings"
	"time"

	"github.com/golang-sql/civil"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// The retention role's upkeep of the config store and the servers it
// compares (dal.RetentionUpkeep, dal.ServerIdentity), made like the MySQL
// DAL's part of internal/dal/mysql/roleconfig.go ("retention's upkeep",
// "the servers retention compares").
//
// What differs, and why:
//   - balance_history is one table on SQL Server, not partitioned by month.
//     A month needs no partition of its own, so LedgerPartitions answers
//     that its partition is there (1) and AddLedgerPartition has nothing to
//     do.
//   - A server has no @@server_uuid. Its identity is its server and
//     machine names (@@SERVERNAME, SERVERPROPERTY('MachineName')): two DSNs
//     that reach one SQL Server by different host names answer the same.
//   - The config store's clock is UTC (SYSUTCDATETIME()), as every SQL
//     Server time is here; MySQL's NOW() was the session's.
//   - Company rows are read a page at a time by id (no unbounded read).

// RetentionStmts is every statement of retention's upkeep, by name.
var RetentionStmts = newRegistry()

// retentionPage is how many companies one read of CompanyHotDays takes.
const retentionPage = 5000

var (
	// hot_days / hot_months as MySQL's CAST(… AS UNSIGNED) reads them: a
	// number, its whole part; anything else 0.
	stRetHotDays = RetentionStmts.Add("retention.company_hot_days", `SELECT TOP (@p1) id,
		        COALESCE(TRY_CAST(FLOOR(TRY_CAST(JSON_VALUE(data_retention, '$.hot_days') AS FLOAT)) AS BIGINT), 0),
		        COALESCE(TRY_CAST(FLOOR(TRY_CAST(JSON_VALUE(data_retention, '$.hot_months') AS FLOAT)) AS BIGINT), 0)
		   FROM companies WHERE id > @p2 ORDER BY id`, false)
	stRetPruneBatches = RetentionStmts.Add("retention.prune_usage_batches",
		"DELETE TOP (5000) FROM usage_batches WHERE created_at < DATEADD(DAY, -@p1, SYSUTCDATETIME())", false)
	stRetPaidPeriods = RetentionStmts.Add("retention.paid_periods", `
		SELECT i.company_id, CAST(i.period_from AS DATE), CAST(i.period_to AS DATE)
		FROM invoices i
		WHERE i.status = N'SENT' AND i.payment_status = N'PAID'
		  AND EXISTS (SELECT 1 FROM balance_history b
		              WHERE b.company_id = i.company_id AND b.type = 'commit'
		                AND b.created_at >= i.period_from
		                AND b.created_at < DATEADD(DAY, 1, CAST(i.period_to AS DATE)))`, false)
	stRetCommitDays = RetentionStmts.Add("retention.commit_days", `
		SELECT DISTINCT CAST(created_at AS DATE) FROM balance_history
		WHERE company_id = @p1 AND type = 'commit'
		  AND created_at >= @p2 AND created_at < DATEADD(DAY, 1, @p3)
		ORDER BY 1`, false)
	// MySQL's INSERT IGNORE: a re-run of a day already rolled keeps its
	// first totals.
	stRetRollup = RetentionStmts.Add("retention.rollup_day", `
		INSERT INTO balance_history_rollup ([day], company_id, account_id, commits, amount_total)
		SELECT s.d, s.company_id, s.account_id, s.n, s.total FROM (
		  SELECT CAST(created_at AS DATE) AS d, company_id, account_id, COUNT_BIG(*) AS n, SUM(amount) AS total
		    FROM balance_history
		   WHERE company_id = @p1 AND type IN ('commit', 'invoice_item')
		     AND created_at >= @p2 AND created_at < DATEADD(DAY, 1, @p2)
		   GROUP BY CAST(created_at AS DATE), company_id, account_id) s
		 WHERE NOT EXISTS (SELECT 1 FROM balance_history_rollup r WITH (UPDLOCK, HOLDLOCK)
		                    WHERE r.[day] = s.d AND r.company_id = s.company_id AND r.account_id = s.account_id)`, false)
	stRetRollupDrop = RetentionStmts.Add("retention.rollup_drop", `
		DELETE FROM balance_history
		WHERE company_id = @p1 AND type = 'commit'
		  AND created_at >= @p2 AND created_at < DATEADD(DAY, 1, @p2)`, false)
	stRetServer = RetentionStmts.Add("server.identity", `SELECT TOP (1) CONCAT(CONVERT(NVARCHAR(128), SERVERPROPERTY('ServerName')),
		N'|', CONVERT(NVARCHAR(128), SERVERPROPERTY('MachineName')))`, false)
)

// retentionUpkeep is retention's upkeep on the config store's pool.
type retentionUpkeep struct{ db *sql.DB }

var _ dal.RetentionUpkeep = (*retentionUpkeep)(nil)

// NewRetentionUpkeep is retention's upkeep on a config store pool (nil db =
// nil).
func NewRetentionUpkeep(db *sql.DB) dal.RetentionUpkeep {
	if db == nil {
		return nil
	}
	return &retentionUpkeep{db: db}
}

// CompanyHotDays hands each company's days and months to each, a page of
// companies at a time. Like MySQL's, it does not ask whether a read was cut
// short.
func (c *retentionUpkeep) CompanyHotDays(ctx context.Context, each func(company int64, days, months int, err error)) error {
	var after int64
	for {
		rows, err := c.db.QueryContext(ctx, stRetHotDays.SQL, retentionPage, after)
		if err != nil {
			return err
		}
		n := 0
		for rows.Next() {
			var id int64
			var d, m int
			err := rows.Scan(&id, &d, &m)
			each(id, d, m, err)
			if id > after {
				after = id
			}
			n++
		}
		rows.Close()
		if n < retentionPage {
			return nil
		}
	}
}

func (c *retentionUpkeep) PruneUsageBatches(ctx context.Context, keepDays int) (int64, error) {
	res, err := c.db.ExecContext(ctx, stRetPruneBatches.SQL, keepDays)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// LedgerPartitions says the month named "pYYYYMM" has its place: on one
// table it always has (1).
func (c *retentionUpkeep) LedgerPartitions(ctx context.Context, name string) (int, error) {
	return 1, nil
}

// AddLedgerPartition has nothing to do: balance_history is one table
// (LedgerPartitions never asks for it).
func (c *retentionUpkeep) AddLedgerPartition(ctx context.Context, name, next string) error {
	return nil
}

func (c *retentionUpkeep) PaidPeriods(ctx context.Context) ([]dal.PaidPeriod, error) {
	rows, err := c.db.QueryContext(ctx, stRetPaidPeriods.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var eligible []dal.PaidPeriod
	for rows.Next() {
		var pd dal.PaidPeriod
		if rows.Scan(&pd.Company, &pd.From, &pd.To) == nil {
			eligible = append(eligible, pd)
		}
	}
	return eligible, nil
}

func (c *retentionUpkeep) CommitDays(ctx context.Context, company int64, from, to time.Time) ([]string, error) {
	rows, err := c.db.QueryContext(ctx, stRetCommitDays.SQL, company, civil.DateOf(from), civil.DateOf(to))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var d time.Time
		if rows.Scan(&d) == nil {
			out = append(out, d.Format("2006-01-02"))
		}
	}
	return out, rows.Err()
}

// RollupDay adds the day's totals and removes its commit rows in one
// transaction.
func (c *retentionUpkeep) RollupDay(ctx context.Context, company int64, day string) (int64, error) {
	d, err := civil.ParseDate(day)
	if err != nil {
		return 0, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, stRetRollup.SQL, company, d); err != nil {
		return 0, err
	}
	res, err := tx.ExecContext(ctx, stRetRollupDrop.SQL, company, d)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// serverIdentity is one pool, asked which server it is on.
type serverIdentity struct{ db *sql.DB }

func (s serverIdentity) ServerUUID(ctx context.Context) (string, error) {
	var id string
	err := s.db.QueryRowContext(ctx, stRetServer.SQL).Scan(&id)
	return id, err
}

// NewServerIdentity asks db which server it is on (nil db = nil).
func NewServerIdentity(db *sql.DB) dal.ServerIdentity {
	if db == nil {
		return nil
	}
	return serverIdentity{db: db}
}

// ServerOnly is a SQL Server DSN without its database: the server it names,
// reached on the login's own database. "" for an empty DSN, or one that is
// not of the sqlserver:// form.
func ServerOnly(dsn string) string {
	u, err := url.Parse(strings.TrimSpace(dsn))
	if dsn == "" || err != nil || !strings.EqualFold(u.Scheme, "sqlserver") {
		return ""
	}
	q := u.Query()
	for k := range q {
		if strings.EqualFold(k, "database") {
			q.Del(k)
		}
	}
	u.RawQuery = q.Encode()
	u.Path = ""
	return u.String()
}

// openRetention is retention's opener, on the process's pools: the config
// store's (the upkeep), and, when the cold DSN names a server, that server's
// and the hot store's. A pool that cannot be opened leaves its part nil, as
// on MySQL.
func openRetention(cfg config.Config) (*dal.Retention, error) {
	r := &dal.Retention{}
	if srv := ServerOnly(cfg.ColdDSN); srv != "" {
		cold, _ := Get(srv)
		hot, _ := Get(cfg.HotDSN)
		r.ColdServer, r.HotServer = NewServerIdentity(cold), NewServerIdentity(hot)
	}
	if c, err := Get(cfg.ConfigDSN); err == nil {
		r.Upkeep = NewRetentionUpkeep(c)
	}
	return r, nil
}

var _ = serves(func(d *dal.Driver) { d.Retention = openRetention })
