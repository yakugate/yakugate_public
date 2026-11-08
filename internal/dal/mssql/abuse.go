package mssql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/yakugate/yakugate/internal/billingstore"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// The abuse monitor's reads and writes (dal/abuse.go) on SQL Server, made
// like the MySQL DAL's internal/dal/mysql/abuse.go: the rules and overrides,
// the facts of accounts and companies, the audit rows and the one
// transaction that acts, the reputation's inputs and scores, and the windows
// of messages read from the day tables (on the hot store's read instance
// when it has one).
//
// What differs, and why:
//   - No index hint on the message windows. MySQL walked a whole day table
//     through idx_concat to save itself a GROUP BY, so every window query
//     said FORCE INDEX (idx_drill) and asked again without it on 1176 (a
//     table made before the key). On SQL Server every day table is made from
//     tpl_day_mt, which has idx_drill (accepted_at), so the key is always
//     there; and the optimizer costs the range seek against a scan from the
//     table's statistics instead of trading a sort for an index's order. A
//     forced WITH (INDEX(idx_drill)) would also force a key lookup a row on a
//     baseline of many hours, where a scan is cheaper. So: none, and no
//     retry. If a plan ever reads a whole day table for a few minutes'
//     window, WITH (INDEX(idx_drill)) after the table's name is the drop-in.
//   - Every clock is UTC (SYSUTCDATETIME()): the hot store's sessions are
//     UTC on MySQL too; the config store's NOW() was its session's, and its
//     "config clock" (ReputationResetRow.Config, MonitorActionsSince's cut)
//     is the same UTC text here.
//   - The reads of every account, every company's money and every override
//     go a page at a time by id (no unbounded read).
//   - The role tells a store older than a table or column by MySQL's error
//     numbers in the text (1146, 1054). SQL Server's (208 "Invalid object
//     name", 207 "Invalid column name") are answered under those numbers
//     (mysqlCoded), so a month with no rejects table yet is as quiet as on
//     MySQL.
//   - An account's updated_at (ON UPDATE CURRENT_TIMESTAMP on MySQL) is set
//     by the throttle and the suspension when they change the row, as MySQL
//     would.

// AbuseStmts is every statement of the abuse monitor, by name. The ones
// built at run time (a day table's name, a predicate, an IN list, a prefix
// length) are registered in their template form, with fmt verbs where the
// call fills them in, as on MySQL.
var AbuseStmts = newRegistry()

// abusePage is how many rows one read of a whole list takes.
const abusePage = 5000

var _ = serves(func(d *dal.Driver) { d.AbuseData = openAbuseData })

// openAbuseData is the abuse monitor's opener: the process's one pool on the
// hot store and its read instance, and its one pool on the config store.
func openAbuseData(cfg config.Config) (*dal.AbuseData, error) {
	hot, err := Get(cfg.HotDSN)
	if err != nil {
		return nil, err
	}
	rd, _ := Reader(cfg.HotDSN)
	cdb, err := Get(cfg.ConfigDSN)
	if err != nil {
		return nil, err
	}
	return NewAbuseData(cdb, hot, rd), nil
}

// NewAbuseData builds every part on the given pools: cfgDB the config
// store's, hot the hot store's, hotRd its read instance (nil = none). A part
// whose pool is nil is left nil.
func NewAbuseData(cfgDB, hot *sql.DB, hotRd *Read) *dal.AbuseData {
	out := &dal.AbuseData{}
	if cfgDB != nil {
		c := &abuseConfig{db: cfgDB}
		out.Rules, out.Accounts, out.Actions = c, c, c
	}
	if hot != nil {
		out.Windows = NewAbuseWindows(hot, hotRd)
	}
	if cfgDB != nil || hot != nil {
		out.Reputation = &reputationData{cfg: cfgDB, hot: hot, hotRd: hotRd}
	}
	return out
}

var (
	// ---- the rules ----
	stAbuseRules = AbuseStmts.Add("abuse.rules",
		`SELECT signal_name, params FROM abuse_rules WHERE is_disabled = 0`, false)
	stAbuseOverrides = AbuseStmts.Add("abuse.overrides",
		`SELECT TOP (@p1) id, signal_name, account_id, COALESCE(params, N'{}'), is_disabled FROM abuse_rule_overrides
		  WHERE id > @p2 ORDER BY id`, false)

	// ---- accounts, companies, senders, opt-outs ----
	stAbuseCompanies = AbuseStmts.Add("abuse.account_companies",
		`SELECT TOP (@p1) id, COALESCE(company_id, 0) FROM sms_service_config WHERE id > @p2 ORDER BY id`, false)
	stAbuseUnenforced = AbuseStmts.Add("abuse.unenforced_accounts",
		`SELECT id, COALESCE(company_id, 0) FROM sms_service_config WHERE enforce_template_check = 0`, false)
	stAbuseMoney = AbuseStmts.Add("abuse.company_money",
		`SELECT TOP (@p1) a.id, COALESCE(a.company_id, 0), COALESCE(c.currency, ''), COALESCE(c.base_conversion_rate, 0),
		        COALESCE(c.base_thirdparty_crp_id, N'')
		   FROM sms_service_config a LEFT JOIN companies c ON c.id = a.company_id
		  WHERE a.id > @p2 ORDER BY a.id`, false)
	stAbuseProtected = AbuseStmts.Add("abuse.protected_names",
		`SELECT DISTINCT UPPER(TRIM(sender)) FROM sender_ids WHERE body_protected = 1`, false)
	stAbuseApproved = AbuseStmts.Add("abuse.approved_senders",
		`SELECT UPPER(TRIM(sender)), account_id FROM sender_ids
		  WHERE status = 'approved' AND account_id IS NOT NULL`, false)
	stAbuseSenderLists = AbuseStmts.Add("abuse.allowed_sender_lists",
		`SELECT id, allowed_sender_ids FROM sms_service_config WHERE allowed_sender_ids IS NOT NULL`, false)
	stAbuseOptOuts = AbuseStmts.Add("abuse.opt_outs",
		`SELECT account_id, COUNT(*) FROM blocklist
		  WHERE source = 'unsub' AND is_disabled = 0 AND account_id IS NOT NULL
		    AND GREATEST(created_at, COALESCE(updated_at, created_at)) >= DATEADD(MINUTE, -@p1, SYSUTCDATETIME())
		  GROUP BY account_id`, false)

	// ---- acting ----
	stAbuseCooldown       = AbuseStmts.Add("abuse.cooldown", abuseCooldownSQL, false)
	stAbuseCooldownSignal = AbuseStmts.Add("abuse.cooldown_signal",
		abuseCooldownSQL+` AND JSON_VALUE(detail, '$.signal') = @p4`, false)
	stAbuseTarget = AbuseStmts.Add("abuse.target",
		`SELECT a.username, COALESCE(a.deployments, N''), a.abuse_throttle_tps, a.is_disabled,
		        COALESCE(a.company_id, 0), COALESCE(c.name, N'')
		   FROM sms_service_config a LEFT JOIN companies c ON c.id = a.company_id WHERE a.id = @p1`, false)
	stAbuseAuditRow = AbuseStmts.Add("abuse.audit_row",
		`INSERT INTO audit_log (actor, account_id, company_id, company_name, action, entity, entity_id, target_label, detail)
		 VALUES (N'abuse-monitor', @p1, @p2, @p3, @p4, N'account', @p5, @p6, @p7)`, false)
	// updated_at moves only when the row changes, as MySQL's ON UPDATE does.
	stAbuseThrottle = AbuseStmts.Add("abuse.throttle",
		`UPDATE sms_service_config SET abuse_throttle_tps = @p1,
		        updated_at = CASE WHEN abuse_throttle_tps = @p1 THEN updated_at ELSE SYSUTCDATETIME() END
		  WHERE id = @p2`, false)
	stAbuseSuspend = AbuseStmts.Add("abuse.suspend",
		`UPDATE sms_service_config SET is_disabled = 1,
		        updated_at = CASE WHEN is_disabled = 1 THEN updated_at ELSE SYSUTCDATETIME() END
		  WHERE id = @p1`, false)

	// ---- reputation ----
	// Both clocks are UTC here.
	stRepResets = AbuseStmts.Add("reputation.resets", `SELECT account_id, CONVERT(VARCHAR(19), reset_at, 120),
		       CONVERT(VARCHAR(19), reset_at, 120)
		  FROM account_reputation
		 WHERE reset_at IS NOT NULL AND reset_at >= DATEADD(DAY, -90, SYSUTCDATETIME())`, false)
	stRepClock = AbuseStmts.Add("reputation.clock",
		`SELECT TOP (1) CONVERT(VARCHAR(19), SYSUTCDATETIME(), 120)`, false)
	// MySQL's TIMESTAMPDIFF(HOUR, …) DIV 24: whole days.
	stRepPending = AbuseStmts.Add("reputation.pending", `SELECT r.account_id,
		       GREATEST(DATEDIFF_BIG(SECOND, a.created_at, SYSUTCDATETIME()) / 86400, 0)
		  FROM account_reputation r JOIN sms_service_config a ON a.id = r.account_id
		 WHERE r.reset_at IS NOT NULL AND r.reset_at >= r.computed_at`, false)
	// The reset columns are left as they are.
	stRepWrite = AddUpsert(AbuseStmts, "reputation.write_score", UpsertSpec{
		Table: "account_reputation", Keys: []string{"account_id"},
		Set: []string{"score = $score", "reasons = $reasons", "inputs = $inputs", "computed_at = $computed_at"},
	}, false)
	stRepLast = AbuseStmts.Add("reputation.last_computed",
		`SELECT MAX(computed_at) FROM account_reputation`, false)
	stRepAccounts = AbuseStmts.Add("reputation.accounts",
		`SELECT TOP (@p1) id, created_at FROM sms_service_config WHERE id > @p2 ORDER BY id`, false)
	stRepActions = AbuseStmts.Add("reputation.monitor_actions", `SELECT account_id, action, COUNT(*) FROM audit_log
		  WHERE actor = N'abuse-monitor' AND created_at >= DATEADD(DAY, -90, SYSUTCDATETIME()) AND account_id IS NOT NULL
		  GROUP BY account_id, action`, false)
	stRepActionsSince = AbuseStmts.Add("reputation.monitor_actions_since", `SELECT action, COUNT(*) FROM audit_log
			  WHERE actor = N'abuse-monitor' AND account_id = @p1
			    AND created_at >= DATEADD(DAY, -90, SYSUTCDATETIME()) AND created_at >= @p2
			  GROUP BY action`, false)
	stRepDelivery = AbuseStmts.Add("reputation.delivery", `SELECT account_id, COALESCE(SUM(delivered_parts), 0),
		       COALESCE(SUM(delivered_parts + undeliv_parts + expired_parts), 0), COALESCE(SUM(submitted_parts), 0)
		  FROM summary_mt WHERE bucket_start >= DATEADD(DAY, -30, SYSUTCDATETIME()) GROUP BY account_id`, false)
	stRepDeliverySince = AbuseStmts.Add("reputation.delivery_since", `SELECT COALESCE(SUM(delivered_parts), 0),
			       COALESCE(SUM(delivered_parts + undeliv_parts + expired_parts), 0), COALESCE(SUM(submitted_parts), 0)
			  FROM summary_mt WHERE account_id = @p1 AND bucket_start >= DATEADD(DAY, -30, SYSUTCDATETIME()) AND bucket_start >= @p2`, false)
	stRepLookups = AbuseStmts.Add("reputation.lookups", `SELECT account_id, COALESCE(SUM(found + not_found), 0), COALESCE(SUM(not_found), 0)
		  FROM summary_lookup WHERE bucket_start >= DATEADD(DAY, -30, SYSUTCDATETIME()) GROUP BY account_id`, false)
	stRepLookupsSince = AbuseStmts.Add("reputation.lookups_since", `SELECT COALESCE(SUM(found + not_found), 0), COALESCE(SUM(not_found), 0)
			  FROM summary_lookup WHERE account_id = @p1 AND bucket_start >= DATEADD(DAY, -30, SYSUTCDATETIME()) AND bucket_start >= @p2`, false)
	stRepOptOuts = AbuseStmts.Add("reputation.opt_outs", `SELECT account_id, COUNT(*) FROM blocklist
		  WHERE source = 'unsub' AND account_id IS NOT NULL AND created_at >= DATEADD(DAY, -30, SYSUTCDATETIME())
		  GROUP BY account_id`, false)
	stRepOptOutsSince = AbuseStmts.Add("reputation.opt_outs_since", `SELECT COUNT(*) FROM blocklist
			  WHERE source = 'unsub' AND account_id = @p1 AND created_at >= DATEADD(DAY, -30, SYSUTCDATETIME()) AND created_at >= @p2`, false)
	// A rejects table of the last 31 days (rejects_<company>_<YYYYMMDD>): %s.
	stRepRefusals = AbuseStmts.Add("reputation.refusals", `SELECT account_id, COALESCE(SUM(GREATEST(parts, 1)), 0) FROM %s
			  WHERE rejected_at >= DATEADD(DAY, -30, SYSUTCDATETIME())
			    AND reason_code IN (N'SPAM_KEYWORD', N'DESTINATION_BLOCKED')
			  GROUP BY account_id`, false)
	stRepRefusalsSince = AbuseStmts.Add("reputation.refusals_since", `SELECT COALESCE(SUM(GREATEST(parts, 1)), 0) FROM %s
				  WHERE account_id = @p1 AND rejected_at >= DATEADD(DAY, -30, SYSUTCDATETIME()) AND rejected_at >= @p2
				    AND reason_code IN (N'SPAM_KEYWORD', N'DESTINATION_BLOCKED')`, false)

	// ---- the message windows ----
	// In each: the table, then mtWhere's predicate; the minutes are
	// parameters.
	stWinCounts = AbuseStmts.Add("window.counts", `SELECT account_id, COUNT(*), COUNT(DISTINCT destination) FROM %s
			 WHERE %s AND accepted_at >= DATEADD(MINUTE, -@p1, SYSUTCDATETIME())
			 GROUP BY account_id`, false)
	// The prefix length, the table, the predicate, the length.
	stWinPrefixes = AbuseStmts.Add("window.prefixes", `SELECT account_id, LEFT(destination, %d), COUNT(*) FROM %s
			 WHERE %s AND accepted_at >= DATEADD(MINUTE, -@p1, SYSUTCDATETIME())
			 GROUP BY account_id, LEFT(destination, %d)`, false)
	// One table's part of the count over several, joined by " UNION ALL "
	// into window.distinct (the account @p1, the minutes @p2).
	stWinDistinctPart = AbuseStmts.Add("window.distinct_part",
		`SELECT destination FROM %s WHERE %s AND account_id = @p1 AND accepted_at >= DATEADD(MINUTE, -@p2, SYSUTCDATETIME())`, false)
	stWinDistinct = AbuseStmts.Add("window.distinct",
		"SELECT COUNT(DISTINCT destination) FROM (%s) w", false)
	// spendCols, the table, the predicate, spendGroup.
	stWinSpend = AbuseStmts.Add("window.spend", `SELECT %s FROM %s
			 WHERE %s AND accepted_at >= DATEADD(MINUTE, -@p1, SYSUTCDATETIME())
			 GROUP BY %s`, false)
	stWinSpendBase = AbuseStmts.Add("window.spend_baseline", `SELECT %s FROM %s
			 WHERE %s
			   AND accepted_at <  DATEADD(MINUTE, -@p1, SYSUTCDATETIME())
			   AND accepted_at >= DATEADD(MINUTE, -@p2, SYSUTCDATETIME())
			 GROUP BY %s`, false)
	stWinMessages = AbuseStmts.Add("window.messages", `SELECT account_id, COUNT(*) FROM %s
			 WHERE %s AND accepted_at >= DATEADD(MINUTE, -@p1, SYSUTCDATETIME())
			 GROUP BY account_id`, false)
	stWinMessagesBase = AbuseStmts.Add("window.messages_baseline", `SELECT account_id, COUNT(*) FROM %s
			 WHERE %s
			   AND accepted_at <  DATEADD(MINUTE, -@p1, SYSUTCDATETIME())
			   AND accepted_at >= DATEADD(MINUTE, -@p2, SYSUTCDATETIME())
			 GROUP BY account_id`, false)
	stWinLookups = AbuseStmts.Add("window.lookups", `SELECT account_id,
			        SUM(CASE WHEN hlr_source IN (N'miss', N'dip_miss') THEN 1 ELSE 0 END),
			        COUNT(*)
			   FROM %s
			  WHERE %s
			    AND accepted_at >= DATEADD(MINUTE, -@p1, SYSUTCDATETIME())
			    AND hlr_source IN (N'db', N'dip', N'miss', N'dip_miss')
			  GROUP BY account_id`, false)
	// The table, the predicate, then encWindowPred or encBasePred.
	stWinEncodings = AbuseStmts.Add("window.encodings", `SELECT account_id, COALESCE(encoding, ''), COUNT(*) FROM %s
				  WHERE %s AND %s GROUP BY account_id, encoding`, false)
	// The table, the predicate, the account ids.
	stWinSent = AbuseStmts.Add("window.sent", `SELECT account_id, COUNT(*) FROM %s
			  WHERE %s AND accepted_at >= DATEADD(MINUTE, -@p1, SYSUTCDATETIME()) AND account_id IN (%s)
			  GROUP BY account_id`, false)
	// The table, the predicate, samplePred; each account's rows @p3, the
	// limit @p4.
	stWinSample = AbuseStmts.Add("window.sample", `SELECT TOP (@p4) account_id, body, destination, sender FROM (
			   SELECT account_id, COALESCE(body, N'') AS body, COALESCE(destination, N'') AS destination, COALESCE(sender, N'') AS sender,
			          ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY accepted_at DESC) AS rn
			     FROM %s WHERE %s AND %s) x
			  WHERE rn <= @p3`, false)
)

// abuseCooldownSQL is the cooldown read; a warning's adds its rule.
const abuseCooldownSQL = `SELECT COUNT(*) FROM audit_log WHERE account_id = @p1 AND action = @p2
	AND created_at >= DATEADD(MINUTE, -@p3, SYSUTCDATETIME())`

// spendCols / spendGroup are an account's spend as a log row keeps it: the
// rate version that priced it and the conversion rate (issue 16).
const spendCols = "account_id, COALESCE(sell_rate_id, 0), COALESCE(fx_rate, 1), SUM(COALESCE(segments, 1))"

// spendGroup is spendCols' GROUP BY.
const spendGroup = "account_id, sell_rate_id, fx_rate"

// The encoding read's window and baseline (window.encodings).
const (
	encWindowPred = "accepted_at >= DATEADD(MINUTE, -@p1, SYSUTCDATETIME())"
	encBasePred   = "accepted_at < DATEADD(MINUTE, -@p1, SYSUTCDATETIME()) AND accepted_at >= DATEADD(MINUTE, -@p2, SYSUTCDATETIME())"
)

// mysqlCodedErr is a SQL Server error answered under the MySQL number the
// abuse monitor looks for in an error's text.
type mysqlCodedErr struct {
	code int
	err  error
}

func (e *mysqlCodedErr) Error() string { return fmt.Sprintf("Error %d: %v", e.code, e.err) }
func (e *mysqlCodedErr) Unwrap() error { return e.err }

// mysqlCoded answers a missing table (208) as MySQL's 1146 and a missing
// column (207) as 1054; any other error as it is.
func mysqlCoded(err error) error {
	n, ok := ErrorNumber(err)
	switch {
	case !ok:
		return err
	case n == 208:
		return &mysqlCodedErr{code: 1146, err: err}
	case n == 207:
		return &mysqlCodedErr{code: 1054, err: err}
	}
	return err
}

// abusePageByID reads a whole list a page at a time: q takes the page size (@p1)
// and the last id read (@p2), and its first column is the id. row scans one
// row and answers its id (also when the rest of the row failed to scan:
// the id is read first). As the readers it mirrors, a read cut short is
// not reported.
func abusePageByID(ctx context.Context, db *sql.DB, q string, row func(rows *sql.Rows) int64) error {
	after := int64(math.MinInt64)
	for {
		rows, err := db.QueryContext(ctx, q, abusePage, after)
		if err != nil {
			return mysqlCoded(err)
		}
		n := 0
		for rows.Next() {
			if id := row(rows); id > after {
				after = id
			}
			n++
		}
		failed := rows.Err() != nil
		rows.Close()
		if n < abusePage || failed {
			return nil
		}
	}
}

// ---- the config store's parts ----

type abuseConfig struct{ db *sql.DB }

var (
	_ dal.AbuseRules    = (*abuseConfig)(nil)
	_ dal.AbuseAccounts = (*abuseConfig)(nil)
	_ dal.AbuseActions  = (*abuseConfig)(nil)
)

func (c *abuseConfig) Enabled(ctx context.Context) ([]dal.AbuseRuleRow, error) {
	rows, err := c.db.QueryContext(ctx, stAbuseRules.SQL)
	if err != nil {
		return nil, mysqlCoded(err)
	}
	defer rows.Close()
	var out []dal.AbuseRuleRow
	for rows.Next() {
		var r dal.AbuseRuleRow
		var params string
		if rows.Scan(&r.Signal, &params) == nil {
			r.Params = []byte(params)
			out = append(out, r)
		}
	}
	return out, nil
}

func (c *abuseConfig) Overrides(ctx context.Context) ([]dal.AbuseOverrideRow, error) {
	var out []dal.AbuseOverrideRow
	err := abusePageByID(ctx, c.db, stAbuseOverrides.SQL, func(rows *sql.Rows) int64 {
		var id int64
		var r dal.AbuseOverrideRow
		var params string
		if rows.Scan(&id, &r.Signal, &r.Account, &params, &r.Disabled) == nil {
			r.Params = []byte(params)
			out = append(out, r)
		}
		return id
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *abuseConfig) Companies(ctx context.Context) ([]dal.AccountCompany, error) {
	var out []dal.AccountCompany
	err := abusePageByID(ctx, c.db, stAbuseCompanies.SQL, func(rows *sql.Rows) int64 {
		var r dal.AccountCompany
		if rows.Scan(&r.Account, &r.Company) == nil {
			out = append(out, r)
		}
		return r.Account
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *abuseConfig) Unenforced(ctx context.Context) ([]dal.AccountCompany, error) {
	rows, err := c.db.QueryContext(ctx, stAbuseUnenforced.SQL)
	if err != nil {
		return nil, mysqlCoded(err)
	}
	defer rows.Close()
	var out []dal.AccountCompany
	for rows.Next() {
		var r dal.AccountCompany
		if rows.Scan(&r.Account, &r.Company) == nil {
			out = append(out, r)
		}
	}
	return out, nil
}

func (c *abuseConfig) Money(ctx context.Context) ([]dal.AccountMoney, error) {
	var out []dal.AccountMoney
	err := abusePageByID(ctx, c.db, stAbuseMoney.SQL, func(rows *sql.Rows) int64 {
		var r dal.AccountMoney
		if rows.Scan(&r.Account, &r.Company, &r.Currency, &r.BaseRate, &r.RateProvider) == nil {
			out = append(out, r)
		}
		return r.Account
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *abuseConfig) ProtectedNames(ctx context.Context) ([]string, error) {
	rows, err := c.db.QueryContext(ctx, stAbuseProtected.SQL)
	if err != nil {
		return nil, mysqlCoded(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if rows.Scan(&n) == nil {
			out = append(out, n)
		}
	}
	return out, nil
}

func (c *abuseConfig) ApprovedSenders(ctx context.Context) ([]dal.SenderAccount, error) {
	rows, err := c.db.QueryContext(ctx, stAbuseApproved.SQL)
	if err != nil {
		return nil, mysqlCoded(err)
	}
	defer rows.Close()
	var out []dal.SenderAccount
	for rows.Next() {
		var r dal.SenderAccount
		if rows.Scan(&r.Sender, &r.Account) == nil {
			out = append(out, r)
		}
	}
	return out, nil
}

func (c *abuseConfig) AllowedSenderLists(ctx context.Context) ([]dal.AccountSenderList, error) {
	rows, err := c.db.QueryContext(ctx, stAbuseSenderLists.SQL)
	if err != nil {
		return nil, mysqlCoded(err)
	}
	defer rows.Close()
	var out []dal.AccountSenderList
	for rows.Next() {
		var r dal.AccountSenderList
		var list string
		if rows.Scan(&r.Account, &list) == nil {
			r.List = []byte(list)
			out = append(out, r)
		}
	}
	return out, nil
}

func (c *abuseConfig) OptOuts(ctx context.Context, windowMin int) ([]dal.AccountCount, error) {
	rows, err := c.db.QueryContext(ctx, stAbuseOptOuts.SQL, windowMin)
	if err != nil {
		return nil, mysqlCoded(err)
	}
	defer rows.Close()
	var out []dal.AccountCount
	for rows.Next() {
		var r dal.AccountCount
		if rows.Scan(&r.Account, &r.N) == nil {
			out = append(out, r)
		}
	}
	return out, nil
}

func (c *abuseConfig) Recent(ctx context.Context, account int64, action string, minutes int, bySignal bool, signal string) (int, error) {
	q := stAbuseCooldown.SQL
	args := []any{account, action, minutes}
	if bySignal {
		q = stAbuseCooldownSignal.SQL
		args = append(args, signal)
	}
	var n int
	if err := c.db.QueryRowContext(ctx, q, args...).Scan(&n); err != nil {
		return 0, mysqlCoded(err)
	}
	return n, nil
}

func (c *abuseConfig) Target(ctx context.Context, account int64) (dal.AbuseTarget, error) {
	var t dal.AbuseTarget
	var already sql.NullInt64
	if err := c.db.QueryRowContext(ctx, stAbuseTarget.SQL, account).
		Scan(&t.Username, &t.Deployments, &already, &t.Disabled, &t.CompanyID, &t.CompanyName); err != nil {
		return dal.AbuseTarget{}, err
	}
	t.ThrottleTPS = dal.NullInt64{Int64: already.Int64, Valid: already.Valid}
	return t, nil
}

// abuseNullID / abuseNullStr store 0 / "" as NULL.
func abuseNullID(id int64) any {
	if id == 0 {
		return nil
	}
	return id
}

func abuseNullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// Act writes the audit row and changes the account in one transaction, the
// row first: both are there or neither.
func (c *abuseConfig) Act(ctx context.Context, a dal.AbuseAct) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, stAbuseAuditRow.SQL,
		a.Account, abuseNullID(a.CompanyID), abuseNullStr(a.CompanyName), a.Action, a.EntityID, a.Label, a.Detail); err != nil {
		_ = tx.Rollback()
		return &dal.AuditRowError{Err: err}
	}
	var change string
	var changeArgs []any
	switch a.Change {
	case dal.ChangeThrottle:
		change, changeArgs = stAbuseThrottle.SQL, []any{a.ThrottleTPS, a.Account}
	case dal.ChangeSuspend:
		change, changeArgs = stAbuseSuspend.SQL, []any{a.Account}
	}
	if change != "" {
		if _, err := tx.ExecContext(ctx, change, changeArgs...); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

// ---- reputation ----

type reputationData struct {
	cfg, hot *sql.DB
	hotRd    *Read
}

var _ dal.ReputationData = (*reputationData)(nil)

// read is the pool the hot store's summaries are read from.
func (r *reputationData) read() *sql.DB { return abuseHotReader(r.hot, r.hotRd) }

// abuseHotReader is the read instance when one is set and answering, else
// the hot store itself.
func abuseHotReader(hot *sql.DB, rd *Read) *sql.DB {
	if rd != nil {
		if d := rd.DB(); d != nil {
			return d
		}
	}
	return hot
}

func (r *reputationData) Resets(ctx context.Context) ([]dal.ReputationResetRow, error) {
	rows, err := r.cfg.QueryContext(ctx, stRepResets.SQL)
	if err != nil {
		return nil, mysqlCoded(err)
	}
	defer rows.Close()
	var out []dal.ReputationResetRow
	for rows.Next() {
		var x dal.ReputationResetRow
		if rows.Scan(&x.Account, &x.UTC, &x.Config) == nil {
			out = append(out, x)
		}
	}
	return out, nil
}

func (r *reputationData) Clock(ctx context.Context) (dal.NullString, error) {
	var s sql.NullString
	if err := r.cfg.QueryRowContext(ctx, stRepClock.SQL).Scan(&s); err != nil {
		return dal.NullString{}, err
	}
	return dal.NullString{String: s.String, Valid: s.Valid}, nil
}

func (r *reputationData) Pending(ctx context.Context) ([]dal.AccountAge, error) {
	rows, err := r.cfg.QueryContext(ctx, stRepPending.SQL)
	if err != nil {
		return nil, mysqlCoded(err)
	}
	defer rows.Close()
	var out []dal.AccountAge
	for rows.Next() {
		var id int64
		var age sql.NullInt64
		if rows.Scan(&id, &age) == nil {
			out = append(out, dal.AccountAge{Account: id, AgeDays: dal.NullInt64{Int64: age.Int64, Valid: age.Valid}})
		}
	}
	return out, nil
}

func (r *reputationData) WriteScore(ctx context.Context, account int64, score int, reasons, inputs, stamp string) error {
	_, err := stRepWrite.Exec(ctx, r.cfg, sql.Named("account_id", account), sql.Named("score", score),
		sql.Named("reasons", reasons), sql.Named("inputs", inputs), sql.Named("computed_at", stamp))
	return mysqlCoded(err)
}

func (r *reputationData) LastComputed(ctx context.Context) (dal.NullTime, error) {
	var last sql.NullTime
	if err := r.cfg.QueryRowContext(ctx, stRepLast.SQL).Scan(&last); err != nil {
		return dal.NullTime{}, mysqlCoded(err)
	}
	return dal.NullTime{Time: last.Time, Valid: last.Valid}, nil
}

func (r *reputationData) Accounts(ctx context.Context) ([]dal.AccountCreated, error) {
	var out []dal.AccountCreated
	err := abusePageByID(ctx, r.cfg, stRepAccounts.SQL, func(rows *sql.Rows) int64 {
		var id int64
		var created sql.NullTime
		if rows.Scan(&id, &created) == nil {
			out = append(out, dal.AccountCreated{Account: id, Created: dal.NullTime{Time: created.Time, Valid: created.Valid}})
		}
		return id
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (r *reputationData) MonitorActions(ctx context.Context) ([]dal.ActionCount, error) {
	rows, err := r.cfg.QueryContext(ctx, stRepActions.SQL)
	if err != nil {
		return nil, mysqlCoded(err)
	}
	defer rows.Close()
	var out []dal.ActionCount
	for rows.Next() {
		var x dal.ActionCount
		if rows.Scan(&x.Account, &x.Action, &x.N) == nil {
			out = append(out, x)
		}
	}
	return out, nil
}

func (r *reputationData) MonitorActionsSince(ctx context.Context, account int64, cutConfig string) ([]dal.ActionCount, error) {
	rows, err := r.cfg.QueryContext(ctx, stRepActionsSince.SQL, account, cutConfig)
	if err != nil {
		return nil, mysqlCoded(err)
	}
	defer rows.Close()
	var out []dal.ActionCount
	for rows.Next() {
		x := dal.ActionCount{Account: account}
		if rows.Scan(&x.Action, &x.N) == nil {
			out = append(out, x)
		}
	}
	return out, nil
}

func (r *reputationData) Delivery(ctx context.Context) (map[int64][]int, error) {
	return abuseSums(ctx, r.read(), stRepDelivery.SQL, 3)
}

func (r *reputationData) DeliverySince(ctx context.Context, account int64, cutUTC string) (delivered, final, sent int, err error) {
	err = mysqlCoded(r.read().QueryRowContext(ctx, stRepDeliverySince.SQL, account, cutUTC).Scan(&delivered, &final, &sent))
	return
}

func (r *reputationData) Lookups(ctx context.Context) (map[int64][]int, error) {
	return abuseSums(ctx, r.read(), stRepLookups.SQL, 2)
}

func (r *reputationData) LookupsSince(ctx context.Context, account int64, cutUTC string) (lookedUp, unknown int, err error) {
	err = mysqlCoded(r.read().QueryRowContext(ctx, stRepLookupsSince.SQL, account, cutUTC).Scan(&lookedUp, &unknown))
	return
}

func (r *reputationData) OptOuts(ctx context.Context) (map[int64][]int, error) {
	return abuseSums(ctx, r.cfg, stRepOptOuts.SQL, 1)
}

func (r *reputationData) OptOutsSince(ctx context.Context, account int64, cutConfig string) (int, error) {
	var n int
	err := r.cfg.QueryRowContext(ctx, stRepOptOutsSince.SQL, account, cutConfig).Scan(&n)
	return n, mysqlCoded(err)
}

func (r *reputationData) Refusals(ctx context.Context, table string) (map[int64][]int, error) {
	return abuseSums(ctx, r.read(), fmt.Sprintf(stRepRefusals.SQL, Ident(table)), 1)
}

func (r *reputationData) RefusalsSince(ctx context.Context, table string, account int64, cutUTC string) (int, error) {
	var n int
	err := r.read().QueryRowContext(ctx, fmt.Sprintf(stRepRefusalsSince.SQL, Ident(table)), account, cutUTC).Scan(&n)
	return n, mysqlCoded(err)
}

// abuseSums runs a query whose first column is an account id and whose
// other n columns are counts. A row that cannot be read is left out.
func abuseSums(ctx context.Context, db *sql.DB, q string, n int) (map[int64][]int, error) {
	out := map[int64][]int{}
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return out, mysqlCoded(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		vals := make([]int, n)
		dst := make([]any, n+1)
		dst[0] = &id
		for i := range vals {
			dst[i+1] = &vals[i]
		}
		if rows.Scan(dst...) == nil {
			out[id] = vals
		}
	}
	return out, nil
}

// ---- the message windows ----

// AbuseWindows reads the windows of messages from the day tables, on the
// hot store's read instance when it has one.
type AbuseWindows struct {
	hot *sql.DB
	rd  *Read
}

var _ dal.MessageWindows = (*AbuseWindows)(nil)

// NewAbuseWindows reads from hot, or from rd's read instance when one is set
// and answering.
func NewAbuseWindows(hot *sql.DB, rd *Read) *AbuseWindows {
	return &AbuseWindows{hot: hot, rd: rd}
}

func (w *AbuseWindows) read() *sql.DB { return abuseHotReader(w.hot, w.rd) }

// abuseMTWhere is the family-correct "this row is a countable MT accept"
// predicate: the legacy records_* tables mix directions and statuses; an mt_
// table is the MT family.
func abuseMTWhere(table string) string {
	if billingstore.TableFamily(table) == billingstore.KindMT {
		return "1=1"
	}
	return "direction = 'MT' AND status <> 'rejected'"
}

func (w *AbuseWindows) query(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	rows, err := w.read().QueryContext(ctx, q, args...)
	return rows, mysqlCoded(err)
}

func (w *AbuseWindows) Counts(ctx context.Context, t string, windowMin int) ([]dal.WindowCount, error) {
	rows, err := w.query(ctx, fmt.Sprintf(stWinCounts.SQL, Ident(t), abuseMTWhere(t)), windowMin)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.WindowCount
	for rows.Next() {
		var r dal.WindowCount
		if rows.Scan(&r.Account, &r.Msgs, &r.Uniq) == nil {
			out = append(out, r)
		}
	}
	return out, nil
}

func (w *AbuseWindows) Prefixes(ctx context.Context, t string, prefixLen, windowMin int) ([]dal.PrefixCount, error) {
	rows, err := w.query(ctx, fmt.Sprintf(stWinPrefixes.SQL, prefixLen, Ident(t), abuseMTWhere(t), prefixLen), windowMin)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.PrefixCount
	for rows.Next() {
		var r dal.PrefixCount
		if rows.Scan(&r.Account, &r.Prefix, &r.Msgs) == nil {
			out = append(out, r)
		}
	}
	return out, nil
}

func (w *AbuseWindows) Distinct(ctx context.Context, tables []string, account int64, windowMin int) (int, error) {
	if len(tables) == 0 {
		return 0, errors.New("abuse windows: a distinct count over no tables")
	}
	parts := make([]string, len(tables))
	for i, t := range tables {
		parts[i] = fmt.Sprintf(stWinDistinctPart.SQL, Ident(t), abuseMTWhere(t))
	}
	rows, err := w.query(ctx, fmt.Sprintf(stWinDistinct.SQL, strings.Join(parts, " UNION ALL ")), account, windowMin)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, dal.ErrNoRows
	}
	var u int
	if err := rows.Scan(&u); err != nil {
		return 0, err
	}
	return u, nil
}

func (w *AbuseWindows) Spend(ctx context.Context, t string, windowMin, sinceMin int) ([]dal.SpendRow, error) {
	q := fmt.Sprintf(stWinSpend.SQL, spendCols, Ident(t), abuseMTWhere(t), spendGroup)
	args := []any{windowMin}
	if sinceMin > 0 {
		q = fmt.Sprintf(stWinSpendBase.SQL, spendCols, Ident(t), abuseMTWhere(t), spendGroup)
		args = append(args, sinceMin)
	}
	rows, err := w.query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.SpendRow
	for rows.Next() {
		var r dal.SpendRow
		if rows.Scan(&r.Account, &r.Rate, &r.FX, &r.Parts) == nil {
			out = append(out, r)
		}
	}
	return out, nil
}

func (w *AbuseWindows) Messages(ctx context.Context, t string, windowMin, sinceMin int) ([]dal.AccountCount, error) {
	if sinceMin > 0 {
		return w.accountCounts(ctx, fmt.Sprintf(stWinMessagesBase.SQL, Ident(t), abuseMTWhere(t)), windowMin, sinceMin)
	}
	return w.accountCounts(ctx, fmt.Sprintf(stWinMessages.SQL, Ident(t), abuseMTWhere(t)), windowMin)
}

func (w *AbuseWindows) accountCounts(ctx context.Context, q string, args ...any) ([]dal.AccountCount, error) {
	rows, err := w.query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.AccountCount
	for rows.Next() {
		var r dal.AccountCount
		if rows.Scan(&r.Account, &r.N) == nil {
			out = append(out, r)
		}
	}
	return out, nil
}

func (w *AbuseWindows) Lookups(ctx context.Context, t string, windowMin int) ([]dal.LookupCount, error) {
	rows, err := w.query(ctx, fmt.Sprintf(stWinLookups.SQL, Ident(t), abuseMTWhere(t)), windowMin)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.LookupCount
	for rows.Next() {
		var r dal.LookupCount
		if rows.Scan(&r.Account, &r.Misses, &r.Consulted) == nil {
			out = append(out, r)
		}
	}
	return out, nil
}

func (w *AbuseWindows) Encodings(ctx context.Context, t string, windowMin, sinceMin int) ([]dal.EncodingCount, error) {
	pred, args := encWindowPred, []any{windowMin}
	if sinceMin > 0 {
		pred, args = encBasePred, []any{windowMin, sinceMin}
	}
	rows, err := w.query(ctx, fmt.Sprintf(stWinEncodings.SQL, Ident(t), abuseMTWhere(t), pred), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.EncodingCount
	for rows.Next() {
		var r dal.EncodingCount
		if rows.Scan(&r.Account, &r.Encoding, &r.N) == nil {
			out = append(out, r)
		}
	}
	return out, nil
}

// abuseIDList is account ids as an IN list's text (numbers only).
func abuseIDList(ids []int64) string {
	s := make([]string, 0, len(ids))
	for _, id := range ids {
		s = append(s, fmt.Sprint(id))
	}
	return strings.Join(s, ",")
}

func (w *AbuseWindows) Sent(ctx context.Context, t string, windowMin int, accounts []int64) ([]dal.AccountCount, error) {
	return w.accountCounts(ctx, fmt.Sprintf(stWinSent.SQL, Ident(t), abuseMTWhere(t), abuseIDList(accounts)), windowMin)
}

func (w *AbuseWindows) Sample(ctx context.Context, t string, fromMin, toMin int, accounts []int64, perAcct, limit int) ([]dal.SampledMessage, error) {
	pred := "accepted_at >= DATEADD(MINUTE, -@p1, SYSUTCDATETIME())"
	if toMin > 0 {
		pred += " AND accepted_at < DATEADD(MINUTE, -@p2, SYSUTCDATETIME())"
	}
	if len(accounts) > 0 {
		pred += " AND account_id IN (" + abuseIDList(accounts) + ")"
	}
	// A table is a company's day: the newest of EACH account, so one busy
	// account does not take every place of the sample.
	rows, err := w.query(ctx, fmt.Sprintf(stWinSample.SQL, Ident(t), abuseMTWhere(t), pred), fromMin, toMin, perAcct, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.SampledMessage
	for rows.Next() {
		var m dal.SampledMessage
		if rows.Scan(&m.Account, &m.Body, &m.Destination, &m.Sender) == nil {
			out = append(out, m)
		}
	}
	return out, nil
}
