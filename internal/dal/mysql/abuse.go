package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	mysqldrv "github.com/go-sql-driver/mysql"

	"github.com/yakugate/yakugate/internal/billingstore"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/dal/stmt"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/platform/dbpool"
)

// The abuse monitor's reads and writes (dal/abuse.go) — moved here unchanged
// on 30 Oct 2026 (the DAL plan) from internal/process/abusemonitor
// (workings.go, content.go, overrides.go, reputation.go). Every statement is
// the text the role ran; the ones built at run time (a day table's name, its
// time key, a window's minutes, an IN list) are registered in their template
// form, with fmt verbs where the role filled them in.

// The abuse monitor's data, on the config and hot stores' pools.
var _ = serves(func(d *dal.Driver) { d.AbuseData = openAbuseData })

// openAbuseData is the abuse monitor's opener: the process's one pool on the
// hot store and its read instance, and its one pool on the config store —
// the pools the role used itself.
func openAbuseData(cfg config.Config) (*dal.AbuseData, error) {
	hot, err := dbpool.Get(cfg.HotDSN)
	if err != nil {
		return nil, err
	}
	rd, _ := dbpool.Reader(cfg.HotDSN)
	cdb, err := dbpool.Get(cfg.ConfigDSN)
	if err != nil {
		return nil, err
	}
	return NewAbuseData(cdb, hot, rd), nil
}

// NewAbuseData builds every part on the given pools: cfgDB the config
// store's, hot the hot store's, hotRd its read instance (nil = none: the
// windows and summaries are read from hot). A part whose pool is nil is
// left nil.
func NewAbuseData(cfgDB, hot *sql.DB, hotRd *dbpool.Read) *dal.AbuseData {
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

// AbuseStmts is every statement of the abuse monitor, by name.
var AbuseStmts = stmt.New("mysql")

// AbuseStatements is the registry of these statements (the lint test).
func AbuseStatements() *stmt.Registry { return AbuseStmts }

var (
	// ---- the rules ----
	stAbuseRules = AbuseStmts.Add("abuse.rules",
		`SELECT signal_name, params FROM abuse_rules WHERE is_disabled=0`, false)
	stAbuseOverrides = AbuseStmts.Add("abuse.overrides",
		`SELECT signal_name, account_id, COALESCE(params, JSON_OBJECT()), is_disabled FROM abuse_rule_overrides`, false)

	// ---- accounts, companies, senders, opt-outs ----
	stAbuseCompanies = AbuseStmts.Add("abuse.account_companies",
		`SELECT id, COALESCE(company_id,0) FROM sms_service_config`, false)
	stAbuseUnenforced = AbuseStmts.Add("abuse.unenforced_accounts",
		`SELECT id, COALESCE(company_id,0) FROM sms_service_config WHERE enforce_template_check=0`, false)
	stAbuseMoney = AbuseStmts.Add("abuse.company_money",
		`SELECT a.id, COALESCE(a.company_id,0), COALESCE(c.currency,''), COALESCE(c.base_conversion_rate,0),
		        COALESCE(c.base_thirdparty_crp_id,'')
		   FROM sms_service_config a LEFT JOIN companies c ON c.id = a.company_id`, false)
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
		    AND GREATEST(created_at, COALESCE(updated_at, created_at)) >= DATE_SUB(NOW(), INTERVAL ? MINUTE)
		  GROUP BY account_id`, false)

	// ---- acting ----
	stAbuseCooldown = AbuseStmts.Add("abuse.cooldown",
		abuseCooldownSQL, false)
	stAbuseCooldownSignal = AbuseStmts.Add("abuse.cooldown_signal",
		abuseCooldownSQL+` AND JSON_UNQUOTE(JSON_EXTRACT(detail, '$.signal')) = ?`, false)
	stAbuseTarget = AbuseStmts.Add("abuse.target",
		`SELECT a.username, COALESCE(a.deployments,''), a.abuse_throttle_tps, a.is_disabled,
		        COALESCE(a.company_id,0), COALESCE(c.name,'')
		   FROM sms_service_config a LEFT JOIN companies c ON c.id = a.company_id WHERE a.id=?`, false)
	stAbuseAuditRow = AbuseStmts.Add("abuse.audit_row",
		`INSERT INTO audit_log (actor, account_id, company_id, company_name, action, entity, entity_id, target_label, detail)
		 VALUES ('abuse-monitor', ?, ?, ?, ?, 'account', ?, ?, CAST(? AS JSON))`, false)
	stAbuseThrottle = AbuseStmts.Add("abuse.throttle",
		`UPDATE sms_service_config SET abuse_throttle_tps=? WHERE id=?`, false)
	stAbuseSuspend = AbuseStmts.Add("abuse.suspend",
		`UPDATE sms_service_config SET is_disabled=1 WHERE id=?`, false)

	// ---- reputation ----
	stRepResets = AbuseStmts.Add("reputation.resets", `SELECT account_id, DATE_FORMAT(reset_at, '%Y-%m-%d %H:%i:%s'),
		       DATE_FORMAT(DATE_ADD(reset_at, INTERVAL TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(), NOW()) SECOND), '%Y-%m-%d %H:%i:%s')
		  FROM account_reputation
		 WHERE reset_at IS NOT NULL AND reset_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 90 DAY)`, false)
	stRepClock = AbuseStmts.Add("reputation.clock",
		`SELECT DATE_FORMAT(UTC_TIMESTAMP(), '%Y-%m-%d %H:%i:%s')`, false)
	stRepPending = AbuseStmts.Add("reputation.pending", `SELECT r.account_id, GREATEST(TIMESTAMPDIFF(HOUR, a.created_at, NOW()) DIV 24, 0)
		  FROM account_reputation r JOIN sms_service_config a ON a.id = r.account_id
		 WHERE r.reset_at IS NOT NULL AND r.reset_at >= r.computed_at`, false)
	stRepWrite = AbuseStmts.Add("reputation.write_score", `INSERT INTO account_reputation (account_id, score, reasons, inputs, computed_at)
		VALUES (?, ?, CAST(? AS JSON), CAST(? AS JSON), ?)
		ON DUPLICATE KEY UPDATE score = VALUES(score), reasons = VALUES(reasons), inputs = VALUES(inputs), computed_at = VALUES(computed_at)`, false)
	stRepLast = AbuseStmts.Add("reputation.last_computed",
		`SELECT MAX(computed_at) FROM account_reputation`, false)
	stRepAccounts = AbuseStmts.Add("reputation.accounts",
		`SELECT id, created_at FROM sms_service_config`, false)
	stRepActions = AbuseStmts.Add("reputation.monitor_actions", `SELECT account_id, action, COUNT(*) FROM audit_log
		  WHERE actor = 'abuse-monitor' AND created_at >= DATE_SUB(NOW(), INTERVAL 90 DAY) AND account_id IS NOT NULL
		  GROUP BY account_id, action`, false)
	stRepActionsSince = AbuseStmts.Add("reputation.monitor_actions_since", `SELECT action, COUNT(*) FROM audit_log
			  WHERE actor = 'abuse-monitor' AND account_id = ?
			    AND created_at >= DATE_SUB(NOW(), INTERVAL 90 DAY) AND created_at >= ?
			  GROUP BY action`, false)
	stRepDelivery = AbuseStmts.Add("reputation.delivery", `SELECT account_id, COALESCE(SUM(delivered_parts),0),
		       COALESCE(SUM(delivered_parts + undeliv_parts + expired_parts),0), COALESCE(SUM(submitted_parts),0)
		  FROM summary_mt WHERE bucket_start >= DATE_SUB(NOW(), INTERVAL 30 DAY) GROUP BY account_id`, false)
	stRepDeliverySince = AbuseStmts.Add("reputation.delivery_since", `SELECT COALESCE(SUM(delivered_parts),0), COALESCE(SUM(delivered_parts + undeliv_parts + expired_parts),0),
			       COALESCE(SUM(submitted_parts),0)
			  FROM summary_mt WHERE account_id = ? AND bucket_start >= DATE_SUB(NOW(), INTERVAL 30 DAY) AND bucket_start >= ?`, false)
	stRepLookups = AbuseStmts.Add("reputation.lookups", `SELECT account_id, COALESCE(SUM(found + not_found),0), COALESCE(SUM(not_found),0)
		  FROM summary_lookup WHERE bucket_start >= DATE_SUB(NOW(), INTERVAL 30 DAY) GROUP BY account_id`, false)
	stRepLookupsSince = AbuseStmts.Add("reputation.lookups_since", `SELECT COALESCE(SUM(found + not_found),0), COALESCE(SUM(not_found),0)
			  FROM summary_lookup WHERE account_id = ? AND bucket_start >= DATE_SUB(NOW(), INTERVAL 30 DAY) AND bucket_start >= ?`, false)
	stRepOptOuts = AbuseStmts.Add("reputation.opt_outs", `SELECT account_id, COUNT(*) FROM blocklist
		  WHERE source = 'unsub' AND account_id IS NOT NULL AND created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
		  GROUP BY account_id`, false)
	stRepOptOutsSince = AbuseStmts.Add("reputation.opt_outs_since", `SELECT COUNT(*) FROM blocklist
			  WHERE source = 'unsub' AND account_id = ? AND created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY) AND created_at >= ?`, false)
	// A rejects table of the last 31 days (rejects_<company>_<YYYYMMDD>): %s.
	stRepRefusals = AbuseStmts.Add("reputation.refusals", `SELECT account_id, COALESCE(SUM(GREATEST(parts,1)),0) FROM %s
			  WHERE rejected_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 30 DAY)
			    AND reason_code IN ('SPAM_KEYWORD', 'DESTINATION_BLOCKED')
			  GROUP BY account_id`, false)
	stRepRefusalsSince = AbuseStmts.Add("reputation.refusals_since", `SELECT COALESCE(SUM(GREATEST(parts,1)),0) FROM %s
				  WHERE account_id = ? AND rejected_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 30 DAY) AND rejected_at >= ?
				    AND reason_code IN ('SPAM_KEYWORD', 'DESTINATION_BLOCKED')`, false)

	// ---- the message windows ----
	// In each: the table as drill names it, then mtWhere's predicate, then
	// the window's minutes.
	stWinCounts = AbuseStmts.Add("window.counts", `SELECT account_id, COUNT(*), COUNT(DISTINCT destination) FROM %s
			 WHERE %s AND accepted_at >= DATE_SUB(NOW(), INTERVAL %d MINUTE)
			 GROUP BY account_id`, false)
	// The prefix length, the table, the predicate, the minutes, the length.
	stWinPrefixes = AbuseStmts.Add("window.prefixes", `SELECT account_id, LEFT(destination, %d), COUNT(*) FROM %s
			 WHERE %s AND accepted_at >= DATE_SUB(NOW(), INTERVAL %d MINUTE)
			 GROUP BY account_id, LEFT(destination, %d)`, false)
	// One table's part of the count over several (the account id fourth),
	// joined by " UNION ALL " into window.distinct.
	stWinDistinctPart = AbuseStmts.Add("window.distinct_part",
		`SELECT destination FROM %s WHERE %s AND account_id = %d AND accepted_at >= DATE_SUB(NOW(), INTERVAL %d MINUTE)`, false)
	stWinDistinct = AbuseStmts.Add("window.distinct",
		"SELECT COUNT(DISTINCT destination) FROM (%s) w", false)
	// spendCols, the table, the predicate, the minutes, spendGroup.
	stWinSpend = AbuseStmts.Add("window.spend", `SELECT %s FROM %s
			 WHERE %s AND accepted_at >= DATE_SUB(NOW(), INTERVAL %d MINUTE)
			 GROUP BY %s`, false)
	stWinSpendBase = AbuseStmts.Add("window.spend_baseline", `SELECT %s FROM %s
			 WHERE %s
			   AND accepted_at <  DATE_SUB(NOW(), INTERVAL %d MINUTE)
			   AND accepted_at >= DATE_SUB(NOW(), INTERVAL %d MINUTE)
			 GROUP BY %s`, false)
	stWinMessages = AbuseStmts.Add("window.messages", `SELECT account_id, COUNT(*) FROM %s
			 WHERE %s AND accepted_at >= DATE_SUB(NOW(), INTERVAL %d MINUTE)
			 GROUP BY account_id`, false)
	stWinMessagesBase = AbuseStmts.Add("window.messages_baseline", `SELECT account_id, COUNT(*) FROM %s
			 WHERE %s
			   AND accepted_at <  DATE_SUB(NOW(), INTERVAL %d MINUTE)
			   AND accepted_at >= DATE_SUB(NOW(), INTERVAL %d MINUTE)
			 GROUP BY account_id`, false)
	stWinLookups = AbuseStmts.Add("window.lookups", `SELECT account_id,
			        SUM(hlr_source IN ('miss','dip_miss')),
			        COUNT(*)
			   FROM %s
			  WHERE %s
			    AND accepted_at >= DATE_SUB(NOW(), INTERVAL %d MINUTE)
			    AND hlr_source IN ('db','dip','miss','dip_miss')
			  GROUP BY account_id`, false)
	// The table, the predicate, then winPred or basePred.
	stWinEncodings = AbuseStmts.Add("window.encodings", `SELECT account_id, COALESCE(encoding,''), COUNT(*) FROM %s
				  WHERE %s AND %s GROUP BY account_id, encoding`, false)
	// The table, the predicate, the minutes, the account ids.
	stWinSent = AbuseStmts.Add("window.sent", `SELECT account_id, COUNT(*) FROM %s
			  WHERE %s AND accepted_at >= DATE_SUB(NOW(), INTERVAL %d MINUTE) AND account_id IN (%s)
			  GROUP BY account_id`, false)
	// The table, the predicate, samplePred, each account's rows, the limit.
	stWinSample = AbuseStmts.Add("window.sample", `SELECT account_id, body, destination, sender FROM (
			   SELECT account_id, COALESCE(body,'') AS body, COALESCE(destination,'') AS destination, COALESCE(sender,'') AS sender,
			          ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY accepted_at DESC) AS rn
			     FROM %s WHERE %s AND %s) x
			  WHERE rn <= %d LIMIT %d`, false)
)

// abuseCooldownSQL is the cooldown read; a warning's adds its rule.
const abuseCooldownSQL = `SELECT COUNT(*) FROM audit_log WHERE account_id=? AND action=? AND created_at >= DATE_SUB(NOW(), INTERVAL ? MINUTE)`

// spendCols / spendGroup are an account's spend as a log row keeps it: the
// rate version that priced it and the conversion rate into the company's
// currency (issue 16), not an amount. The parts are summed by rate and rate;
// the role prices each sum (billing.Prices).
const spendCols = "account_id, COALESCE(sell_rate_id,0), COALESCE(fx_rate,1), SUM(COALESCE(segments,1))"

// spendGroup is spendCols' GROUP BY.
const spendGroup = "account_id, sell_rate_id, fx_rate"

// The encoding read's window and baseline (window.encodings).
const (
	encWindowPred = "accepted_at >= DATE_SUB(NOW(), INTERVAL %d MINUTE)"
	encBasePred   = "accepted_at < DATE_SUB(NOW(), INTERVAL %d MINUTE) AND accepted_at >= DATE_SUB(NOW(), INTERVAL %d MINUTE)"
)

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
		return nil, err
	}
	defer rows.Close()
	var out []dal.AbuseRuleRow
	for rows.Next() {
		var r dal.AbuseRuleRow
		if rows.Scan(&r.Signal, &r.Params) == nil {
			out = append(out, r)
		}
	}
	return out, nil
}

func (c *abuseConfig) Overrides(ctx context.Context) ([]dal.AbuseOverrideRow, error) {
	rows, err := c.db.QueryContext(ctx, stAbuseOverrides.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.AbuseOverrideRow
	for rows.Next() {
		var r dal.AbuseOverrideRow
		if rows.Scan(&r.Signal, &r.Account, &r.Params, &r.Disabled) == nil {
			out = append(out, r)
		}
	}
	return out, nil
}

func (c *abuseConfig) accountCompanies(ctx context.Context, q string) ([]dal.AccountCompany, error) {
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
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

func (c *abuseConfig) Companies(ctx context.Context) ([]dal.AccountCompany, error) {
	return c.accountCompanies(ctx, stAbuseCompanies.SQL)
}

func (c *abuseConfig) Unenforced(ctx context.Context) ([]dal.AccountCompany, error) {
	return c.accountCompanies(ctx, stAbuseUnenforced.SQL)
}

func (c *abuseConfig) Money(ctx context.Context) ([]dal.AccountMoney, error) {
	rows, err := c.db.QueryContext(ctx, stAbuseMoney.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.AccountMoney
	for rows.Next() {
		var r dal.AccountMoney
		if rows.Scan(&r.Account, &r.Company, &r.Currency, &r.BaseRate, &r.RateProvider) == nil {
			out = append(out, r)
		}
	}
	return out, nil
}

func (c *abuseConfig) ProtectedNames(ctx context.Context) ([]string, error) {
	rows, err := c.db.QueryContext(ctx, stAbuseProtected.SQL)
	if err != nil {
		return nil, err
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
		return nil, err
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
		return nil, err
	}
	defer rows.Close()
	var out []dal.AccountSenderList
	for rows.Next() {
		var r dal.AccountSenderList
		if rows.Scan(&r.Account, &r.List) == nil {
			out = append(out, r)
		}
	}
	return out, nil
}

func (c *abuseConfig) OptOuts(ctx context.Context, windowMin int) ([]dal.AccountCount, error) {
	rows, err := c.db.QueryContext(ctx, stAbuseOptOuts.SQL, windowMin)
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

func (c *abuseConfig) Recent(ctx context.Context, account int64, action string, minutes int, bySignal bool, signal string) (int, error) {
	q := stAbuseCooldown.SQL
	args := []any{account, action, minutes}
	if bySignal {
		q = stAbuseCooldownSignal.SQL
		args = append(args, signal)
	}
	var n int
	if err := c.db.QueryRowContext(ctx, q, args...).Scan(&n); err != nil {
		return 0, err
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

// Act writes the audit row and changes the account in one transaction, the
// row first (2026-10-18, go-live P10 X1): both are there or neither.
func (c *abuseConfig) Act(ctx context.Context, a dal.AbuseAct) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, stAbuseAuditRow.SQL,
		a.Account, nullID(a.CompanyID), nullStr(a.CompanyName), a.Action, a.EntityID, a.Label, a.Detail); err != nil {
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
	hotRd    *dbpool.Read
}

var _ dal.ReputationData = (*reputationData)(nil)

// read is the pool the hot store's summaries are read from: the read
// instance when one is set and answering, else the store itself.
func (r *reputationData) read() *sql.DB { return hotReader(r.hot, r.hotRd) }

func hotReader(hot *sql.DB, rd *dbpool.Read) *sql.DB {
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
		return nil, err
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
		return nil, err
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
	_, err := r.cfg.ExecContext(ctx, stRepWrite.SQL, account, score, reasons, inputs, stamp)
	return err
}

func (r *reputationData) LastComputed(ctx context.Context) (dal.NullTime, error) {
	var last sql.NullTime
	if err := r.cfg.QueryRowContext(ctx, stRepLast.SQL).Scan(&last); err != nil {
		return dal.NullTime{}, err
	}
	return dal.NullTime{Time: last.Time, Valid: last.Valid}, nil
}

func (r *reputationData) Accounts(ctx context.Context) ([]dal.AccountCreated, error) {
	rows, err := r.cfg.QueryContext(ctx, stRepAccounts.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.AccountCreated
	for rows.Next() {
		var id int64
		var created sql.NullTime
		if rows.Scan(&id, &created) == nil {
			out = append(out, dal.AccountCreated{Account: id, Created: dal.NullTime{Time: created.Time, Valid: created.Valid}})
		}
	}
	return out, nil
}

func (r *reputationData) MonitorActions(ctx context.Context) ([]dal.ActionCount, error) {
	rows, err := r.cfg.QueryContext(ctx, stRepActions.SQL)
	if err != nil {
		return nil, err
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
		return nil, err
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
	return accountSums(ctx, r.read(), stRepDelivery.SQL, 3)
}

func (r *reputationData) DeliverySince(ctx context.Context, account int64, cutUTC string) (delivered, final, sent int, err error) {
	err = r.read().QueryRowContext(ctx, stRepDeliverySince.SQL, account, cutUTC).Scan(&delivered, &final, &sent)
	return
}

func (r *reputationData) Lookups(ctx context.Context) (map[int64][]int, error) {
	return accountSums(ctx, r.read(), stRepLookups.SQL, 2)
}

func (r *reputationData) LookupsSince(ctx context.Context, account int64, cutUTC string) (lookedUp, unknown int, err error) {
	err = r.read().QueryRowContext(ctx, stRepLookupsSince.SQL, account, cutUTC).Scan(&lookedUp, &unknown)
	return
}

func (r *reputationData) OptOuts(ctx context.Context) (map[int64][]int, error) {
	return accountSums(ctx, r.cfg, stRepOptOuts.SQL, 1)
}

func (r *reputationData) OptOutsSince(ctx context.Context, account int64, cutConfig string) (int, error) {
	var n int
	err := r.cfg.QueryRowContext(ctx, stRepOptOutsSince.SQL, account, cutConfig).Scan(&n)
	return n, err
}

func (r *reputationData) Refusals(ctx context.Context, table string) (map[int64][]int, error) {
	return accountSums(ctx, r.read(), fmt.Sprintf(stRepRefusals.SQL, table), 1)
}

func (r *reputationData) RefusalsSince(ctx context.Context, table string, account int64, cutUTC string) (int, error) {
	var n int
	err := r.read().QueryRowContext(ctx, fmt.Sprintf(stRepRefusalsSince.SQL, table), account, cutUTC).Scan(&n)
	return n, err
}

// accountSums runs a query whose first column is an account id and whose other n
// columns are counts. A row that cannot be read is left out.
func accountSums(ctx context.Context, db *sql.DB, q string, n int) (map[int64][]int, error) {
	out := map[int64][]int{}
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return out, err
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
	rd  *dbpool.Read
	// noDrill names the message logs found to lack the time key idx_drill
	// (drill): their window queries run without the hint.
	noDrill map[string]bool
}

var _ dal.MessageWindows = (*AbuseWindows)(nil)

// NewAbuseWindows reads from hot, or from rd's read instance when one is set
// and answering.
func NewAbuseWindows(hot *sql.DB, rd *dbpool.Read) *AbuseWindows {
	return &AbuseWindows{hot: hot, rd: rd}
}

// read is the pool the scans use: the read instance when one is set and
// answering, else the store itself.
func (w *AbuseWindows) read() *sql.DB { return hotReader(w.hot, w.rd) }

// mtWhere returns the family-correct "this row is a countable MT accept"
// predicate: the legacy records_* tables mix directions/statuses; an mt_
// table IS the MT family (a message a vendor took; rejects live in rejects_).
func mtWhere(table string) string {
	if isMT(table) {
		return "1=1"
	}
	return "direction='MT' AND status<>'rejected'"
}

// isMT says a table name (with or without its index hint) is a day of
// outgoing messages.
func isMT(table string) bool {
	return billingstore.TableFamily(strings.TrimSuffix(table, drillHint)) == billingstore.KindMT
}

// drillHint makes a window query of a message log go through its time key.
//
// Every query here asks for the rows of the last few minutes (or of the
// baseline hours) and groups them by account. Left alone, MySQL walked the
// whole day table through idx_concat (account_id, …) to save itself the
// grouping, and then looked at each row's time: about 9 billion rows and
// 70,000 seconds of MySQL in 20 hours on the dev box, one connection busy
// all the time with no traffic at all (2026-10-13). With idx_drill
// (accepted_at, vendor_id) it reads the rows of the window and no others.
const drillHint = " FORCE INDEX (idx_drill)"

// Drill is a record table as a window query names it: a message log with
// its time key forced. A legacy records_ table, and a message log found to
// lack the key, are named as they are.
func (w *AbuseWindows) Drill(t string) string {
	if !isMT(t) || w.noDrill[t] {
		return t
	}
	return t + drillHint
}

// query runs a query built with Drill. A table made before the key existed
// answers error 1176; the query is then run again without the hint and the
// table (t, "" = not one table) is remembered, so the next sweep does not
// ask twice.
func (w *AbuseWindows) query(ctx context.Context, t, q string) (*sql.Rows, error) {
	rows, err := w.read().QueryContext(ctx, q)
	if err == nil || !strings.Contains(q, drillHint) || !noSuchKey(err) {
		return rows, err
	}
	if t != "" {
		if w.noDrill == nil {
			w.noDrill = map[string]bool{}
		}
		w.noDrill[t] = true
	}
	return w.read().QueryContext(ctx, strings.ReplaceAll(q, drillHint, ""))
}

// noSuchKey reports MySQL's "Key … doesn't exist in table" (1176).
func noSuchKey(err error) bool {
	var me *mysqldrv.MySQLError
	return errors.As(err, &me) && me.Number == 1176
}

func (w *AbuseWindows) Counts(ctx context.Context, t string, windowMin int) ([]dal.WindowCount, error) {
	rows, err := w.query(ctx, t, fmt.Sprintf(stWinCounts.SQL, w.Drill(t), mtWhere(t), windowMin))
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
	rows, err := w.query(ctx, t, fmt.Sprintf(stWinPrefixes.SQL, prefixLen, w.Drill(t), mtWhere(t), windowMin, prefixLen))
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
	parts := make([]string, len(tables))
	for i, t := range tables {
		parts[i] = fmt.Sprintf(stWinDistinctPart.SQL, w.Drill(t), mtWhere(t), account, windowMin)
	}
	rows, err := w.query(ctx, "", fmt.Sprintf(stWinDistinct.SQL, strings.Join(parts, " UNION ALL ")))
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
	q := fmt.Sprintf(stWinSpend.SQL, spendCols, w.Drill(t), mtWhere(t), windowMin, spendGroup)
	if sinceMin > 0 {
		q = fmt.Sprintf(stWinSpendBase.SQL, spendCols, w.Drill(t), mtWhere(t), windowMin, sinceMin, spendGroup)
	}
	rows, err := w.query(ctx, t, q)
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
	q := fmt.Sprintf(stWinMessages.SQL, w.Drill(t), mtWhere(t), windowMin)
	if sinceMin > 0 {
		q = fmt.Sprintf(stWinMessagesBase.SQL, w.Drill(t), mtWhere(t), windowMin, sinceMin)
	}
	return w.accountCounts(ctx, t, q)
}

func (w *AbuseWindows) accountCounts(ctx context.Context, t, q string) ([]dal.AccountCount, error) {
	rows, err := w.query(ctx, t, q)
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
	rows, err := w.query(ctx, t, fmt.Sprintf(stWinLookups.SQL, w.Drill(t), mtWhere(t), windowMin))
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
	pred := fmt.Sprintf(encWindowPred, windowMin)
	if sinceMin > 0 {
		pred = fmt.Sprintf(encBasePred, windowMin, sinceMin)
	}
	rows, err := w.query(ctx, t, fmt.Sprintf(stWinEncodings.SQL, w.Drill(t), mtWhere(t), pred))
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

// idList is account ids as an IN list's text.
func idList(ids []int64) string {
	s := make([]string, 0, len(ids))
	for _, id := range ids {
		s = append(s, fmt.Sprint(id))
	}
	return strings.Join(s, ",")
}

func (w *AbuseWindows) Sent(ctx context.Context, t string, windowMin int, accounts []int64) ([]dal.AccountCount, error) {
	return w.accountCounts(ctx, t, fmt.Sprintf(stWinSent.SQL, w.Drill(t), mtWhere(t), windowMin, idList(accounts)))
}

func (w *AbuseWindows) Sample(ctx context.Context, t string, fromMin, toMin int, accounts []int64, perAcct, limit int) ([]dal.SampledMessage, error) {
	pred := fmt.Sprintf("accepted_at >= DATE_SUB(NOW(), INTERVAL %d MINUTE)", fromMin)
	if toMin > 0 {
		pred += fmt.Sprintf(" AND accepted_at < DATE_SUB(NOW(), INTERVAL %d MINUTE)", toMin)
	}
	if len(accounts) > 0 {
		pred += " AND account_id IN (" + idList(accounts) + ")"
	}
	// A table is a company's month (D-136): the newest of EACH account,
	// so one busy account does not take every place of the sample.
	rows, err := w.query(ctx, t, fmt.Sprintf(stWinSample.SQL, w.Drill(t), mtWhere(t), pred, perAcct, limit))
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
