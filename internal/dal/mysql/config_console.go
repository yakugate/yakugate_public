package mysql

import (
	"context"
	"fmt"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/dal/stmt"
)

// The console's own screens (dal.ConsoleViews), moved here from
// internal/process/console on 29 Oct 2026 with the same statements.

// menuCounts are the main menu's counts, by the name the console asks with.
var menuCounts = map[string]*stmt.Stmt{
	"accounts":  ConfigStmts.Add("config.console.count_accounts", "SELECT COUNT(*) FROM sms_service_config", false),
	"companies": ConfigStmts.Add("config.console.count_companies", "SELECT COUNT(*) FROM companies", false),
	"vendors":   ConfigStmts.Add("config.console.count_vendors", "SELECT COUNT(*) FROM vendors", false),
	"calls":     ConfigStmts.Add("config.console.count_calls", "SELECT COUNT(*) FROM http_endpoints", false),
	"plans":     ConfigStmts.Add("config.console.count_plans", "SELECT COUNT(DISTINCT account_id) FROM routes WHERE direction = 'MT' AND account_id IS NOT NULL", false),
	"dialects":  ConfigStmts.Add("config.console.count_dialects", "SELECT COUNT(*) FROM sms_service_dialects", false),
	"cards":     ConfigStmts.Add("config.console.count_cards", "SELECT COUNT(*) FROM rate_cards", false),
	"spam":      ConfigStmts.Add("config.console.count_spam", "SELECT COUNT(*) FROM spam_keywords", false),
	"blocked":   ConfigStmts.Add("config.console.count_blocked", "SELECT COUNT(*) FROM blocklist WHERE is_disabled = 0", false),
	"operators": ConfigStmts.Add("config.console.count_operators", "SELECT COUNT(DISTINCT mcc, mnc) FROM operators", false),
}

var (
	stRecentLedger = ConfigStmts.Add("config.console.recent_ledger", `
		SELECT DATE_FORMAT(created_at,'%Y-%m-%d %H:%i'), type, amount, new_balance,
		       COALESCE(reason,''), COALESCE(created_by_user_id,'')
		  FROM balance_history WHERE company_id = ? AND type IN ('topup','adjust','refund')
		 ORDER BY created_at DESC, id DESC LIMIT 15`, false)
	stActiveDeployments = ConfigStmts.Add("config.console.active_deployments",
		"SELECT deployment_id, COALESCE(name,'') FROM deployments WHERE is_disabled=0 ORDER BY deployment_id", false)
	stMaxRulePriority = ConfigStmts.Add("config.console.max_rule_priority",
		`SELECT MAX(priority) FROM routes WHERE account_id = ? AND direction = ?`, false)
	stAccountUsername = ConfigStmts.Add("config.console.account_username",
		"SELECT username FROM sms_service_config WHERE id = ?", false)
)

func (c configQ) MenuCount(ctx context.Context, what string) (int, error) {
	st, ok := menuCounts[what]
	if !ok {
		return 0, fmt.Errorf("no count named %q", what)
	}
	return c.count(ctx, st)
}

func (c configQ) RecentLedger(ctx context.Context, companyID int64) ([]dal.LedgerLine, error) {
	rows, err := c.q.QueryContext(ctx, stRecentLedger.SQL, companyID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.LedgerLine
	for rows.Next() {
		var l dal.LedgerLine
		if rows.Scan(&l.When, &l.Type, &l.Amount, &l.After, &l.Reason, &l.By) != nil {
			continue
		}
		out = append(out, l)
	}
	return out, nil
}

func (c configQ) ActiveDeployments(ctx context.Context) ([]dal.DeploymentName, error) {
	rows, err := c.q.QueryContext(ctx, stActiveDeployments.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.DeploymentName
	for rows.Next() {
		var d dal.DeploymentName
		if rows.Scan(&d.ID, &d.Name) == nil {
			out = append(out, d)
		}
	}
	return out, nil
}

func (c configQ) MaxRulePriority(ctx context.Context, accountID int64, direction string) (dal.NullInt64, error) {
	var max dal.NullInt64
	err := c.q.QueryRowContext(ctx, stMaxRulePriority.SQL, accountID, direction).Scan(&max)
	return max, err
}

func (c configQ) AccountUsername(ctx context.Context, id int64) (string, error) {
	var name string
	err := c.q.QueryRowContext(ctx, stAccountUsername.SQL, id).Scan(&name)
	return name, err
}
