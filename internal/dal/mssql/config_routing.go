package mssql

import (
	"context"

	"github.com/yakugate/yakugate/internal/dal"
)

// The routing rules and what their checks and screens read (dal.Routing) on
// SQL Server, made like internal/dal/mysql/config_routing.go.

// ruleColumns is a routes row as provision reads it (RuleRow).
const ruleColumns = `r.id, COALESCE(r.name,''), r.priority, COALESCE(r.route_type,'direct'), COALESCE(r.weight,100),
	COALESCE(r.direction,'MT'), COALESCE(r.account_id,0),
	COALESCE(r.match_mccmncs,''), COALESCE(r.accept_undetected_network,0),
	COALESCE(r.sender_regex,''), COALESCE(r.receiver_regex,''), COALESCE(r.keyword_regex,''), COALESCE(r.priority_regex,''),
	COALESCE(r.vendor_id,0), COALESCE(r.send_plan,''),
	r.retry_rounds, r.retry_first_sec, r.retry_backoff, r.retry_max_wait_sec,
	COALESCE(r.selling_rate_card_id,0), COALESCE(r.buying_rate_card_id,0),
	COALESCE(r.dlr_selling_rate_card_id,0), COALESCE(r.dlr_buying_rate_card_id,0), COALESCE(r.is_disabled,0)`

// directionOrder sorts routes.direction as MySQL sorts its ENUM('MT','MO',
// 'DLR'): by the list's order, not by the letters.
const directionOrder = "CASE r.direction WHEN 'MT' THEN 1 WHEN 'MO' THEN 2 WHEN 'DLR' THEN 3 ELSE 0 END"

// stampRoute is the updated_at MySQL moves on every change of a rule.
const stampRoute = ", updated_at = SYSUTCDATETIME()"

var (
	stRuleRows = addConfig("config.routing.rule_rows", `SELECT `+ruleColumns+` FROM routes r WHERE r.account_id = ? AND r.direction = ? ORDER BY r.priority, r.id`)
	// FOR UPDATE at MySQL's REPEATABLE READ locks the rows read and the gaps
	// between them (no new rule of the account slips in): UPDLOCK + HOLDLOCK
	// holds the range read the same way, to the end of the transaction.
	stRuleRowsLocked = addConfig("config.routing.rule_rows_locked", `SELECT `+ruleColumns+` FROM routes r WITH (UPDLOCK, HOLDLOCK) WHERE r.account_id = ? AND r.direction = ? ORDER BY r.priority, r.id`)
	stRuleByID       = addConfig("config.routing.rule_by_id", `SELECT `+ruleColumns+` FROM routes r WHERE r.id = ?`)
	stOutgoingRules  = addConfig("config.routing.outgoing_rules", `SELECT `+ruleColumns+` FROM routes r
		WHERE r.direction = 'MT' AND r.account_id IS NOT NULL ORDER BY r.account_id, r.priority, r.id`)
	stPlanAccount = addConfig("config.routing.plan_account", `
		SELECT a.username, COALESCE(a.company_id,0), COALESCE(c.name,''), COALESCE(a.deployments,''),
		       COALESCE(a.default_sender,''), COALESCE(a.enforce_hlr_db_check,0), COALESCE(a.loss_detection_enabled,0)
		  FROM sms_service_config a LEFT JOIN companies c ON c.id = a.company_id
		 WHERE a.id = ?`)
	stPlanAccounts = addConfig("config.routing.plan_accounts", `
		SELECT `+wholeTable+` a.id, a.username, COALESCE(a.company_id,0), COALESCE(c.name,''), COALESCE(a.deployments,'')
		  FROM sms_service_config a LEFT JOIN companies c ON c.id = a.company_id
		 ORDER BY a.username`)
	stPlanVendors      = addConfig("config.routing.plan_vendors", "SELECT "+wholeTable+" id, cid, COALESCE(transport,''), COALESCE(service_status,'') FROM vendors")
	stPlanCardNames    = addConfig("config.routing.plan_card_names", "SELECT "+wholeTable+" id, COALESCE(name,'') FROM rate_cards")
	stSetRulePriority  = addConfig("config.routing.set_rule_priority", "UPDATE routes SET priority = ?"+stampRoute+" WHERE id = ?")
	stOutgoingRuleAcct = addConfig("config.routing.outgoing_rule_account", "SELECT COALESCE(account_id,0) FROM routes WHERE id = ? AND direction = 'MT'")
	stRuleAccount      = addConfig("config.routing.rule_account", "SELECT COALESCE(account_id,0) FROM routes WHERE id = ?")
	stOtherMORules     = addConfig("config.routing.other_mo_rules",
		`SELECT id, COALESCE(name,''), account_id, COALESCE(receiver_regex,''), COALESCE(keyword_regex,''),
		        COALESCE(match_mccmncs,''), COALESCE(vendor_id,0)
		   FROM routes WHERE direction='MO' AND account_id IS NOT NULL AND account_id <> ? AND id <> ?`)
	stRuleAccountName = addConfig("config.routing.rule_account_name", "SELECT username FROM sms_service_config WHERE id=?")
	stRuleList        = addConfig("config.routing.rule_list", `
		SELECT TOP (@p1) r.id, COALESCE(r.name,''), r.direction, r.priority,
		       COALESCE(a.username,'every account'), COALESCE(v.cid,N'—'),
		       COALESCE(r.receiver_regex,''), COALESCE(r.sender_regex,''),
		       COALESCE(r.keyword_regex,''), COALESCE(r.is_disabled,0),
		       COALESCE(v.transport,''), COALESCE(v.adapter_config,'{}'), COALESCE(v.service_status,''),
		       COALESCE(r.send_plan,''), CASE WHEN r.account_id IS NULL THEN 1 ELSE 0 END
		  FROM routes r
		  LEFT JOIN sms_service_config a ON a.id = r.account_id
		  LEFT JOIN vendors v ON v.id = r.vendor_id
		 ORDER BY `+directionOrder+`, r.priority, r.id`)
	stRuleVendorCIDs = addConfig("config.routing.rule_vendor_cids", "SELECT "+wholeTable+" id, cid FROM vendors")
	// Rows CHANGED, as MySQL counts them.
	stSetRuleDisabled = addConfig("config.routing.set_rule_disabled", "UPDATE routes SET is_disabled = ?"+stampRoute+" WHERE id = ? AND is_disabled IS DISTINCT FROM @p1")
	stDeleteRule      = addConfig("config.routing.delete_rule", "DELETE FROM routes WHERE id = ?")
	stRuleCards       = addConfig("config.routing.rule_cards", `
		SELECT direction, COALESCE(selling_rate_card_id,0), COALESCE(buying_rate_card_id,0),
		       COALESCE(dlr_selling_rate_card_id,0), COALESCE(dlr_buying_rate_card_id,0)
		  FROM routes WHERE id = ?`)
	stRuleCardName = addConfig("config.routing.rule_card_name", "SELECT name FROM rate_cards WHERE id = ?")
	stRuleShape    = addConfig("config.routing.rule_shape",
		"SELECT direction, COALESCE(send_plan,''), COALESCE(account_id,0) FROM routes WHERE id = ?")
	stRuleCardKind = addConfig("config.routing.rule_card_kind",
		"SELECT kind, COALESCE(type,'') FROM rate_cards WHERE id = ?")
	// The column is one of a rule's four card columns (RouteCardSlots).
	stSetRuleCard = addConfig("config.routing.set_rule_card",
		"UPDATE routes SET selling_rate_card_id = ?"+stampRoute+" WHERE id = ?")
	stRuleAccountDeps = addConfig("config.routing.rule_account_deployments",
		"SELECT COALESCE(deployments,'') FROM sms_service_config WHERE id = ?")
	stRuleVendorDeps = addConfig("config.routing.rule_vendor_deployments",
		"SELECT COALESCE(deployments,'') FROM vendors WHERE id = ?")
	stMTPriorityTaken = addConfig("config.routing.mt_priority_taken",
		`SELECT TOP (1) id, COALESCE(name,'') FROM routes WHERE direction='MT' AND account_id = ? AND priority = ? AND id <> ?`)
	stMOPriorityTaken = addConfig("config.routing.mo_priority_taken",
		`SELECT TOP (1) id, COALESCE(name,'') FROM routes WHERE direction='MO' AND account_id = ? AND priority = ? AND id <> ?`)
	stMaxMOPriority = addConfig("config.routing.max_mo_priority",
		`SELECT MAX(priority) FROM routes WHERE account_id = ? AND direction = 'MO'`)
	stInsertRule = addConfig("config.routing.insert_rule", `
		INSERT INTO routes (name, direction, route_type, priority, account_id, vendor_id,
		                    receiver_regex, sender_regex, keyword_regex, priority_regex, match_mccmncs,
		                    is_disabled, accept_undetected_network, send_plan,
		                    retry_rounds, retry_first_sec, retry_backoff, retry_max_wait_sec,
		                    selling_rate_card_id, buying_rate_card_id, dlr_selling_rate_card_id, dlr_buying_rate_card_id,
		                    created_by_user_id)
		OUTPUT INSERTED.id
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'console')`)
	stRuleStoredPlan = addConfig("config.routing.rule_stored_plan",
		"SELECT COALESCE(send_plan,''), route_type, COALESCE(weight,100) FROM routes WHERE id = ?")
	stUpdateRule = addConfig("config.routing.update_rule", `
		UPDATE routes SET name = ?, route_type = ?, weight = ?, priority = ?, account_id = ?, vendor_id = ?,
		       receiver_regex = ?, sender_regex = ?, keyword_regex = ?, priority_regex = ?, match_mccmncs = ?,
		       is_disabled = ?, accept_undetected_network = ?, send_plan = ?,
		       retry_rounds = ?, retry_first_sec = ?, retry_backoff = ?, retry_max_wait_sec = ?,
		       selling_rate_card_id = ?, buying_rate_card_id = ?, dlr_selling_rate_card_id = ?, dlr_buying_rate_card_id = ?`+stampRoute+`
		 WHERE id = ?`)
	stRuleCardCompany = addConfig("config.routing.rule_card_company", "SELECT COALESCE(company_id,0) FROM rate_cards WHERE id = ?")
	stRuleVendorCID   = addConfig("config.routing.rule_vendor_cid", "SELECT cid FROM vendors WHERE id = ?")
	stOperatorCatalog = addConfig("config.routing.operator_catalog", `
		SELECT `+wholeTable+` COALESCE(TRIM(o.ccc),''), COALESCE(o.mcc,''), COALESCE(o.mnc,''), COALESCE(o.country,''),
		       COALESCE(o.operator_name,''), f.[first], f.n
		  FROM operators o
		  JOIN (SELECT MIN(id) id, MIN(from_series) [first], COUNT(*) n FROM operators GROUP BY ccc, mcc, mnc) f ON f.id = o.id`)
	stSeriesBelow = addConfig("config.routing.series_below", "SELECT TOP (60) "+seriesColumns+" FROM operators WHERE from_series <= ? ORDER BY from_series DESC")
	stSeriesLike  = addConfig("config.routing.series_like", "SELECT TOP (300) "+seriesColumns+" FROM operators WHERE from_series LIKE ? ORDER BY from_series")
)

// seriesColumns is a range of the operators table (NumberSeriesRow).
const seriesColumns = `id, COALESCE(TRIM(ccc),''), COALESCE(mcc,''), COALESCE(mnc,''), COALESCE(operator_name,''), COALESCE(country,''),
	              COALESCE(from_series,''), COALESCE(to_series,'')`

func scanRule(sc interface{ Scan(...any) error }) (dal.RuleRow, error) {
	var r dal.RuleRow
	err := sc.Scan(&r.ID, &r.Name, &r.Priority, &r.RouteType, &r.Weight, &r.Direction, &r.AccountID,
		&r.Networks, &r.AcceptUnknown, &r.Sender, &r.Receiver, &r.Keyword, &r.Pri,
		&r.VendorID, &r.Plan, &r.Rounds, &r.First, &r.Backoff, &r.MaxWait,
		&r.Sell, &r.Buy, &r.DLRSell, &r.DLRBuy, &r.Disabled)
	return r, err
}

// ruleRows reads the rows of a rule statement; an error met while reading
// them is a dal.RowsError.
func (c configQ) ruleRows(ctx context.Context, q string, args ...any) ([]dal.RuleRow, error) {
	rows, err := c.q.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.RuleRow
	for rows.Next() {
		r, err := scanRule(rows)
		if err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, dal.RowsError{Err: err}
	}
	return out, nil
}

func (c configQ) RuleRows(ctx context.Context, accountID int64, direction string, lock bool) ([]dal.RuleRow, error) {
	st := stRuleRows
	if lock {
		st = stRuleRowsLocked
	}
	return c.ruleRows(ctx, st.SQL, accountID, Varchar(direction))
}

func (c configQ) RuleByID(ctx context.Context, id int64) (dal.RuleRow, error) {
	return scanRule(c.q.QueryRowContext(ctx, stRuleByID.SQL, id))
}

func (c configQ) OutgoingRuleRows(ctx context.Context) ([]dal.RuleRow, error) {
	return c.ruleRows(ctx, stOutgoingRules.SQL)
}

func (c configQ) PlanAccount(ctx context.Context, id int64) (dal.PlanAccountRow, error) {
	var a dal.PlanAccountRow
	err := c.q.QueryRowContext(ctx, stPlanAccount.SQL, id).Scan(&a.Username, &a.CompanyID, &a.Company, &a.Deployments,
		&a.DefaultSender, &a.HLRCheck, &a.LossCheck)
	return a, err
}

func (c configQ) PlanAccounts(ctx context.Context) ([]dal.PlanListAccount, error) {
	rows, err := c.q.QueryContext(ctx, stPlanAccounts.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.PlanListAccount
	for rows.Next() {
		var p dal.PlanListAccount
		if err := rows.Scan(&p.ID, &p.Username, &p.CompanyID, &p.Company, &p.Deployments); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, dal.RowsError{Err: err}
	}
	return out, nil
}

func (c configQ) PlanVendors(ctx context.Context) ([]dal.PlanVendorRow, error) {
	rows, err := c.q.QueryContext(ctx, stPlanVendors.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.PlanVendorRow
	for rows.Next() {
		var v dal.PlanVendorRow
		if err := rows.Scan(&v.ID, &v.CID, &v.Transport, &v.Status); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, dal.RowsError{Err: err}
	}
	return out, nil
}

func (c configQ) PlanCardNames(ctx context.Context) (map[int64]string, error) {
	out := map[int64]string{}
	rows, err := c.q.QueryContext(ctx, stPlanCardNames.SQL)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var n string
		if rows.Scan(&id, &n) == nil {
			out[id] = n
		}
	}
	return out, nil
}

func (c configQ) SetRulePriority(ctx context.Context, id int64, priority int) error {
	_, err := c.q.ExecContext(ctx, stSetRulePriority.SQL, priority, id)
	return err
}

func (c configQ) RuleAccount(ctx context.Context, id int64) (int64, error) {
	var a int64
	err := c.q.QueryRowContext(ctx, stRuleAccount.SQL, id).Scan(&a)
	return a, err
}

func (c configQ) OutgoingRuleAccount(ctx context.Context, id int64) (int64, error) {
	var a int64
	err := c.q.QueryRowContext(ctx, stOutgoingRuleAcct.SQL, id).Scan(&a)
	return a, err
}

func (c configQ) EachOtherMORule(ctx context.Context, accountID, ignoreID int64, fn func(dal.MORuleRow) error) error {
	rows, err := c.q.QueryContext(ctx, stOtherMORules.SQL, accountID, ignoreID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var r dal.MORuleRow
		if err := rows.Scan(&r.ID, &r.Name, &r.AccountID, &r.Receiver, &r.Keyword, &r.Nets, &r.VendorID); err != nil {
			return err
		}
		if err := fn(r); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (c configQ) RuleAccountName(ctx context.Context, id int64) (string, error) {
	var who string
	err := c.q.QueryRowContext(ctx, stRuleAccountName.SQL, id).Scan(&who)
	return who, err
}

func (c configQ) RuleList(ctx context.Context, limit int) ([]dal.RuleListRow, error) {
	rows, err := c.q.QueryContext(ctx, stRuleList.SQL, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.RuleListRow
	for rows.Next() {
		var r dal.RuleListRow
		if err := rows.Scan(&r.ID, &r.Name, &r.Direction, &r.Priority, &r.Account, &r.Vendor,
			&r.Receiver, &r.Sender, &r.Keyword, &r.Disabled, &r.Transport, &r.AdapterConfig, &r.Status, &r.Plan, &r.Global); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, dal.RowsError{Err: err}
	}
	return out, nil
}

func (c configQ) RuleVendorCIDs(ctx context.Context) (map[int64]string, error) {
	out := map[int64]string{}
	rows, err := c.q.QueryContext(ctx, stRuleVendorCIDs.SQL)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var cid string
		if rows.Scan(&id, &cid) == nil {
			out[id] = cid
		}
	}
	return out, nil
}

func (c configQ) SetRuleDisabled(ctx context.Context, id int64, disabled int) (int64, error) {
	return c.execRows(ctx, stSetRuleDisabled.SQL, disabled, id)
}

func (c configQ) DeleteRuleRow(ctx context.Context, id int64) (int64, error) {
	return c.execRows(ctx, stDeleteRule.SQL, id)
}

func (c configQ) RuleCards(ctx context.Context, id int64) (dal.RuleCardsRow, error) {
	var r dal.RuleCardsRow
	err := c.q.QueryRowContext(ctx, stRuleCards.SQL, id).Scan(&r.Direction, &r.Sell, &r.Buy, &r.DLRSell, &r.DLRBuy)
	return r, err
}

func (c configQ) RuleCardName(ctx context.Context, cardID int64) (string, error) {
	var name string
	err := c.q.QueryRowContext(ctx, stRuleCardName.SQL, cardID).Scan(&name)
	return name, err
}

func (c configQ) RuleShape(ctx context.Context, id int64) (string, string, int64, error) {
	var direction, plan string
	var accountID int64
	err := c.q.QueryRowContext(ctx, stRuleShape.SQL, id).Scan(&direction, &plan, &accountID)
	return direction, plan, accountID, err
}

func (c configQ) RuleCardKind(ctx context.Context, cardID int64) (string, string, error) {
	var kind, typ string
	err := c.q.QueryRowContext(ctx, stRuleCardKind.SQL, cardID).Scan(&kind, &typ)
	return kind, typ, err
}

func (c configQ) SetRuleCard(ctx context.Context, column string, cardID, ruleID int64) error {
	_ = stSetRuleCard // the registered form; the column is the slot's
	_, err := c.q.ExecContext(ctx, "UPDATE routes SET "+quoteIdent(column)+" = @p1"+stampRoute+" WHERE id = @p2", cfgID(cardID), ruleID)
	return err
}

func (c configQ) RuleAccountDeployments(ctx context.Context, accountID int64) (string, error) {
	var s string
	err := c.q.QueryRowContext(ctx, stRuleAccountDeps.SQL, accountID).Scan(&s)
	return s, err
}

func (c configQ) RuleVendorDeployments(ctx context.Context, vendorID int64) (string, error) {
	var s string
	err := c.q.QueryRowContext(ctx, stRuleVendorDeps.SQL, vendorID).Scan(&s)
	return s, err
}

func (c configQ) RulePriorityTaken(ctx context.Context, direction string, accountID int64, priority int, ignoreID int64) (int64, string, error) {
	st := stMTPriorityTaken
	if direction == "MO" {
		st = stMOPriorityTaken
	}
	var id int64
	var name string
	err := c.q.QueryRowContext(ctx, st.SQL, accountID, priority, ignoreID).Scan(&id, &name)
	return id, name, err
}

func (c configQ) MaxIncomingPriority(ctx context.Context, accountID int64) (dal.NullInt64, error) {
	var max dal.NullInt64
	err := c.q.QueryRowContext(ctx, stMaxMOPriority.SQL, accountID).Scan(&max)
	return max, err
}

// retryArgs is the four retry columns: NULL when the rule has none of its own.
func retryArgs(r *dal.RuleRetry) (rounds, first, backoff, maxWait any) {
	if r == nil {
		return nil, nil, nil, nil
	}
	return r.Rounds, r.FirstSec, r.Backoff, r.MaxWaitSec
}

func (c configQ) InsertRule(ctx context.Context, r dal.RuleWrite) (int64, error) {
	rounds, first, backoff, maxWait := retryArgs(r.Retry)
	return c.insertID(ctx, stInsertRule.SQL,
		r.Name, Varchar(r.Direction), Varchar(r.Type), r.Priority,
		cfgID(r.AccountID), cfgID(r.VendorID),
		r.Receiver, r.Sender, r.Keyword, r.PriorityMatch,
		r.Networks, r.Disabled, r.AcceptUnknown, r.Plan,
		rounds, first, backoff, maxWait,
		cfgID(r.Sell), cfgID(r.Buy), cfgID(r.DLRSell), cfgID(r.DLRBuy))
}

func (c configQ) RuleStoredPlan(ctx context.Context, id int64) (string, string, int, error) {
	var plan, routeType string
	var weight int
	err := c.q.QueryRowContext(ctx, stRuleStoredPlan.SQL, id).Scan(&plan, &routeType, &weight)
	return plan, routeType, weight, err
}

func (c configQ) UpdateRule(ctx context.Context, id int64, r dal.RuleWrite) error {
	rounds, first, backoff, maxWait := retryArgs(r.Retry)
	_, err := c.q.ExecContext(ctx, stUpdateRule.SQL,
		r.Name, Varchar(r.Type), r.Weight, r.Priority, r.AccountID, cfgID(r.VendorID),
		r.Receiver, r.Sender, r.Keyword, r.PriorityMatch, r.Networks,
		r.Disabled, r.AcceptUnknown, r.Plan,
		rounds, first, backoff, maxWait,
		cfgID(r.Sell), cfgID(r.Buy), cfgID(r.DLRSell), cfgID(r.DLRBuy),
		id)
	return err
}

func (c configQ) RuleCardCompany(ctx context.Context, cardID int64) (int64, error) {
	var co int64
	err := c.q.QueryRowContext(ctx, stRuleCardCompany.SQL, cardID).Scan(&co)
	return co, err
}

func (c configQ) RuleVendorCID(ctx context.Context, vendorID int64) (string, error) {
	var cid string
	err := c.q.QueryRowContext(ctx, stRuleVendorCID.SQL, vendorID).Scan(&cid)
	return cid, err
}

func (c configQ) OperatorCatalog(ctx context.Context) ([]dal.OperatorCatalogRow, error) {
	rows, err := c.q.QueryContext(ctx, stOperatorCatalog.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.OperatorCatalogRow
	for rows.Next() {
		var n dal.OperatorCatalogRow
		if err := rows.Scan(&n.CCC, &n.MCC, &n.MNC, &n.Country, &n.Name, &n.First, &n.Ranges); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		return nil, dal.RowsError{Err: err}
	}
	return out, nil
}

func (c configQ) numberSeries(ctx context.Context, q string, arg any) ([]dal.NumberSeriesRow, error) {
	rows, err := c.q.QueryContext(ctx, q, arg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.NumberSeriesRow
	for rows.Next() {
		var r dal.NumberSeriesRow
		if err := rows.Scan(&r.ID, &r.CCC, &r.MCC, &r.MNC, &r.Name, &r.Country, &r.From, &r.To); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (c configQ) NumberSeriesBelow(ctx context.Context, p string) ([]dal.NumberSeriesRow, error) {
	return c.numberSeries(ctx, stSeriesBelow.SQL, p)
}

func (c configQ) NumberSeriesLike(ctx context.Context, pattern string) ([]dal.NumberSeriesRow, error) {
	return c.numberSeries(ctx, stSeriesLike.SQL, pattern)
}
