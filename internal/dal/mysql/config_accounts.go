package mysql

import (
	"context"
	"database/sql"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
)

// The accounts and companies of the config family (dal.Accounts,
// dal.Companies), moved from internal/provision (account.go, credentials.go,
// company.go, balance.go, senders.go, pushchannel.go, smppwindow.go,
// dialect.go) on 29 Oct 2026, word for word. A list read answers a row that
// would not scan, or a read cut short, as a dal.RowsError.

// prepaidSQL reads billing_config the way the engine does: only the number
// 1 is prepaid (provision's prepaidSQL, the same text).
const prepaidSQL = "COALESCE(JSON_EXTRACT(billing_config,'$.prepaid'),0) = 1"

// defaultTemplateApproval is provision.DefaultTemplateApproval: what a
// company with none stored reads as.
const defaultTemplateApproval = "ADMIN"

var (
	// ---- accounts ----
	stAccountUsernameTaken        = ConfigStmts.Add("config.accounts.username_taken", "SELECT COUNT(*) FROM sms_service_config WHERE username = ?", false)
	stAccountUsernameTakenByOther = ConfigStmts.Add("config.accounts.username_taken_by_other", "SELECT COUNT(*) FROM sms_service_config WHERE username = ? AND id <> ?", false)
	stAccountInsert               = ConfigStmts.Add("config.accounts.insert",
		"INSERT INTO sms_service_config (username,company_id,jwt,allowed_sender_ids,smpp_config,http,smpp,charge_point,deployments) VALUES (?,?,?,?,?,?,?,?,?)", false)
	stAccountList = ConfigStmts.Add("config.accounts.list", `
		SELECT a.id, a.username, COALESCE(c.name,'—'), COALESCE(a.is_disabled,0),
		       COALESCE(a.deployments,''), COALESCE(a.charge_point,'acceptance')
		  FROM sms_service_config a
		  LEFT JOIN companies c ON c.id = a.company_id
		 ORDER BY a.id LIMIT ?`, false)
	stAccountForSwitch = ConfigStmts.Add("config.accounts.for_switch", `
		SELECT a.username, COALESCE(a.company_id,0), COALESCE(c.name,''), COALESCE(a.is_disabled,0)
		  FROM sms_service_config a LEFT JOIN companies c ON c.id = a.company_id WHERE a.id = ?`, false)
	stAccountSetDisabled = ConfigStmts.Add("config.accounts.set_disabled", "UPDATE sms_service_config SET is_disabled = ? WHERE id = ?", false)
	stAccountDetail      = ConfigStmts.Add("config.accounts.detail", `
		SELECT a.id, a.username, COALESCE(c.name,'—'), COALESCE(a.is_disabled,0),
		       COALESCE(a.deployments,''), COALESCE(a.charge_point,'acceptance'),
		       COALESCE(a.company_id,0), COALESCE(a.http,0), COALESCE(a.smpp,0),
		       COALESCE(a.jwt,'{}'), COALESCE(a.smpp_config,'{}'),
		       a.dedupe_ttl_sec, COALESCE(DATE_FORMAT(a.created_at,'%Y-%m-%d'),''),
		       COALESCE(a.enforce_spam_check,1), COALESCE(a.enforce_spam_strict,0),
		       COALESCE(a.default_sender,''), COALESCE(a.allowed_sender_ids,'[]'),
		       COALESCE(a.dlr_channel,'both'), COALESCE(a.dlr_bind_attempts,3),
		       COALESCE(a.mo_channel,'both'), COALESCE(a.mo_bind_attempts,3)
		  FROM sms_service_config a
		  LEFT JOIN companies c ON c.id = a.company_id
		 WHERE a.id = ?`, false)
	stAccountSetUsername = ConfigStmts.Add("config.accounts.set_username", `UPDATE sms_service_config
			    SET username = ?, smpp_config = JSON_SET(COALESCE(smpp_config,'{}'),'$.system_id',?)
			  WHERE id = ?`, false)
	stAccountCompanyOf      = ConfigStmts.Add("config.accounts.company_of", "SELECT COALESCE(company_id,0) FROM sms_service_config WHERE id = ?", false)
	stAccountSetCompany     = ConfigStmts.Add("config.accounts.set_company", "UPDATE sms_service_config SET company_id = ? WHERE id = ?", false)
	stAccountSetDeployments = ConfigStmts.Add("config.accounts.set_deployments", "UPDATE sms_service_config SET deployments = ? WHERE id = ?", false)
	stAccountSetChargePoint = ConfigStmts.Add("config.accounts.set_charge_point", "UPDATE sms_service_config SET charge_point = ? WHERE id = ?", false)
	stAccountSetMultipart   = ConfigStmts.Add("config.accounts.set_multipart",
		`UPDATE sms_service_config SET smpp_config = JSON_SET(COALESCE(smpp_config,'{}'),'$.multipart_mode',?) WHERE id = ?`, false)
	stAccountSetHTTP          = ConfigStmts.Add("config.accounts.set_http", "UPDATE sms_service_config SET http = ? WHERE id = ?", false)
	stAccountSetSMPP          = ConfigStmts.Add("config.accounts.set_smpp", "UPDATE sms_service_config SET smpp = ? WHERE id = ?", false)
	stAccountSetSpamCheck     = ConfigStmts.Add("config.accounts.set_spam_check", "UPDATE sms_service_config SET enforce_spam_check = ? WHERE id = ?", false)
	stAccountSetSpamStrict    = ConfigStmts.Add("config.accounts.set_spam_strict", "UPDATE sms_service_config SET enforce_spam_strict = ? WHERE id = ?", false)
	stAccountSetDefaultSender = ConfigStmts.Add("config.accounts.set_default_sender", "UPDATE sms_service_config SET default_sender = NULLIF(?, '') WHERE id = ?", false)
	stAccountDelete           = ConfigStmts.Add("config.accounts.delete", "DELETE FROM sms_service_config WHERE id = ?", false)
	stAccountRules            = ConfigStmts.Add("config.accounts.rules",
		"SELECT id, COALESCE(name,''), COALESCE(direction,'') FROM routes WHERE account_id = ? ORDER BY id", false)

	// ---- senders ----
	stAccountAllowedSenders    = ConfigStmts.Add("config.accounts.allowed_senders", "SELECT allowed_sender_ids FROM sms_service_config WHERE id = ?", false)
	stAccountApprovedSenders   = ConfigStmts.Add("config.accounts.approved_senders", "SELECT sender FROM sender_ids WHERE account_id = ? AND status = 'approved' ORDER BY id", false)
	stAccountSetAllowedSenders = ConfigStmts.Add("config.accounts.set_allowed_senders", "UPDATE sms_service_config SET allowed_sender_ids = ? WHERE id = ?", false)

	// ---- push channels ----
	// The column is one of dlr_channel, dlr_bind_attempts, mo_channel,
	// mo_bind_attempts, which provision has picked.
	stAccountSetPush = ConfigStmts.Add("config.accounts.set_push", "UPDATE sms_service_config SET `dlr_channel` = ? WHERE id = ?", false)
	// The account names its webhook of each kind (config/0174, 6 Nov 2026);
	// http_endpoints.account_id is bookkeeping no rule reads.
	stAccountDLRWebhookRetry = ConfigStmts.Add("config.accounts.webhook_retry_dlr", `
		SELECT COALESCE(e.retry,'') FROM sms_service_config a
		  JOIN http_endpoints e ON e.id = a.dlr_webhook_id
		 WHERE a.id = ? AND e.type = 'dlr' AND e.is_active = 1`, false)
	stAccountMOWebhookRetry = ConfigStmts.Add("config.accounts.webhook_retry_mo", `
		SELECT COALESCE(e.retry,'') FROM sms_service_config a
		  JOIN http_endpoints e ON e.id = a.mo_webhook_id
		 WHERE a.id = ? AND e.type = 'mo' AND e.is_active = 1`, false)
	stActiveWebhookRetry = ConfigStmts.Add("config.accounts.active_webhook_retry",
		"SELECT COALESCE(retry,'') FROM http_endpoints WHERE id = ? AND type = ? AND is_active = 1", false)
	stAccountWebhookIDs = ConfigStmts.Add("config.accounts.webhook_ids",
		"SELECT COALESCE(dlr_webhook_id,0), COALESCE(mo_webhook_id,0) FROM sms_service_config WHERE id = ?", false)
	stAccountSetDLRWebhook = ConfigStmts.Add("config.accounts.set_dlr_webhook",
		"UPDATE sms_service_config SET dlr_webhook_id = ? WHERE id = ?", false)
	stAccountSetMOWebhook = ConfigStmts.Add("config.accounts.set_mo_webhook",
		"UPDATE sms_service_config SET mo_webhook_id = ? WHERE id = ?", false)

	// ---- SMPP windows ----
	// The path is "$.window_size" or "$.deliver_window", which provision has
	// picked.
	stAccountSetWindow = ConfigStmts.Add("config.accounts.set_smpp_window",
		`UPDATE sms_service_config SET smpp_config = JSON_SET(COALESCE(smpp_config,'{}'),'$.window_size',?) WHERE id = ?`, false)
	stAccountRemoveWindow = ConfigStmts.Add("config.accounts.remove_smpp_window",
		`UPDATE sms_service_config SET smpp_config = JSON_REMOVE(COALESCE(smpp_config,'{}'),'$.window_size') WHERE id = ?`, false)

	// ---- credentials ----
	stAccountSecretsTables = ConfigStmts.Add("config.accounts.secrets_tables", `
		SELECT COUNT(*) FROM information_schema.TABLES
		 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'account_secrets'`, false)
	stAccountCredentials = ConfigStmts.Add("config.accounts.credentials", `
		SELECT id, COALESCE(company_id,0), username, COALESCE(jwt,'{}'), COALESCE(smpp_config,'{}'),
		       password, COALESCE(http,0), COALESCE(smpp,0)
		  FROM sms_service_config WHERE id = ?`, false)
	stAccountCredentialsLocked = ConfigStmts.Add("config.accounts.credentials_locked", `
		SELECT id, COALESCE(company_id,0), username, COALESCE(jwt,'{}'), COALESCE(smpp_config,'{}'),
		       password, COALESCE(http,0), COALESCE(smpp,0)
		  FROM sms_service_config WHERE id = ? FOR UPDATE`, false)
	stAccountKeptSecrets = ConfigStmts.Add("config.accounts.kept_secrets", "SELECT api_token, password FROM account_secrets WHERE account_id = ?", false)
	stAccountSetJWT      = ConfigStmts.Add("config.accounts.set_jwt", "UPDATE sms_service_config SET jwt = CAST(? AS JSON) WHERE id = ?", false)
	stAccountKeepToken   = ConfigStmts.Add("config.accounts.keep_token", `
			INSERT INTO account_secrets (account_id, api_token) VALUES (?, ?)
			ON DUPLICATE KEY UPDATE api_token = VALUES(api_token)`, false)
	stAccountSetPassword = ConfigStmts.Add("config.accounts.set_password",
		"UPDATE sms_service_config SET password = ?, smpp_config = CAST(? AS JSON) WHERE id = ?", false)
	stAccountKeepPassword = ConfigStmts.Add("config.accounts.keep_password", `
			INSERT INTO account_secrets (account_id, password) VALUES (?, ?)
			ON DUPLICATE KEY UPDATE password = VALUES(password)`, false)
	stAccountClearPassword = ConfigStmts.Add("config.accounts.clear_password",
		"UPDATE sms_service_config SET password = NULL, smpp_config = CAST(? AS JSON) WHERE id = ?", false)
	stAccountClearKeptPassword = ConfigStmts.Add("config.accounts.clear_kept_password",
		"UPDATE account_secrets SET password = NULL WHERE account_id = ?", false)
	stAccountCredentialAudit = ConfigStmts.Add("config.accounts.credential_audit", `
		INSERT INTO audit_log (actor, actor_name, account_id, company_id, action, summary,
		                       entity, entity_id, target_label, feature, detail)
		VALUES ('console','console',?,?,?,?,'account',?,?,'accounts',CAST(? AS JSON))`, false)

	// ---- companies ----
	stCompanyNameType = ConfigStmts.Add("config.companies.name_type",
		"SELECT COALESCE(name,''), COALESCE(type,'') FROM companies WHERE id = ?", false)
	stCompanyList = ConfigStmts.Add("config.companies.list",
		"SELECT id, name, COALESCE(CONCAT(FORMAT(balance,4),' ',COALESCE(currency,'')),'—'), "+prepaidSQL+" FROM companies ORDER BY id LIMIT ?", false)
	stCompanyNameTaken        = ConfigStmts.Add("config.companies.name_taken", "SELECT COUNT(*) FROM companies WHERE name = ?", false)
	stCompanyNameTakenByOther = ConfigStmts.Add("config.companies.name_taken_by_other", "SELECT COUNT(*) FROM companies WHERE name = ? AND id <> ?", false)
	stCompanyInsert           = ConfigStmts.Add("config.companies.insert",
		"INSERT INTO companies (company_id, name, master_number, currency, base_conversion_rate, company_email, support_email, billing_email, rates_email, billing_config, created_by_user_id)"+
			" VALUES (?,?,?,?,?,?,?,?,?,JSON_OBJECT('prepaid', ?),'console')", false)
	stCompanyDetail = ConfigStmts.Add("config.companies.detail", `
		SELECT id, company_id, name, COALESCE(master_number,''), COALESCE(currency,''),
		       COALESCE(company_email,''), COALESCE(support_email,''),
		       COALESCE(billing_email,''), COALESCE(rates_email,''),
		       `+prepaidSQL+`, balance, COALESCE(is_disabled,0),
		       COALESCE(CAST(base_conversion_rate AS CHAR),''), COALESCE(base_thirdparty_crp_id,''),
		       COALESCE(CAST(data_retention->>'$.hot_days' AS UNSIGNED), CAST(data_retention->>'$.hot_months' AS UNSIGNED)*30, 0),
		       COALESCE(type,''), COALESCE(template_approval,'`+defaultTemplateApproval+`')
		  FROM companies WHERE id = ?`, false)
	// The column is one of the console's fixed set, which provision has picked.
	stCompanySetColumn = ConfigStmts.Add("config.companies.set_column",
		"UPDATE companies SET `name` = ?, updated_at = GREATEST(CURRENT_TIMESTAMP, updated_at + INTERVAL 1 SECOND) WHERE id = ?", false)
	stCompanySetHotDays = ConfigStmts.Add("config.companies.set_hot_days", `UPDATE companies
		   SET data_retention = JSON_REMOVE(JSON_SET(IF(JSON_TYPE(data_retention) = 'OBJECT', data_retention, JSON_OBJECT()), '$.hot_days', ?), '$.hot_months'),
		       updated_at = GREATEST(CURRENT_TIMESTAMP, updated_at + INTERVAL 1 SECOND)
		 WHERE id = ?`, false)
	stCompanyClearHotDays = ConfigStmts.Add("config.companies.clear_hot_days", `UPDATE companies
		   SET data_retention = IF(JSON_TYPE(data_retention) = 'OBJECT', JSON_REMOVE(data_retention, '$.hot_days', '$.hot_months'), data_retention),
		       updated_at = GREATEST(CURRENT_TIMESTAMP, updated_at + INTERVAL 1 SECOND)
		 WHERE id = ?`, false)
	stCompanyBillingConfig = ConfigStmts.Add("config.companies.billing_config", "SELECT billing_config FROM companies WHERE id = ?", false)
	stCompanySetPrepaid    = ConfigStmts.Add("config.companies.set_prepaid", `
		UPDATE companies
		   SET billing_config = JSON_SET(IF(JSON_TYPE(billing_config) = 'OBJECT', billing_config, JSON_OBJECT()), '$.prepaid', ?),
		       updated_at = GREATEST(CURRENT_TIMESTAMP, updated_at + INTERVAL 1 SECOND)
		 WHERE id = ?`, false)
	stCompanyName = ConfigStmts.Add("config.companies.name", "SELECT COALESCE(name,'') FROM companies WHERE id = ?", false)

	// ---- the balance, as a person moves it ----
	stCompanyBalanceForUpdate = ConfigStmts.Add("config.companies.balance_for_update",
		"SELECT name, balance, COALESCE(currency,''), "+prepaidSQL+" FROM companies WHERE id = ? FOR UPDATE", false)
	stCompanySetBalanceText = ConfigStmts.Add("config.companies.set_balance", "UPDATE companies SET balance = ? WHERE id = ?", false)
	stCompanyBalanceChange  = ConfigStmts.Add("config.companies.balance_change", `
		INSERT INTO balance_history (account_id, company_id, type, amount, previous_balance, new_balance,
		                             reference, reason, created_by_user_id)
		VALUES (0,?,?,?,?,?,?,?,'console')`, false)
	stCompanyBalanceAudit = ConfigStmts.Add("config.companies.balance_audit", `
		INSERT INTO audit_log (actor, actor_name, company_id, company_name, action, summary,
		                       entity, entity_id, target_label, feature, detail)
		VALUES ('console','console',?,?,'companies.balance.adjust',?,'company',?,?,'companies',CAST(? AS JSON))`, false)
)

// ---- accounts ----

func (c configQ) AccountUsernameTaken(ctx context.Context, username string) (bool, error) {
	return c.exists(ctx, stAccountUsernameTaken, username)
}

func (c configQ) AccountUsernameTakenByOther(ctx context.Context, username string, id int64) (bool, error) {
	return c.exists(ctx, stAccountUsernameTakenByOther, username, id)
}

// InsertAccount names only the columns given: charge_point and deployments
// are left to their defaults when not set, as before.
func (c configQ) InsertAccount(ctx context.Context, a dal.AccountInsert) (int64, error) {
	_ = stAccountInsert
	cols := []string{"username", "company_id", "jwt", "allowed_sender_ids", "smpp_config", "http", "smpp"}
	vals := []any{a.Username, a.CompanyID, a.JWT, a.AllowedSenders, a.SMPPConfig, a.HTTP, a.SMPP}
	if a.ChargePoint != "" {
		cols = append(cols, "charge_point")
		vals = append(vals, a.ChargePoint)
	}
	if a.Deployments.Valid {
		cols = append(cols, "deployments")
		vals = append(vals, a.Deployments.String)
	}
	q := "INSERT INTO sms_service_config (" + strings.Join(cols, ",") + ") VALUES (" +
		strings.TrimSuffix(strings.Repeat("?,", len(cols)), ",") + ")"
	res, err := c.q.ExecContext(ctx, q, vals...)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (c configQ) AccountList(ctx context.Context, limit int) ([]dal.AccountListRow, error) {
	rows, err := c.q.QueryContext(ctx, stAccountList.SQL, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.AccountListRow
	for rows.Next() {
		var r dal.AccountListRow
		if err := rows.Scan(&r.ID, &r.Username, &r.Company, &r.Disabled, &r.Deployments, &r.ChargePoint); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, r)
	}
	return out, accountRowsErr(rows)
}

func (c configQ) AccountForSwitch(ctx context.Context, id int64) (dal.AccountSwitchRow, error) {
	var r dal.AccountSwitchRow
	err := c.q.QueryRowContext(ctx, stAccountForSwitch.SQL, id).Scan(&r.Username, &r.CompanyID, &r.Company, &r.Disabled)
	return r, err
}

func (c configQ) SetAccountDisabled(ctx context.Context, id int64, disabled int) error {
	_, err := c.q.ExecContext(ctx, stAccountSetDisabled.SQL, disabled, id)
	return err
}

func (c configQ) AccountDetail(ctx context.Context, id int64) (dal.AccountDetailRow, error) {
	var d dal.AccountDetailRow
	err := c.q.QueryRowContext(ctx, stAccountDetail.SQL, id).
		Scan(&d.ID, &d.Username, &d.Company, &d.Disabled, &d.Deployments, &d.ChargePoint,
			&d.CompanyID, &d.HTTP, &d.SMPP, &d.JWT, &d.SMPPConfig, &d.DedupeTTL, &d.Created,
			&d.SpamCheck, &d.SpamStrict, &d.DefaultSender, &d.AllowedSenders,
			&d.DLRChannel, &d.DLRBindAttempts, &d.MOChannel, &d.MOBindAttempts)
	return d, err
}

func (c configQ) SetAccountUsername(ctx context.Context, id int64, username string) error {
	_, err := c.q.ExecContext(ctx, stAccountSetUsername.SQL, username, username, id)
	return err
}

func (c configQ) AccountCompanyOf(ctx context.Context, id int64) (int64, error) {
	var has int64
	err := c.q.QueryRowContext(ctx, stAccountCompanyOf.SQL, id).Scan(&has)
	return has, err
}

func (c configQ) SetAccountCompany(ctx context.Context, id, companyID int64) error {
	_, err := c.q.ExecContext(ctx, stAccountSetCompany.SQL, companyID, id)
	return err
}

func (c configQ) SetAccountDeployments(ctx context.Context, id int64, deploymentsJSON string) error {
	_, err := c.q.ExecContext(ctx, stAccountSetDeployments.SQL, deploymentsJSON, id)
	return err
}

func (c configQ) SetAccountChargePoint(ctx context.Context, id int64, point string) error {
	_, err := c.q.ExecContext(ctx, stAccountSetChargePoint.SQL, point, id)
	return err
}

func (c configQ) SetAccountMultipart(ctx context.Context, id int64, mode string) error {
	_, err := c.q.ExecContext(ctx, stAccountSetMultipart.SQL, mode, id)
	return err
}

func (c configQ) SetAccountHTTP(ctx context.Context, id int64, on int) error {
	_, err := c.q.ExecContext(ctx, stAccountSetHTTP.SQL, on, id)
	return err
}

func (c configQ) SetAccountSMPP(ctx context.Context, id int64, on int) error {
	_, err := c.q.ExecContext(ctx, stAccountSetSMPP.SQL, on, id)
	return err
}

func (c configQ) SetAccountSpamCheck(ctx context.Context, id int64, on int) error {
	_, err := c.q.ExecContext(ctx, stAccountSetSpamCheck.SQL, on, id)
	return err
}

func (c configQ) SetAccountSpamStrict(ctx context.Context, id int64, on int) error {
	_, err := c.q.ExecContext(ctx, stAccountSetSpamStrict.SQL, on, id)
	return err
}

func (c configQ) SetAccountDefaultSender(ctx context.Context, id int64, sender string) error {
	_, err := c.q.ExecContext(ctx, stAccountSetDefaultSender.SQL, sender, id)
	return err
}

func (c configQ) DeleteAccountRow(ctx context.Context, id int64) (int64, error) {
	res, err := c.q.ExecContext(ctx, stAccountDelete.SQL, id)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (c configQ) AccountRuleRows(ctx context.Context, id int64) ([]dal.AccountRuleRow, error) {
	rows, err := c.q.QueryContext(ctx, stAccountRules.SQL, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.AccountRuleRow
	for rows.Next() {
		var r dal.AccountRuleRow
		if err := rows.Scan(&r.ID, &r.Name, &r.Direction); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, r)
	}
	return out, accountRowsErr(rows)
}

// ---- senders ----

func (c configQ) AccountAllowedSenders(ctx context.Context, id int64) (dal.NullString, error) {
	var raw dal.NullString
	err := c.q.QueryRowContext(ctx, stAccountAllowedSenders.SQL, id).Scan(&raw)
	return raw, err
}

func (c configQ) ApprovedSenderRows(ctx context.Context, id int64) ([]string, error) {
	rows, err := c.q.QueryContext(ctx, stAccountApprovedSenders.SQL, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, v)
	}
	return out, accountRowsErr(rows)
}

func (c configQ) SetAccountAllowedSenders(ctx context.Context, id int64, listJSON string) error {
	_, err := c.q.ExecContext(ctx, stAccountSetAllowedSenders.SQL, listJSON, id)
	return err
}

// ---- push channels ----

func (c configQ) SetAccountPushSetting(ctx context.Context, id int64, column string, value any) error {
	_ = stAccountSetPush
	_, err := c.q.ExecContext(ctx, "UPDATE sms_service_config SET `"+column+"` = ? WHERE id = ?", value, id)
	return err
}

func (c configQ) AccountWebhookRetry(ctx context.Context, accountID int64, kind string) (string, error) {
	q := stAccountDLRWebhookRetry.SQL
	if kind == "mo" {
		q = stAccountMOWebhookRetry.SQL
	} else if kind != "dlr" {
		return "", dal.ErrNoRows
	}
	var retry string
	err := c.q.QueryRowContext(ctx, q, accountID).Scan(&retry)
	return retry, err
}

func (c configQ) ActiveWebhookRetry(ctx context.Context, endpointID int64, kind string) (string, error) {
	var retry string
	err := c.q.QueryRowContext(ctx, stActiveWebhookRetry.SQL, endpointID, kind).Scan(&retry)
	return retry, err
}

func (c configQ) AccountWebhookIDs(ctx context.Context, accountID int64) (dlr, mo int64, err error) {
	err = c.q.QueryRowContext(ctx, stAccountWebhookIDs.SQL, accountID).Scan(&dlr, &mo)
	return dlr, mo, err
}

func (c configQ) SetAccountWebhookID(ctx context.Context, accountID int64, kind string, endpointID int64) error {
	q := stAccountSetDLRWebhook.SQL
	if kind == "mo" {
		q = stAccountSetMOWebhook.SQL
	}
	_, err := c.q.ExecContext(ctx, q, nullID(endpointID), accountID)
	return err
}

// ---- SMPP windows ----

func (c configQ) SetAccountSMPPWindow(ctx context.Context, id int64, path string, n int) error {
	_ = stAccountSetWindow
	_, err := c.q.ExecContext(ctx,
		`UPDATE sms_service_config SET smpp_config = JSON_SET(COALESCE(smpp_config,'{}'),'`+path+`',?) WHERE id = ?`,
		n, id)
	return err
}

func (c configQ) RemoveAccountSMPPWindow(ctx context.Context, id int64, path string) error {
	_ = stAccountRemoveWindow
	_, err := c.q.ExecContext(ctx,
		`UPDATE sms_service_config SET smpp_config = JSON_REMOVE(COALESCE(smpp_config,'{}'),'`+path+`') WHERE id = ?`,
		id)
	return err
}

// ---- credentials ----

func (c configQ) AccountSecretsTables(ctx context.Context) (int, error) {
	return c.count(ctx, stAccountSecretsTables)
}

func (c configQ) AccountCredentials(ctx context.Context, id int64, lock bool) (dal.AccountCredRow, error) {
	st := stAccountCredentials
	if lock {
		st = stAccountCredentialsLocked
	}
	var r dal.AccountCredRow
	err := c.q.QueryRowContext(ctx, st.SQL, id).
		Scan(&r.ID, &r.Company, &r.Username, &r.JWT, &r.SMPPConfig, &r.Password, &r.HTTP, &r.SMPP)
	return r, err
}

func (c configQ) AccountKeptSecrets(ctx context.Context, id int64) (token, password dal.NullString, err error) {
	err = c.q.QueryRowContext(ctx, stAccountKeptSecrets.SQL, id).Scan(&token, &password)
	return token, password, err
}

func (c configQ) SetAccountJWT(ctx context.Context, id int64, jwtJSON string) error {
	_, err := c.q.ExecContext(ctx, stAccountSetJWT.SQL, jwtJSON, id)
	return err
}

func (c configQ) KeepAccountToken(ctx context.Context, id int64, token string) error {
	_, err := c.q.ExecContext(ctx, stAccountKeepToken.SQL, id, token)
	return err
}

func (c configQ) SetAccountPassword(ctx context.Context, id int64, hash, smppJSON string) error {
	_, err := c.q.ExecContext(ctx, stAccountSetPassword.SQL, hash, smppJSON, id)
	return err
}

func (c configQ) KeepAccountPassword(ctx context.Context, id int64, plaintext string) error {
	_, err := c.q.ExecContext(ctx, stAccountKeepPassword.SQL, id, plaintext)
	return err
}

func (c configQ) ClearAccountPassword(ctx context.Context, id int64, smppJSON dal.NullString) error {
	var v any
	if smppJSON.Valid {
		v = smppJSON.String
	}
	_, err := c.q.ExecContext(ctx, stAccountClearPassword.SQL, v, id)
	return err
}

func (c configQ) ClearKeptPassword(ctx context.Context, id int64) error {
	_, err := c.q.ExecContext(ctx, stAccountClearKeptPassword.SQL, id)
	return err
}

func (c configQ) WriteCredentialAudit(ctx context.Context, r dal.CredentialAuditRow) error {
	var d any
	if r.Detail.Valid {
		d = r.Detail.String
	}
	var co any
	if r.CompanyID > 0 {
		co = r.CompanyID
	}
	_, err := c.q.ExecContext(ctx, stAccountCredentialAudit.SQL,
		r.AccountID, co, r.Action, r.Summary, strconv.FormatInt(r.AccountID, 10), r.Username, d)
	return err
}

// ---- companies ----

func (c configQ) CompanyNameType(ctx context.Context, id int64) (name, typ string, err error) {
	err = c.q.QueryRowContext(ctx, stCompanyNameType.SQL, id).Scan(&name, &typ)
	return name, typ, err
}

func (c configQ) CompanyList(ctx context.Context, limit int) ([]dal.CompanyListRow, error) {
	rows, err := c.q.QueryContext(ctx, stCompanyList.SQL, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.CompanyListRow
	for rows.Next() {
		var r dal.CompanyListRow
		if err := rows.Scan(&r.ID, &r.Name, &r.Balance, &r.Prepaid); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, r)
	}
	return out, accountRowsErr(rows)
}

func (c configQ) CompanyNameTaken(ctx context.Context, name string) (bool, error) {
	return c.exists(ctx, stCompanyNameTaken, name)
}

func (c configQ) CompanyNameTakenByOther(ctx context.Context, name string, id int64) (bool, error) {
	return c.exists(ctx, stCompanyNameTakenByOther, name, id)
}

func (c configQ) InsertCompany(ctx context.Context, r dal.CompanyInsert) (int64, error) {
	res, err := c.q.ExecContext(ctx, stCompanyInsert.SQL,
		r.UUID, r.Name, nullStr(r.MasterNumber), r.Currency, r.BaseRate, r.CompanyEmail, r.SupportEmail, r.BillingEmail, r.RatesEmail, r.Prepaid)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (c configQ) CompanyDetail(ctx context.Context, id int64) (dal.CompanyDetailRow, error) {
	var d dal.CompanyDetailRow
	err := c.q.QueryRowContext(ctx, stCompanyDetail.SQL, id).
		Scan(&d.ID, &d.UUID, &d.Name, &d.MasterNumber, &d.Currency, &d.CompanyEmail, &d.SupportEmail,
			&d.BillingEmail, &d.RatesEmail, &d.Prepaid, &d.Balance, &d.Disabled, &d.BaseRate, &d.BaseProvider, &d.HotDays,
			&d.Type, &d.TemplateApproval)
	return d, err
}

func (c configQ) SetCompanyColumn(ctx context.Context, id int64, column string, value dal.NullString) error {
	_ = stCompanySetColumn
	var v any
	if value.Valid {
		v = value.String
	}
	_, err := c.q.ExecContext(ctx,
		"UPDATE companies SET `"+column+"` = ?, updated_at = GREATEST(CURRENT_TIMESTAMP, updated_at + INTERVAL 1 SECOND) WHERE id = ?",
		v, id)
	return err
}

func (c configQ) SetCompanyHotDays(ctx context.Context, id int64, days int) error {
	_, err := c.q.ExecContext(ctx, stCompanySetHotDays.SQL, days, id)
	return err
}

func (c configQ) ClearCompanyHotDays(ctx context.Context, id int64) error {
	_, err := c.q.ExecContext(ctx, stCompanyClearHotDays.SQL, id)
	return err
}

func (c configQ) CompanyBillingConfig(ctx context.Context, id int64) (dal.NullString, error) {
	var v dal.NullString
	err := c.q.QueryRowContext(ctx, stCompanyBillingConfig.SQL, id).Scan(&v)
	return v, err
}

func (c configQ) SetCompanyPrepaid(ctx context.Context, id int64, prepaid int) error {
	_, err := c.q.ExecContext(ctx, stCompanySetPrepaid.SQL, prepaid, id)
	return err
}

func (c configQ) CompanyName(ctx context.Context, id int64) (string, error) {
	var name string
	err := c.q.QueryRowContext(ctx, stCompanyName.SQL, id).Scan(&name)
	return name, err
}

// ---- the balance, as a person moves it ----

func (c configQ) CompanyBalanceForUpdate(ctx context.Context, id int64) (dal.CompanyBalanceRow, error) {
	var r dal.CompanyBalanceRow
	err := c.q.QueryRowContext(ctx, stCompanyBalanceForUpdate.SQL, id).Scan(&r.Name, &r.Balance, &r.Currency, &r.Prepaid)
	return r, err
}

func (c configQ) SetCompanyBalanceText(ctx context.Context, id int64, balance string) error {
	_, err := c.q.ExecContext(ctx, stCompanySetBalanceText.SQL, balance, id)
	return err
}

func (c configQ) InsertBalanceChange(ctx context.Context, r dal.BalanceChangeRow) error {
	_, err := c.q.ExecContext(ctx, stCompanyBalanceChange.SQL,
		r.CompanyID, r.Type, r.Amount, r.Previous, r.New, r.Reference, r.Reason)
	return err
}

func (c configQ) WriteBalanceAudit(ctx context.Context, companyID int64, name, summary, detailJSON string) error {
	_, err := c.q.ExecContext(ctx, stCompanyBalanceAudit.SQL,
		companyID, name, summary, strconv.FormatInt(companyID, 10), name, detailJSON)
	return err
}

// accountRowsErr is a read's own error after its rows, as a dal.RowsError.
func accountRowsErr(rows *sql.Rows) error {
	if err := rows.Err(); err != nil {
		return dal.RowsError{Err: err}
	}
	return nil
}
