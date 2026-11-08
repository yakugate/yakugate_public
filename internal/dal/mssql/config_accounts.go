package mssql

import (
	"context"
	"database/sql"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
)

// The accounts and companies of the config family (dal.Accounts,
// dal.Companies) on SQL Server, made like internal/dal/mysql/config_accounts.go.
// A list read answers a row that would not scan, or a read cut short, as a
// dal.RowsError.
//
// MySQL keeps updated_at itself (ON UPDATE CURRENT_TIMESTAMP); here every
// UPDATE of sms_service_config, companies and account_secrets sets it.

// prepaidSQL reads billing_config the way the engine does: only the NUMBER
// 1 is prepaid (MySQL's COALESCE(JSON_EXTRACT(…,'$.prepaid'),0) = 1 — a JSON
// true or the string "1" is not).
var prepaidSQL = prepaidOf("billing_config")

// prepaidOf is prepaidSQL on a billing_config column named col.
func prepaidOf(col string) string {
	return "CASE WHEN ISJSON(" + col + ", OBJECT) = 1 THEN" +
		" CASE WHEN EXISTS (SELECT 1 FROM OPENJSON(" + col + ") WHERE [key] = 'prepaid' AND [type] = 2" +
		" AND TRY_CAST([value] AS FLOAT) = 1) THEN 1 ELSE 0 END ELSE 0 END"
}

// defaultTemplateApproval is provision.DefaultTemplateApproval: what a
// company with none stored reads as.
const defaultTemplateApproval = "ADMIN"

// stampAccount is the updated_at MySQL moves on every change of an account.
const stampAccount = ", updated_at = SYSUTCDATETIME()"

// companyStamp moves a company's updated_at on by at least a second (the
// console's rule, as on MySQL).
const companyStamp = "updated_at = GREATEST(SYSUTCDATETIME(), DATEADD(SECOND, 1, updated_at))"

var (
	// ---- accounts ----
	stAccountUsernameTaken        = addConfig("config.accounts.username_taken", "SELECT COUNT(*) FROM sms_service_config WHERE username = ?")
	stAccountUsernameTakenByOther = addConfig("config.accounts.username_taken_by_other", "SELECT COUNT(*) FROM sms_service_config WHERE username = ? AND id <> ?")
	stAccountInsert               = addConfig("config.accounts.insert",
		"INSERT INTO sms_service_config (username,company_id,jwt,allowed_sender_ids,smpp_config,http,smpp,charge_point,deployments) OUTPUT INSERTED.id VALUES (?,?,?,?,?,?,?,?,?)")
	stAccountList = addConfig("config.accounts.list", `
		SELECT TOP (@p1) a.id, a.username, COALESCE(c.name,N'—'), COALESCE(a.is_disabled,0),
		       COALESCE(a.deployments,''), COALESCE(a.charge_point,'acceptance')
		  FROM sms_service_config a
		  LEFT JOIN companies c ON c.id = a.company_id
		 ORDER BY a.id`)
	stAccountForSwitch = addConfig("config.accounts.for_switch", `
		SELECT a.username, COALESCE(a.company_id,0), COALESCE(c.name,''), COALESCE(a.is_disabled,0)
		  FROM sms_service_config a LEFT JOIN companies c ON c.id = a.company_id WHERE a.id = ?`)
	stAccountSetDisabled = addConfig("config.accounts.set_disabled", "UPDATE sms_service_config SET is_disabled = ?"+stampAccount+" WHERE id = ?")
	stAccountDetail      = addConfig("config.accounts.detail", `
		SELECT a.id, a.username, COALESCE(c.name,N'—'), COALESCE(a.is_disabled,0),
		       COALESCE(a.deployments,''), COALESCE(a.charge_point,'acceptance'),
		       COALESCE(a.company_id,0), COALESCE(a.http,0), COALESCE(a.smpp,0),
		       COALESCE(a.jwt,'{}'), COALESCE(a.smpp_config,'{}'),
		       a.dedupe_ttl_sec, COALESCE(CONVERT(CHAR(10), a.created_at, 23),''),
		       COALESCE(a.enforce_spam_check,1), COALESCE(a.enforce_spam_strict,0),
		       COALESCE(a.default_sender,''), COALESCE(a.allowed_sender_ids,'[]'),
		       COALESCE(a.dlr_channel,'both'), COALESCE(a.dlr_bind_attempts,3),
		       COALESCE(a.mo_channel,'both'), COALESCE(a.mo_bind_attempts,3)
		  FROM sms_service_config a
		  LEFT JOIN companies c ON c.id = a.company_id
		 WHERE a.id = ?`)
	stAccountSetUsername = addConfig("config.accounts.set_username", `UPDATE sms_service_config
			    SET username = ?, smpp_config = JSON_MODIFY(COALESCE(smpp_config,N'{}'),'$.system_id',?)`+stampAccount+`
			  WHERE id = ?`)
	stAccountCompanyOf      = addConfig("config.accounts.company_of", "SELECT COALESCE(company_id,0) FROM sms_service_config WHERE id = ?")
	stAccountSetCompany     = addConfig("config.accounts.set_company", "UPDATE sms_service_config SET company_id = ?"+stampAccount+" WHERE id = ?")
	stAccountSetDeployments = addConfig("config.accounts.set_deployments", "UPDATE sms_service_config SET deployments = ?"+stampAccount+" WHERE id = ?")
	stAccountSetChargePoint = addConfig("config.accounts.set_charge_point", "UPDATE sms_service_config SET charge_point = ?"+stampAccount+" WHERE id = ?")
	stAccountSetMultipart   = addConfig("config.accounts.set_multipart",
		`UPDATE sms_service_config SET smpp_config = JSON_MODIFY(COALESCE(smpp_config,N'{}'),'$.multipart_mode',?)`+stampAccount+` WHERE id = ?`)
	stAccountSetHTTP          = addConfig("config.accounts.set_http", "UPDATE sms_service_config SET http = ?"+stampAccount+" WHERE id = ?")
	stAccountSetSMPP          = addConfig("config.accounts.set_smpp", "UPDATE sms_service_config SET smpp = ?"+stampAccount+" WHERE id = ?")
	stAccountSetSpamCheck     = addConfig("config.accounts.set_spam_check", "UPDATE sms_service_config SET enforce_spam_check = ?"+stampAccount+" WHERE id = ?")
	stAccountSetSpamStrict    = addConfig("config.accounts.set_spam_strict", "UPDATE sms_service_config SET enforce_spam_strict = ?"+stampAccount+" WHERE id = ?")
	stAccountSetDefaultSender = addConfig("config.accounts.set_default_sender", "UPDATE sms_service_config SET default_sender = NULLIF(?, '')"+stampAccount+" WHERE id = ?")
	stAccountDelete           = addConfig("config.accounts.delete", "DELETE FROM sms_service_config WHERE id = ?")
	stAccountRules            = addConfig("config.accounts.rules",
		"SELECT id, COALESCE(name,''), COALESCE(direction,'') FROM routes WHERE account_id = ? ORDER BY id")

	// ---- senders ----
	stAccountAllowedSenders    = addConfig("config.accounts.allowed_senders", "SELECT allowed_sender_ids FROM sms_service_config WHERE id = ?")
	stAccountApprovedSenders   = addConfig("config.accounts.approved_senders", "SELECT sender FROM sender_ids WHERE account_id = ? AND status = 'approved' ORDER BY id")
	stAccountSetAllowedSenders = addConfig("config.accounts.set_allowed_senders", "UPDATE sms_service_config SET allowed_sender_ids = ?"+stampAccount+" WHERE id = ?")

	// ---- push channels ----
	// The column is one of dlr_channel, dlr_bind_attempts, mo_channel,
	// mo_bind_attempts, which provision has picked.
	stAccountSetPush = addConfig("config.accounts.set_push", "UPDATE sms_service_config SET [dlr_channel] = ?"+stampAccount+" WHERE id = ?")
	// The account names its webhook of each kind (config/0174, 6 Nov 2026);
	// http_endpoints.account_id is bookkeeping no rule reads.
	stAccountDLRWebhookRetry = addConfig("config.accounts.webhook_retry_dlr", `
		SELECT COALESCE(e.retry,'') FROM sms_service_config a
		  JOIN http_endpoints e ON e.id = a.dlr_webhook_id
		 WHERE a.id = ? AND e.type = 'dlr' AND e.is_active = 1`)
	stAccountMOWebhookRetry = addConfig("config.accounts.webhook_retry_mo", `
		SELECT COALESCE(e.retry,'') FROM sms_service_config a
		  JOIN http_endpoints e ON e.id = a.mo_webhook_id
		 WHERE a.id = ? AND e.type = 'mo' AND e.is_active = 1`)
	stActiveWebhookRetry = addConfig("config.accounts.active_webhook_retry",
		"SELECT COALESCE(retry,'') FROM http_endpoints WHERE id = ? AND type = ? AND is_active = 1")
	stAccountWebhookIDs = addConfig("config.accounts.webhook_ids",
		"SELECT COALESCE(dlr_webhook_id,0), COALESCE(mo_webhook_id,0) FROM sms_service_config WHERE id = ?")
	stAccountSetDLRWebhook = addConfig("config.accounts.set_dlr_webhook",
		"UPDATE sms_service_config SET dlr_webhook_id = ?"+stampAccount+" WHERE id = ?")
	stAccountSetMOWebhook = addConfig("config.accounts.set_mo_webhook",
		"UPDATE sms_service_config SET mo_webhook_id = ?"+stampAccount+" WHERE id = ?")

	// ---- SMPP windows ----
	// The path is "$.window_size" or "$.deliver_window", which provision has
	// picked. JSON_MODIFY with NULL removes the key (JSON_REMOVE).
	stAccountSetWindow = addConfig("config.accounts.set_smpp_window",
		`UPDATE sms_service_config SET smpp_config = JSON_MODIFY(COALESCE(smpp_config,N'{}'),'$.window_size',?)`+stampAccount+` WHERE id = ?`)
	stAccountRemoveWindow = addConfig("config.accounts.remove_smpp_window",
		`UPDATE sms_service_config SET smpp_config = JSON_MODIFY(COALESCE(smpp_config,N'{}'),'$.window_size',NULL)`+stampAccount+` WHERE id = ?`)

	// ---- credentials ----
	stAccountSecretsTables = addConfig("config.accounts.secrets_tables",
		`SELECT COUNT(*) FROM sys.tables WHERE name = 'account_secrets' AND schema_id = SCHEMA_ID()`)
	stAccountCredentials = addConfig("config.accounts.credentials", `
		SELECT id, COALESCE(company_id,0), username, COALESCE(jwt,'{}'), COALESCE(smpp_config,'{}'),
		       password, COALESCE(http,0), COALESCE(smpp,0)
		  FROM sms_service_config WHERE id = ?`)
	// FOR UPDATE: the row's update lock, held to the end of the transaction.
	stAccountCredentialsLocked = addConfig("config.accounts.credentials_locked", `
		SELECT id, COALESCE(company_id,0), username, COALESCE(jwt,'{}'), COALESCE(smpp_config,'{}'),
		       password, COALESCE(http,0), COALESCE(smpp,0)
		  FROM sms_service_config WITH (UPDLOCK, ROWLOCK) WHERE id = ?`)
	stAccountKeptSecrets = addConfig("config.accounts.kept_secrets", "SELECT api_token, password FROM account_secrets WHERE account_id = ?")
	stAccountSetJWT      = addConfig("config.accounts.set_jwt", "UPDATE sms_service_config SET jwt = ?"+stampAccount+" WHERE id = ?")
	// ON DUPLICATE KEY UPDATE: the Upsert shape (helpers_upsert.go).
	stAccountKeepToken = AddUpsert(ConfigStmts, "config.accounts.keep_token", UpsertSpec{
		Table: "account_secrets", Keys: []string{"account_id"},
		Set:    []string{"api_token = $api_token", "updated_at = SYSUTCDATETIME()"},
		Insert: []string{"account_id", "api_token"},
	}, false)
	stAccountSetPassword = addConfig("config.accounts.set_password",
		"UPDATE sms_service_config SET password = ?, smpp_config = ?"+stampAccount+" WHERE id = ?")
	stAccountKeepPassword = AddUpsert(ConfigStmts, "config.accounts.keep_password", UpsertSpec{
		Table: "account_secrets", Keys: []string{"account_id"},
		Set:    []string{"password = $password", "updated_at = SYSUTCDATETIME()"},
		Insert: []string{"account_id", "password"},
	}, false)
	stAccountClearPassword = addConfig("config.accounts.clear_password",
		"UPDATE sms_service_config SET password = NULL, smpp_config = ?"+stampAccount+" WHERE id = ?")
	stAccountClearKeptPassword = addConfig("config.accounts.clear_kept_password",
		"UPDATE account_secrets SET password = NULL, updated_at = SYSUTCDATETIME() WHERE account_id = ?")
	stAccountCredentialAudit = addConfig("config.accounts.credential_audit", `
		INSERT INTO audit_log (actor, actor_name, account_id, company_id, action, summary,
		                       entity, entity_id, target_label, feature, detail)
		VALUES ('console','console',?,?,?,?,'account',?,?,'accounts',?)`)

	// ---- companies ----
	stCompanyNameType = addConfig("config.companies.name_type",
		"SELECT COALESCE(name,''), COALESCE(type,'') FROM companies WHERE id = ?")
	// MySQL's FORMAT(balance,4): thousands with commas, four decimals.
	stCompanyList = addConfig("config.companies.list",
		"SELECT TOP (@p1) id, name, CASE WHEN balance IS NULL THEN N'—' ELSE CONCAT(FORMAT(balance,'N4','en-US'),N' ',COALESCE(RTRIM(currency),'')) END, "+
			prepaidSQL+" FROM companies ORDER BY id")
	stCompanyNameTaken        = addConfig("config.companies.name_taken", "SELECT COUNT(*) FROM companies WHERE name = ?")
	stCompanyNameTakenByOther = addConfig("config.companies.name_taken_by_other", "SELECT COUNT(*) FROM companies WHERE name = ? AND id <> ?")
	stCompanyInsert           = addConfig("config.companies.insert",
		"INSERT INTO companies (company_id, name, master_number, currency, base_conversion_rate, company_email, support_email, billing_email, rates_email, billing_config, created_by_user_id)"+
			" OUTPUT INSERTED.id VALUES (?,?,?,?,?,?,?,?,?,JSON_OBJECT('prepaid': ?),'console')")
	stCompanyDetail = addConfig("config.companies.detail", `
		SELECT id, RTRIM(company_id), name, COALESCE(master_number,''), COALESCE(RTRIM(currency),''),
		       COALESCE(company_email,''), COALESCE(support_email,''),
		       COALESCE(billing_email,''), COALESCE(rates_email,''),
		       `+prepaidSQL+`, balance, COALESCE(is_disabled,0),
		       COALESCE(CAST(base_conversion_rate AS NVARCHAR(40)),''), COALESCE(base_thirdparty_crp_id,''),
		       COALESCE(TRY_CAST(JSON_VALUE(data_retention,'$.hot_days') AS BIGINT),
		                TRY_CAST(JSON_VALUE(data_retention,'$.hot_months') AS BIGINT)*30, 0),
		       COALESCE(type,''), COALESCE(template_approval,'`+defaultTemplateApproval+`')
		  FROM companies WHERE id = ?`)
	// The column is one of the console's fixed set, which provision has picked.
	stCompanySetColumn = addConfig("config.companies.set_column",
		"UPDATE companies SET [name] = ?, "+companyStamp+" WHERE id = ?")
	stCompanySetHotDays = addConfig("config.companies.set_hot_days", `UPDATE companies
		   SET data_retention = JSON_MODIFY(JSON_MODIFY(CASE WHEN ISJSON(data_retention, OBJECT) = 1 THEN data_retention ELSE N'{}' END,
		                                                '$.hot_days', ?), '$.hot_months', NULL),
		       `+companyStamp+`
		 WHERE id = ?`)
	stCompanyClearHotDays = addConfig("config.companies.clear_hot_days", `UPDATE companies
		   SET data_retention = CASE WHEN ISJSON(data_retention, OBJECT) = 1
		                             THEN JSON_MODIFY(JSON_MODIFY(data_retention, '$.hot_days', NULL), '$.hot_months', NULL)
		                             ELSE data_retention END,
		       `+companyStamp+`
		 WHERE id = ?`)
	stCompanyBillingConfig = addConfig("config.companies.billing_config", "SELECT billing_config FROM companies WHERE id = ?")
	stCompanySetPrepaid    = addConfig("config.companies.set_prepaid", `
		UPDATE companies
		   SET billing_config = JSON_MODIFY(CASE WHEN ISJSON(billing_config, OBJECT) = 1 THEN billing_config ELSE N'{}' END, '$.prepaid', ?),
		       `+companyStamp+`
		 WHERE id = ?`)
	stCompanyName = addConfig("config.companies.name", "SELECT COALESCE(name,'') FROM companies WHERE id = ?")

	// ---- the balance, as a person moves it ----
	stCompanyBalanceForUpdate = addConfig("config.companies.balance_for_update",
		"SELECT name, balance, COALESCE(RTRIM(currency),''), "+prepaidSQL+" FROM companies WITH (UPDLOCK, ROWLOCK) WHERE id = ?")
	stCompanySetBalanceText = addConfig("config.companies.set_balance", "UPDATE companies SET balance = ?, updated_at = SYSUTCDATETIME() WHERE id = ?")
	stCompanyBalanceChange  = addConfig("config.companies.balance_change", `
		INSERT INTO balance_history (account_id, company_id, type, amount, previous_balance, new_balance,
		                             reference, reason, created_by_user_id)
		VALUES (0,?,?,?,?,?,?,?,'console')`)
	stCompanyBalanceAudit = addConfig("config.companies.balance_audit", `
		INSERT INTO audit_log (actor, actor_name, company_id, company_name, action, summary,
		                       entity, entity_id, target_label, feature, detail)
		VALUES ('console','console',?,?,'companies.balance.adjust',?,'company',?,?,'companies',?)`)
)

// ---- accounts ----

func (c configQ) AccountUsernameTaken(ctx context.Context, username string) (bool, error) {
	return c.exists(ctx, stAccountUsernameTaken, username)
}

func (c configQ) AccountUsernameTakenByOther(ctx context.Context, username string, id int64) (bool, error) {
	return c.exists(ctx, stAccountUsernameTakenByOther, username, id)
}

// InsertAccount names only the columns given: charge_point and deployments
// are left to their defaults when not set, as on MySQL.
func (c configQ) InsertAccount(ctx context.Context, a dal.AccountInsert) (int64, error) {
	_ = stAccountInsert
	cols := []string{"username", "company_id", "jwt", "allowed_sender_ids", "smpp_config", "http", "smpp"}
	vals := []any{a.Username, a.CompanyID, a.JWT, a.AllowedSenders, a.SMPPConfig, a.HTTP, a.SMPP}
	if a.ChargePoint != "" {
		cols = append(cols, "charge_point")
		vals = append(vals, Varchar(a.ChargePoint))
	}
	if a.Deployments.Valid {
		cols = append(cols, "deployments")
		vals = append(vals, a.Deployments.String)
	}
	q := "INSERT INTO sms_service_config (" + strings.Join(cols, ",") + ") OUTPUT INSERTED.id VALUES (" +
		strings.TrimSuffix(strings.Repeat("?,", len(cols)), ",") + ")"
	return c.insertID(ctx, positional(q), vals...)
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
	return out, rowsErr(rows)
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
	_, err := c.q.ExecContext(ctx, stAccountSetChargePoint.SQL, Varchar(point), id)
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
	return c.execRows(ctx, stAccountDelete.SQL, id)
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
	return out, rowsErr(rows)
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
	return out, rowsErr(rows)
}

func (c configQ) SetAccountAllowedSenders(ctx context.Context, id int64, listJSON string) error {
	_, err := c.q.ExecContext(ctx, stAccountSetAllowedSenders.SQL, listJSON, id)
	return err
}

// ---- push channels ----

func (c configQ) SetAccountPushSetting(ctx context.Context, id int64, column string, value any) error {
	_ = stAccountSetPush
	if s, ok := value.(string); ok {
		value = Varchar(s) // dlr_channel, mo_channel: VARCHAR
	}
	_, err := c.q.ExecContext(ctx, "UPDATE sms_service_config SET "+quoteIdent(column)+" = @p1"+stampAccount+" WHERE id = @p2", value, id)
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
	err := c.q.QueryRowContext(ctx, stActiveWebhookRetry.SQL, endpointID, Varchar(kind)).Scan(&retry)
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
	_, err := c.q.ExecContext(ctx, q, cfgID(endpointID), accountID)
	return err
}

// ---- SMPP windows ----

// windowPath is a JSON path provision has picked, as a literal.
func windowPath(path string) string { return "'" + strings.ReplaceAll(path, "'", "''") + "'" }

func (c configQ) SetAccountSMPPWindow(ctx context.Context, id int64, path string, n int) error {
	_ = stAccountSetWindow
	_, err := c.q.ExecContext(ctx,
		`UPDATE sms_service_config SET smpp_config = JSON_MODIFY(COALESCE(smpp_config,N'{}'),`+windowPath(path)+`,@p1)`+stampAccount+` WHERE id = @p2`,
		n, id)
	return err
}

func (c configQ) RemoveAccountSMPPWindow(ctx context.Context, id int64, path string) error {
	_ = stAccountRemoveWindow
	_, err := c.q.ExecContext(ctx,
		`UPDATE sms_service_config SET smpp_config = JSON_MODIFY(COALESCE(smpp_config,N'{}'),`+windowPath(path)+`,NULL)`+stampAccount+` WHERE id = @p1`,
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
	_, err := stAccountKeepToken.Exec(ctx, c.q, sql.Named("account_id", id), sql.Named("api_token", token))
	return err
}

func (c configQ) SetAccountPassword(ctx context.Context, id int64, hash, smppJSON string) error {
	_, err := c.q.ExecContext(ctx, stAccountSetPassword.SQL, hash, smppJSON, id)
	return err
}

func (c configQ) KeepAccountPassword(ctx context.Context, id int64, plaintext string) error {
	_, err := stAccountKeepPassword.Exec(ctx, c.q, sql.Named("account_id", id), sql.Named("password", plaintext))
	return err
}

func (c configQ) ClearAccountPassword(ctx context.Context, id int64, smppJSON dal.NullString) error {
	_, err := c.q.ExecContext(ctx, stAccountClearPassword.SQL, cfgNullStr(smppJSON), id)
	return err
}

func (c configQ) ClearKeptPassword(ctx context.Context, id int64) error {
	_, err := c.q.ExecContext(ctx, stAccountClearKeptPassword.SQL, id)
	return err
}

func (c configQ) WriteCredentialAudit(ctx context.Context, r dal.CredentialAuditRow) error {
	var co any
	if r.CompanyID > 0 {
		co = r.CompanyID
	}
	_, err := c.q.ExecContext(ctx, stAccountCredentialAudit.SQL,
		r.AccountID, co, r.Action, r.Summary, strconv.FormatInt(r.AccountID, 10), r.Username, cfgNullStr(r.Detail))
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
	return out, rowsErr(rows)
}

func (c configQ) CompanyNameTaken(ctx context.Context, name string) (bool, error) {
	return c.exists(ctx, stCompanyNameTaken, name)
}

func (c configQ) CompanyNameTakenByOther(ctx context.Context, name string, id int64) (bool, error) {
	return c.exists(ctx, stCompanyNameTakenByOther, name, id)
}

func (c configQ) InsertCompany(ctx context.Context, r dal.CompanyInsert) (int64, error) {
	return c.insertID(ctx, stCompanyInsert.SQL,
		Varchar(r.UUID), r.Name, cfgStr(r.MasterNumber), Varchar(r.Currency), r.BaseRate, r.CompanyEmail, r.SupportEmail,
		r.BillingEmail, r.RatesEmail, r.Prepaid)
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
	_, err := c.q.ExecContext(ctx,
		"UPDATE companies SET "+quoteIdent(column)+" = @p1, "+companyStamp+" WHERE id = @p2",
		cfgNullStr(value), id)
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
		r.CompanyID, Varchar(r.Type), r.Amount, r.Previous, r.New, r.Reference, r.Reason)
	return err
}

func (c configQ) WriteBalanceAudit(ctx context.Context, companyID int64, name, summary, detailJSON string) error {
	_, err := c.q.ExecContext(ctx, stCompanyBalanceAudit.SQL,
		companyID, name, summary, strconv.FormatInt(companyID, 10), name, detailJSON)
	return err
}
