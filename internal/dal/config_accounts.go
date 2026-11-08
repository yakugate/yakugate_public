package dal

import "context"

// Accounts is sms_service_config — the API-client identity — and what hangs
// off one account (credentials, senders, push channels, SMPP windows).
type Accounts interface {
	// AccountExists says an account with this id is there.
	AccountExists(ctx context.Context, id int64) (bool, error)

	// ---- the account row ----

	// AccountUsernameTaken says an account has this username.
	AccountUsernameTaken(ctx context.Context, username string) (bool, error)
	// AccountUsernameTakenByOther says an account other than id has it.
	AccountUsernameTakenByOther(ctx context.Context, username string, id int64) (bool, error)
	// InsertAccount writes a new account and answers its id.
	InsertAccount(ctx context.Context, a AccountInsert) (int64, error)
	// AccountList is the console's account list, by id, at most limit.
	AccountList(ctx context.Context, limit int) ([]AccountListRow, error)
	// AccountForSwitch is what switching an account off or on reads first.
	AccountForSwitch(ctx context.Context, id int64) (AccountSwitchRow, error)
	// SetAccountDisabled writes is_disabled (1 off, 0 on).
	SetAccountDisabled(ctx context.Context, id int64, disabled int) error
	// AccountDetail is one account's row as the console shows it.
	AccountDetail(ctx context.Context, id int64) (AccountDetailRow, error)
	// SetAccountUsername writes the username and stamps it into
	// smpp_config.system_id.
	SetAccountUsername(ctx context.Context, id int64, username string) error
	// AccountCompanyOf is an account's company_id (0 = none).
	AccountCompanyOf(ctx context.Context, id int64) (int64, error)
	// SetAccountCompany writes company_id.
	SetAccountCompany(ctx context.Context, id, companyID int64) error
	// SetAccountDeployments writes deployments (JSON text).
	SetAccountDeployments(ctx context.Context, id int64, deploymentsJSON string) error
	// SetAccountChargePoint writes charge_point.
	SetAccountChargePoint(ctx context.Context, id int64, point string) error
	// SetAccountMultipart sets smpp_config.multipart_mode.
	SetAccountMultipart(ctx context.Context, id int64, mode string) error
	// SetAccountHTTP, SetAccountSMPP, SetAccountSpamCheck and
	// SetAccountSpamStrict write the switch (1 on, 0 off).
	SetAccountHTTP(ctx context.Context, id int64, on int) error
	SetAccountSMPP(ctx context.Context, id int64, on int) error
	SetAccountSpamCheck(ctx context.Context, id int64, on int) error
	SetAccountSpamStrict(ctx context.Context, id int64, on int) error
	// SetAccountDefaultSender writes default_sender ("" = NULL).
	SetAccountDefaultSender(ctx context.Context, id int64, sender string) error
	// DeleteAccountRow removes an account; n = rows removed.
	DeleteAccountRow(ctx context.Context, id int64) (n int64, err error)
	// AccountRuleRows are the routing rules of an account, any direction,
	// by id.
	AccountRuleRows(ctx context.Context, id int64) ([]AccountRuleRow, error)

	// ---- senders ----

	// AccountAllowedSenders is allowed_sender_ids as stored.
	AccountAllowedSenders(ctx context.Context, id int64) (NullString, error)
	// ApprovedSenderRows are the account's approved sender IDs (sender_ids),
	// by id.
	ApprovedSenderRows(ctx context.Context, id int64) ([]string, error)
	// SetAccountAllowedSenders writes allowed_sender_ids (JSON text).
	SetAccountAllowedSenders(ctx context.Context, id int64, listJSON string) error

	// ---- where receipts and incoming messages go ----

	// SetAccountPushSetting writes one of dlr_channel, dlr_bind_attempts,
	// mo_channel, mo_bind_attempts (column) to value (text or a number).
	SetAccountPushSetting(ctx context.Context, id int64, column string, value any) error
	// AccountWebhookRetry is the retry policy (JSON text, "" when none) of
	// the webhook of kind (dlr | mo) the account names (dlr_webhook_id /
	// mo_webhook_id, config/0174), when it is of that kind and switched on.
	// ErrNoRows: it names none, or that one is off.
	AccountWebhookRetry(ctx context.Context, accountID int64, kind string) (string, error)
	// ActiveWebhookRetry is the retry policy (JSON text, "" when none) of
	// webhook id when it is of kind (dlr | mo) and switched on. ErrNoRows:
	// it is not.
	ActiveWebhookRetry(ctx context.Context, endpointID int64, kind string) (string, error)
	// AccountWebhookIDs is the webhook of each kind the account names
	// (0 = none). ErrNoRows: there is no such account.
	AccountWebhookIDs(ctx context.Context, accountID int64) (dlr, mo int64, err error)
	// SetAccountWebhookID points the account's webhook of kind (dlr | mo)
	// at endpointID (0 = none, written NULL).
	SetAccountWebhookID(ctx context.Context, accountID int64, kind string, endpointID int64) error

	// ---- SMPP windows ----

	// SetAccountSMPPWindow sets one key (path, "$.<key>") of smpp_config.
	SetAccountSMPPWindow(ctx context.Context, id int64, path string, n int) error
	// RemoveAccountSMPPWindow removes one key (path) of smpp_config.
	RemoveAccountSMPPWindow(ctx context.Context, id int64, path string) error

	// ---- credentials ----

	// AccountSecretsTables counts the account_secrets tables of the store
	// (0 = the Admin API's table is not there).
	AccountSecretsTables(ctx context.Context) (int, error)
	// AccountCredentials is an account's credential columns; lock = FOR
	// UPDATE (inside a transaction).
	AccountCredentials(ctx context.Context, id int64, lock bool) (AccountCredRow, error)
	// AccountKeptSecrets is the token and password account_secrets keeps.
	AccountKeptSecrets(ctx context.Context, id int64) (token, password NullString, err error)
	// SetAccountJWT writes jwt (JSON text).
	SetAccountJWT(ctx context.Context, id int64, jwtJSON string) error
	// KeepAccountToken writes the token into account_secrets.
	KeepAccountToken(ctx context.Context, id int64, token string) error
	// SetAccountPassword writes the password hash and smpp_config (JSON text).
	SetAccountPassword(ctx context.Context, id int64, hash, smppJSON string) error
	// KeepAccountPassword writes the password into account_secrets.
	KeepAccountPassword(ctx context.Context, id int64, plaintext string) error
	// ClearAccountPassword sets password NULL and smpp_config (JSON text,
	// NULL when not Valid).
	ClearAccountPassword(ctx context.Context, id int64, smppJSON NullString) error
	// ClearKeptPassword sets account_secrets.password NULL.
	ClearKeptPassword(ctx context.Context, id int64) error
	// WriteCredentialAudit writes the audit row of a credential change.
	WriteCredentialAudit(ctx context.Context, r CredentialAuditRow) error
}

// AccountInsert is a new account's row. ChargePoint "" and Deployments not
// Valid are not written (the column defaults).
type AccountInsert struct {
	Username       string
	CompanyID      int64
	JWT            string
	AllowedSenders string
	SMPPConfig     string
	HTTP, SMPP     int
	ChargePoint    string
	Deployments    NullString
}

// AccountListRow is one line of the console's account list.
type AccountListRow struct {
	ID          int64
	Username    string
	Company     string
	Disabled    int
	Deployments string
	ChargePoint string
}

// AccountSwitchRow is what switching an account off or on reads first.
type AccountSwitchRow struct {
	Username  string
	CompanyID int64
	Company   string
	Disabled  int
}

// AccountDetailRow is one account's row as the console shows it.
type AccountDetailRow struct {
	ID                            int64
	Username, Company             string
	Disabled                      bool
	Deployments, ChargePoint      string
	CompanyID                     int64
	HTTP, SMPP                    bool
	JWT, SMPPConfig               string
	DedupeTTL                     NullInt64
	Created                       string
	SpamCheck, SpamStrict         bool
	DefaultSender, AllowedSenders string
	DLRChannel                    string
	DLRBindAttempts               int
	MOChannel                     string
	MOBindAttempts                int
}

// AccountRuleRow is one routing rule of an account.
type AccountRuleRow struct {
	ID              int64
	Name, Direction string
}

// AccountCredRow is an account's credential columns.
type AccountCredRow struct {
	ID, Company     int64
	Username        string
	JWT, SMPPConfig string
	Password        NullString
	HTTP, SMPP      int
}

// CredentialAuditRow is the audit row of a credential change, naming the
// account and never the secret. CompanyID 0 or less is written NULL; Detail
// is JSON text, NULL when not Valid.
type CredentialAuditRow struct {
	AccountID, CompanyID int64
	Username             string
	Action, Summary      string
	Detail               NullString
}

// Companies is companies: the tenant, its settings and the balance of
// record as the console moves it.
type Companies interface {
	// CompanyExists says a company with this id is there.
	CompanyExists(ctx context.Context, id int64) (bool, error)

	// CompanyNameType is a company's name and type ("" when not set).
	CompanyNameType(ctx context.Context, id int64) (name, typ string, err error)
	// CompanyList is the console's company list, by id, at most limit.
	CompanyList(ctx context.Context, limit int) ([]CompanyListRow, error)
	// CompanyNameTaken says a company has this name.
	CompanyNameTaken(ctx context.Context, name string) (bool, error)
	// CompanyNameTakenByOther says a company other than id has it.
	CompanyNameTakenByOther(ctx context.Context, name string, id int64) (bool, error)
	// InsertCompany writes a new company made from the console and answers
	// its id.
	InsertCompany(ctx context.Context, c CompanyInsert) (int64, error)
	// CompanyDetail is one company's row as the console shows it.
	CompanyDetail(ctx context.Context, id int64) (CompanyDetailRow, error)
	// SetCompanyColumn writes one column of a company (one of the console's
	// fixed set: name, master_number, base_conversion_rate,
	// template_approval and the four email lists; NULL when not Valid) and
	// moves updated_at on by at least a second.
	SetCompanyColumn(ctx context.Context, id int64, column string, value NullString) error
	// SetCompanyHotDays sets data_retention.hot_days and removes hot_months.
	SetCompanyHotDays(ctx context.Context, id int64, days int) error
	// ClearCompanyHotDays removes data_retention.hot_days and hot_months.
	ClearCompanyHotDays(ctx context.Context, id int64) error
	// CompanyBillingConfig is billing_config as stored.
	CompanyBillingConfig(ctx context.Context, id int64) (NullString, error)
	// SetCompanyPrepaid sets billing_config.prepaid (1 | 0), keeping the rest.
	SetCompanyPrepaid(ctx context.Context, id int64, prepaid int) error
	// CompanyName is a company's name ("" when not set). ErrNoRows: none.
	CompanyName(ctx context.Context, id int64) (string, error)

	// ---- the balance of record, as a person moves it ----

	// CompanyBalanceForUpdate is a company's name, balance (as stored),
	// currency and prepaid, locked.
	CompanyBalanceForUpdate(ctx context.Context, id int64) (CompanyBalanceRow, error)
	// SetCompanyBalanceText writes the balance (a decimal as text).
	SetCompanyBalanceText(ctx context.Context, id int64, balance string) error
	// InsertBalanceChange writes the ledger row of a top-up or correction.
	InsertBalanceChange(ctx context.Context, r BalanceChangeRow) error
	// WriteBalanceAudit writes the audit row of a top-up or correction.
	WriteBalanceAudit(ctx context.Context, companyID int64, name, summary, detailJSON string) error
}

// CompanyListRow is one company of the console's list.
type CompanyListRow struct {
	ID      int64
	Name    string
	Balance string
	Prepaid bool
}

// CompanyInsert is a new company's row. MasterNumber "" is written NULL.
type CompanyInsert struct {
	UUID, Name, MasterNumber   string
	Currency, BaseRate         string
	CompanyEmail, SupportEmail string
	BillingEmail, RatesEmail   string
	Prepaid                    int
}

// CompanyDetailRow is one company's row as the console shows it.
type CompanyDetailRow struct {
	ID                         int64
	UUID, Name, MasterNumber   string
	Currency                   string
	CompanyEmail, SupportEmail string
	BillingEmail, RatesEmail   string
	Prepaid                    bool
	Balance                    string
	Disabled                   bool
	BaseRate, BaseProvider     string
	HotDays                    int
	Type, TemplateApproval     string
}

// CompanyBalanceRow is a company's balance as AdjustBalance locks it.
type CompanyBalanceRow struct {
	Name, Balance, Currency string
	Prepaid                 bool
}

// BalanceChangeRow is the balance_history row of a top-up or correction,
// every amount a decimal as text.
type BalanceChangeRow struct {
	CompanyID             int64
	Type                  string
	Amount, Previous, New string
	Reference, Reason     string
}
