package mssql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	mssqldrv "github.com/microsoft/go-mssqldb"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// Billing's part of the config store on SQL Server (dal.BillingStore). It
// mirrors internal/dal/mysql/billing.go: the same rows, the same answers, the
// same money to the last digit, written for SQL Server.
//
// What differs in the writing, and why:
//
//   - A read that must see the latest committed row (the balance a hold is
//     judged on) is WITH (UPDLOCK, ROWLOCK), as MySQL's FOR UPDATE: with
//     READ_COMMITTED_SNAPSHOT a plain read gets the last committed version
//     and would not wait for a hold in flight.
//   - A float amount goes in as its shortest decimal text, CAST to DECIMAL:
//     MySQL rounds a double into DECIMAL by that text (2.00005 → 2.0001),
//     SQL Server by its binary value (2.00005 → 2.0000). The text makes the
//     two agree.
//   - companies.updated_at and usage_buckets.updated_at are ON UPDATE
//     CURRENT_TIMESTAMP on MySQL; here the statement sets them, and only
//     when the row's value changes, as MySQL does.
//   - A read of a whole table is paged by id (billPage rows a statement):
//     every statement is bounded.
//   - The usage rows of a batch go as one JSON parameter (OPENJSON): the
//     schema has no table types for them (a TVP needs CREATE TYPE).

// BillingStore is billing's statements on the config store.
type BillingStore struct {
	db *sql.DB
}

// NewBillingStore reads and writes billing's rows on db (the config store).
func NewBillingStore(db *sql.DB) *BillingStore {
	if db == nil {
		return nil
	}
	return &BillingStore{db: db}
}

var _ dal.BillingStore = (*BillingStore)(nil)

// The billing family's openers: the config store, on its one pool a
// process. The balance book and the queue are billing_events.go's.
var _ = serves(func(d *dal.Driver) {
	d.BillingStore = func(cfg config.Config) (dal.BillingStore, error) {
		db, err := Get(cfg.ConfigDSN)
		if err != nil {
			return nil, err
		}
		return NewBillingStore(db), nil
	}
	d.BalanceBook = openBillingEvents
	d.BillingQueue = openBillingQueue
})

// BillingStmts is every statement of billing on SQL Server, by name: what
// the lint test reads.
var BillingStmts = newRegistry()

// billPage is how many rows one page of a whole-table read brings.
const billPage = 10000

// billPrepaid is MySQL's COALESCE(JSON_EXTRACT(col,'$.prepaid'),0) = 1:
// true only when prepaid is a JSON number equal to 1 (not true, not "1").
func billPrepaid(col string) string {
	return fmt.Sprintf(`COALESCE((SELECT TOP (1) 1 FROM OPENJSON(CASE WHEN ISJSON(%[1]s, OBJECT) = 1 THEN %[1]s END) AS pj
		WHERE pj.[key] = N'prepaid' AND pj.[type] = 2 AND TRY_CAST(pj.[value] AS FLOAT) = 1), 0) = 1`, col)
}

// billNowSec is the database clock in whole seconds, as MySQL's NOW().
const billNowSec = "CAST(CONVERT(VARCHAR(19), SYSUTCDATETIME(), 120) AS DATETIME2(0))"

// billDec is a parameter given as decimal text, as an exact DECIMAL.
func billDec(p string) string { return "CAST(" + p + " AS DECIMAL(38,18))" }

var (
	// The balance of record.
	stBalance           = BillingStmts.Add("billing.balance", "SELECT balance FROM companies WHERE id = @p1", false)
	stBalanceForUpdate  = BillingStmts.Add("billing.balance_for_update", "SELECT CAST(balance * 10000 AS BIGINT) FROM companies WITH (UPDLOCK, ROWLOCK) WHERE id = @p1", true)
	stAccountBalance    = BillingStmts.Add("billing.account_balance", "SELECT balance FROM sms_service_config WHERE id = @p1", false)
	stPrepaid           = BillingStmts.Add("billing.prepaid", "SELECT TOP (@n) id, balance FROM companies WHERE id > @after AND "+billPrepaid("billing_config")+" ORDER BY id", false)
	stSetBalance        = BillingStmts.Add("billing.set_balance", "UPDATE companies SET balance = "+billDec("@p1")+", updated_at = CASE WHEN balance <> CAST("+billDec("@p1")+" AS DECIMAL(14,4)) THEN SYSUTCDATETIME() ELSE updated_at END WHERE id = @p2", true)
	stSetAccountBalance = BillingStmts.Add("billing.set_account_balance", "UPDATE sms_service_config SET balance = "+billDec("@p1")+" WHERE id = @p2", false)
	stHoldIfCovered     = BillingStmts.Add("billing.hold_if_covered", "UPDATE companies SET balance = balance - "+billDec("@p1")+
		", updated_at = CASE WHEN CAST(balance - "+billDec("@p1")+" AS DECIMAL(14,4)) <> balance THEN SYSUTCDATETIME() ELSE updated_at END WHERE id = @p2 AND balance >= "+billDec("@p1"), true)
	stGiveBack = BillingStmts.Add("billing.give_back", "UPDATE companies SET balance = balance + "+billDec("@p1")+
		", updated_at = CASE WHEN CAST(balance + "+billDec("@p1")+" AS DECIMAL(14,4)) <> balance THEN SYSUTCDATETIME() ELSE updated_at END WHERE id = @p2", true)
	stHoldRow = BillingStmts.Add("billing.hold_row",
		`INSERT INTO balance_history (account_id, company_id, type, amount, previous_balance, new_balance, reference, reason)
			 VALUES (@p1, @p2, 'commit', @p3, @p4, @p5, @p6, @p7)`, true)
	stGiveBackRow = BillingStmts.Add("billing.give_back_row",
		`INSERT INTO balance_history (account_id, company_id, type, amount, previous_balance, new_balance, reference, reason)
			 VALUES (@p1, @p2, 'refund', @p3, @p4, @p5, @p6, N'reservation returned: the message never reached the queue')`, true)
	stHoldRows = BillingStmts.Add("billing.hold_rows", "SELECT COUNT(*) FROM balance_history WHERE type = 'commit' AND reference = @p1", true)

	// The usage books.
	stUsageGuard = BillingStmts.Add("billing.usage_guard", "INSERT INTO usage_batches (batch_id, company_id) VALUES (@p1, 0)", false)
	stUsageRows  = BillingStmts.Add("billing.usage_rows", usageRowsSQL, false)

	// Terms.
	stAccountTerms = BillingStmts.Add("billing.account_terms", `
		SELECT TOP (@n) a.id, COALESCE(a.company_id, 0), COALESCE(RTRIM(c.currency), ''),
		       COALESCE(a.charge_point, 'acceptance'), COALESCE(a.dlr_charge_mode, 'all_receipts'),
		       CAST(CASE WHEN `+billPrepaid("c.billing_config")+` THEN 1 ELSE 0 END AS BIT),
		       COALESCE(a.default_mo_rate_card_id, 0), COALESCE(a.default_mt_rate_card_id, 0),
		       COALESCE(a.default_dlr_rate_card_id, 0)
		FROM sms_service_config AS a
		LEFT JOIN companies AS c ON c.id = a.company_id
		WHERE a.id > @after ORDER BY a.id`, false)
	stVendorPoints = BillingStmts.Add("billing.vendor_points", `SELECT TOP (@n) id, COALESCE(charge_point, 'acceptance') FROM vendors WHERE id > @after ORDER BY id`, false)

	// The rate tables. The times are written yyyy-mm-dd hh:mi:ss, so the
	// rater can compare them as text (Last >= Now).
	stRateMark = BillingStmts.Add("billing.rate_mark", `SELECT TOP (1) CONVERT(VARCHAR(19), `+billNowSec+`, 120),
	(SELECT COUNT_BIG(*) FROM rate_cards), (SELECT CONVERT(VARCHAR(19), MAX(updated_at), 120) FROM rate_cards),
	(SELECT COUNT_BIG(*) FROM rate_card_rates), (SELECT CONVERT(VARCHAR(19), MAX(updated_at), 120) FROM rate_card_rates),
	(SELECT COUNT_BIG(*) FROM routes), (SELECT CONVERT(VARCHAR(19), MAX(updated_at), 120) FROM routes),
	(SELECT COUNT_BIG(*) FROM vendors), (SELECT CONVERT(VARCHAR(19), MAX(updated_at), 120) FROM vendors),
	(SELECT COUNT_BIG(*) FROM companies), (SELECT CONVERT(VARCHAR(19), MAX(updated_at), 120) FROM companies)`, false)
	stRateCards     = BillingStmts.Add("billing.rate_cards", `SELECT TOP (@n) id, RTRIM(currency), COALESCE(company_id, 0) FROM rate_cards WHERE is_disabled = 0 AND id > @after ORDER BY id`, false)
	stRateCompanies = BillingStmts.Add("billing.rate_companies",
		`SELECT TOP (@n) id, COALESCE(RTRIM(currency), ''), COALESCE(base_conversion_rate, 0), COALESCE(base_thirdparty_crp_id, N'')
		 FROM companies WHERE id > @after ORDER BY id`, false)
	stRateRows        = BillingStmts.Add("billing.rate_rows", rateRowsHead+`valid_to IS NULL) ORDER BY id`, false)
	stRateRowsHistory = BillingStmts.Add("billing.rate_rows_history", rateRowsHead+`valid_to IS NULL OR valid_to >= @since) ORDER BY id`, false)
	stRateRoutes      = BillingStmts.Add("billing.rate_routes",
		`SELECT TOP (@n) id, COALESCE(buying_rate_card_id, 0), COALESCE(selling_rate_card_id, 0),
		        COALESCE(dlr_buying_rate_card_id, 0), COALESCE(dlr_selling_rate_card_id, 0),
		        COALESCE(vendor_id, 0), COALESCE(direction, 'MT'), send_plan
		 FROM routes WHERE id > @after ORDER BY id`, false)
	stRateVendors = BillingStmts.Add("billing.rate_vendors",
		`SELECT TOP (@n) id, COALESCE(default_mt_buying_rate_card_id, 0),
		        COALESCE(default_mo_buying_rate_card_id, 0),
		        COALESCE(default_dlr_buying_rate_card_id, 0),
		        COALESCE(company_id, 0)
		 FROM vendors WHERE id > @after ORDER BY id`, false)
	// Every id asked in one statement (MySQL: 500 a statement).
	stRatePrices = BillingStmts.Add("billing.rate_prices",
		`SELECT r.id, COALESCE(r.price_per_segment, 0), COALESCE(r.tax_percentage, 0), COALESCE(RTRIM(c.currency), '')
			FROM rate_card_rates AS r LEFT JOIN rate_cards AS c ON c.id = r.rate_card_id
			WHERE r.id IN (SELECT j.id FROM OPENJSON(@p1) WITH (id BIGINT '$') AS j)`, false)

	// Exchange rates. Their times are whole seconds of the database clock,
	// as MySQL's NOW().
	stFXProviders = BillingStmts.Add("billing.fx_providers",
		`SELECT id, name,
		        COALESCE(JSON_VALUE(config, '$.provider_type'), JSON_QUERY(config, '$.provider_type'), N''),
		        COALESCE(config, N'{}'), COALESCE(cache_ttl_sec, 0)
		   FROM http_endpoints WHERE type = 'crp' AND is_active = 1`, false)
	stFXStored = BillingStmts.Add("billing.fx_stored",
		`SELECT rate, CASE WHEN expires_at > SYSUTCDATETIME() THEN 1 ELSE 0 END FROM fx_rates
		  WHERE endpoint_id = @p1 AND from_ccy = @p2 AND to_ccy = @p3
		    AND expires_at > DATEADD(SECOND, -@p4, SYSUTCDATETIME())`, false)
	stFXSave = AddUpsert(BillingStmts, "billing.fx_save", UpsertSpec{
		Table: "dbo.fx_rates",
		Keys:  []string{"endpoint_id", "from_ccy", "to_ccy"},
		Set: []string{"rate = " + billDec("$rate"), "fetched_at = " + billNowSec,
			"expires_at = DATEADD(SECOND, $ttl, " + billNowSec + ")"},
		Insert:       []string{"endpoint_id", "from_ccy", "to_ccy", "rate", "fetched_at", "expires_at"},
		InsertValues: []string{"", "", "", billDec("$rate"), billNowSec, "DATEADD(SECOND, $ttl, " + billNowSec + ")"},
	}, false)
	stFXRequest = BillingStmts.Add("billing.fx_request", `
		SELECT id, COALESCE(name, N''), COALESCE(url, N''), COALESCE(method, N'GET'),
		       COALESCE(headers, N'{}'), COALESCE(auth, N'{}'),
		       COALESCE(timeout_ms, 0), COALESCE(cache_ttl_sec, 0), COALESCE(config, N'{}'),
		       COALESCE(body_template, N'')
		FROM http_endpoints WHERE id = @p1 AND is_active = 1`, false)
	stFXRecent = BillingStmts.Add("billing.fx_recent",
		`SELECT from_ccy, to_ccy, rate, CASE WHEN expires_at <= SYSUTCDATETIME() THEN 1 ELSE 0 END FROM fx_rates
			  WHERE rate > 0 AND expires_at > DATEADD(SECOND, -@p1, SYSUTCDATETIME())
			  ORDER BY fetched_at DESC, endpoint_id`, false)
)

const (
	rateRowsHead = `SELECT TOP (@n) id, rate_card_id, COALESCE(match_mcc, N''), COALESCE(match_mnc, N''),
		        COALESCE(destination_country_code, N''), price_per_segment,
		        COALESCE(tax_percentage, 0), effective_from,
		        COALESCE(conversion_rate, 1), COALESCE(thirdparty_crp_id, N''),
		        valid_from, valid_to
		 FROM rate_card_rates WHERE id > @after AND (`

	// usageRowsSQL books a batch's rows (one JSON array, @rows): rows of one
	// key are summed first (MySQL's multi-row upsert adds them one after
	// another: the same sums; a converted amount stays NULL only when every
	// row's is NULL), then one UPDATE of the buckets that are there and one
	// INSERT of the rest, the keys held under UPDLOCK, SERIALIZABLE. The text
	// columns are read wide, so a value too long for its column is refused
	// by the INSERT (2628), as MySQL refuses it (1406), never cut.
	usageRowsSQL = `SET NOCOUNT ON; SET XACT_ABORT ON;
DECLARE @r TABLE (bucket_start DATETIME2(0) NOT NULL, side NVARCHAR(4000) NOT NULL, company_id BIGINT NOT NULL,
  account_id BIGINT NOT NULL, vendor_id BIGINT NOT NULL, kind NVARCHAR(4000) NOT NULL, model NVARCHAR(4000) NOT NULL,
  currency NVARCHAR(4000) NOT NULL, card_currency NVARCHAR(4000) NOT NULL, company_currency NVARCHAR(4000) NOT NULL,
  fx_rate DECIMAL(24,12) NOT NULL, items BIGINT NOT NULL, parts BIGINT NOT NULL, amount DECIMAL(18,4) NOT NULL,
  card_amount DECIMAL(18,4) NULL, company_amount DECIMAL(18,4) NULL, base_amount DECIMAL(18,4) NULL,
  conv_missing BIGINT NOT NULL, base_missing BIGINT NOT NULL);
INSERT INTO @r
SELECT j.b, j.s, j.c, j.a, j.v, j.k, j.m, j.cur, j.ccur, j.ocur, j.fx,
       SUM(j.i), SUM(j.p), SUM(j.u), SUM(j.cu), SUM(j.ou), SUM(j.bu), SUM(j.cm), SUM(j.bm)
  FROM OPENJSON(@rows) WITH (b DATETIME2(0) '$.b', s NVARCHAR(4000) '$.s', c BIGINT '$.c', a BIGINT '$.a',
       v BIGINT '$.v', k NVARCHAR(4000) '$.k', m NVARCHAR(4000) '$.m', cur NVARCHAR(4000) '$.cur',
       ccur NVARCHAR(4000) '$.ccur', ocur NVARCHAR(4000) '$.ocur', fx DECIMAL(24,12) '$.fx',
       i BIGINT '$.i', p BIGINT '$.p', u DECIMAL(18,4) '$.u', cu DECIMAL(18,4) '$.cu',
       ou DECIMAL(18,4) '$.ou', bu DECIMAL(18,4) '$.bu', cm BIGINT '$.cm', bm BIGINT '$.bm') AS j
 GROUP BY j.b, j.s, j.c, j.a, j.v, j.k, j.m, j.cur, j.ccur, j.ocur, j.fx;
UPDATE t SET items = t.items + s.items, parts = t.parts + s.parts, amount = t.amount + s.amount,
  card_amount = CASE WHEN s.card_amount IS NULL THEN t.card_amount ELSE COALESCE(t.card_amount, 0) + s.card_amount END,
  company_amount = CASE WHEN s.company_amount IS NULL THEN t.company_amount ELSE COALESCE(t.company_amount, 0) + s.company_amount END,
  base_amount = CASE WHEN s.base_amount IS NULL THEN t.base_amount ELSE COALESCE(t.base_amount, 0) + s.base_amount END,
  conv_missing = t.conv_missing + s.conv_missing, base_missing = t.base_missing + s.base_missing,
  updated_at = SYSUTCDATETIME()
  FROM usage_buckets AS t WITH (UPDLOCK, SERIALIZABLE) INNER JOIN @r AS s ON ` + usageKeys + `;
INSERT INTO usage_buckets (bucket_start, side, company_id, account_id, vendor_id, kind, model, currency,
  card_currency, company_currency, fx_rate, items, parts, amount, card_amount, company_amount, base_amount,
  conv_missing, base_missing)
SELECT s.bucket_start, s.side, s.company_id, s.account_id, s.vendor_id, s.kind, s.model, s.currency,
  s.card_currency, s.company_currency, s.fx_rate, s.items, s.parts, s.amount, s.card_amount, s.company_amount,
  s.base_amount, s.conv_missing, s.base_missing
  FROM @r AS s
 WHERE NOT EXISTS (SELECT 1 FROM usage_buckets AS t WITH (UPDLOCK, SERIALIZABLE) WHERE ` + usageKeys + `);`

	usageKeys = `t.bucket_start = s.bucket_start AND t.side = s.side AND t.company_id = s.company_id
  AND t.account_id = s.account_id AND t.vendor_id = s.vendor_id AND t.kind = s.kind AND t.model = s.model
  AND t.currency = s.currency AND t.card_currency = s.card_currency AND t.company_currency = s.company_currency
  AND t.fx_rate = s.fx_rate`
)

// ---- the balance of record -------------------------------------------------

// moneyScale is the ledger columns' scale: DECIMAL(14,4).
const moneyScale = 10000

// unitsStr renders units as an exact decimal literal, so the value never
// passes through a float on its way into the DECIMAL column.
func unitsStr(u int64) string {
	sign := ""
	if u < 0 {
		sign, u = "-", -u
	}
	return fmt.Sprintf("%s%d.%04d", sign, u/moneyScale, u%moneyScale)
}

// floatStr is a float amount as its shortest decimal text (see the file's
// top comment).
func floatStr(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }

// Balance is a company's balance as it stands now (companies.balance).
func (s *BillingStore) Balance(ctx context.Context, companyID int64) (float64, error) {
	var b float64
	err := s.db.QueryRowContext(ctx, stBalance.SQL, companyID).Scan(&b)
	return b, err
}

// AccountBalance is an unlinked account's own balance.
func (s *BillingStore) AccountBalance(ctx context.Context, accountID int64) (float64, error) {
	var b float64
	err := s.db.QueryRowContext(ctx, stAccountBalance.SQL, accountID).Scan(&b)
	return b, err
}

// Prepaid is every prepaid company's balance.
func (s *BillingStore) Prepaid(ctx context.Context) ([]dal.CompanyBalance, error) {
	var cos []dal.CompanyBalance
	err := billPaged(ctx, s.db, stPrepaid.SQL, nil, func(rows *sql.Rows) (int64, error) {
		var c dal.CompanyBalance
		if err := rows.Scan(&c.Company, &c.Balance); err != nil {
			return 0, err
		}
		cos = append(cos, c)
		return c.Company, nil
	})
	if err != nil {
		return nil, err
	}
	return cos, nil
}

// SetBalance writes a live balance back over companies.balance.
func (s *BillingStore) SetBalance(ctx context.Context, companyID int64, balance float64) error {
	_, err := s.db.ExecContext(ctx, stSetBalance.SQL, floatStr(balance), companyID)
	return err
}

// SetAccountBalance writes an unlinked account's live balance back.
func (s *BillingStore) SetAccountBalance(ctx context.Context, accountID int64, balance float64) error {
	_, err := s.db.ExecContext(ctx, stSetAccountBalance.SQL, floatStr(balance), accountID)
	return err
}

// HoldUnits is one transaction: lock the company row, refuse below zero,
// debit, append the hold's row when row is set. The amount is taken in
// whole ledger units and every figure is written as an exact decimal (see
// MySQL's HoldUnits: the row, the balance and the reply agree to the unit).
func (s *BillingStore) HoldUnits(ctx context.Context, h dal.Hold, row bool) (bool, int64, error) {
	amt := h.Units
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, 0, err
	}
	defer tx.Rollback()
	var bal int64
	if err := tx.QueryRowContext(ctx, stBalanceForUpdate.SQL, h.Company).Scan(&bal); err != nil {
		return false, 0, err
	}
	if bal < amt {
		return false, 0, nil
	}
	left := bal - amt
	if _, err := tx.ExecContext(ctx, stSetBalance.SQL, unitsStr(left), h.Company); err != nil {
		return false, 0, err
	}
	if row {
		if _, err := tx.ExecContext(ctx, stHoldRow.SQL,
			h.Account, h.Company, unitsStr(-amt), unitsStr(bal), unitsStr(left), h.Reference, h.Reason); err != nil {
			return false, 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return false, 0, err
	}
	return true, left, nil
}

// GiveBackUnits returns a hold in one transaction, with its row when row is
// set (type refund, reference as given).
func (s *BillingStore) GiveBackUnits(ctx context.Context, h dal.Hold, row bool) error {
	amt := h.Units // in units, as HoldUnits took it
	if amt <= 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var bal int64
	if err := tx.QueryRowContext(ctx, stBalanceForUpdate.SQL, h.Company).Scan(&bal); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, stSetBalance.SQL, unitsStr(bal+amt), h.Company); err != nil {
		return err
	}
	if row {
		if _, err := tx.ExecContext(ctx, stGiveBackRow.SQL,
			h.Account, h.Company, unitsStr(amt), unitsStr(bal), unitsStr(bal+amt), h.Reference); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// HoldIfCovered deducts amount IF AND ONLY IF the balance covers it, in ONE
// statement (the UPDATE's own row lock makes it right across processes).
// No row changed means the balance is short — not an error.
func (s *BillingStore) HoldIfCovered(ctx context.Context, companyID int64, amount float64) (bool, error) {
	res, err := s.db.ExecContext(ctx, stHoldIfCovered.SQL, floatStr(amount), companyID)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// GiveBack reverses a HoldIfCovered: one statement, no ledger.
func (s *BillingStore) GiveBack(ctx context.Context, companyID int64, amount float64) error {
	_, err := s.db.ExecContext(ctx, stGiveBack.SQL, floatStr(amount), companyID)
	return err
}

// AddHoldRow writes the row of a hold taken on a live counter.
func (s *BillingStore) AddHoldRow(ctx context.Context, r dal.LedgerRow) error {
	_, err := s.db.ExecContext(ctx, stHoldRow.SQL,
		r.Account, r.Company, unitsStr(r.Units), unitsStr(r.Prev), unitsStr(r.After), r.Reference, r.Reason)
	return err
}

// AddGiveBackRow writes the row of a hold given back on a live counter.
func (s *BillingStore) AddGiveBackRow(ctx context.Context, r dal.LedgerRow) error {
	_, err := s.db.ExecContext(ctx, stGiveBackRow.SQL,
		r.Account, r.Company, unitsStr(r.Units), unitsStr(r.Prev), unitsStr(r.After), r.Reference)
	return err
}

// HoldRows counts the hold rows under a reference.
func (s *BillingStore) HoldRows(ctx context.Context, reference string) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, stHoldRows.SQL, reference).Scan(&n)
	return n, err
}

// ---- the usage books -------------------------------------------------------

// usageJSON is one usage row as usageRowsSQL reads it.
type usageJSON struct {
	B   string  `json:"b"`
	S   string  `json:"s"`
	C   int64   `json:"c"`
	A   int64   `json:"a"`
	V   int64   `json:"v"`
	K   string  `json:"k"`
	M   string  `json:"m"`
	Cur string  `json:"cur"`
	CC  string  `json:"ccur"`
	OC  string  `json:"ocur"`
	FX  string  `json:"fx"`
	I   int64   `json:"i"`
	P   int64   `json:"p"`
	U   string  `json:"u"`
	CU  *string `json:"cu"`
	OU  *string `json:"ou"`
	BU  *string `json:"bu"`
	CM  int64   `json:"cm"`
	BM  int64   `json:"bm"`
}

// bucketText is a bucket's time as the MySQL driver sends it (UTC, to the
// microsecond); the column rounds it to the second, as MySQL's does.
func bucketText(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000000")
}

// Book writes a batch in one transaction: the guard row, then one upsert of
// every row into usage_buckets.
func (s *BillingStore) Book(ctx context.Context, guard string, rs []dal.UsageRow) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after Commit
	if _, err := tx.ExecContext(ctx, stUsageGuard.SQL, Varchar(guard)); err != nil {
		if IsDuplicate(err) {
			return true, nil // booked already: a worker stopped after its commit
		}
		return false, err
	}
	if len(rs) > 0 {
		js := make([]usageJSON, len(rs))
		for i, r := range rs {
			j := usageJSON{B: bucketText(r.Bucket), S: r.Side, C: r.Company, A: r.Account, V: r.Vendor, K: r.Kind, M: r.Model,
				Cur: r.Currency, CC: r.CardCurrency, OC: r.CompanyCurrency, FX: r.FX, I: r.Items, P: r.Parts,
				U: unitsStr(r.Units), CM: r.ConvMissing, BM: r.BaseMissing}
			if r.Conv {
				card, own, base := unitsStr(r.CardUnits), unitsStr(r.CompanyUnits), unitsStr(r.BaseUnits)
				j.CU, j.OU, j.BU = &card, &own, &base
			}
			js[i] = j
		}
		body, err := json.Marshal(js)
		if err != nil {
			return false, err
		}
		if _, err := tx.ExecContext(ctx, stUsageRows.SQL, sql.Named("rows", string(body))); err != nil {
			return false, err
		}
	}
	return false, tx.Commit()
}

// Away says a booking failed for a reason that is not the events' own: the
// server could not be reached, was busy, is read-only, lacks a table. Only an
// error about a row's VALUES is the events' (billRowFault). Anything unknown
// counts as away: a lane that waits is seen on Monitoring; money set aside
// is not.
func (s *BillingStore) Away(err error) bool {
	if err == nil {
		return false
	}
	var e mssqldrv.Error
	if !errors.As(err, &e) {
		var p *mssqldrv.Error
		if !errors.As(err, &p) || p == nil {
			return true
		}
		e = *p
	}
	if billRowFault[e.Number] {
		return false
	}
	for _, one := range e.All {
		if billRowFault[one.Number] {
			return false
		}
	}
	return true
}

// billRowFault is SQL Server's numbers for a value a column will not take:
// MySQL's 1048 (NULL), 1264/1690 (out of range), 1265/1406 (too long),
// 1292/1366/1367 (not a value of the type), 1300 (bad text), 1364 (no value).
var billRowFault = map[int32]bool{
	515:  true, // NULL into a NOT NULL column
	2628: true, // too long (truncated)
	8152: true, // too long (truncated, before 2019)
	8114: true, // not a value of the type
	245:  true, // not a number
	241:  true, // not a date
	242:  true, // a date out of range
	517:  true, // a date out of range (DATEADD)
	8115: true, // out of the type's range
	220:  true, // out of the type's range
	232:  true, // out of the type's range
}

// ---- terms -----------------------------------------------------------------

// AccountTerms reads every account's terms as written.
func (s *BillingStore) AccountTerms(ctx context.Context) ([]dal.AccountTermsRow, error) {
	var out []dal.AccountTermsRow
	err := billPaged(ctx, s.db, stAccountTerms.SQL, nil, func(rows *sql.Rows) (int64, error) {
		var a dal.AccountTermsRow
		if err := rows.Scan(&a.ID, &a.Company, &a.Currency, &a.Point, &a.DLRMode, &a.Prepaid, &a.MOSellCard,
			&a.MTSellCard, &a.DLRSellCard); err != nil {
			return 0, err
		}
		out = append(out, a)
		return a.ID, nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// VendorPoints reads each vendor's charge point as written.
func (s *BillingStore) VendorPoints(ctx context.Context) (map[int64]string, error) {
	vendors := map[int64]string{}
	err := billPaged(ctx, s.db, stVendorPoints.SQL, nil, func(rows *sql.Rows) (int64, error) {
		var id int64
		var p string
		if err := rows.Scan(&id, &p); err != nil {
			return 0, err
		}
		vendors[id] = p
		return id, nil
	})
	if err != nil {
		// A store without config/0150 yet (Invalid column name): every
		// vendor charges on acceptance.
		if n, ok := ErrorNumber(err); ok && n == 207 {
			return nil, nil
		}
		return nil, err
	}
	return vendors, nil
}

// ---- the rate tables -------------------------------------------------------

// RateMark fingerprints every table pricing reads: row counts and the last
// change of each, plus the database clock. One round trip.
func (s *BillingStore) RateMark(ctx context.Context) (dal.RateMark, error) {
	var now sql.NullString
	var n [5]int64
	var last [5]sql.NullString
	if err := s.db.QueryRowContext(ctx, stRateMark.SQL).Scan(&now,
		&n[0], &last[0], &n[1], &last[1], &n[2], &last[2], &n[3], &last[3], &n[4], &last[4]); err != nil {
		return dal.RateMark{}, err
	}
	m := dal.RateMark{Now: now.String, NowOK: now.Valid, Count: n}
	for i := range last {
		m.Last[i], m.LastOK[i] = last[i].String, last[i].Valid
	}
	return m, nil
}

// ReadRates reads the cards, the companies' money, the rates (the current
// versions, and with closedSince those closed since then), the rules and the
// vendors — in that order, a row that cannot be read left out.
func (s *BillingStore) ReadRates(ctx context.Context, closedSince time.Time) (dal.RateRead, error) {
	var out dal.RateRead
	err := billPaged(ctx, s.db, stRateCards.SQL, nil, func(rows *sql.Rows) (int64, error) {
		var c dal.RateCardRow
		if rows.Scan(&c.ID, &c.Currency, &c.Owner) == nil {
			out.Cards = append(out.Cards, c)
		}
		return c.ID, nil
	})
	if err != nil {
		return out, err
	}

	err = billPaged(ctx, s.db, stRateCompanies.SQL, nil, func(rows *sql.Rows) (int64, error) {
		var cm dal.CompanyMoneyRow
		if rows.Scan(&cm.ID, &cm.Currency, &cm.BaseRate, &cm.BaseCRP) == nil {
			out.Companies = append(out.Companies, cm)
		}
		return cm.ID, nil
	})
	if err != nil {
		return out, err
	}

	q, args := stRateRows.SQL, []any(nil)
	if !closedSince.IsZero() {
		q = stRateRowsHistory.SQL
		args = append(args, sql.Named("since", Time(closedSince)))
	}
	err = billPaged(ctx, s.db, q, args, func(rows *sql.Rows) (int64, error) {
		var row dal.RateRow
		var eff, from, to sql.NullTime
		if rows.Scan(&row.ID, &row.Card, &row.MCC, &row.MNC, &row.Prefix, &row.PricePerPart,
			&row.TaxPct, &eff, &row.ConvRate, &row.CRP, &from, &to) != nil {
			return row.ID, nil
		}
		if eff.Valid {
			row.EffectiveFrom = eff.Time
		}
		if from.Valid {
			row.ValidFrom = from.Time
		}
		if to.Valid {
			row.ValidTo = to.Time
		}
		out.Rates = append(out.Rates, row)
		return row.ID, nil
	})
	if err != nil {
		return out, err
	}

	err = billPaged(ctx, s.db, stRateRoutes.SQL, nil, func(rows *sql.Rows) (int64, error) {
		var rc dal.RouteCardsRow
		var plan sql.NullString
		if rows.Scan(&rc.ID, &rc.Buy, &rc.Sell, &rc.DLRBuy, &rc.DLRSell, &rc.Vendor, &rc.Direction, &plan) == nil {
			rc.SendPlan = plan.String
			out.Routes = append(out.Routes, rc)
		}
		return rc.ID, nil
	})
	if err != nil {
		return out, err
	}

	err = billPaged(ctx, s.db, stRateVendors.SQL, nil, func(rows *sql.Rows) (int64, error) {
		var vc dal.VendorCardsRow
		if rows.Scan(&vc.ID, &vc.MT, &vc.MO, &vc.DLR, &vc.Company) == nil {
			out.Vendors = append(out.Vendors, vc)
		}
		return vc.ID, nil
	})
	return out, err
}

// RatePrices reads the price per part and tax of the rate versions named,
// with their cards' currency, in one statement.
func (s *BillingStore) RatePrices(ctx context.Context, ids []int64) ([]dal.RatePriceRow, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	body, err := json.Marshal(ids)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, stRatePrices.SQL, string(body))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.RatePriceRow
	for rows.Next() {
		var r dal.RatePriceRow
		if err := rows.Scan(&r.ID, &r.PricePerPart, &r.TaxPct, &r.Currency); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// billPaged reads a whole table a page at a time, by id: q selects TOP (@n)
// rows WHERE id > @after ORDER BY id. row scans one row and answers its id;
// an error from row ends the read. The read ends at a page that comes back
// short.
func billPaged(ctx context.Context, db *sql.DB, q string, args []any, row func(*sql.Rows) (int64, error)) error {
	after := int64(math.MinInt64)
	for {
		a := append([]any{sql.Named("n", billPage), sql.Named("after", after)}, args...)
		rows, err := db.QueryContext(ctx, q, a...)
		if err != nil {
			return err
		}
		n := 0
		for rows.Next() {
			id, err := row(rows)
			if err != nil {
				rows.Close()
				return err
			}
			n++
			after = id
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if n < billPage {
			return nil
		}
	}
}

// ---- exchange rates --------------------------------------------------------

// FXProviders reads the active providers; a row that cannot be read is left
// out.
func (s *BillingStore) FXProviders(ctx context.Context) ([]dal.FXProviderRow, error) {
	rows, err := s.db.QueryContext(ctx, stFXProviders.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.FXProviderRow
	for rows.Next() {
		var p dal.FXProviderRow
		if rows.Scan(&p.ID, &p.Name, &p.Type, &p.Config, &p.CacheTTLSec) != nil {
			continue
		}
		out = append(out, p)
	}
	return out, nil
}

// billSeconds is a count of seconds as DATEADD takes it (an INT).
func billSeconds(s int64) int32 {
	return int32(max(math.MinInt32, min(math.MaxInt32, s)))
}

// StoredRate consults the shared fx_rates store. Expiry is compared on the
// database clock — the same clock SaveRate writes with.
func (s *BillingStore) StoredRate(ctx context.Context, endpointID int64, from, to string, staleSec int64) (float64, bool, error) {
	var rate float64
	var isFresh int
	err := s.db.QueryRowContext(ctx, stFXStored.SQL, endpointID, from, to, billSeconds(staleSec)).Scan(&rate, &isFresh)
	return rate, isFresh == 1, err
}

// SaveRate publishes a fetched rate.
func (s *BillingStore) SaveRate(ctx context.Context, endpointID int64, from, to string, rate float64, ttlSec int64) error {
	_, err := stFXSave.Exec(ctx, s.db, sql.Named("endpoint_id", endpointID), sql.Named("from_ccy", from),
		sql.Named("to_ccy", to), sql.Named("rate", floatStr(rate)), sql.Named("ttl", billSeconds(ttlSec)))
	return err
}

// RateRequest reads a provider's rate request endpoint.
func (s *BillingStore) RateRequest(ctx context.Context, endpointID int64) (dal.FXRequestRow, error) {
	var r dal.FXRequestRow
	err := s.db.QueryRowContext(ctx, stFXRequest.SQL, endpointID).
		Scan(&r.ID, &r.Name, &r.URL, &r.Method, &r.Headers, &r.Auth,
			&r.TimeoutMS, &r.CacheTTLSec, &r.Config, &r.BodyTemplate)
	return r, err
}

// RecentRates reads every rate published in the last keepSec, newest first;
// a row that cannot be read is left out.
func (s *BillingStore) RecentRates(ctx context.Context, keepSec int64) ([]dal.FXRateRow, error) {
	rows, err := s.db.QueryContext(ctx, stFXRecent.SQL, billSeconds(keepSec))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.FXRateRow
	for rows.Next() {
		var r dal.FXRateRow
		var stale int
		if rows.Scan(&r.From, &r.To, &r.Rate, &stale) == nil {
			r.Stale = stale == 1
			out = append(out, r)
		}
	}
	return out, nil
}
