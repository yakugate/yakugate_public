package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	mysqldrv "github.com/go-sql-driver/mysql"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/dal/stmt"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/platform/dbpool"
)

// Billing's part of the config store (dal.BillingStore), moved here from
// internal/billing and internal/billingq on 29 Oct 2026, unchanged: the same
// statements, the same transactions. What a statement means for money (what
// a moment costs, who pays) stayed in internal/billing.

// BillingStore is billing's statements on the config store.
type BillingStore struct {
	db *sql.DB
}

// Billing's rows are in the config store (YAKU_CONFIG_DSN), on its one pool
// a process.
var _ = serves(func(d *dal.Driver) {
	d.BillingStore = func(cfg config.Config) (dal.BillingStore, error) {
		db, err := dbpool.Get(cfg.ConfigDSN)
		if err != nil {
			return nil, err
		}
		return NewBillingStore(db), nil
	}
})

// NewBillingStore reads and writes billing's rows on db (the config store).
func NewBillingStore(db *sql.DB) *BillingStore {
	if db == nil {
		return nil
	}
	return &BillingStore{db: db}
}

var _ dal.BillingStore = (*BillingStore)(nil)

// BillingStmts is every statement of billing on MySQL, by name: what the
// lint and plan tests read. A statement built with a list (IN, a multi-row
// VALUES) is registered with one item.
var BillingStmts = stmt.New("mysql")

var (
	// The balance of record.
	stBalance           = BillingStmts.Add("billing.balance", "SELECT balance FROM companies WHERE id=?", false)
	stBalanceForUpdate  = BillingStmts.Add("billing.balance_for_update", "SELECT balance FROM companies WHERE id=? FOR UPDATE", true)
	stAccountBalance    = BillingStmts.Add("billing.account_balance", "SELECT balance FROM sms_service_config WHERE id=?", false)
	stPrepaid           = BillingStmts.Add("billing.prepaid", `SELECT id, balance FROM companies WHERE COALESCE(JSON_EXTRACT(billing_config,'$.prepaid'),0) = 1`, false)
	stSetBalance        = BillingStmts.Add("billing.set_balance", "UPDATE companies SET balance=? WHERE id=?", true)
	stSetAccountBalance = BillingStmts.Add("billing.set_account_balance", "UPDATE sms_service_config SET balance=? WHERE id=?", false)
	stHoldIfCovered     = BillingStmts.Add("billing.hold_if_covered", "UPDATE companies SET balance = balance - ? WHERE id = ? AND balance >= ?", true)
	stGiveBack          = BillingStmts.Add("billing.give_back", "UPDATE companies SET balance = balance + ? WHERE id = ?", true)
	stHoldRow           = BillingStmts.Add("billing.hold_row",
		`INSERT INTO balance_history (account_id, company_id, type, amount, previous_balance, new_balance, reference, reason)
			 VALUES (?, ?, 'commit', ?, ?, ?, ?, ?)`, true)
	stGiveBackRow = BillingStmts.Add("billing.give_back_row",
		`INSERT INTO balance_history (account_id, company_id, type, amount, previous_balance, new_balance, reference, reason)
			 VALUES (?, ?, 'refund', ?, ?, ?, ?, 'reservation returned: the message never reached the queue')`, true)
	stHoldRows = BillingStmts.Add("billing.hold_rows", "SELECT COUNT(*) FROM balance_history WHERE type='commit' AND reference=?", true)

	// The usage books.
	stUsageGuard = BillingStmts.Add("billing.usage_guard", "INSERT INTO usage_batches (batch_id, company_id) VALUES (?, 0)", false)
	stUsageRows  = BillingStmts.Add("billing.usage_rows", usageRowsHead+usageRow+usageRowsTail, false)

	// Terms.
	stAccountTerms = BillingStmts.Add("billing.account_terms", `
		SELECT a.id, COALESCE(a.company_id,0), COALESCE(c.currency,''),
		       COALESCE(a.charge_point,'acceptance'), COALESCE(a.dlr_charge_mode,'all_receipts'),
		       COALESCE(JSON_EXTRACT(c.billing_config,'$.prepaid'),0) = 1,
		       COALESCE(a.default_mo_rate_card_id,0), COALESCE(a.default_mt_rate_card_id,0),
		       COALESCE(a.default_dlr_rate_card_id,0)
		FROM sms_service_config a
		LEFT JOIN companies c ON c.id = a.company_id`, false)
	stVendorPoints = BillingStmts.Add("billing.vendor_points", `SELECT id, COALESCE(charge_point,'acceptance') FROM vendors`, false)

	// The rate tables.
	stRateMark = BillingStmts.Add("billing.rate_mark", `SELECT NOW(),
	(SELECT COUNT(*) FROM rate_cards), (SELECT MAX(updated_at) FROM rate_cards),
	(SELECT COUNT(*) FROM rate_card_rates), (SELECT MAX(updated_at) FROM rate_card_rates),
	(SELECT COUNT(*) FROM routes), (SELECT MAX(updated_at) FROM routes),
	(SELECT COUNT(*) FROM vendors), (SELECT MAX(updated_at) FROM vendors),
	(SELECT COUNT(*) FROM companies), (SELECT MAX(updated_at) FROM companies)`, false)
	stRateCards     = BillingStmts.Add("billing.rate_cards", `SELECT id, currency, COALESCE(company_id,0) FROM rate_cards WHERE is_disabled=0`, false)
	stRateCompanies = BillingStmts.Add("billing.rate_companies",
		`SELECT id, COALESCE(currency,''), COALESCE(base_conversion_rate,0), COALESCE(CAST(base_thirdparty_crp_id AS CHAR),'')
		 FROM companies`, false)
	stRateRows        = BillingStmts.Add("billing.rate_rows", rateRowsHead+`valid_to IS NULL`, false)
	stRateRowsHistory = BillingStmts.Add("billing.rate_rows_history", rateRowsHead+`valid_to IS NULL OR valid_to >= ?`, false)
	stRateRoutes      = BillingStmts.Add("billing.rate_routes",
		`SELECT id, COALESCE(buying_rate_card_id,0), COALESCE(selling_rate_card_id,0),
		        COALESCE(dlr_buying_rate_card_id,0), COALESCE(dlr_selling_rate_card_id,0),
		        COALESCE(vendor_id,0), COALESCE(direction,'MT'), send_plan
		 FROM routes`, false)
	stRateVendors = BillingStmts.Add("billing.rate_vendors",
		`SELECT id, COALESCE(default_mt_buying_rate_card_id,0),
		        COALESCE(default_mo_buying_rate_card_id,0),
		        COALESCE(default_dlr_buying_rate_card_id,0),
		        COALESCE(company_id,0)
		 FROM vendors`, false)
	stRatePrices = BillingStmts.Add("billing.rate_prices", ratePricesHead+"?)", false)

	// Exchange rates.
	stFXProviders = BillingStmts.Add("billing.fx_providers",
		`SELECT id, name,
		        COALESCE(JSON_UNQUOTE(JSON_EXTRACT(config,'$.provider_type')),''),
		        COALESCE(config,'{}'), COALESCE(cache_ttl_sec,0)
		   FROM http_endpoints WHERE type='crp' AND is_active=1`, false)
	stFXStored = BillingStmts.Add("billing.fx_stored",
		`SELECT rate, expires_at > NOW() FROM fx_rates
		  WHERE endpoint_id=? AND from_ccy=? AND to_ccy=?
		    AND expires_at > NOW() - INTERVAL ? SECOND`, false)
	stFXSave = BillingStmts.Add("billing.fx_save",
		`INSERT INTO fx_rates (endpoint_id, from_ccy, to_ccy, rate, fetched_at, expires_at)
		 VALUES (?,?,?,?,NOW(),DATE_ADD(NOW(), INTERVAL ? SECOND))
		 ON DUPLICATE KEY UPDATE rate=VALUES(rate), fetched_at=VALUES(fetched_at), expires_at=VALUES(expires_at)`, false)
	stFXRequest = BillingStmts.Add("billing.fx_request", `
		SELECT id, COALESCE(name,''), COALESCE(url,''), COALESCE(method,'GET'),
		       COALESCE(headers,'{}'), COALESCE(auth,'{}'),
		       COALESCE(timeout_ms,0), COALESCE(cache_ttl_sec,0), COALESCE(config,'{}'),
		       COALESCE(body_template,'')
		FROM http_endpoints WHERE id=? AND is_active=1`, false)
	stFXRecent = BillingStmts.Add("billing.fx_recent",
		`SELECT from_ccy, to_ccy, rate, expires_at <= NOW() FROM fx_rates
			  WHERE rate > 0 AND expires_at > NOW() - INTERVAL ? SECOND
			  ORDER BY fetched_at DESC, endpoint_id`, false)
)

const (
	usageRowsHead = `INSERT INTO usage_buckets
			(bucket_start, side, company_id, account_id, vendor_id, kind, model, currency,
			 card_currency, company_currency, fx_rate, items, parts, amount,
			 card_amount, company_amount, base_amount, conv_missing, base_missing)
			VALUES `
	usageRow      = "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	usageRowsTail = `
			ON DUPLICATE KEY UPDATE items = items + VALUES(items), parts = parts + VALUES(parts),
			amount = amount + VALUES(amount),
			card_amount = IF(VALUES(card_amount) IS NULL, card_amount, COALESCE(card_amount, 0) + VALUES(card_amount)),
			company_amount = IF(VALUES(company_amount) IS NULL, company_amount, COALESCE(company_amount, 0) + VALUES(company_amount)),
			base_amount = IF(VALUES(base_amount) IS NULL, base_amount, COALESCE(base_amount, 0) + VALUES(base_amount)),
			conv_missing = conv_missing + VALUES(conv_missing), base_missing = base_missing + VALUES(base_missing)`

	rateRowsHead = `SELECT id, rate_card_id, COALESCE(match_mcc,''), COALESCE(match_mnc,''),
		        COALESCE(destination_country_code,''), price_per_segment,
		        COALESCE(tax_percentage,0), effective_from,
		        COALESCE(conversion_rate,1), COALESCE(thirdparty_crp_id,''),
		        valid_from, valid_to
		 FROM rate_card_rates WHERE `

	ratePricesHead = `SELECT r.id, COALESCE(r.price_per_segment,0), COALESCE(r.tax_percentage,0), COALESCE(c.currency,'')
			FROM rate_card_rates r LEFT JOIN rate_cards c ON c.id = r.rate_card_id
			WHERE r.id IN (`
)

// ---- the balance of record -------------------------------------------------

// moneyScale is the ledger columns' scale: DECIMAL(14,4).
const moneyScale = 10000

// toUnits converts an amount to whole ten-thousandths, rounding half away
// from zero as MySQL does when it stores into DECIMAL(14,4) (billing's own,
// for the figures read here).
func toUnits(v float64) int64 { return int64(math.Round(v * moneyScale)) }

// unitsStr renders units as an exact decimal literal, so the driver never
// passes the value through a float on its way into the DECIMAL column.
func unitsStr(u int64) string {
	sign := ""
	if u < 0 {
		sign, u = "-", -u
	}
	return fmt.Sprintf("%s%d.%04d", sign, u/moneyScale, u%moneyScale)
}

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
	rows, err := s.db.QueryContext(ctx, stPrepaid.SQL)
	if err != nil {
		return nil, err
	}
	var cos []dal.CompanyBalance
	for rows.Next() {
		var c dal.CompanyBalance
		if err := rows.Scan(&c.Company, &c.Balance); err != nil {
			rows.Close()
			return nil, err
		}
		cos = append(cos, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return cos, nil
}

// SetBalance writes a live balance back over companies.balance.
func (s *BillingStore) SetBalance(ctx context.Context, companyID int64, balance float64) error {
	_, err := s.db.ExecContext(ctx, stSetBalance.SQL, balance, companyID)
	return err
}

// SetAccountBalance writes an unlinked account's live balance back.
func (s *BillingStore) SetAccountBalance(ctx context.Context, accountID int64, balance float64) error {
	_, err := s.db.ExecContext(ctx, stSetAccountBalance.SQL, balance, accountID)
	return err
}

// HoldUnits is one transaction: lock the company row, refuse below zero,
// debit, append the hold's row when row is set. The amount is taken in
// whole ledger units and every figure is written as an exact decimal: a
// float handed to the DECIMAL column is rounded by MySQL's own rule, which
// differs from Go's at a half (0.00015 is stored 0.0002, Go and the live
// counter take 0.0001), so the row, the balance and the reply could disagree
// by a unit.
func (s *BillingStore) HoldUnits(ctx context.Context, h dal.Hold, row bool) (bool, int64, error) {
	amt := h.Units
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, 0, err
	}
	defer tx.Rollback()
	var balance float64
	if err := tx.QueryRowContext(ctx, stBalanceForUpdate.SQL, h.Company).Scan(&balance); err != nil {
		return false, 0, err
	}
	bal := toUnits(balance)
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
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	amt := h.Units // in units, as HoldUnits took it
	if amt <= 0 {
		return nil
	}
	var balance float64
	if err := tx.QueryRowContext(ctx, stBalanceForUpdate.SQL, h.Company).Scan(&balance); err != nil {
		return err
	}
	bal := toUnits(balance)
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

// HoldIfCovered atomically deducts amount IF AND ONLY IF the balance can
// cover it, in ONE statement: the WHERE guard and the deduction happen
// together, so InnoDB's row lock makes this correct across any number of
// http-server/smpp-server instances with no separate coordination service.
// RowsAffected==0 means insufficient balance — not an error.
func (s *BillingStore) HoldIfCovered(ctx context.Context, companyID int64, amount float64) (bool, error) {
	res, err := s.db.ExecContext(ctx, stHoldIfCovered.SQL, amount, companyID, amount)
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
	_, err := s.db.ExecContext(ctx, stGiveBack.SQL, amount, companyID)
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

// Book writes a batch in one transaction: the guard row, then one
// multi-row upsert into usage_buckets (rows in the order given).
func (s *BillingStore) Book(ctx context.Context, guard string, rs []dal.UsageRow) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after Commit
	if _, err := tx.ExecContext(ctx, stUsageGuard.SQL, guard); err != nil {
		var me *mysqldrv.MySQLError
		if errors.As(err, &me) && me.Number == 1062 {
			return true, nil // booked already: a worker stopped after its commit
		}
		return false, err
	}
	if len(rs) > 0 {
		var sb strings.Builder
		args := make([]any, 0, len(rs)*19)
		for i, r := range rs {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(usageRow)
			var card, own, base any
			if r.Conv {
				card, own, base = unitsStr(r.CardUnits), unitsStr(r.CompanyUnits), unitsStr(r.BaseUnits)
			}
			args = append(args, r.Bucket, r.Side, r.Company, r.Account, r.Vendor, r.Kind, r.Model, r.Currency,
				r.CardCurrency, r.CompanyCurrency, r.FX, r.Items, r.Parts, unitsStr(r.Units), card, own, base, r.ConvMissing, r.BaseMissing)
		}
		if _, err := tx.ExecContext(ctx, usageRowsHead+sb.String()+usageRowsTail, args...); err != nil {
			return false, err
		}
	}
	return false, tx.Commit()
}

// Away says a booking failed for a reason that is not the events' own:
// MySQL could not be reached, was busy, is read-only, lacks a table. Only an
// error about a row's VALUES is the events' (rowFault). Anything unknown
// counts as away: a lane that waits is seen on Monitoring; money set aside
// is not.
func (s *BillingStore) Away(err error) bool {
	if err == nil {
		return false
	}
	var me *mysqldrv.MySQLError
	if errors.As(err, &me) {
		return !rowFault[me.Number]
	}
	return true
}

var rowFault = map[uint16]bool{
	1048: true, 1264: true, 1265: true, 1292: true, 1300: true, 1364: true,
	1366: true, 1367: true, 1406: true, 1690: true,
}

// ---- terms -----------------------------------------------------------------

// AccountTerms reads every account's terms as written.
func (s *BillingStore) AccountTerms(ctx context.Context) ([]dal.AccountTermsRow, error) {
	rows, err := s.db.QueryContext(ctx, stAccountTerms.SQL)
	if err != nil {
		return nil, err
	}
	var out []dal.AccountTermsRow
	for rows.Next() {
		var a dal.AccountTermsRow
		if err := rows.Scan(&a.ID, &a.Company, &a.Currency, &a.Point, &a.DLRMode, &a.Prepaid, &a.MOSellCard,
			&a.MTSellCard, &a.DLRSellCard); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, a)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// VendorPoints reads each vendor's charge point as written.
func (s *BillingStore) VendorPoints(ctx context.Context) (map[int64]string, error) {
	vrows, err := s.db.QueryContext(ctx, stVendorPoints.SQL)
	if err != nil {
		// A store without config/0150 yet: every vendor charges on acceptance.
		if !strings.Contains(err.Error(), "Unknown column") {
			return nil, err
		}
		return nil, nil
	}
	vendors := map[int64]string{}
	for vrows.Next() {
		var id int64
		var p string
		if err := vrows.Scan(&id, &p); err != nil {
			vrows.Close()
			return nil, err
		}
		vendors[id] = p
	}
	vrows.Close()
	if err := vrows.Err(); err != nil {
		return nil, err
	}
	return vendors, nil
}

// ---- the rate tables -------------------------------------------------------

// RateMark fingerprints every table pricing reads: row counts and the last
// change of each (every one carries updated_at ON UPDATE), plus the
// database clock. One round trip.
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
	rows, err := s.db.QueryContext(ctx, stRateCards.SQL)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var c dal.RateCardRow
		if rows.Scan(&c.ID, &c.Currency, &c.Owner) == nil {
			out.Cards = append(out.Cards, c)
		}
	}
	rows.Close()

	cr, err := s.db.QueryContext(ctx, stRateCompanies.SQL)
	if err != nil {
		return out, err
	}
	for cr.Next() {
		var cm dal.CompanyMoneyRow
		if cr.Scan(&cm.ID, &cm.Currency, &cm.BaseRate, &cm.BaseCRP) == nil {
			out.Companies = append(out.Companies, cm)
		}
	}
	cr.Close()

	q, args := stRateRows.SQL, []any{}
	if !closedSince.IsZero() {
		q = stRateRowsHistory.SQL
		args = append(args, closedSince)
	}
	rr, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return out, err
	}
	for rr.Next() {
		var row dal.RateRow
		var eff, from, to sql.NullTime
		if rr.Scan(&row.ID, &row.Card, &row.MCC, &row.MNC, &row.Prefix, &row.PricePerPart,
			&row.TaxPct, &eff, &row.ConvRate, &row.CRP, &from, &to) != nil {
			continue
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
	}
	rr.Close()

	rt, err := s.db.QueryContext(ctx, stRateRoutes.SQL)
	if err != nil {
		return out, err
	}
	for rt.Next() {
		var rc dal.RouteCardsRow
		var plan sql.NullString
		if rt.Scan(&rc.ID, &rc.Buy, &rc.Sell, &rc.DLRBuy, &rc.DLRSell, &rc.Vendor, &rc.Direction, &plan) == nil {
			rc.SendPlan = plan.String
			out.Routes = append(out.Routes, rc)
		}
	}
	rt.Close()

	vr, err := s.db.QueryContext(ctx, stRateVendors.SQL)
	if err != nil {
		return out, err
	}
	for vr.Next() {
		var vc dal.VendorCardsRow
		if vr.Scan(&vc.ID, &vc.MT, &vc.MO, &vc.DLR, &vc.Company) == nil {
			out.Vendors = append(out.Vendors, vc)
		}
	}
	vr.Close()
	return out, nil
}

// RatePrices reads the price per part and tax of the rate versions named,
// with their cards' currency, 500 a statement.
func (s *BillingStore) RatePrices(ctx context.Context, ids []int64) ([]dal.RatePriceRow, error) {
	var out []dal.RatePriceRow
	for from := 0; from < len(ids); from += 500 {
		part := ids[from:min(from+500, len(ids))]
		args := make([]any, len(part))
		for i, id := range part {
			args[i] = id
		}
		rows, err := s.db.QueryContext(ctx, ratePricesHead+placeholders(len(part))+`)`, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var r dal.RatePriceRow
			if err := rows.Scan(&r.ID, &r.PricePerPart, &r.TaxPct, &r.Currency); err != nil {
				rows.Close()
				return nil, err
			}
			out = append(out, r)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
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

// StoredRate consults the shared fx_rates store. Expiry is compared on the
// SQL session clock — the same clock SaveRate writes with.
func (s *BillingStore) StoredRate(ctx context.Context, endpointID int64, from, to string, staleSec int64) (float64, bool, error) {
	var rate float64
	var isFresh int
	err := s.db.QueryRowContext(ctx, stFXStored.SQL, endpointID, from, to, staleSec).Scan(&rate, &isFresh)
	return rate, isFresh == 1, err
}

// SaveRate publishes a fetched rate.
func (s *BillingStore) SaveRate(ctx context.Context, endpointID int64, from, to string, rate float64, ttlSec int64) error {
	_, err := s.db.ExecContext(ctx, stFXSave.SQL, endpointID, from, to, rate, ttlSec)
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
	rows, err := s.db.QueryContext(ctx, stFXRecent.SQL, keepSec)
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
