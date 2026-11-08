package mssql

import (
	"context"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
)

// Rate cards and operators (dal.RateCards, dal.Operators) on SQL Server,
// made like internal/dal/mysql/config_ratecards.go. The eight rules of a
// price and the rules of a range stay in provision.
//
// MySQL's null-safe <=> is IS NOT DISTINCT FROM; NOW() is the UTC clock.

const stampRate = ", updated_at = SYSUTCDATETIME()"

var (
	// ---- rate cards ----
	stRateCardNameTaken = addConfig("config.ratecards.name_taken", "SELECT COUNT(*) FROM rate_cards WHERE name = ?")
	stRateCardCompany   = addConfig("config.ratecards.company",
		"SELECT name, COALESCE(type,'') FROM companies WHERE id = ?")
	stRateCardInsert = addConfig("config.ratecards.insert", `
		INSERT INTO rate_cards (name, kind, type, currency, company_id, is_disabled, notify_customers, created_by_user_id)
		OUTPUT INSERTED.id
		VALUES (?,?,?,?,?,0,?,'console')`)
	stRateCardKind = addConfig("config.ratecards.kind",
		"SELECT kind FROM rate_cards WHERE id = ?")
	stRateCardKindType = addConfig("config.ratecards.kind_type",
		"SELECT kind, COALESCE(type,'') FROM rate_cards WHERE id = ?")
	stRateSettle = addConfig("config.ratecards.settle_due", `
		UPDATE o SET o.valid_to = n.effective_from, o.updated_at = SYSUTCDATETIME()
		  FROM rate_card_rates o
		  JOIN rate_card_rates n ON n.id = o.superseded_by
		 WHERE o.valid_to IS NULL AND n.valid_to IS NULL
		   AND n.effective_from IS NOT NULL AND n.effective_from <= SYSUTCDATETIME()`)
	// FOR UPDATE at REPEATABLE READ: the open price and the gap around it
	// (no second open price of the destination slips in) — UPDLOCK +
	// HOLDLOCK, to the end of the transaction.
	stRateOpenForUpdate = addConfig("config.ratecards.open_for_update", `
		SELECT TOP (1) id, COALESCE(version,1), price_per_segment, COALESCE(tax_percentage,0),
		       effective_from, superseded_by
		  FROM rate_card_rates WITH (UPDLOCK, HOLDLOCK)
		 WHERE rate_card_id = ? AND valid_to IS NULL
		   AND match_mcc IS NOT DISTINCT FROM ? AND match_mnc IS NOT DISTINCT FROM ?
		   AND destination_country_code IS NOT DISTINCT FROM ?
		 ORDER BY id DESC`)
	stRateSnapshot = addConfig("config.ratecards.snapshot", `
		SELECT match_mcc, match_mnc, destination_country_code, price_per_segment,
		       COALESCE(tax_percentage,0), COALESCE(costing_unit,'segment'),
		       COALESCE(conversion_rate,1), effective_from, COALESCE(version,1)
		  FROM rate_card_rates WHERE id = ?`)
	stRateInsert = addConfig("config.ratecards.insert_rate", `
		INSERT INTO rate_card_rates (rate_card_id, version, match_mcc, match_mnc,
		                             destination_country_code, price_per_segment, tax_percentage,
		                             valid_from, effective_from)
		OUTPUT INSERTED.id
		VALUES (?,?,?,?,?,?,?,?,?)`)
	stRateCorrectWaiting = addConfig("config.ratecards.correct_waiting", `
			UPDATE rate_card_rates SET price_per_segment = ?, tax_percentage = ?, effective_from = ?`+stampRate+`
			 WHERE id = ?`)
	stRateCloseSupersededBy = addConfig("config.ratecards.close_superseded_by",
		"UPDATE rate_card_rates SET valid_to = SYSUTCDATETIME()"+stampRate+" WHERE superseded_by = ? AND valid_to IS NULL")
	stRateMaxVersion = addConfig("config.ratecards.max_version", `
			SELECT COALESCE(MAX(version),0) FROM rate_card_rates
			 WHERE rate_card_id = ? AND match_mcc IS NOT DISTINCT FROM ? AND match_mnc IS NOT DISTINCT FROM ?
			   AND destination_country_code IS NOT DISTINCT FROM ?`)
	stRateSupersedeClose = addConfig("config.ratecards.supersede_close",
		"UPDATE rate_card_rates SET valid_to = SYSUTCDATETIME(), superseded_by = ?"+stampRate+" WHERE id = ?")
	stRateSupersede = addConfig("config.ratecards.supersede",
		"UPDATE rate_card_rates SET superseded_by = ?"+stampRate+" WHERE id = ?")
	stRateForClose = addConfig("config.ratecards.for_close", `
		SELECT match_mcc, match_mnc, destination_country_code, effective_from, valid_to
		  FROM rate_card_rates WHERE id = ? AND rate_card_id = ?`)
	stRateCloseNow = addConfig("config.ratecards.close_now",
		"UPDATE rate_card_rates SET valid_to = SYSUTCDATETIME()"+stampRate+" WHERE id = ?")
	stRateClearSuccessor = addConfig("config.ratecards.clear_successor",
		"UPDATE rate_card_rates SET superseded_by = NULL"+stampRate+" WHERE superseded_by = ? AND valid_to IS NULL")
	stRateLiveAny = addConfig("config.ratecards.live_any_destination", `
		SELECT COUNT(*) FROM rate_card_rates
		 WHERE rate_card_id = ? AND valid_to IS NULL
		   AND match_mcc IS NULL AND match_mnc IS NULL AND destination_country_code IS NULL
		   AND (effective_from IS NULL OR effective_from <= SYSUTCDATETIME())`)
	stRateCardNotice = addConfig("config.ratecards.notice_settings",
		"SELECT kind, COALESCE(notify_customers,1) FROM rate_cards WHERE id = ?")
	stRateNoticeInsert = addConfig("config.ratecards.notice_insert", `
		INSERT INTO rate_change_notices
		       (rate_card_id, rate_id, change_type, before_json, after_json, effective_from, changed_by_user_id)
		VALUES (?,?,?,?,?,?,'console')`)
	stRateCardList = addConfig("config.ratecards.list", `
		SELECT TOP (@p1) c.id, COALESCE(c.name,''), c.kind, COALESCE(c.type,''), COALESCE(RTRIM(c.currency),''),
		       COALESCE(co.name,'shared'), COALESCE(c.is_disabled,0),
		       (SELECT COUNT(*) FROM rate_card_rates r
		         WHERE r.rate_card_id = c.id AND r.valid_to IS NULL
		           AND (r.effective_from IS NULL OR r.effective_from <= SYSUTCDATETIME())),
		       (SELECT COUNT(*) FROM rate_card_rates r
		         WHERE r.rate_card_id = c.id AND r.valid_to IS NULL
		           AND r.match_mcc IS NULL AND r.match_mnc IS NULL AND r.destination_country_code IS NULL
		           AND (r.effective_from IS NULL OR r.effective_from <= SYSUTCDATETIME()))
		  FROM rate_cards c
		  LEFT JOIN companies co ON co.id = c.company_id
		 ORDER BY c.id`)
	// Built per table from the card columns provision names; one form here.
	stRateCardColumnUsers = addConfig("config.ratecards.column_users",
		"SELECT COUNT(*) FROM routes WHERE buying_rate_card_id = ? OR selling_rate_card_id = ?")
	stRateCardPlanUsers = addConfig("config.ratecards.plan_users", "WITH "+planNodes+`
		SELECT COUNT(*) FROM routes WHERE send_plan IS NOT NULL AND (
		       id IN (`+planHasNumber("buy_rate_card_id", "?")+`)
		    OR id IN (`+planHasNumber("sell_rate_card_id", "?")+`))`)
	stRateListHead = `
		SELECT id, COALESCE(version,1), COALESCE(match_mcc,''), COALESCE(match_mnc,''),
		       COALESCE(destination_country_code,''), price_per_segment, COALESCE(tax_percentage,0),
		       effective_from, valid_to, COALESCE(superseded_by,0)
		  FROM rate_card_rates `
	stRateListTail = `
		 ORDER BY COALESCE(match_mcc,''), COALESCE(match_mnc,''),
		          COALESCE(destination_country_code,''), version`
	stRateListOpen = addConfig("config.ratecards.rates_open",
		stRateListHead+"WHERE rate_card_id = ? AND valid_to IS NULL"+stRateListTail)
	stRateListAll = addConfig("config.ratecards.rates_all",
		stRateListHead+"WHERE rate_card_id = ?"+stRateListTail)
	// Rows CHANGED, as MySQL counts them.
	stRateCardSetDisabled = addConfig("config.ratecards.set_disabled",
		"UPDATE rate_cards SET is_disabled = ?"+stampRate+" WHERE id = ? AND is_disabled IS DISTINCT FROM @p1")
	stRateCardPriceCount = addConfig("config.ratecards.price_count",
		"SELECT COUNT(*) FROM rate_card_rates WHERE rate_card_id = ?")
	stRateCardDelete = addConfig("config.ratecards.delete",
		"DELETE FROM rate_cards WHERE id = ?")
	stRateCardOwner = addConfig("config.ratecards.owner",
		"SELECT COALESCE(company_id,0), COALESCE(name,'') FROM rate_cards WHERE id = ?")
	stSellCardAccountCompany = addConfig("config.ratecards.account_company",
		"SELECT COALESCE(company_id,0) FROM sms_service_config WHERE id = ?")
	// The column is one of provision's card columns; one form here.
	stRateCardOnAccount = addConfig("config.ratecards.on_account",
		"UPDATE sms_service_config SET default_mt_rate_card_id = ?"+stampRate+" WHERE id = ?")
	stRateCardOnVendor = addConfig("config.ratecards.on_vendor",
		"UPDATE vendors SET default_mt_buying_rate_card_id = ?"+stampRate+" WHERE id = ?")

	// ---- operators ----
	// COUNT(DISTINCT mcc, mnc): the pairs with neither NULL.
	stOperatorCounts = addConfig("config.operators.counts", `
		SELECT (SELECT COUNT(*) FROM (SELECT DISTINCT mcc, mnc FROM operators WHERE mcc IS NOT NULL AND mnc IS NOT NULL) d),
		       COUNT(*) FROM operators`)
	stOperatorFind = addConfig("config.operators.find", `
		SELECT TOP (@p6) COALESCE(mcc,''), COALESCE(mnc,''), COALESCE(MIN(ccc),''), COALESCE(MIN(operator_name),''),
		       COALESCE(MIN(country),''), COALESCE(MIN(region),''), COUNT(*)
		  FROM operators
		 WHERE ? = '' OR country LIKE ? OR operator_name LIKE ? OR mccmnc LIKE ? OR mcc = ?
		 GROUP BY mcc, mnc
		 ORDER BY MIN(country), mcc, mnc`)
	stOperatorNetwork = addConfig("config.operators.network", `
		SELECT COALESCE(MIN(ccc),''), COALESCE(MIN(operator_name),''), COALESCE(MIN(country),''),
		       COALESCE(MIN(region),''), COUNT(*)
		  FROM operators WHERE mcc = ? AND mnc = ?`)
	stOperatorSpelling = addConfig("config.operators.spelling",
		"SELECT TOP (1) mnc FROM operators WHERE mcc = ? AND mnc IN (?,?,?) GROUP BY mnc ORDER BY COUNT(*) DESC")
	stOperatorRangeCount = addConfig("config.operators.range_count",
		"SELECT COUNT(*) FROM operators WHERE mcc = ? AND mnc = ? AND from_series LIKE ?")
	stOperatorRanges = addConfig("config.operators.ranges", `
		SELECT TOP (@p4) id, COALESCE(from_series,''), COALESCE(to_series,''), COALESCE(ccc,''),
		       COALESCE(operator_name,''), COALESCE(country,''), COALESCE(region,'')
		  FROM operators WHERE mcc = ? AND mnc = ? AND from_series LIKE ?
		 ORDER BY from_series`)
	// CONCAT on MySQL is NULL when a part is; + is the same here.
	stOperatorOverlap = addConfig("config.operators.overlap", `
		SELECT TOP (1) id, COALESCE(operator_name,'') + N' (' + mcc + N'-' + mnc + N')', from_series, to_series
		  FROM operators
		 WHERE LEN(from_series) = ? AND from_series <= ? AND to_series >= ?`)
	stOperatorInsert = addConfig("config.operators.insert", `
		INSERT INTO operators (ccc, mcc, mnc, mccmnc, operator_name, country, region, allow_mcc_only,
		                       from_series, to_series, hlr_shard_key)
		OUTPUT INSERTED.id
		VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)`)
	// The column is operator_name, country or region; one form here. Rows
	// CHANGED, as MySQL counts them (compared byte for byte, as MySQL does).
	stOperatorSetColumn = addConfig("config.operators.set_column",
		"UPDATE operators SET operator_name = ?"+stampRate+" WHERE mcc = ? AND mnc = ?"+
			" AND operator_name COLLATE Latin1_General_100_BIN2 IS DISTINCT FROM @p1 COLLATE Latin1_General_100_BIN2")
	stOperatorRange = addConfig("config.operators.range", `
		SELECT id, COALESCE(ccc,''), COALESCE(mcc,''), COALESCE(mnc,''), COALESCE(operator_name,''),
		       COALESCE(country,''), COALESCE(from_series,''), COALESCE(to_series,'')
		  FROM operators WHERE id = ?`)
	stOperatorDeleteRange = addConfig("config.operators.delete_range",
		"DELETE FROM operators WHERE id = ?")
	stOperatorRouteUsers = addConfig("config.operators.route_users",
		"SELECT COUNT(*) FROM routes WHERE "+jsonArrayHasString("match_mccmncs", "?"))
	stOperatorPriceUsers = addConfig("config.operators.price_users",
		"SELECT COUNT(*) FROM rate_card_rates WHERE match_mcc = ? AND match_mnc IN (?,?,?) AND valid_to IS NULL")
	stOperatorDeleteNetwork = addConfig("config.operators.delete_network",
		"DELETE FROM operators WHERE mcc = ? AND mnc = ?")
)

// nullTrim is NULL for a value that is empty once spaces are trimmed, else
// the trimmed value (provision's nullIfEmpty).
func nullTrim(s string) any {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return strings.TrimSpace(s)
}

// ---- rate cards -------------------------------------------------------------

func (c configQ) RateCardNameTaken(ctx context.Context, name string) (bool, error) {
	return c.exists(ctx, stRateCardNameTaken, name)
}

func (c configQ) RateCardCompany(ctx context.Context, companyID int64) (name, typ string, err error) {
	err = c.q.QueryRowContext(ctx, stRateCardCompany.SQL, companyID).Scan(&name, &typ)
	return
}

func (c configQ) InsertRateCard(ctx context.Context, r dal.RateCardInsert) (int64, error) {
	return c.insertID(ctx, stRateCardInsert.SQL,
		r.Name, Varchar(r.Kind), Varchar(r.Type), Varchar(r.Currency), r.CompanyID, r.Notify)
}

func (c configQ) RateCardKind(ctx context.Context, id int64) (string, error) {
	var kind string
	err := c.q.QueryRowContext(ctx, stRateCardKind.SQL, id).Scan(&kind)
	return kind, err
}

func (c configQ) RateCardKindType(ctx context.Context, id int64) (kind, typ string, err error) {
	err = c.q.QueryRowContext(ctx, stRateCardKindType.SQL, id).Scan(&kind, &typ)
	return
}

func (c configQ) SettleDueRateSchedules(ctx context.Context) error {
	_, err := c.q.ExecContext(ctx, stRateSettle.SQL)
	return err
}

func (c configQ) OpenRateForUpdate(ctx context.Context, d dal.RateDestination) (dal.OpenRate, error) {
	var r dal.OpenRate
	err := c.q.QueryRowContext(ctx, stRateOpenForUpdate.SQL,
		d.CardID, nullTrim(d.MCC), nullTrim(d.MNC), nullTrim(d.Prefix),
	).Scan(&r.ID, &r.Version, &r.Price, &r.Tax, &r.Effective, &r.Successor)
	return r, err
}

func (c configQ) RateSnapshotRow(ctx context.Context, id int64) (dal.RateSnapshot, error) {
	var r dal.RateSnapshot
	err := c.q.QueryRowContext(ctx, stRateSnapshot.SQL, id).
		Scan(&r.MCC, &r.MNC, &r.Prefix, &r.Price, &r.Tax, &r.Unit, &r.Conv, &r.Effective, &r.Version)
	return r, err
}

func (c configQ) InsertRate(ctx context.Context, r dal.RateInsert) (int64, error) {
	return c.insertID(ctx, stRateInsert.SQL,
		r.CardID, r.Version, nullTrim(r.MCC), nullTrim(r.MNC), nullTrim(r.Prefix),
		r.Price, r.Tax, Time(r.ValidFrom), cfgNullTime(r.Effective))
}

func (c configQ) CorrectWaitingRate(ctx context.Context, id int64, price, tax string, effective dal.NullTime) error {
	_, err := c.q.ExecContext(ctx, stRateCorrectWaiting.SQL, price, tax, cfgNullTime(effective), id)
	return err
}

func (c configQ) CloseRatesSupersededBy(ctx context.Context, successor int64) error {
	_, err := c.q.ExecContext(ctx, stRateCloseSupersededBy.SQL, successor)
	return err
}

func (c configQ) MaxRateVersion(ctx context.Context, d dal.RateDestination) (int, error) {
	var maxv int
	err := c.q.QueryRowContext(ctx, stRateMaxVersion.SQL,
		d.CardID, nullTrim(d.MCC), nullTrim(d.MNC), nullTrim(d.Prefix)).Scan(&maxv)
	return maxv, err
}

func (c configQ) SupersedeRate(ctx context.Context, oldID, newID int64, closeNow bool) error {
	st := stRateSupersedeClose
	if !closeNow {
		st = stRateSupersede
	}
	_, err := c.q.ExecContext(ctx, st.SQL, newID, oldID)
	return err
}

func (c configQ) RateForClose(ctx context.Context, cardID, rateID int64) (dal.RateCloseRow, error) {
	var r dal.RateCloseRow
	err := c.q.QueryRowContext(ctx, stRateForClose.SQL, rateID, cardID).
		Scan(&r.MCC, &r.MNC, &r.Prefix, &r.Effective, &r.Closed)
	return r, err
}

func (c configQ) CloseRateNow(ctx context.Context, rateID int64) error {
	_, err := c.q.ExecContext(ctx, stRateCloseNow.SQL, rateID)
	return err
}

func (c configQ) ClearSuccessor(ctx context.Context, successor int64) error {
	_, err := c.q.ExecContext(ctx, stRateClearSuccessor.SQL, successor)
	return err
}

func (c configQ) LiveAnyDestinationRates(ctx context.Context, cardID int64) (int, error) {
	return c.count(ctx, stRateLiveAny, cardID)
}

func (c configQ) RateCardNotice(ctx context.Context, cardID int64) (kind string, notify int, err error) {
	err = c.q.QueryRowContext(ctx, stRateCardNotice.SQL, cardID).Scan(&kind, &notify)
	return
}

func (c configQ) InsertRateNotice(ctx context.Context, n dal.RateNotice) error {
	_, err := c.q.ExecContext(ctx, stRateNoticeInsert.SQL,
		n.CardID, n.RateID, Varchar(n.Change), n.Before, n.After, cfgNullTime(n.Effective))
	return err
}

func (c configQ) RateCardList(ctx context.Context, limit int) ([]dal.RateCardListRow, error) {
	rows, err := c.q.QueryContext(ctx, stRateCardList.SQL, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.RateCardListRow
	for rows.Next() {
		var r dal.RateCardListRow
		if err := rows.Scan(&r.ID, &r.Name, &r.Kind, &r.Type, &r.Currency, &r.Company, &r.Disabled, &r.Prices, &r.Wildcard); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, dal.RowsError{Err: err}
	}
	return out, nil
}

func (c configQ) RateCardColumnUsers(ctx context.Context, table string, cols []string, id int64) (int, error) {
	_ = stRateCardColumnUsers
	conds := make([]string, len(cols))
	args := make([]any, len(cols))
	for i, col := range cols {
		conds[i] = quoteIdent(col) + " = ?"
		args[i] = id
	}
	var n int
	q := "SELECT COUNT(*) FROM " + quoteIdent(table) + " WHERE " + strings.Join(conds, " OR ")
	if err := c.q.QueryRowContext(ctx, positional(q), args...).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func (c configQ) RateCardPlanUsers(ctx context.Context, id int64) (int, error) {
	return c.count(ctx, stRateCardPlanUsers, id, id)
}

func (c configQ) RateList(ctx context.Context, cardID int64, history bool) ([]dal.RateListRow, error) {
	st := stRateListOpen
	if history {
		st = stRateListAll
	}
	rows, err := c.q.QueryContext(ctx, st.SQL, cardID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.RateListRow
	for rows.Next() {
		var r dal.RateListRow
		if err := rows.Scan(&r.ID, &r.Version, &r.MCC, &r.MNC, &r.Prefix, &r.Price, &r.Tax,
			&r.Effective, &r.Closed, &r.Replaces); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return out, dal.RowsError{Err: err} // what was read, and why the read stopped
	}
	return out, nil
}

func (c configQ) SetRateCardDisabled(ctx context.Context, id int64, disabled int) (int64, error) {
	return c.execRows(ctx, stRateCardSetDisabled.SQL, disabled, id)
}

func (c configQ) RateCardPriceCount(ctx context.Context, id int64) (int, error) {
	return c.count(ctx, stRateCardPriceCount, id)
}

func (c configQ) DeleteRateCardRow(ctx context.Context, id int64) (int64, error) {
	return c.execRows(ctx, stRateCardDelete.SQL, id)
}

func (c configQ) RateCardOwner(ctx context.Context, cardID int64) (company int64, name string, err error) {
	err = c.q.QueryRowContext(ctx, stRateCardOwner.SQL, cardID).Scan(&company, &name)
	return
}

func (c configQ) SellCardAccountCompany(ctx context.Context, accountID int64) (int64, error) {
	var co int64
	err := c.q.QueryRowContext(ctx, stSellCardAccountCompany.SQL, accountID).Scan(&co)
	return co, err
}

func (c configQ) SetAccountRateCard(ctx context.Context, column string, cardID, accountID int64) error {
	_ = stRateCardOnAccount
	_, err := c.q.ExecContext(ctx,
		"UPDATE sms_service_config SET "+quoteIdent(column)+" = @p1"+stampRate+" WHERE id = @p2", cardID, accountID)
	return err
}

func (c configQ) SetVendorRateCard(ctx context.Context, column string, cardID, vendorID int64) error {
	_ = stRateCardOnVendor
	_, err := c.q.ExecContext(ctx,
		"UPDATE vendors SET "+quoteIdent(column)+" = @p1"+stampRate+" WHERE id = @p2", cardID, vendorID)
	return err
}

// ---- operators --------------------------------------------------------------

func (c configQ) OperatorCounts(ctx context.Context) (networks, ranges int, err error) {
	err = c.q.QueryRowContext(ctx, stOperatorCounts.SQL).Scan(&networks, &ranges)
	return
}

func (c configQ) FindNetworkRows(ctx context.Context, q string, limit int) ([]dal.NetworkRow, error) {
	like := "%" + q + "%"
	rows, err := c.q.QueryContext(ctx, stOperatorFind.SQL, q, like, like, q+"%", q, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.NetworkRow
	for rows.Next() {
		var n dal.NetworkRow
		if err := rows.Scan(&n.MCC, &n.MNC, &n.CCC, &n.Name, &n.Country, &n.Region, &n.Ranges); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		return out, dal.RowsError{Err: err} // what was read, and why the read stopped
	}
	return out, nil
}

func (c configQ) NetworkSummary(ctx context.Context, mcc, mnc string) (dal.NetworkRow, error) {
	n := dal.NetworkRow{MCC: mcc, MNC: mnc}
	err := c.q.QueryRowContext(ctx, stOperatorNetwork.SQL, mcc, mnc).
		Scan(&n.CCC, &n.Name, &n.Country, &n.Region, &n.Ranges)
	return n, err
}

func (c configQ) NetworkSpelling(ctx context.Context, mcc string, forms [3]string) (string, error) {
	var found string
	err := c.q.QueryRowContext(ctx, stOperatorSpelling.SQL, mcc, forms[0], forms[1], forms[2]).Scan(&found)
	return found, err
}

func (c configQ) NetworkRangeCount(ctx context.Context, mcc, mnc, prefix string) (int, error) {
	return c.count(ctx, stOperatorRangeCount, mcc, mnc, prefix+"%")
}

func (c configQ) NetworkRangeRows(ctx context.Context, mcc, mnc, prefix string, limit int) ([]dal.NumberRangeRow, error) {
	rows, err := c.q.QueryContext(ctx, stOperatorRanges.SQL, mcc, mnc, prefix+"%", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.NumberRangeRow
	for rows.Next() {
		r := dal.NumberRangeRow{MCC: mcc, MNC: mnc}
		if err := rows.Scan(&r.ID, &r.From, &r.To, &r.CCC, &r.Name, &r.Country, &r.Region); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return out, dal.RowsError{Err: err} // what was read, and why the read stopped
	}
	return out, nil
}

func (c configQ) OverlappingRange(ctx context.Context, length int, from, to string) (dal.RangeOverlap, error) {
	var o dal.RangeOverlap
	err := c.q.QueryRowContext(ctx, stOperatorOverlap.SQL, length, to, from).Scan(&o.ID, &o.Other, &o.From, &o.To)
	return o, err
}

func (c configQ) InsertRange(ctx context.Context, r dal.NumberRangeRow, shardKey string) (int64, error) {
	return c.insertID(ctx, stOperatorInsert.SQL,
		r.CCC, r.MCC, r.MNC, r.MCC+r.MNC, r.Name, r.Country, cfgStr(r.Region), r.From, r.To, shardKey)
}

func (c configQ) SetNetworkColumn(ctx context.Context, column, value, mcc, mnc string) (int64, error) {
	_ = stOperatorSetColumn
	col := quoteIdent(column)
	q := "UPDATE operators SET " + col + " = @p1" + stampRate + " WHERE mcc = @p2 AND mnc = @p3 AND " +
		col + " COLLATE Latin1_General_100_BIN2 IS DISTINCT FROM @p1 COLLATE Latin1_General_100_BIN2"
	return c.execRows(ctx, q, cfgStr(value), mcc, mnc)
}

func (c configQ) RangeByID(ctx context.Context, id int64) (dal.NumberRangeRow, error) {
	var r dal.NumberRangeRow
	err := c.q.QueryRowContext(ctx, stOperatorRange.SQL, id).
		Scan(&r.ID, &r.CCC, &r.MCC, &r.MNC, &r.Name, &r.Country, &r.From, &r.To)
	return r, err
}

func (c configQ) DeleteRangeRow(ctx context.Context, id int64) error {
	_, err := c.q.ExecContext(ctx, stOperatorDeleteRange.SQL, id)
	return err
}

func (c configQ) RoutesMatchingNetwork(ctx context.Context, mccmnc string) (int, error) {
	return c.count(ctx, stOperatorRouteUsers, mccmnc)
}

func (c configQ) OpenPricesForNetwork(ctx context.Context, mcc string, forms [3]string) (int, error) {
	return c.count(ctx, stOperatorPriceUsers, mcc, forms[0], forms[1], forms[2])
}

func (c configQ) DeleteNetworkRows(ctx context.Context, mcc, mnc string) (int64, error) {
	return c.execRows(ctx, stOperatorDeleteNetwork.SQL, mcc, mnc)
}
