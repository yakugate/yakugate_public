package mssql

import (
	"context"

	"github.com/yakugate/yakugate/internal/dal"
)

// The spam rules and blocked numbers (dal.ContentRules) on SQL Server, made
// like internal/dal/mysql/config_content.go.

// listTypeOrder sorts spam_keywords.list_type as MySQL sorts its
// ENUM('blocked','allowed'): by the list's order.
const listTypeOrder = "CASE r.list_type WHEN 'blocked' THEN 1 WHEN 'allowed' THEN 2 ELSE 0 END"

const stampContent = ", updated_at = SYSUTCDATETIME()"

var (
	stSpamRules = addConfig("config.content.spam_rules", `
		SELECT `+wholeTable+` r.id, COALESCE(r.company_id,0), COALESCE(c.name,''), r.list_type, COALESCE(r.action,'reject'),
		       r.category, COALESCE(r.destination_country_code,''), COALESCE(r.keyword_regex,'null'),
		       COALESCE(r.keywords,'[]')
		  FROM spam_keywords r
		  LEFT JOIN companies c ON c.id = r.company_id
		 ORDER BY CASE WHEN r.company_id IS NOT NULL THEN 1 ELSE 0 END, r.company_id, `+listTypeOrder+` DESC, r.category, r.id`)
	stSpamClash = addConfig("config.content.spam_clash", `
		SELECT TOP (1) id FROM spam_keywords
		 WHERE company_id IS NOT DISTINCT FROM ? AND destination_country_code IS NOT DISTINCT FROM ? AND list_type = ?
		   AND LOWER(category) = LOWER(?) AND id <> ?`)
	stInsertSpam = addConfig("config.content.insert_spam", `
		INSERT INTO spam_keywords (company_id, destination_country_code, list_type, action, category,
		                           keywords, keyword_regex, created_by_user_id)
		OUTPUT INSERTED.id
		VALUES (?, ?, ?, ?, ?, JSON_ARRAY(), ?, 'console')`)
	stSetSpamPattern = addConfig("config.content.set_spam_pattern",
		"UPDATE spam_keywords SET keyword_regex = ?, keywords = JSON_ARRAY()"+stampContent+" WHERE id = ?")
	stSetSpamName     = addConfig("config.content.set_spam_name", "UPDATE spam_keywords SET category = ?"+stampContent+" WHERE id = ?")
	stSetSpamAction   = addConfig("config.content.set_spam_action", "UPDATE spam_keywords SET action = ?"+stampContent+" WHERE id = ?")
	stDeleteSpam      = addConfig("config.content.delete_spam", "DELETE FROM spam_keywords WHERE id = ?")
	stSpamCheckCounts = addConfig("config.content.spam_check_counts",
		"SELECT COUNT(CASE WHEN enforce_spam_check <> 0 THEN 1 END), COUNT(*) FROM sms_service_config")

	stBlockCountAll  = addConfig("config.content.block_count_all", "SELECT COUNT(*) FROM blocklist b ")
	stBlockCountLike = addConfig("config.content.block_count_like", "SELECT COUNT(*) FROM blocklist b "+blockWhereLike)
	stBlockRowsAll   = addConfig("config.content.block_rows_all", blockRowsSQL("@p1", ""))
	stBlockRowsLike  = addConfig("config.content.block_rows_like", blockRowsSQL("@p2", blockWhereLike))
	stBlockByID      = addConfig("config.content.block_by_id", `
		SELECT b.id, b.msisdn, COALESCE(b.account_id,0), COALESCE(b.company_id,0),
		       COALESCE(a.username,''), COALESCE(c.name,''), COALESCE(b.sender,''),
		       b.source, COALESCE(b.reason,''), b.expires_at, b.is_disabled,
		       COALESCE(CONVERT(CHAR(10), b.created_at, 23),'')
		  FROM blocklist b
		  LEFT JOIN sms_service_config a ON a.id = b.account_id
		  LEFT JOIN companies c ON c.id = b.company_id
		 WHERE b.id = ?`)
	stBlockTwin = addConfig("config.content.block_twin", `
		SELECT TOP (1) id FROM blocklist
		 WHERE msisdn = ? AND account_id IS NOT DISTINCT FROM ? AND company_id IS NOT DISTINCT FROM ?
		   AND sender IS NOT DISTINCT FROM ?`)
	stBlockTwinImport = addConfig("config.content.block_twin_import", `
			SELECT TOP (1) id FROM blocklist
			 WHERE msisdn = ? AND account_id IS NOT DISTINCT FROM ? AND company_id IS NOT DISTINCT FROM ?
			   AND sender IS NOT DISTINCT FROM ?`)
	stInsertBlock = addConfig("config.content.insert_block", `
		INSERT INTO blocklist (msisdn, account_id, company_id, sender, source, reason, expires_at,
		                       is_disabled, created_by_user_id)
		OUTPUT INSERTED.id
		VALUES (?, ?, ?, ?, ?, ?, ?, 0, 'console')`)
	stSetBlockDisabled = addConfig("config.content.set_block_disabled", "UPDATE blocklist SET is_disabled = ?"+stampContent+" WHERE id = ?")
	stSetBlockExpiry   = addConfig("config.content.set_block_expiry", "UPDATE blocklist SET expires_at = ?"+stampContent+" WHERE id = ?")
	stDeleteBlock      = addConfig("config.content.delete_block", "DELETE FROM blocklist WHERE id = ?")
	stBlockIDsFor      = addConfig("config.content.block_ids_for", "SELECT id FROM blocklist WHERE msisdn = ? ORDER BY id")
)

// blockWhereLike is the list's filter by the start of the number.
const blockWhereLike = "WHERE b.msisdn LIKE ?"

// blockRowsSQL is the list of blocked numbers: at most top rows, newest
// first, with its filter (or none).
func blockRowsSQL(top, where string) string {
	return `
		SELECT TOP (` + top + `) b.id, b.msisdn, COALESCE(b.account_id,0), COALESCE(b.company_id,0),
		       COALESCE(a.username,''), COALESCE(c.name,''), COALESCE(b.sender,''),
		       b.source, COALESCE(b.reason,''), b.expires_at, b.is_disabled,
		       COALESCE(CONVERT(CHAR(10), b.created_at, 23),'')
		  FROM blocklist b
		  LEFT JOIN sms_service_config a ON a.id = b.account_id
		  LEFT JOIN companies c ON c.id = b.company_id
		 ` + where + `
		 ORDER BY b.id DESC`
}

func (c configQ) SpamRuleRows(ctx context.Context) ([]dal.SpamRuleRow, error) {
	rows, err := c.q.QueryContext(ctx, stSpamRules.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.SpamRuleRow
	for rows.Next() {
		var r dal.SpamRuleRow
		if err := rows.Scan(&r.ID, &r.CompanyID, &r.Company, &r.Kind, &r.Action, &r.Name, &r.Prefix, &r.PatternJSON, &r.WordsJSON); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, dal.RowsError{Err: err}
	}
	return out, nil
}

func (c configQ) SpamRuleClash(ctx context.Context, companyID int64, prefix, kind, name string, self int64) (int64, error) {
	var id int64
	err := c.q.QueryRowContext(ctx, stSpamClash.SQL, cfgID(companyID), cfgStr(prefix), Varchar(kind), name, self).Scan(&id)
	return id, err
}

func (c configQ) InsertSpamRule(ctx context.Context, r dal.SpamRuleInsert) (int64, error) {
	return c.insertID(ctx, stInsertSpam.SQL,
		cfgID(r.CompanyID), cfgStr(r.Prefix), Varchar(r.Kind), Varchar(r.Action), r.Name, r.PatternJSON)
}

func (c configQ) SetSpamPattern(ctx context.Context, id int64, patternJSON string) error {
	_, err := c.q.ExecContext(ctx, stSetSpamPattern.SQL, patternJSON, id)
	return err
}

func (c configQ) SetSpamName(ctx context.Context, id int64, name string) error {
	_, err := c.q.ExecContext(ctx, stSetSpamName.SQL, name, id)
	return err
}

func (c configQ) SetSpamAction(ctx context.Context, id int64, action string) error {
	_, err := c.q.ExecContext(ctx, stSetSpamAction.SQL, Varchar(action), id)
	return err
}

func (c configQ) DeleteSpamRow(ctx context.Context, id int64) error {
	_, err := c.q.ExecContext(ctx, stDeleteSpam.SQL, id)
	return err
}

func (c configQ) SpamCheckAccounts(ctx context.Context) (on, total int, err error) {
	err = c.q.QueryRowContext(ctx, stSpamCheckCounts.SQL).Scan(&on, &total)
	return on, total, err
}

func (c configQ) BlockCount(ctx context.Context, pattern string) (int, error) {
	var total int
	var err error
	if pattern == "" {
		err = c.q.QueryRowContext(ctx, stBlockCountAll.SQL).Scan(&total)
	} else {
		err = c.q.QueryRowContext(ctx, stBlockCountLike.SQL, pattern).Scan(&total)
	}
	return total, err
}

func scanBlock(sc interface{ Scan(...any) error }) (dal.BlockRow, error) {
	var b dal.BlockRow
	err := sc.Scan(&b.ID, &b.Number, &b.AccountID, &b.CompanyID, &b.Account, &b.Company, &b.Sender,
		&b.Source, &b.Reason, &b.Expires, &b.Disabled, &b.Created)
	return b, err
}

func (c configQ) BlockRows(ctx context.Context, pattern string, limit int) ([]dal.BlockRow, error) {
	st, args := stBlockRowsAll, []any{limit}
	if pattern != "" {
		st, args = stBlockRowsLike, []any{pattern, limit}
	}
	rows, err := c.q.QueryContext(ctx, st.SQL, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.BlockRow
	for rows.Next() {
		b, err := scanBlock(rows)
		if err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, b)
	}
	if err := rows.Err(); err != nil {
		return nil, dal.RowsError{Err: err}
	}
	return out, nil
}

func (c configQ) BlockByID(ctx context.Context, id int64) (dal.BlockRow, error) {
	return scanBlock(c.q.QueryRowContext(ctx, stBlockByID.SQL, id))
}

func (c configQ) BlockTwin(ctx context.Context, number string, accountID, companyID int64, sender string) (int64, error) {
	var id int64
	err := c.q.QueryRowContext(ctx, stBlockTwin.SQL, number, cfgID(accountID), cfgID(companyID), cfgStr(sender)).Scan(&id)
	return id, err
}

func (c configQ) BlockTwinInImport(ctx context.Context, number string, accountID, companyID int64, sender string) (int64, error) {
	var id int64
	err := c.q.QueryRowContext(ctx, stBlockTwinImport.SQL, number, cfgID(accountID), cfgID(companyID), cfgStr(sender)).Scan(&id)
	return id, err
}

func (c configQ) InsertBlock(ctx context.Context, b dal.BlockInsert) (int64, error) {
	return c.insertID(ctx, stInsertBlock.SQL,
		b.Number, cfgID(b.AccountID), cfgID(b.CompanyID), cfgStr(b.Sender), Varchar(b.Source), cfgStr(b.Reason), cfgTime(b.Expires))
}

func (c configQ) SetBlockDisabled(ctx context.Context, id int64, off int) error {
	_, err := c.q.ExecContext(ctx, stSetBlockDisabled.SQL, off, id)
	return err
}

func (c configQ) SetBlockExpiry(ctx context.Context, id int64, until dal.NullTime) error {
	_, err := c.q.ExecContext(ctx, stSetBlockExpiry.SQL, cfgNullTime(until), id)
	return err
}

func (c configQ) DeleteBlockRow(ctx context.Context, id int64) error {
	_, err := c.q.ExecContext(ctx, stDeleteBlock.SQL, id)
	return err
}

func (c configQ) BlockIDsFor(ctx context.Context, number string) ([]int64, error) {
	rows, err := c.q.QueryContext(ctx, stBlockIDsFor.SQL, number)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}
