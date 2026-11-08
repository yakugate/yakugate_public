package mysql

import (
	"context"

	"github.com/yakugate/yakugate/internal/dal"
)

// The spam rules and blocked numbers (dal.ContentRules), moved from
// internal/provision (spam.go, blocklist.go) on 29 Oct 2026, word for word.

var (
	stSpamRules = ConfigStmts.Add("config.content.spam_rules", `
		SELECT r.id, COALESCE(r.company_id,0), COALESCE(c.name,''), r.list_type, COALESCE(r.action,'reject'),
		       r.category, COALESCE(r.destination_country_code,''), COALESCE(r.keyword_regex,'null'),
		       COALESCE(r.keywords,'[]')
		  FROM spam_keywords r
		  LEFT JOIN companies c ON c.id = r.company_id
		 ORDER BY r.company_id IS NOT NULL, r.company_id, r.list_type DESC, r.category, r.id`, false)
	stSpamClash = ConfigStmts.Add("config.content.spam_clash", `
		SELECT id FROM spam_keywords
		 WHERE company_id <=> ? AND destination_country_code <=> ? AND list_type = ?
		   AND LOWER(category) = LOWER(?) AND id <> ?
		 LIMIT 1`, false)
	stInsertSpam = ConfigStmts.Add("config.content.insert_spam", `
		INSERT INTO spam_keywords (company_id, destination_country_code, list_type, action, category,
		                           keywords, keyword_regex, created_by_user_id)
		VALUES (?, ?, ?, ?, ?, JSON_ARRAY(), CAST(? AS JSON), 'console')`, false)
	stSetSpamPattern = ConfigStmts.Add("config.content.set_spam_pattern",
		"UPDATE spam_keywords SET keyword_regex = CAST(? AS JSON), keywords = JSON_ARRAY() WHERE id = ?", false)
	stSetSpamName     = ConfigStmts.Add("config.content.set_spam_name", "UPDATE spam_keywords SET category = ? WHERE id = ?", false)
	stSetSpamAction   = ConfigStmts.Add("config.content.set_spam_action", "UPDATE spam_keywords SET action = ? WHERE id = ?", false)
	stDeleteSpam      = ConfigStmts.Add("config.content.delete_spam", "DELETE FROM spam_keywords WHERE id = ?", false)
	stSpamCheckCounts = ConfigStmts.Add("config.content.spam_check_counts",
		"SELECT COALESCE(SUM(enforce_spam_check <> 0),0), COUNT(*) FROM sms_service_config", false)

	stBlockCountAll  = ConfigStmts.Add("config.content.block_count_all", "SELECT COUNT(*) FROM blocklist b ", false)
	stBlockCountLike = ConfigStmts.Add("config.content.block_count_like", "SELECT COUNT(*) FROM blocklist b "+blockWhereLike, false)
	stBlockRowsAll   = ConfigStmts.Add("config.content.block_rows_all", blockRowsHead+`
		 ORDER BY b.id DESC LIMIT ?`, false)
	stBlockRowsLike = ConfigStmts.Add("config.content.block_rows_like", blockRowsHead+blockWhereLike+`
		 ORDER BY b.id DESC LIMIT ?`, false)
	stBlockByID = ConfigStmts.Add("config.content.block_by_id", `
		SELECT b.id, b.msisdn, COALESCE(b.account_id,0), COALESCE(b.company_id,0),
		       COALESCE(a.username,''), COALESCE(c.name,''), COALESCE(b.sender,''),
		       b.source, COALESCE(b.reason,''), b.expires_at, b.is_disabled,
		       COALESCE(DATE_FORMAT(b.created_at,'%Y-%m-%d'),'')
		  FROM blocklist b
		  LEFT JOIN sms_service_config a ON a.id = b.account_id
		  LEFT JOIN companies c ON c.id = b.company_id
		 WHERE b.id = ?`, false)
	stBlockTwin = ConfigStmts.Add("config.content.block_twin", `
		SELECT id FROM blocklist
		 WHERE msisdn = ? AND account_id <=> ? AND company_id <=> ? AND sender <=> ?
		 LIMIT 1`, false)
	stBlockTwinImport = ConfigStmts.Add("config.content.block_twin_import", `
			SELECT id FROM blocklist
			 WHERE msisdn = ? AND account_id <=> ? AND company_id <=> ? AND sender <=> ? LIMIT 1`, false)
	stInsertBlock = ConfigStmts.Add("config.content.insert_block", `
		INSERT INTO blocklist (msisdn, account_id, company_id, sender, source, reason, expires_at,
		                       is_disabled, created_by_user_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, 0, 'console')`, false)
	stSetBlockDisabled = ConfigStmts.Add("config.content.set_block_disabled", "UPDATE blocklist SET is_disabled = ? WHERE id = ?", false)
	stSetBlockExpiry   = ConfigStmts.Add("config.content.set_block_expiry", "UPDATE blocklist SET expires_at = ? WHERE id = ?", false)
	stDeleteBlock      = ConfigStmts.Add("config.content.delete_block", "DELETE FROM blocklist WHERE id = ?", false)
	stBlockIDsFor      = ConfigStmts.Add("config.content.block_ids_for", "SELECT id FROM blocklist WHERE msisdn = ? ORDER BY id", false)
)

// blockWhereLike is the list's filter by the start of the number.
const blockWhereLike = "WHERE b.msisdn LIKE ?"

// blockRowsHead is the list of blocked numbers up to its filter: the text
// before the WHERE (or none) and the ORDER BY.
const blockRowsHead = `
		SELECT b.id, b.msisdn, COALESCE(b.account_id,0), COALESCE(b.company_id,0),
		       COALESCE(a.username,''), COALESCE(c.name,''), COALESCE(b.sender,''),
		       b.source, COALESCE(b.reason,''), b.expires_at, b.is_disabled,
		       COALESCE(DATE_FORMAT(b.created_at,'%Y-%m-%d'),'')
		  FROM blocklist b
		  LEFT JOIN sms_service_config a ON a.id = b.account_id
		  LEFT JOIN companies c ON c.id = b.company_id
		 `

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
	err := c.q.QueryRowContext(ctx, stSpamClash.SQL, nullID(companyID), nullStr(prefix), kind, name, self).Scan(&id)
	return id, err
}

func (c configQ) InsertSpamRule(ctx context.Context, r dal.SpamRuleInsert) (int64, error) {
	res, err := c.q.ExecContext(ctx, stInsertSpam.SQL,
		nullID(r.CompanyID), nullStr(r.Prefix), r.Kind, r.Action, r.Name, r.PatternJSON)
	if err != nil {
		return 0, err
	}
	id, _ := res.LastInsertId()
	return id, nil
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
	_, err := c.q.ExecContext(ctx, stSetSpamAction.SQL, action, id)
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
	err := c.q.QueryRowContext(ctx, stBlockTwin.SQL, number, nullID(accountID), nullID(companyID), nullStr(sender)).Scan(&id)
	return id, err
}

func (c configQ) BlockTwinInImport(ctx context.Context, number string, accountID, companyID int64, sender string) (int64, error) {
	var id int64
	err := c.q.QueryRowContext(ctx, stBlockTwinImport.SQL, number, nullID(accountID), nullID(companyID), nullStr(sender)).Scan(&id)
	return id, err
}

func (c configQ) InsertBlock(ctx context.Context, b dal.BlockInsert) (int64, error) {
	var exp any
	if !b.Expires.IsZero() {
		exp = b.Expires.UTC()
	}
	res, err := c.q.ExecContext(ctx, stInsertBlock.SQL,
		b.Number, nullID(b.AccountID), nullID(b.CompanyID), nullStr(b.Sender), b.Source, nullStr(b.Reason), exp)
	if err != nil {
		return 0, err
	}
	id, _ := res.LastInsertId()
	return id, nil
}

func (c configQ) SetBlockDisabled(ctx context.Context, id int64, off int) error {
	_, err := c.q.ExecContext(ctx, stSetBlockDisabled.SQL, off, id)
	return err
}

func (c configQ) SetBlockExpiry(ctx context.Context, id int64, until dal.NullTime) error {
	var exp any
	if until.Valid {
		exp = until.Time
	}
	_, err := c.q.ExecContext(ctx, stSetBlockExpiry.SQL, exp, id)
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
