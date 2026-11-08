package provision

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/msisdn"
)

// Blocked numbers (table blocklist). [SCOPE P14-BLOCK]
//
// Pipeline stage 4, the first policy gate: a message to a blocked number is
// refused 403 DESTINATION_BLOCKED. One row blocks one number for one scope —
// every account (account_id and company_id NULL), one company's accounts
// (company_id), or one account (account_id) — optionally only from one
// sender (exact match), optionally until a time (expires_at; NULL = for
// good). A disabled row is not loaded at all.
//
// The rules here are the Admin API's (p13-backend resources.js `blocklist`
// and importRoutes.js), so the two doors cannot disagree:
//   - the number is cleaned on WRITE with the engine's own msisdn.Clean, so
//     what is stored is what matches (a row kept as "+92 300…" looks blocked
//     and is delivered — a silent DND bypass);
//   - digits only, at most 15;
//   - a TRAP number (source honeypot) must be global: scoped to one account
//     it protects nobody else, the opposite of what it is for;
//   - one row per (number, account, company, sender), checked NULL-safe —
//     the table's unique key cannot, and the engine collapses duplicates, so
//     deleting one of two would leave the number blocked while it looks
//     free.
//
// Stricter than the portal, never looser: at least 6 digits (the import's
// rule, applied to single adds too), an expiry must be in the future, the
// sender is capped at the column's 32 characters and the reason at 190, and
// "vendor" is not offered as a source — only the engine writes those.

// Block is one blocklist row as the console shows it.
type Block struct {
	ID        int64
	Number    string
	AccountID int64  // 0 = not scoped to an account
	CompanyID int64  // 0 = not scoped to a company
	Scope     string // "every account", "company X", "account Y"
	Sender    string // "" = every sender
	Source    string
	Reason    string
	Expires   time.Time // zero = never
	Disabled  bool
	Created   string
}

// BlockSources are the sources an operator may give a row.
func BlockSources() []string { return []string{"manual", "unsub", "abuse", "honeypot"} }

// BlockSourceWhat says what a source means.
func BlockSourceWhat(s string) string {
	switch s {
	case "manual":
		return "blocked by hand"
	case "unsub":
		return "the subscriber asked to stop"
	case "abuse":
		return "blocked after a complaint or abuse"
	case "honeypot":
		return "a TRAP number nobody owns: refused like any other, and it raises an operator alarm"
	case "vendor":
		return "a vendor's answer asked for it (written by the gateway)"
	}
	return ""
}

// CleanNumber is a number as it will be stored and matched, or a refusal.
func CleanNumber(raw string) (string, error) {
	c := msisdn.Clean(raw)
	if !isDigits(c) {
		return "", refusef("%q is not a number — only digits may remain once +, spaces and brackets are taken out", raw)
	}
	if len(c) < 6 || len(c) > 15 {
		return "", refusef("%q is %d digits; a number is 6 to 15", c, len(c))
	}
	return c, nil
}

// ValidateBlock checks a row before it is written.
func ValidateBlock(b Block, now time.Time) error {
	if _, err := CleanNumber(b.Number); err != nil {
		return err
	}
	if b.AccountID != 0 && b.CompanyID != 0 {
		return refusef("a block is for one account or one company, not both")
	}
	ok := false
	for _, s := range BlockSources() {
		ok = ok || b.Source == s
	}
	if !ok {
		return refusef("the source is manual, unsub, abuse or honeypot, not %q", b.Source)
	}
	if b.Source == "honeypot" && (b.AccountID != 0 || b.CompanyID != 0) {
		return refusef("a trap number must block every account — scoped to one, every other account could still send to it")
	}
	if len([]rune(b.Sender)) > 32 {
		return refusef("the sender is at most 32 characters")
	}
	if len([]rune(b.Reason)) > 190 {
		return refusef("the reason is at most 190 characters")
	}
	if !b.Expires.IsZero() && !b.Expires.After(now) {
		return refusef("the end date %s UTC has already passed", b.Expires.UTC().Format("2006-01-02 15:04"))
	}
	return nil
}

// BlockFilter narrows ListBlocks. Zero values mean no filter.
type BlockFilter struct {
	Number string // digits; matched as a prefix
	Limit  int    // default 50
}

// ListBlocks lists rows, newest first.
func (s Store) ListBlocks(ctx context.Context, f BlockFilter) ([]Block, int, error) {
	if f.Limit <= 0 {
		f.Limit = 50
	}
	like := ""
	if f.Number != "" {
		like = msisdn.Clean(f.Number) + "%"
	}
	total, err := s.DB.BlockCount(ctx, like)
	if err != nil {
		return nil, 0, fmt.Errorf("count blocked numbers: %w", err)
	}
	rows, err := s.DB.BlockRows(ctx, like, f.Limit)
	if err != nil {
		if re, ok := err.(dal.RowsError); ok {
			return nil, 0, re.Err
		}
		return nil, 0, fmt.Errorf("list blocked numbers: %w", err)
	}
	var out []Block
	for _, row := range rows {
		out = append(out, blockOf(row))
	}
	return out, total, nil
}

// blockOf is a row the store read, as the screens show it.
func blockOf(row dal.BlockRow) Block {
	b := Block{ID: row.ID, Number: row.Number, AccountID: row.AccountID, CompanyID: row.CompanyID,
		Sender: row.Sender, Source: row.Source, Reason: row.Reason, Disabled: row.Disabled, Created: row.Created}
	if row.Expires.Valid {
		b.Expires = row.Expires.Time
	}
	b.Scope = scopeName(b.AccountID, row.Account, b.CompanyID, row.Company)
	return b
}

func scopeName(acctID int64, acct string, coID int64, co string) string {
	switch {
	case acctID != 0:
		return "account " + orHash(acct, acctID)
	case coID != 0:
		return "company " + orHash(co, coID)
	}
	return "every account"
}

func orHash(name string, id int64) string {
	if name == "" {
		return "#" + itoa(id)
	}
	return name
}

// GetBlock reads one row.
func (s Store) GetBlock(ctx context.Context, id int64) (Block, error) {
	row, err := s.DB.BlockByID(ctx, id)
	if err == dal.ErrNoRows {
		return Block{}, refusef("there is no blocked-number entry #%d", id)
	}
	if err != nil {
		return Block{}, fmt.Errorf("read blocked number %d: %w", id, err)
	}
	return blockOf(row), nil
}

// blockDupe is the id of a row with the same number, account, company and
// sender (NULL-safe), or 0.
func (s Store) blockDupe(ctx context.Context, b Block) (int64, error) {
	id, err := s.DB.BlockTwin(ctx, b.Number, b.AccountID, b.CompanyID, b.Sender)
	if err == dal.ErrNoRows {
		return 0, nil
	}
	return id, err
}

// checkScope confirms the account or company exists.
func (s Store) checkScope(ctx context.Context, b Block) error {
	if b.AccountID != 0 {
		ok, err := s.DB.AccountExists(ctx, b.AccountID)
		if err != nil {
			return err
		}
		if !ok {
			return refusef("there is no account #%d", b.AccountID)
		}
	}
	if b.CompanyID != 0 {
		ok, err := s.DB.CompanyExists(ctx, b.CompanyID)
		if err != nil {
			return err
		}
		if !ok {
			return refusef("there is no company #%d", b.CompanyID)
		}
	}
	return nil
}

// AddBlock blocks one number and audits it as blocklist.create.
func (s Store) AddBlock(ctx context.Context, b Block) (int64, error) {
	num, err := CleanNumber(b.Number)
	if err != nil {
		return 0, err
	}
	b.Number = num
	b.Sender = strings.TrimSpace(b.Sender)
	b.Reason = strings.TrimSpace(b.Reason)
	if err := ValidateBlock(b, time.Now()); err != nil {
		return 0, err
	}
	if err := s.checkScope(ctx, b); err != nil {
		return 0, err
	}
	if id, err := s.blockDupe(ctx, b); err != nil {
		return 0, err
	} else if id != 0 {
		msg := fmt.Sprintf("%s is already blocked for this scope", b.Number)
		if b.Sender != "" {
			msg += fmt.Sprintf(" and sender %q", b.Sender)
		}
		return 0, refusef("%s — entry #%d. Change that one instead of adding a second", msg, id)
	}
	id, err := s.insertBlock(ctx, s.DB, b)
	if err != nil {
		return 0, err
	}
	return id, s.audit(ctx, auditEntry{
		Action: "blocklist.create", Entity: "blocklist entry", Feature: "blocklist", EntityID: id,
		AccountID: b.AccountID, CompanyID: b.CompanyID, Label: b.Number,
		Summary: "Created blocklist entry from the console", Detail: map[string]any{"after": blockDetail(b)},
	})
}

func (s Store) insertBlock(ctx context.Context, db dal.ConfigQueries, b Block) (int64, error) {
	id, err := db.InsertBlock(ctx, dal.BlockInsert{
		Number: b.Number, AccountID: b.AccountID, CompanyID: b.CompanyID,
		Sender: b.Sender, Source: b.Source, Reason: b.Reason, Expires: b.Expires,
	})
	if err != nil {
		return 0, fmt.Errorf("block %s: %w", b.Number, err)
	}
	return id, nil
}

// BulkResult reports a many-number add.
type BulkResult struct {
	Added, AlreadyBlocked int
	Bad                   []string // "line N: why"
}

// AddBlocks blocks many numbers — one per entry of lines — with the same
// scope, sender, source, reason and end date: the console's import. The
// caller splits its input (a file's first column, or a typed list). Every number is checked first; if any
// is not a number, nothing is written and every bad line is reported. A
// number already blocked for the scope is skipped, so the same file can be
// run twice. Written in one transaction, audited as ONE blocklist.import row,
// as the portal's import is.
func (s Store) AddBlocks(ctx context.Context, lines []string, tmpl Block) (BulkResult, error) {
	var res BulkResult
	tmpl.Sender = strings.TrimSpace(tmpl.Sender)
	tmpl.Reason = strings.TrimSpace(tmpl.Reason)
	tmpl.Number = "100000" // any valid number: the template's other fields are what is checked here
	if err := ValidateBlock(tmpl, time.Now()); err != nil {
		return res, err
	}
	if err := s.checkScope(ctx, tmpl); err != nil {
		return res, err
	}
	var nums []string
	seen := map[string]bool{}
	for i, tok := range lines {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		n, err := CleanNumber(tok)
		if err != nil {
			if i == 0 && !strings.ContainsAny(tok, "0123456789") {
				continue // a header row, e.g. "msisdn"
			}
			res.Bad = append(res.Bad, fmt.Sprintf("line %d: %s", i+1, Reason(err)))
			continue
		}
		if !seen[n] {
			seen[n] = true
			nums = append(nums, n)
		}
	}
	if len(res.Bad) > 0 {
		return res, refusef("%d line(s) are not numbers; nothing was blocked", len(res.Bad))
	}
	if len(nums) == 0 {
		return res, refusef("no numbers given")
	}
	if len(nums) > 100000 {
		return res, refusef("%d numbers; at most 100,000 at a time — split the file", len(nums))
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return res, err
	}
	defer tx.Rollback()
	for _, n := range nums {
		b := tmpl
		b.Number = n
		dupe, err := tx.BlockTwinInImport(ctx, n, b.AccountID, b.CompanyID, b.Sender)
		if err != nil && err != dal.ErrNoRows {
			return res, err
		}
		if dupe != 0 {
			res.AlreadyBlocked++
			continue
		}
		if _, err := s.insertBlock(ctx, tx, b); err != nil {
			return res, err
		}
		res.Added++
	}
	if err := tx.Commit(); err != nil {
		return res, err
	}
	return res, s.audit(ctx, auditEntry{
		Action: "blocklist.import", Entity: "blocklist", Feature: "blocklist",
		AccountID: tmpl.AccountID, CompanyID: tmpl.CompanyID,
		Summary: fmt.Sprintf("Blocked %d numbers from the console", res.Added),
		Detail: map[string]any{"submitted": len(nums), "written": res.Added, "duplicates": res.AlreadyBlocked,
			"source": tmpl.Source, "sender": nullStr(tmpl.Sender)},
	})
}

// SetBlockDisabled switches a row off (kept, not enforced) or back on.
func (s Store) SetBlockDisabled(ctx context.Context, id int64, off bool) error {
	cur, err := s.GetBlock(ctx, id)
	if err != nil {
		return err
	}
	if err := s.DB.SetBlockDisabled(ctx, id, boolInt(off)); err != nil {
		return fmt.Errorf("update blocked number %d: %w", id, err)
	}
	after := cur
	after.Disabled = off
	return s.audit(ctx, auditEntry{
		Action: "blocklist.update", Entity: "blocklist entry", Feature: "blocklist", EntityID: id,
		AccountID: cur.AccountID, CompanyID: cur.CompanyID, Label: cur.Number,
		Summary: "Updated blocklist entry from the console",
		Detail:  map[string]any{"before": blockDetail(cur), "after": blockDetail(after)},
	})
}

// SetBlockExpiry sets or clears (zero time) when a block ends.
func (s Store) SetBlockExpiry(ctx context.Context, id int64, until time.Time) error {
	cur, err := s.GetBlock(ctx, id)
	if err != nil {
		return err
	}
	after := cur
	after.Expires = until
	if err := ValidateBlock(after, time.Now()); err != nil {
		return err
	}
	var exp dal.NullTime
	if !until.IsZero() {
		exp = dal.NullTime{Time: until.UTC(), Valid: true}
	}
	if err := s.DB.SetBlockExpiry(ctx, id, exp); err != nil {
		return fmt.Errorf("update blocked number %d: %w", id, err)
	}
	return s.audit(ctx, auditEntry{
		Action: "blocklist.update", Entity: "blocklist entry", Feature: "blocklist", EntityID: id,
		AccountID: cur.AccountID, CompanyID: cur.CompanyID, Label: cur.Number,
		Summary: "Updated blocklist entry from the console",
		Detail:  map[string]any{"before": blockDetail(cur), "after": blockDetail(after)},
	})
}

// DeleteBlock removes a row: the number can be sent to again (unless another
// row still blocks it).
func (s Store) DeleteBlock(ctx context.Context, id int64) error {
	cur, err := s.GetBlock(ctx, id)
	if err != nil {
		return err
	}
	if err := s.DB.DeleteBlockRow(ctx, id); err != nil {
		return fmt.Errorf("delete blocked number %d: %w", id, err)
	}
	return s.audit(ctx, auditEntry{
		Action: "blocklist.delete", Entity: "blocklist entry", Feature: "blocklist", EntityID: id,
		AccountID: cur.AccountID, CompanyID: cur.CompanyID, Label: cur.Number,
		Summary: "Deleted blocklist entry from the console", Detail: map[string]any{"before": blockDetail(cur)},
	})
}

// BlocksFor lists every row that blocks a number, for the "is this number
// blocked?" check — all of them, because a number blocked twice stays
// blocked when one row goes.
func (s Store) BlocksFor(ctx context.Context, number string) ([]Block, error) {
	n, err := CleanNumber(number)
	if err != nil {
		return nil, err
	}
	ids, err := s.DB.BlockIDsFor(ctx, n)
	if err != nil {
		return nil, err
	}
	var out []Block
	for _, id := range ids {
		b, err := s.GetBlock(ctx, id)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, nil
}

// Active reports whether a row is enforced now.
func (b Block) Active(now time.Time) bool {
	return !b.Disabled && (b.Expires.IsZero() || b.Expires.After(now))
}

func blockDetail(b Block) map[string]any {
	var exp any
	if !b.Expires.IsZero() {
		exp = b.Expires.UTC().Format(time.RFC3339)
	}
	return map[string]any{
		"msisdn": b.Number, "account_id": nullID(b.AccountID), "company_id": nullID(b.CompanyID),
		"sender": nullStr(b.Sender), "source": b.Source, "reason": nullStr(b.Reason),
		"expires_at": exp, "is_disabled": boolInt(b.Disabled),
	}
}
