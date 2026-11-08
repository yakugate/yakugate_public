package provision

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/dal"
)

// Balance top-ups and corrections. [SCOPE P14-BALANCE]
//
// The same rules as the portal's POST /api/companies/:id/balance
// (p13-backend/src/routes/companyRoutes.js adjustBalance), because both write
// the same two rows and the ledger must read the same whichever door was used:
//
//   - the company row is locked, the new balance computed from the locked
//     figure, and the balance and its balance_history row are written in ONE
//     transaction — a balance that moved without its ledger row cannot be
//     explained, and a ledger row without the move is a lie;
//   - the amount is signed and never zero; a change that would take the
//     balance below zero is refused (the billing role may push a balance
//     negative when it charges, but a person may not);
//   - positive = "topup", negative = "adjust", the two types the column says
//     the portal writes; the reference is unique per change;
//   - an audit_log row records who, what, before and after;
//   - a postpaid company is refused: its charges are invoiced and the engine
//     never draws its balance down, so money put there would sit unused
//     (the portal's 409, companyRoutes.js adjustBalance).
//
// Two things here differ from the portal, and neither is looser:
//
//   - a reason is required. Money moved by hand with no stated reason is the
//     one ledger line nobody can ever explain later;
//   - with Redis on, the live balance counter is changed directly (dal/redis
//     CreditLive). Without it the reserver's write-back copies the old live
//     figure over companies.balance within 30 seconds and the top-up is
//     silently erased while its ledger row still says it arrived. The portal
//     does the same through the engine's POST /admin/balance/credit (fixed
//     2026-09-18; before that its top-ups were being erased).
//
// Amounts are handled as decimal strings and integer micro-units, never
// floats: this is money, and 0.1 + 0.2 must be 0.3.

// maxBalanceMicro is the largest balance companies.balance (DECIMAL(14,4))
// holds: 9,999,999,999.9999.
const maxBalanceMicro = 99999999999999

// BalanceChange is what AdjustBalance did.
type BalanceChange struct {
	CompanyID int64
	Previous  string
	Amount    string
	New       string
	Currency  string
	Type      string
	Reference string
	Live      string // what happened to the live (Redis) counter
}

// ParseMicro reads a signed decimal with up to four places — the precision
// of DECIMAL(14,4) — into micro-units, exactly.
func ParseMicro(s string) (int64, error) {
	s = strings.TrimSpace(s)
	neg := false
	switch {
	case strings.HasPrefix(s, "-"):
		neg, s = true, s[1:]
	case strings.HasPrefix(s, "+"):
		s = s[1:]
	}
	if s == "" {
		return 0, fmt.Errorf("no number")
	}
	whole, frac, _ := strings.Cut(s, ".")
	if whole == "" {
		whole = "0"
	}
	if len(frac) > 4 {
		return 0, fmt.Errorf("more than four decimal places")
	}
	for _, part := range []string{whole, frac} {
		for _, r := range part {
			if r < '0' || r > '9' {
				return 0, fmt.Errorf("not a number")
			}
		}
	}
	if len(whole) > 10 { // DECIMAL(14,4) holds ten whole digits
		return 0, fmt.Errorf("too large")
	}
	w, _ := strconv.ParseInt(whole, 10, 64)
	frac += strings.Repeat("0", 4-len(frac))
	f, _ := strconv.ParseInt(frac, 10, 64)
	v := w*billing.MicroFactor + f
	if neg {
		v = -v
	}
	return v, nil
}

// FormatMicro writes micro-units back as a four-place decimal.
func FormatMicro(v int64) string {
	sign := ""
	if v < 0 {
		sign, v = "-", -v
	}
	return fmt.Sprintf("%s%d.%04d", sign, v/billing.MicroFactor, v%billing.MicroFactor)
}

// ValidateBalanceChange checks the amount and reason before anything is locked.
func ValidateBalanceChange(amount, reason string) error {
	v, err := ParseMicro(amount)
	if err != nil {
		return refusef("%q is not an amount: give a number like 500 or 1250.50 (up to four decimal places), with a minus sign to take money away — %v", amount, err)
	}
	if v == 0 {
		return refusef("an amount of zero changes nothing, and would still write a ledger line saying something happened")
	}
	r := strings.TrimSpace(reason)
	if r == "" {
		return refusef("a reason is required: money moved by hand with no reason is the one ledger line nobody can explain later")
	}
	if n := len([]rune(r)); n > 190 {
		return refusef("a reason may be 190 characters; this one is %d", n)
	}
	return nil
}

// AdjustBalance moves a company's balance by a signed amount, with its ledger
// row and its audit row, in one transaction.
func (s Store) AdjustBalance(ctx context.Context, companyID int64, amount, reason string) (BalanceChange, error) {
	out := BalanceChange{CompanyID: companyID}
	if err := ValidateBalanceChange(amount, reason); err != nil {
		return out, err
	}
	delta, _ := ParseMicro(amount)
	reason = strings.TrimSpace(reason)

	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()

	row, err := tx.CompanyBalanceForUpdate(ctx, companyID)
	name, prevStr, currency, prepaid := row.Name, row.Balance, row.Currency, row.Prepaid
	if err == dal.ErrNoRows {
		return out, refusef("there is no company #%d", companyID)
	}
	if err != nil {
		return out, fmt.Errorf("read the balance: %w", err)
	}
	if !prepaid {
		return out, refusef("%s is postpaid: its charges are invoiced and it has no balance to top up or adjust. Make it prepaid first if its messages should be paid for in advance", name)
	}
	prev, err := ParseMicro(prevStr)
	if err != nil {
		return out, fmt.Errorf("the stored balance %q could not be read: %w", prevStr, err)
	}
	next := prev + delta
	if next < 0 {
		return out, refusef("that would take %s's balance below zero (%s %s now, %s after). Take away no more than %s",
			name, FormatMicro(prev), currency, FormatMicro(next), FormatMicro(prev))
	}
	// DECIMAL(14,4)'s ceiling, as the portal refuses it (companyRoutes.js
	// MAX_BALANCE) rather than letting MySQL refuse it in its own words.
	if next > maxBalanceMicro {
		return out, refusef("the balance can be at most %s; that would make it %s", FormatMicro(maxBalanceMicro), FormatMicro(next))
	}
	out.Previous, out.Amount, out.New, out.Currency = FormatMicro(prev), FormatMicro(delta), FormatMicro(next), currency
	out.Type = "topup"
	if delta < 0 {
		out.Type = "adjust"
	}
	out.Reference = fmt.Sprintf("console:%d", time.Now().UnixNano())

	if err := tx.SetCompanyBalanceText(ctx, companyID, out.New); err != nil {
		return out, fmt.Errorf("change the balance: %w", err)
	}
	// account_id 0 is the schema's "no one account" sentinel, as the portal
	// writes it: a company top-up is not triggered by any one account.
	if err := tx.InsertBalanceChange(ctx, dal.BalanceChangeRow{
		CompanyID: companyID, Type: out.Type, Amount: out.Amount, Previous: out.Previous, New: out.New,
		Reference: out.Reference, Reason: reason,
	}); err != nil {
		return out, fmt.Errorf("write the ledger row: %w", err)
	}
	detail, _ := json.Marshal(map[string]any{
		"previous": out.Previous, "amount": out.Amount, "new": out.New,
		"reason": reason, "reference": out.Reference,
	})
	if err := tx.WriteBalanceAudit(ctx, companyID, name,
		fmt.Sprintf("Balance %s by %s from the console", out.Type, out.Amount), string(detail)); err != nil {
		return out, fmt.Errorf("write the audit row: %w", err)
	}

	// The live counter, while the row lock is still held (dal/redis creditScript
	// says why it must be this order). Unreachable Redis refuses the change:
	// a top-up the write-back will erase in 30 seconds is worse than none.
	out.Live = "no live counter (Redis is not configured)"
	if s.Live != nil {
		if err := s.Live.Credit(ctx, companyID, delta, next); err != nil {
			return out, fmt.Errorf("the live balance in Redis could not be changed, so nothing was saved (with Redis on, a change made only in MySQL is overwritten within 30 seconds): %w", err)
		}
		out.Live = "live counter in Redis changed too"
	}
	if err := tx.Commit(); err != nil {
		if s.Live != nil {
			if uerr := s.Live.Undo(ctx, companyID, delta); uerr != nil {
				return out, fmt.Errorf("the change did not save (%v) AND the live balance could not be put back (%v): company #%d's live balance is %s too high until corrected", err, uerr, companyID, out.Amount)
			}
		}
		return out, fmt.Errorf("the change did not save: %w", err)
	}
	return out, nil
}
