package billing

import (
	"encoding/json"
	"fmt"
	"math"
)

// The per-message ledger settlement that lived here (Charge, ChargeBatch,
// ChargeBatchLive, LedgerCharges, Reserve, Refund) had no caller left once
// the billing role was retired (26 Oct 2026); it was retired with the move
// into the DAL (29 Oct 2026) and is kept in _to_delete/internal/billing/
// charge.go. What a hold on the balance of record does is
// dal.CompanyBalances now (DBReserver).

// ChargeOutcome says what happened to one settle attempt.
type ChargeOutcome int

const (
	ChargeSkipped   ChargeOutcome = iota // unlinked account — no company to bill
	ChargeApplied                        // prepaid: balance deducted + ledger row appended
	ChargeDuplicate                      // ledger already has this msg_id — no-op
	ChargeRecorded                       // postpaid: ledger row appended, balance untouched (D-106)
)

// ChargeItem is one message (or receipt) to settle in a ChargeBatch.
type ChargeItem struct {
	AccountID int64
	Amount    float64
	Reference string // the ledger idempotency key: msg_id, or dlr:<msg_id>:<row id>

	// PartsOf, when set, makes this a PART-COUNT CORRECTION of a message
	// charged at acceptance (PO 2026-09-21: the client pays for the parts
	// the vendor received). Amount is then ignored: the correction is the
	// price per part taken at acceptance times (SentParts - ChargedParts),
	// from that message's own commit row, so a later rate change is never
	// charged — only the difference in parts. Booked as type 'adjust' under
	// Reference ("parts:<msg_id>"), prepaid only; skipped when the message
	// has no acceptance charge.
	PartsOf                 string
	ChargedParts, SentParts int

	// Model is the billing model the message was ACCEPTED under
	// (model.BillingPrepaid / BillingPostpaid, stamped on the record,
	// hot/0051). It wins over the company's model at settle time, so a
	// company switched from postpaid to prepaid before its last messages
	// were rated has them invoiced, not taken from the new balance — and the
	// other way round (QA BUG-024). "" = not known: the company's model now.
	Model string
}

// prepaidFor says whether one item settles against the balance: the model
// it was accepted under when known, else the company's model now.
func prepaidFor(it ChargeItem, companyPrepaid bool) bool {
	switch it.Model {
	case "prepaid":
		return true
	case "postpaid":
		return false
	}
	return companyPrepaid
}

// PartsCorrection is the item that settles a message charged at acceptance
// for chargedParts when the vendor was sent sentParts.
func PartsCorrection(accountID int64, msgID string, chargedParts, sentParts int) ChargeItem {
	return ChargeItem{AccountID: accountID, Reference: "parts:" + msgID, PartsOf: msgID,
		ChargedParts: chargedParts, SentParts: sentParts}
}

// moneyScale is the ledger columns' scale: DECIMAL(14,4).
const moneyScale = 10000

// ReasonAtAcceptance is the reason on the 'commit' row an acceptance
// reservation writes (DBReserver / RedisReserver ReserveFor). It is how the
// ledger tells a charge taken AT ACCEPTANCE — for the parts counted then —
// from one the billing role wrote for the parts it rated: only the first is
// ever corrected for parts (PartsCorrection) or followed per part by the
// record (FollowLedger). Change it and older rows stop being recognised.
const ReasonAtAcceptance = "message charge (exact price, taken at acceptance)"

// RoundLedger is an amount as the ledger takes it: whole ten-thousandths,
// rounded as every charge is (toUnits). The acceptance reservation takes
// exactly this, so the reply, the ledger row and the live balance agree.
func RoundLedger(v float64) float64 { return float64(toUnits(v)) / moneyScale }

// toUnits converts an amount to whole ten-thousandths, rounding half away
// from zero as MySQL does when it stores into DECIMAL(14,4). Exact for every
// value the column can hold (1e10 * 1e4 is far inside float64's 2^53).
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

// LedgerCharge is what the ledger already charged for one message: the
// amount of its 'commit' or 'invoice_item' row, as a positive figure in the
// company's currency, and whether it was taken at acceptance (the
// reservation's row, for the parts counted then) rather than written by the
// billing role (for the parts it rated).
type LedgerCharge struct {
	Amount       float64
	AtAcceptance bool
}

// FollowLedger makes a rating agree with what the ledger already charged for
// the message. charged is the ledger amount (company currency, positive) for
// chargedParts parts; the rating r was made now for ratedParts parts. When
// the two prices per part agree to the ledger's ten-thousandth, r is returned
// unchanged. Otherwise the record keeps the price taken then — per part, as
// charged, times the parts rated (the parts correction settles any
// difference in parts at that same price) — and the audit detail says so.
// The sell figures are scaled together, so net + tax still make the total and
// a converted price stays in its card's currency; with nothing priced now the
// charge itself is recorded, in companyCurrency.
func FollowLedger(r Rated, charged float64, chargedParts, ratedParts int, companyCurrency string) Rated {
	if chargedParts < 1 {
		chargedParts = 1
	}
	if ratedParts < 1 {
		ratedParts = 1
	}
	want := charged / float64(chargedParts) * float64(ratedParts)
	// The price per part now is the price per part taken: nothing to follow.
	// (A charge the billing role wrote was for the parts it rated, so the
	// caller passes chargedParts == ratedParts for it. Comparing the totals
	// as well, as this once did, also matched an acceptance charge whose
	// total happened to equal a new price for a different number of parts —
	// 0.10 for 2 parts then, 0.10 for 3 now — and kept a record the parts
	// correction then contradicted.)
	if r.SellCharge != nil && toUnits(*r.SellCharge/float64(ratedParts)*float64(chargedParts)) == toUnits(charged) {
		return r
	}
	// Only the exchange rate moved (QA billing report #4, 2026-10-07): the
	// card's price is what it was, so the record keeps it — and the rate the
	// charge works out to — instead of a rescaled price that is on no card
	// beside a rate this message was never charged at.
	if fx, ok := followFX(r, want); ok {
		return fx
	}
	var ratedNow *float64
	if r.SellCharge != nil && *r.SellCharge > 0 && r.Sell != nil {
		now := *r.SellCharge
		ratedNow = &now
		k := want / now
		scale := func(p *float64) *float64 {
			if p == nil {
				return nil
			}
			v := *p * k
			return &v
		}
		r.Sell, r.SellNet, r.SellTax = scale(r.Sell), scale(r.SellNet), scale(r.SellTax)
	} else {
		v := want
		r.Sell, r.SellNet, r.SellTax, r.SellTaxPct = &v, nil, nil, nil
		if companyCurrency != "" {
			r.Currency = companyCurrency
		}
	}
	w := want
	r.SellCharge = &w
	m := map[string]any{}
	if r.Detail != "" {
		if json.Unmarshal([]byte(r.Detail), &m) != nil {
			m = map[string]any{}
		}
	}
	note := map[string]any{
		"amount": float64(toUnits(charged)) / moneyScale, "parts": chargedParts,
		"note": "the record keeps the price this message was charged; the rate cards price it differently now",
	}
	if companyCurrency != "" {
		note["currency"] = companyCurrency
	}
	if ratedNow != nil {
		note["rated_now"] = *ratedNow
	}
	m["charged"] = note
	if b, err := json.Marshal(m); err == nil {
		r.Detail = string(b)
	}
	return r
}

// fxMoveMax is how far apart the rate a charge works out to and the rate now
// may be for the difference to be read as the exchange rate having moved. A
// rate does not move a twentieth between a message being accepted and being
// rated; a bigger gap is the card's price having changed, and the record
// follows the ledger by price as before.
const fxMoveMax = 0.05

// followFX is FollowLedger for a message whose card price stands and whose
// exchange rate moved after it was charged. want is the charge the record
// must show (company currency). ok=false when the rating converted nothing,
// or the gap is too wide to be the rate.
func followFX(r Rated, want float64) (Rated, bool) {
	if r.Sell == nil || *r.Sell <= 0 || r.SellCharge == nil || *r.SellCharge <= 0 || r.Detail == "" {
		return r, false
	}
	m := map[string]any{}
	if json.Unmarshal([]byte(r.Detail), &m) != nil {
		return r, false
	}
	fx, _ := m["fx"].(map[string]any)
	now, _ := fx["rate"].(float64)
	if fx == nil || now <= 0 {
		return r, false
	}
	then := want / *r.Sell
	if d := then/now - 1; d > fxMoveMax || d < -fxMoveMax {
		return r, false
	}
	then = roundSig(then, 6)
	m["fx"] = map[string]any{"rate": then, "from": fx["from"], "to": fx["to"], "at": "acceptance"}
	m["charged"] = map[string]any{
		"amount": float64(toUnits(want)) / moneyScale, "currency": fx["to"],
		"rate_now": now, "rated_now": *r.SellCharge,
		"note": "the exchange rate moved after this message was charged; the record keeps the card's price and the rate the charge works out to",
	}
	b, err := json.Marshal(m)
	if err != nil {
		return r, false
	}
	w := want
	r.SellCharge, r.Detail = &w, string(b)
	return r, true
}

// roundSig rounds v to n significant figures.
func roundSig(v float64, n int) float64 {
	if v == 0 {
		return 0
	}
	p := math.Pow(10, float64(n)-math.Ceil(math.Log10(math.Abs(v))))
	return math.Round(v*p) / p
}
