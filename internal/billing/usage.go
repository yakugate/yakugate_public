package billing

import (
	"encoding/json"
)

// Usage without a ledger row for each message (Saleem 2026-10-13: "we don't
// need to record per message"). See billingstore.ChargeMarker for the three
// steps; this file was the second: a batch applied in the config store —
// the balance, the day's usage and ONE row for the batch — in one
// transaction. That settlement (SettleBatch, LegacyCharges, LegacyReferences)
// had no caller left once the billing role was retired (26 Oct 2026); it
// was retired with the move into the DAL (29 Oct 2026) and is kept in
// _to_delete/internal/billing/usage.go. The marks below are still read.

// The kinds of usage, as usage_daily.kind holds them (config/0144).
const (
	KindMT  = "mt"
	KindMO  = "mo"
	KindDLR = "dlr"
	KindHLR = "hlr"
)

// ChargeMark is what one record adds to its account's usage and what it
// moves on a prepaid balance — written on the record when its batch is
// claimed (hot/0059 `charge`) and read back if the batch has to be applied
// again, so a second try applies exactly the same amounts.
//
// The usage amounts are what the customer pays for the record in the
// company's currency, by kind. Debit is what THIS batch still has to take
// from a prepaid balance (negative = give back): for a message whose price
// was taken at acceptance (Held) that is only a difference in parts, not the
// price again.
type ChargeMark struct {
	MT    float64 `json:"mt,omitempty"`
	HLR   float64 `json:"hlr,omitempty"`
	DLR   float64 `json:"dlr,omitempty"`
	MO    float64 `json:"mo,omitempty"`
	Debit float64 `json:"debit,omitempty"`
	Model string  `json:"model,omitempty"` // prepaid | postpaid; "" = the company's model when applied
	Held  bool    `json:"held,omitempty"`  // the message's own price was taken at acceptance
	// Note is what the record's receipt may still book (the billing queue,
	// 26 Oct 2026): read back when a receipt is matched through the record.
	Note *BillNote `json:"n,omitempty"`
}

// Empty reports whether the mark charges nothing and moves nothing.
func (m ChargeMark) Empty() bool {
	return (m.Note == nil || m.Note.Empty()) && toUnits(m.MT) == 0 && toUnits(m.HLR) == 0 && toUnits(m.DLR) == 0 && toUnits(m.MO) == 0 && toUnits(m.Debit) == 0
}

// String is the mark as the record keeps it; "" for an empty one.
func (m ChargeMark) String() string {
	if m.Empty() {
		return ""
	}
	b, err := json.Marshal(m)
	if err != nil {
		return ""
	}
	return string(b)
}

// ParseChargeMark reads a mark off a record. ok=false = none, or unreadable.
func ParseChargeMark(s string) (ChargeMark, bool) {
	if s == "" {
		return ChargeMark{}, false
	}
	var m ChargeMark
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return ChargeMark{}, false
	}
	return m, true
}

// UsageItem is one record of a batch: whose it is and its mark.
type UsageItem struct {
	AccountID int64
	Mark      ChargeMark
}

// SettleOutcome says what a batch did for one item.
type SettleOutcome int

const (
	SettleSkipped   SettleOutcome = iota // unlinked account: no company to bill
	SettleApplied                        // applied now
	SettleDuplicate                      // this batch had already been applied for the company
)
