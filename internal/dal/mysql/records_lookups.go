package mysql

// Number lookups (hlr_ rows) and what an accepted lookup cost.

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/billingstore"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/model"
)

// quoteLookup is the price of the message's number lookup fixed when it was
// accepted (the quote, billing.Quote: "hlr": {"c": charge, "cur": …}), in the
// company's currency. 0 = not charged.
func quoteLookup(q string) (float64, string) {
	if q == "" {
		return 0, ""
	}
	var v struct {
		HLR *struct {
			Charge   float64 `json:"c"`
			Currency string  `json:"cur"`
		} `json:"hlr"`
	}
	if json.Unmarshal([]byte(q), &v) != nil || v.HLR == nil {
		return 0, ""
	}
	return v.HLR.Charge, v.HLR.Currency
}

// hlrLogged is Saleem's rule for the lookup log (2026-10-14): every lookup an
// HLR provider answered is logged; one answered from our own data only when
// it was charged.
// refusedLookupLogged is which lookups of a REFUSED message are logged
// (6 Nov 2026): every one an HLR provider was asked for, found or not — the
// calls the gateway paid for. Our own data and the ranges cost nothing.
func refusedLookupLogged(source string) bool { return source == "dip" || source == "dip_miss" }

func hlrLogged(source, quote string) bool {
	switch source {
	case "dip", "dip_miss":
		return true
	case "":
		return false
	}
	amt, _ := quoteLookup(quote)
	return amt > 0
}

// writeLookups files the lookups worth logging (hlrLogged) of the records a
// write made, in hlr_<company>_<day>.
//
// refused (6 Nov 2026, Saleem: "save all hlr calls, whether sms accepted or
// not"): the messages are refusals written now; each live lookup (dip,
// dip_miss) is logged with the refusal's code in refused_reason and no
// charge — a refused message is never charged.
func (s *Store) writeLookups(ctx context.Context, x dbtx, made []model.Message, refused bool) error {
	byTable := map[string][]model.Message{}
	for _, m := range made {
		if refused && !refusedLookupLogged(m.HLRSource) || !refused && !hlrLogged(m.HLRSource, m.Quote) {
			continue
		}
		t := recTable(billingstore.KindHLR, m.CompanyID, m.ID)
		byTable[t] = append(byTable[t], m)
	}
	now := time.Now()
	for table, ms := range byTable {
		if err := s.ensureRecords(ctx, table); err != nil {
			return err
		}
		vals := strings.TrimSuffix(strings.Repeat("(?,?,?,?,?,?,?,?,?,?,FROM_UNIXTIME(CAST(? AS DECIMAL(17,3))),?,?,?,?"+
			strings.Repeat(",?", len(dal.LookupCallCols))+"),", len(ms)), ",")
		args := make([]any, 0, len(ms)*(15+len(dal.LookupCallCols)))
		for _, m := range ms {
			rates := billing.RatesOf(m)
			var rate, reason any
			if amt, _ := quoteLookup(m.Quote); amt > 0 && !refused {
				rate = nz(rates.HLRSell)
			}
			if refused {
				code, _ := model.SplitReason(m.RejectReason)
				reason = nsStr(code)
			}
			at := billingstore.IDTime(m.ID, now)
			args = append(args, m.ID, nz(m.AccountID), m.Destination, m.HLRSource, nsStr(m.MCC), nsStr(m.MNC),
				boolInt(m.HLRPorted), rate, nsFx(rates.FX), nsFx(rates.BaseRate), ackAtArg(at), nsStr(m.DeploymentID),
				nsStr(m.HLROperator), nsStr(m.HLRCCC), reason)
			// The call made to the HLR provider (hot/0080, 6 Nov 2026).
			args = append(args, dal.LookupCallArgs(m.HLRCall)...)
		}
		if _, err := x.ExecContext(ctx, "INSERT INTO "+table+
			" (msg_id,account_id,destination,source,mcc,mnc,ported,sell_rate_id,fx_rate,base_rate,looked_up_at,deployment_id,operator_name,ccc,refused_reason,"+
			strings.Join(dal.LookupCallCols, ",")+") VALUES "+vals, args...); err != nil {
			return err
		}
	}
	return nil
}

// WriteLookup files one lookup that has no message (GET /sms/v4/operator,
// answered by an HLR provider), in hlr_<company>_<day of now>.
func (s *Store) WriteLookup(ctx context.Context, r LookupRow) error {
	if r.At.IsZero() {
		r.At = time.Now()
	}
	return s.WriteLookups(ctx, []LookupRow{r}, false)
}
