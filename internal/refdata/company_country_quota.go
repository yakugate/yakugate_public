package refdata

import (
	"encoding/json"
	"strings"
)

// companies.country_quota (config/0044) — a company's own per-country
// message quota, shared by every account of the company.
//
// The column was added in July as "the forward-looking home" for a company
// country quota, and the Admin API has accepted and shown it ever since, but
// no engine code read it: a company limited to one message a day to Pakistan
// could send as many as it liked (QA companies API-020, 2026-10-01). The
// quota stage now enforces it, beside the account-level rules of
// quota_by_countries (both apply; the stricter refuses first).
//
// The shape, keyed by the destination's dialling-code prefix (a leading "+"
// is ignored; a key that is not all digits is ignored):
//
//	{"92":  {"daily_quota": 1, "monthly_quota": 100},
//	 "971": {"monthly_quota": 50}}
//
// daily_used / monthly_used are read as the operator-set starting counts,
// exactly as companies.quota and quota_by_countries.quota read them. "daily"
// and "monthly" are accepted as short names for the two limits, and a list
// of {"country_code"|"destination_country_code": "92", ...} rows is read the
// same way. Anything unreadable is no quota (the engine stays forgiving: one
// bad value must not stop a company's traffic).

// ParseCompanyCountryQuota reads companies.country_quota into prefix -> quota.
// Entries with no limit at all are dropped; nil = nothing to enforce.
func ParseCompanyCountryQuota(raw string) map[string]Quota {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "null" {
		return nil
	}
	out := map[string]Quota{}
	add := func(cc string, v json.RawMessage) {
		cc = strings.TrimPrefix(strings.TrimSpace(cc), "+")
		if cc == "" || strings.Trim(cc, "0123456789") != "" {
			return
		}
		if q, ok := parseQuotaEntry(v); ok {
			out[cc] = q
		}
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal([]byte(raw), &obj) == nil {
		for k, v := range obj {
			add(k, v)
		}
	} else {
		var list []map[string]json.RawMessage
		if json.Unmarshal([]byte(raw), &list) != nil {
			return nil
		}
		for _, row := range list {
			var cc string
			for _, k := range []string{"country_code", "destination_country_code", "prefix"} {
				if v, ok := row[k]; ok {
					var s string
					if json.Unmarshal(v, &s) != nil {
						var n json.Number
						if json.Unmarshal(v, &n) == nil {
							s = n.String()
						}
					}
					cc = s
					break
				}
			}
			b, _ := json.Marshal(row)
			add(cc, b)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// parseQuotaEntry reads one country's limits. ok=false = no limit set.
func parseQuotaEntry(v json.RawMessage) (Quota, bool) {
	var f map[string]json.RawMessage
	if json.Unmarshal(v, &f) != nil {
		return Quota{}, false
	}
	num := func(keys ...string) (*int, bool) {
		for _, k := range keys {
			raw, ok := f[k]
			if !ok || string(raw) == "null" {
				continue
			}
			var n float64
			if json.Unmarshal(raw, &n) != nil {
				var s string
				if json.Unmarshal(raw, &s) != nil {
					continue
				}
				if json.Unmarshal([]byte(strings.TrimSpace(s)), &n) != nil {
					continue
				}
			}
			if n < 0 {
				continue
			}
			i := int(n)
			return &i, true
		}
		return nil, false
	}
	var q Quota
	q.DailyQuota, _ = num("daily_quota", "daily")
	q.MonthlyQuota, _ = num("monthly_quota", "monthly")
	if q.DailyQuota == nil && q.MonthlyQuota == nil {
		return Quota{}, false
	}
	if n, ok := num("daily_used"); ok {
		q.DailyUsed = *n
	}
	if n, ok := num("monthly_used"); ok {
		q.MonthlyUsed = *n
	}
	return q, true
}

// CompanyCountryQuotaFor finds the company country quota for a destination:
// the longest matching prefix. ok=false = none applies.
func (a Account) CompanyCountryQuotaFor(msisdn string) (cc string, q Quota, ok bool) {
	if a.CompanyID == 0 {
		return "", Quota{}, false
	}
	for pfx, r := range a.CompanyCountryQuota {
		if strings.HasPrefix(msisdn, pfx) && (!ok || len(pfx) > len(cc)) {
			cc, q, ok = pfx, r, true
		}
	}
	return cc, q, ok
}
