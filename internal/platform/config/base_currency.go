package config

import (
	"os"
	"strings"
)

// BaseCurrency is the system base currency: YAKU_BASE_CURRENCY when it is a
// three-letter code, else USD — the reference gateway's own default. Every
// company's currency is turned into it (companies.base_conversion_rate, or
// the company's base currency provider) before two companies' amounts are
// compared, and the dashboards state margin in it.
//
// The Admin API's SYSTEM_BASE_CURRENCY and the portal's
// REACT_APP_BASE_CURRENCY must name the same one.
func BaseCurrency() string {
	c := strings.ToUpper(strings.TrimSpace(os.Getenv("YAKU_BASE_CURRENCY")))
	if len(c) != 3 {
		return "USD"
	}
	return c
}
