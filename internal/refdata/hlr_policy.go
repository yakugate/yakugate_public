package refdata

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
)

// The steps an account's HLR order may list (2026-09-14). See the pipeline's
// HLR stage for what each one does.
const (
	HLRStepClient  = "client"  // the operator code the client sent
	HLRStepData    = "data"    // our HLR data (hlr_<first4>)
	HLRStepRange   = "range"   // the operator the number range was issued to
	HLRStepLive    = "live"    // a live lookup with the account's providers
	HLRStepCountry = "country" // the country code only
)

// HLRSteps lists every valid step.
var HLRSteps = []string{HLRStepClient, HLRStepData, HLRStepRange, HLRStepLive, HLRStepCountry}

// DefaultHLROrder is the order when neither the account nor the gateway sets
// one: the client's code, our data, the range, then a live lookup.
var DefaultHLROrder = []string{HLRStepClient, HLRStepData, HLRStepRange, HLRStepLive}

// DefaultHLRCacheTTLDays is the gateway default when gateway_settings has none.
const DefaultHLRCacheTTLDays = 30

// CleanHLROrder keeps the valid steps, lower-cased, each once, in the order
// given. nil when none is valid.
func CleanHLROrder(steps []string) []string {
	var out []string
	seen := map[string]bool{}
	for _, s := range steps {
		s = strings.ToLower(strings.TrimSpace(s))
		valid := false
		for _, v := range HLRSteps {
			if s == v {
				valid = true
				break
			}
		}
		if valid && !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

// ApplyHLRDefaults fills in the gateway default for every account that has no
// order of its own (nil) or no cache time limit (-1).
func ApplyHLRDefaults(accts []Account, order []string, ttlDays int) {
	for i := range accts {
		if accts[i].HLROrder == nil {
			accts[i].HLROrder = append([]string(nil), order...)
		}
		if accts[i].HLRCacheTTLDays < 0 {
			accts[i].HLRCacheTTLDays = ttlDays
		}
	}
}

// hlrDefaults reads the gateway defaults. Any problem (table missing on a
// store that has not run config/0102 yet, a bad value) gives the built-in
// defaults, never an error: the accounts must still load.
func (l *MySQL) hlrDefaults(ctx context.Context) ([]string, int) {
	order, ttl := append([]string(nil), DefaultHLROrder...), DefaultHLRCacheTTLDays
	settings, err := l.ref.HLRDefaultSettings(ctx)
	if err != nil {
		return order, ttl
	}
	for _, s := range settings {
		name, val := s.Name, s.Value
		switch name {
		case "hlr_default_order":
			var steps []string
			if json.Unmarshal([]byte(val), &steps) == nil {
				if c := CleanHLROrder(steps); c != nil {
					order = c
				}
			}
		case "hlr_default_cache_ttl_days":
			if n, err := strconv.Atoi(strings.Trim(strings.TrimSpace(val), `"`)); err == nil && n >= 0 {
				ttl = n
			}
		}
	}
	return order, ttl
}
