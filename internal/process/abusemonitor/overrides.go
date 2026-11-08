package abusemonitor

import (
	"context"
	"encoding/json"
	"strings"
)

// Per-account overrides of the abuse rules (config/0106, PO 2026-09-15:
// "per account + Abuse Rules screen").
//
// A row changes one signal for one account: its params are laid over the
// gateway row's params when THAT account is judged, and is_disabled takes the
// signal off the account altogether. Thresholds, weights, actions and
// cooldown may differ per account. The time windows and sample sizes may not:
// each signal is measured over one window for every account in one pass, so
// those keys are dropped from an override here (the Admin API refuses them
// too).

// windowKeys are the params that shape the measurement, not the verdict.
var windowKeys = []string{"window_min", "baseline_hours", "scan_limit", "sample_msgs", "prefix_len"}

type override struct {
	params   json.RawMessage // window keys removed
	disabled bool
}

// overrides is signal -> account -> override.
type overrides map[string]map[int64]override

// loadOverrides reads every override. A store without the table (before
// config/0106) or a failed read means none: every account gets the gateway
// rules, as before.
func (p *Process) loadOverrides(ctx context.Context) overrides {
	rows, err := p.data.Rules.Overrides(ctx)
	if err != nil {
		if !strings.Contains(err.Error(), "1146") { // missing table: not yet migrated
			p.log.Printf("load abuse_rule_overrides: %v", err)
		}
		return nil
	}
	out := overrides{}
	for _, r := range rows {
		sig, acct, raw, off := r.Signal, r.Account, r.Params, r.Disabled
		if out[sig] == nil {
			out[sig] = map[int64]override{}
		}
		out[sig][acct] = override{params: stripWindowKeys(raw), disabled: off}
	}
	return out
}

// stripWindowKeys also drops a value nobody gave ("" or null), which would
// otherwise stop the keys after it from being read (dropBlanks).
func stripWindowKeys(raw []byte) json.RawMessage {
	var m map[string]json.RawMessage
	if clean, err := dropBlanks(raw); err == nil {
		raw = clean
	}
	if json.Unmarshal(raw, &m) != nil || len(m) == 0 {
		return nil
	}
	for _, k := range windowKeys {
		delete(m, k)
	}
	if len(m) == 0 {
		return nil
	}
	b, _ := json.Marshal(m)
	return b
}

// apply lays account acct's override for signal over dst, a COPY of the
// signal's gateway params. false = the signal is off for this account.
func (o overrides) apply(signal string, acct int64, dst any) bool {
	ov, ok := o[signal][acct]
	if !ok {
		return true
	}
	if ov.disabled {
		return false
	}
	if len(ov.params) > 0 {
		_ = json.Unmarshal(ov.params, dst)
	}
	return true
}
