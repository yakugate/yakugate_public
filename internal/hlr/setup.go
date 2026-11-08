package hlr

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/httpx"
)

// errNoConfigStore is what a load says with no config store to read.
var errNoConfigStore = errors.New("hlr: no config store")

// LiveSettings are the settings-file knobs for live lookups (YAKU_HLR_*).
type LiveSettings struct {
	Timeout     time.Duration // YAKU_HLR_DIP_TIMEOUT_MS, default 300 ms
	MaxInflight int           // YAKU_HLR_MAX_INFLIGHT, default 64
	// The older single provider from the settings file, used only when the
	// gateway has no hlr provider rows at all (see Live).
	LegacyURL, MCCField, MNCField, PortedField, OKField string
}

// LiveSettingsFrom reads the knobs through getenv(name, default).
func LiveSettingsFrom(getenv func(string, string) string) LiveSettings {
	ms, _ := strconv.Atoi(getenv("YAKU_HLR_DIP_TIMEOUT_MS", "300"))
	if ms < 1 {
		ms = 300
	}
	inflight, _ := strconv.Atoi(getenv("YAKU_HLR_MAX_INFLIGHT", "64"))
	return LiveSettings{
		Timeout: time.Duration(ms) * time.Millisecond, MaxInflight: inflight,
		LegacyURL:   getenv("YAKU_HLR_DIP_URL", ""),
		MCCField:    getenv("YAKU_HLR_DIP_MCC_FIELD", "mcc"),
		MNCField:    getenv("YAKU_HLR_DIP_MNC_FIELD", "mnc"),
		PortedField: getenv("YAKU_HLR_DIP_PORTED_FIELD", ""),
		OKField:     getenv("YAKU_HLR_DIP_OK_FIELD", ""),
	}
}

// StartLookups builds what the HLR order's "range", "country" and "live"
// steps need, for a role that accepts traffic (http-server, smpp-server): the
// number-range index and the per-account live-lookup providers, both loaded
// from the config store and kept current in the background. writer receives
// live answers (in-memory cache, Redis copy, hlr_<first4>). cfgDB nil (a
// config store other than MySQL) gives no ranges and only the legacy
// provider.
func StartLookups(ctx context.Context, cfgDB dal.HLRLookupConfig, writer Writer, s LiveSettings, logf func(string, ...any)) (*Ranges, *Live) {
	ranges := NewRanges(logf)
	live := NewLive(writer, s.Timeout, s.MaxInflight, logf)
	// A number answered with another network than its range's is ported
	// (6 Nov 2026), and saved so.
	live.SetPortedByRange(ranges.Differs)
	if s.LegacyURL != "" {
		live.SetLegacy(NewHTTPProvider(HTTPProviderConfig{
			URL: s.LegacyURL, MCCField: s.MCCField, MNCField: s.MNCField,
			PortedFld: s.PortedField, OKField: s.OKField,
		}, httpx.New(httpx.WithTimeout(s.Timeout))))
	}
	if cfgDB != nil {
		ranges.Start(ctx, cfgDB, 5*time.Minute)
		live.Start(ctx, cfgDB, time.Minute)
	}
	return ranges, live
}
