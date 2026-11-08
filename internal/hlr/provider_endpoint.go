package hlr

// EndpointProvider (GW-15 Phase B, 2026-08-27) dips via an http_endpoints row
// of type='hlr' — the "richer config-driven adapter" provider_http.go's doc
// anticipated. The row's Hadal config drives the request (params with the
// fixed hlr source vocabulary: msisdn) and the extraction (fixed response
// aliases: mcc / mnc / ported / ok), through the same call machinery P4 uses.
// The env-configured HTTPProvider remains the fallback when no row exists.

import (
	"context"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/vendors/httpadapter"
)

// EndpointProvider satisfies Provider.
type EndpointProvider struct {
	call     httpadapter.Call
	cl       *http.Client
	callMode string // YAKU_HLR_CALL_LOG: which lookups keep their call
}

// The modes of YAKU_HLR_CALL_LOG (6 Nov 2026): which live lookups keep the
// call they made on their log row — every one (all, the default), only those
// that found nothing or failed (failed), none (off). The log row itself is
// always written.
const (
	hlrCallAll    = "all"
	hlrCallFailed = "failed"
	hlrCallOff    = "off"
)

// hlrCallMode reads YAKU_HLR_CALL_LOG; anything else is all.
func hlrCallMode() string {
	switch m := strings.ToLower(strings.TrimSpace(os.Getenv("YAKU_HLR_CALL_LOG"))); m {
	case hlrCallFailed, hlrCallOff:
		return m
	}
	return hlrCallAll
}

// lookupCall is the newest call rec kept, as the log row keeps it; nil when
// none was made.
func lookupCall(rec *httpadapter.CallRecorder) *model.LookupCall {
	cc, ok := rec.Last()
	if !ok {
		return nil
	}
	return &model.LookupCall{
		VendorCall: model.VendorCall{Method: cc.Method, URL: cc.URL, ReqHeaders: cc.ReqHeaders, ReqBody: cc.ReqBody,
			ReqCut: cc.ReqCut, RespHeaders: cc.RespHeaders, RespBody: cc.RespBody, RespCut: cc.RespCut,
			TookMS: cc.Took.Milliseconds()},
		Status: cc.Status, Err: cc.Err,
	}
}

// NewEndpointProvider builds a provider from a hydrated endpoint call.
func NewEndpointProvider(call httpadapter.Call, timeout time.Duration) *EndpointProvider {
	if timeout <= 0 {
		timeout = 300 * time.Millisecond
	}
	return &EndpointProvider{call: call, cl: &http.Client{Timeout: timeout}, callMode: hlrCallMode()}
}

// Dip resolves one MSISDN. ok=false = definitive not-found (empty mcc, or the
// row's `ok` alias extracted falsy); an error = transport failure (retryable).
// The call it made comes back on the Entry (Call) whatever the outcome — a
// not-found and a failed call too — as YAKU_HLR_CALL_LOG allows.
func (p *EndpointProvider) Dip(ctx context.Context, msisdn string) (Entry, bool, error) {
	e, ok, err := p.dip(ctx, msisdn)
	if p.callMode == hlrCallOff || p.callMode == hlrCallFailed && ok && err == nil {
		e.Call = nil
	}
	return e, ok, err
}

func (p *EndpointProvider) dip(ctx context.Context, msisdn string) (Entry, bool, error) {
	var rec *httpadapter.CallRecorder
	if p.callMode != hlrCallOff {
		ctx, rec = httpadapter.WithCallRecorder(ctx)
	}
	vals, err := httpadapter.FetchValues(ctx, p.cl, p.call, map[string]string{"msisdn": msisdn})
	call := lookupCall(rec)
	if err != nil {
		if call != nil && call.Err == "" {
			call.Err = cut(err.Error(), 500)
		}
		return Entry{Call: call}, false, err
	}
	if okv, has := vals["ok"]; has && okv != "" {
		switch strings.ToLower(okv) {
		case "false", "0", "no", "n":
			return Entry{Call: call}, false, nil
		}
	}
	mcc, mnc := vals["mcc"], vals["mnc"]
	if mcc == "" {
		return Entry{Call: call}, false, nil
	}
	ported := false
	switch strings.ToLower(vals["ported"]) {
	case "true", "1", "yes", "y":
		ported = true
	}
	// The operator's name and the country calling code, when the provider's
	// answer gives them (response values operator_name and ccc, 6 Nov 2026).
	return Entry{MSISDN: msisdn, MCC: mcc, MNC: mnc, Ported: ported,
		OperatorName: strings.TrimSpace(vals["operator_name"]), CCC: CleanCCC(vals["ccc"]), Call: call}, true, nil
}

// cut is s at most n bytes long.
func cut(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// CleanCCC is a country calling code as digits only ("+92", " 92" -> "92");
// "" when it has none.
func CleanCCC(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	if b.Len() > 6 {
		return ""
	}
	return b.String()
}
