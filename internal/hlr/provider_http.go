package hlr

// HTTPProvider is a configurable HLR provider over the shared httpx client
// (P1 scope finalize 2026-08-09). It GETs a URL with the MSISDN substituted for
// {msisdn} and reads MCC/MNC/ported from named JSON fields in the response.
// This is a pragmatic first provider covering the common REST/JSON HLR API; a
// richer config-driven adapter (XML/SOAP, regex extraction, session auth —
// the vendor-adapter Profile shape) can replace it later without touching the
// Dipper, which only depends on the Provider interface.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/yakugate/yakugate/internal/httpx"
)

// HTTPProviderConfig configures an HTTPProvider. URL must contain the
// {msisdn} placeholder. The *Field names locate the values in the JSON
// response (dot paths are NOT interpreted here — flat top-level fields, matching
// the majority of HLR APIs; a nested provider can be added when one appears).
type HTTPProviderConfig struct {
	URL        string      // e.g. https://hlr.example.com/lookup?msisdn={msisdn}
	Method     string      // default GET
	Header     http.Header // static headers (auth token, accept, ...)
	MCCField   string      // JSON field holding the MCC (default "mcc")
	MNCField   string      // JSON field holding the MNC (default "mnc")
	PortedFld  string      // optional JSON field holding a ported flag
	OKField    string      // optional: a field that must be truthy for a hit (e.g. "found")
}

// HTTPProvider satisfies Provider.
type HTTPProvider struct {
	cfg HTTPProviderConfig
	c   *httpx.Client
}

// NewHTTPProvider builds a provider over a shared httpx client (whose timeout
// should already be the dip timeout).
func NewHTTPProvider(cfg HTTPProviderConfig, c *httpx.Client) *HTTPProvider {
	if cfg.Method == "" {
		cfg.Method = http.MethodGet
	}
	if cfg.MCCField == "" {
		cfg.MCCField = "mcc"
	}
	if cfg.MNCField == "" {
		cfg.MNCField = "mnc"
	}
	return &HTTPProvider{cfg: cfg, c: c}
}

// Dip resolves one MSISDN. ok=false = a definitive not-found (2xx but no
// network, or OKField falsy); a non-nil error = transport/HTTP failure, which
// the Dipper treats as retry-worthy.
func (p *HTTPProvider) Dip(ctx context.Context, msisdn string) (Entry, bool, error) {
	url := strings.ReplaceAll(p.cfg.URL, "{msisdn}", msisdn)
	resp, err := p.c.Do(ctx, p.cfg.Method, url, p.cfg.Header, nil)
	if err != nil {
		return Entry{}, false, err
	}
	if resp.Status < 200 || resp.Status >= 300 {
		return Entry{}, false, fmt.Errorf("hlr provider status %d", resp.Status)
	}
	var body map[string]any
	if err := json.Unmarshal(resp.Body, &body); err != nil {
		return Entry{}, false, fmt.Errorf("hlr provider bad json: %w", err)
	}
	if p.cfg.OKField != "" && !truthy(body[p.cfg.OKField]) {
		return Entry{}, false, nil // definitive not-found
	}
	mcc := asString(body[p.cfg.MCCField])
	mnc := asString(body[p.cfg.MNCField])
	if mcc == "" || mnc == "" {
		return Entry{}, false, nil // no network resolved
	}
	e := Entry{MSISDN: msisdn, MCC: mcc, MNC: mnc}
	if p.cfg.PortedFld != "" {
		e.Ported = truthy(body[p.cfg.PortedFld])
	}
	return e, true, nil
}

func asString(v any) string {
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case float64:
		// JSON numbers decode to float64; render MCC/MNC without a decimal point.
		return strings.TrimSuffix(fmt.Sprintf("%.0f", t), ".0")
	case nil:
		return ""
	default:
		return fmt.Sprint(t)
	}
}

func truthy(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case float64:
		return t != 0
	case string:
		s := strings.ToLower(strings.TrimSpace(t))
		return s == "true" || s == "1" || s == "yes" || s == "found"
	default:
		return v != nil
	}
}
