package mo

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/yakugate/yakugate/internal/dialect"
	"github.com/yakugate/yakugate/internal/model"
)

// WebhookBody is the JSON body POSTed to a client's mo_url.
//
// Shared by the mo role and the portal's webhook tester (PO 2026-09-12), so
// the sample a customer tests against is the real shape, not a copy of it.
type WebhookBody struct {
	MessageID string `json:"messageId"`
	From      string `json:"from"` // subscriber MSISDN
	To        string `json:"to"`   // client shortcode
	Text      string `json:"text"`
	// AcceptedAt is when the gateway took the message in (Saleem 4 Nov 2026):
	// RFC 3339, UTC, as the receipt webhook's submittedAt.
	AcceptedAt string `json:"acceptedAt,omitempty"`
	// The subscriber's network and the vendor it came from, each only when
	// known (Saleem 4 Nov 2026; PushFacts): MCC, MNC, the two together as
	// GET /sms/v4/operator gives them, whether the number is ported ("true"
	// or "false", from our HLR data), and the vendor's cid and name.
	MCC        string `json:"mcc,omitempty"`
	MNC        string `json:"mnc,omitempty"`
	Operator   string `json:"operator,omitempty"`
	Ported     string `json:"ported,omitempty"`
	Vendor     string `json:"vendor,omitempty"`
	VendorName string `json:"vendorName,omitempty"`
}

// AcceptedAt is when the gateway took an incoming message in: the time in
// its id (a ULID made then), else when it was queued; zero if neither.
func AcceptedAt(m model.Message) time.Time {
	// The id's own time, read with the ULID library (this lane may not reach
	// internal/monitor, whose ULIDTime it once used).
	if id, err := ulid.ParseStrict(m.ID); err == nil {
		return ulid.Time(id.Time()).UTC()
	}
	return m.CreatedAt
}

// AcceptedText is AcceptedAt as it is pushed: RFC 3339, UTC; "" when unknown.
func AcceptedText(m model.Message) string {
	at := AcceptedAt(m)
	if at.IsZero() {
		return ""
	}
	return at.UTC().Format(time.RFC3339)
}

// NewWebhookRequest builds the delivery of one MO to a client's mo_url: a
// GET with the fields in the query string when the endpoint's method is GET,
// else a JSON POST.
// names is the account's dialect for pushes (dialect.PushNames, PO
// 2026-09-17): our keys renamed to what the customer calls them; nil keeps
// the native shape.
func NewWebhookRequest(ctx context.Context, moURL, method string, m model.Message, names map[string]string) (*http.Request, error) {
	return NewWebhookRequestWith(ctx, moURL, method, m, names, PushFacts{})
}

// NewWebhookRequestWith is NewWebhookRequest with the message's push facts:
// its network and vendor, each added only when known.
func NewWebhookRequestWith(ctx context.Context, moURL, method string, m model.Message, names map[string]string, facts PushFacts) (*http.Request, error) {
	native := map[string]string{"messageId": m.ID, "from": m.Sender, "to": m.Destination, "text": m.Body}
	if at := AcceptedText(m); at != "" {
		native["acceptedAt"] = at
	}
	facts.native(native)
	fields := dialect.Renamed(native, names)
	if strings.EqualFold(method, http.MethodGet) {
		q := url.Values{}
		for k, v := range fields {
			q.Set(k, v)
		}
		sep := "?"
		if strings.Contains(moURL, "?") {
			sep = "&"
		}
		return http.NewRequestWithContext(ctx, http.MethodGet, moURL+sep+q.Encode(), nil)
	}
	body, _ := json.Marshal(fields)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, moURL, strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}
