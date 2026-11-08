package dlr

import (
	"context"
	"encoding/json"
	"github.com/yakugate/yakugate/internal/dialect"
	"github.com/yakugate/yakugate/internal/monitor"
	"net/http"
	"regexp"
	"strings"
	"time"
)

// WebhookBody is the JSON body POSTed to a client's dlr_url.
//
// Reference is the id the CLIENT gave the message at submit (PO 2026-09-06),
// so a receipt can be matched without keeping a map from our id to theirs.
// Omitted when they gave none, so the payload is unchanged for every client
// that does not use references.
//
// Shared by the dlr role and the portal's webhook tester (PO 2026-09-12), so
// the sample a customer tests against is the real shape, not a copy of it.
type WebhookBody struct {
	MessageID string `json:"messageId"`
	Reference string `json:"reference,omitempty"`
	Status    string `json:"status"`
	// Stat is the SMPP word this receipt carried — one of the eight, so an
	// HTTP customer can tell DELETED from UNDELIV and ENROUTE from ACCEPTD
	// without parsing the receipt text (2026-09-23). Omitted when the
	// gateway decided the receipt itself and the status says everything.
	Stat string `json:"stat,omitempty"`
	// The receipt's own facts as fields of their own (Saleem 2026-10-15: "no
	// delivery datetime in dlr webhook call"), so a client need not parse the
	// receipt text for them:
	//   ErrorCode   the receipt's error code, "000" when there is none — the
	//               text's err: field (ErrorCodeOf).
	//   To, From    the message's destination (the handset) and sender.
	//               Omitted on receipts queued before they were carried.
	//   SubmittedAt when the gateway accepted the message — read from our
	//               message id, as the text's submit date is.
	//   DoneAt      when this state was reached: the vendor's delivery time
	//               when it gave one, else when the receipt reached us.
	// Times are RFC 3339 in UTC, to the second (WebhookTime).
	ErrorCode   string `json:"errorCode,omitempty"`
	To          string `json:"to,omitempty"`
	From        string `json:"from,omitempty"`
	SubmittedAt string `json:"submittedAt,omitempty"`
	DoneAt      string `json:"doneAt,omitempty"`
	Receipt     string `json:"receipt,omitempty"`
}

// WebhookTime is how a webhook body writes a time: RFC 3339, UTC, to the
// second ("2026-10-15T10:31:07Z"). The zero time is "".
func WebhookTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// SubmittedAt is when the gateway accepted the message id names — the time
// in the ULID — or the zero time for an id that is not one of ours.
func SubmittedAt(id string) time.Time {
	if at, ok := monitor.ULIDTime(id); ok {
		return at
	}
	return time.Time{}
}

// receiptErr reads the err: field of a receipt's text.
var receiptErr = regexp.MustCompile(`\berr:(\S+)`)

// ErrorCodeOf is the err: field of a receipt text: "000" for none, else the
// code as the text carries it (errField). "" when the text has no err: field
// (a vendor's raw text on a row written before ReceiptText).
func ErrorCodeOf(receipt string) string {
	if m := receiptErr.FindStringSubmatch(receipt); m != nil {
		return m[1]
	}
	return ""
}

// NewWebhookRequest builds the POST of one receipt to a dlr_url. names is
// the account's dialect for pushes (dialect.PushNames, PO 2026-09-17): our
// keys renamed to what the customer calls them; nil keeps the native shape.
func NewWebhookRequest(ctx context.Context, url string, b WebhookBody, names map[string]string) (*http.Request, error) {
	fields := map[string]string{"messageId": b.MessageID, "status": b.Status}
	for k, v := range map[string]string{
		"stat": b.Stat, "reference": b.Reference, "receipt": b.Receipt,
		"errorCode": b.ErrorCode, "to": b.To, "from": b.From,
		"submittedAt": b.SubmittedAt, "doneAt": b.DoneAt,
	} {
		if v != "" {
			fields[k] = v
		}
	}
	body, _ := json.Marshal(dialect.Renamed(fields, names))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}
