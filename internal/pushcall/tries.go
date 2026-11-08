package pushcall

import (
	"context"
	"os"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
)

// A webhook try's call, kept on its try row (mo_sub_, dlr_sub_; D-140, 5 Nov
// 2026 — it was the hook_ table of 4 Nov, never shipped): the request as it
// went and the answer as it came, secrets masked (capture.go), an encrypted
// company's bodies sealed before the row leaves the role.

// Sealer encrypts a text under a company's key (crypto.Service).
type Sealer interface {
	Encrypt(ctx context.Context, accountID, companyID int64, plaintext string) (string, error)
}

// The modes of YAKU_WEBHOOK_CALL_LOG: which tries keep their call (headers
// and bodies). The try row itself is always written.
const (
	LogAll    = "all"
	LogFailed = "failed"
	LogOff    = "off"
)

// CallLogMode reads YAKU_WEBHOOK_CALL_LOG; anything else is all.
func CallLogMode() string {
	switch m := strings.ToLower(strings.TrimSpace(os.Getenv("YAKU_WEBHOOK_CALL_LOG"))); m {
	case LogFailed, LogOff:
		return m
	}
	return LogAll
}

// Fill puts a webhook call on its try: the channel, the result, the client's
// status, the error, the time; and the call as it went — headers and bodies
// — when mode keeps it (all; failed: only a call the client did not accept).
func Fill(t *dal.ClientTry, c Call, mode string) {
	t.Channel = dal.TryHTTP
	t.Result = c.Result
	t.HTTPStatus = c.Status
	t.Error = c.Err
	t.TookMS = c.Took.Milliseconds()
	if !c.SentAt.IsZero() {
		t.At = c.SentAt
	}
	t.Method, t.URL = c.Method, c.URL
	if mode == LogOff || (mode == LogFailed && c.Result == ResultAccepted) {
		return
	}
	t.ReqHeaders, t.ReqBody, t.ReqCut = c.ReqHeaders, c.ReqBody, c.ReqCut
	t.RespHeaders, t.RespBody, t.RespCut = c.RespHeaders, c.RespBody, c.RespCut
}

// NewTryLog is a log of tries whose private rows are sealed before they are
// written (seal nil: their bodies are not kept).
func NewTryLog(w dal.ClientTries, seal Sealer, logf func(string, ...any)) *dal.TryLog {
	if w == nil {
		return nil
	}
	return dal.NewTryLog(w, func(ctx context.Context, ts []dal.ClientTry) []dal.ClientTry {
		for i := range ts {
			if ts[i].Private {
				ts[i] = Seal(ctx, seal, ts[i])
			}
		}
		return ts
	}, logf)
}

// Seal is a private try whose two bodies are encrypted under the company's
// key. Fail closed (KMS rule): a body that cannot be sealed is not kept. A
// URL's query (a GET push carries the text there) moves into the sealed
// request body, first, as "?<query>"; the URL keeps its scheme, host and
// path.
func Seal(ctx context.Context, seal Sealer, t dal.ClientTry) dal.ClientTry {
	t.Private = false
	if t.Channel != dal.TryHTTP {
		return t
	}
	t.Sealed = true
	if base, q, ok := strings.Cut(t.URL, "?"); ok {
		t.URL = base
		if t.ReqBody != "" {
			t.ReqBody = "?" + q + "\n\n" + t.ReqBody
		} else {
			t.ReqBody = "?" + q
		}
	}
	for _, b := range []*string{&t.ReqBody, &t.RespBody} {
		if *b == "" {
			continue
		}
		if seal == nil {
			*b = ""
			continue
		}
		s, err := seal.Encrypt(ctx, t.AccountID, t.CompanyID, *b)
		if err != nil {
			*b = ""
			continue
		}
		*b = s
	}
	return t
}
