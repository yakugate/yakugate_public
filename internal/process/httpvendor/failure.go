package httpvendor

import (
	"context"
	"errors"
	"fmt"
	"net"
	"regexp"

	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/vendors/httpadapter"
)

// vendorFailure names what went wrong with a send, for the message's record
// and the next vendor of its round (Saleem 2026-10-15: "reason is not clear,
// i.e with error code or status code from api or smpp error codes, or if body
// failed then body details, if session error then session details … from
// vendor_rejected i cant judge any thing"). Until then everything that was
// not a refusal with a status was the one word VENDOR_REJECTED, and the
// vendor's answer stayed in the log.
//
// code: HTTP_<status> for any answer the vendor gave (a refusal, an answer
// the success rule does not accept, its own 5xx); NO_SESSION when no login
// could be had; TIMEOUT; CONNECT_FAILED when the vendor could not be reached;
// VENDOR_REJECTED for a permanent refusal with no status; VENDOR_ERROR else.
// text: the error as the adapter wrote it — the status and the vendor's body,
// or the login's own failure — with every URL's query string taken off (a
// vendor URL often carries its API key there).
func vendorFailure(err error) (code, text string) {
	if err == nil {
		return "", ""
	}
	text = maskURLs(err.Error())
	var rj *httpadapter.Rejection
	var na *httpadapter.NotAccepted
	var ae *httpadapter.AuthError
	var ne net.Error
	switch {
	case errors.As(err, &rj):
		return fmt.Sprintf("HTTP_%d", rj.Code), text
	case errors.As(err, &ae):
		return "NO_SESSION", text
	case errors.As(err, &na):
		return fmt.Sprintf("HTTP_%d", na.Code), text
	}
	if m := answerStatus.FindStringSubmatch(text); m != nil {
		return "HTTP_" + m[1], text
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &ne) && ne.Timeout():
		return "TIMEOUT", text
	case isUnreachable(err):
		return "CONNECT_FAILED", text
	case errors.Is(err, httpadapter.ErrPermanent):
		return "VENDOR_REJECTED", text
	}
	return "VENDOR_ERROR", text
}

// answerStatus finds the status in the adapter's "vendor response not OK
// (http 503): …", the error it gives for the vendor's own trouble.
var answerStatus = regexp.MustCompile(`\(http (\d{3})\)`)

// isUnreachable is a call that never reached the vendor: refused, no route,
// a name that does not resolve.
func isUnreachable(err error) bool {
	var op *net.OpError
	var dns *net.DNSError
	return errors.As(err, &op) || errors.As(err, &dns)
}

// urlQuery is a URL's query string, to be taken off before an error is kept.
var urlQuery = regexp.MustCompile(`(https?://[^\s"'?]+)\?[^\s"']*`)

// maskURLs keeps each URL's scheme, host and path and drops its query.
func maskURLs(s string) string { return urlQuery.ReplaceAllString(s, "$1?…") }

// failed gives messages the code and the vendor's answer of a failed send,
// for their records and for the re-route inbox (queue/0041).
func failed(msgs []model.Message, err error) {
	code, text := vendorFailure(err)
	for i := range msgs {
		msgs[i].ErrorCode, msgs[i].ErrorText = code, text
	}
}
