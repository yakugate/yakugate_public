package pushcall

import (
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/yakugate/yakugate/internal/vendors/httpadapter"
)

// Every call to a client's webhook is kept, its request and its answer as they
// went (Saleem 4 Nov 2026: "every webhook call should save raw request and
// response and show on frontend"). SendCaptured is Send that also hands back
// the Call; Fill (tries.go) puts it on its try, a row of mo_sub_ or dlr_sub_
// (D-140), which the role's try log writes off the push path.

// How much of a call is kept: a body past its cap is cut (and marked so), a
// header block or a URL past theirs too.
const (
	ReqBodyCap  = httpadapter.ReqBodyCap
	RespBodyCap = httpadapter.RespBodyCap
	errorCap    = httpadapter.ErrorCap
	respReadCap = 64 << 10 // what Send has always read of an answer
)

// Call is one push as it went.
type Call struct {
	Method, URL string
	ReqHeaders  string // "Name: value" lines, secrets masked
	ReqBody     string
	ReqCut      bool
	Status      int // 0 = no answer
	RespHeaders string
	RespBody    string
	RespCut     bool
	Result      string // accepted | refused | no_answer
	Err         string
	SentAt      time.Time
	Took        time.Duration
}

// The results a call can have.
const (
	ResultAccepted = "accepted"
	ResultRefused  = "refused"
	ResultNoAnswer = "no_answer"
)

// SendCaptured is Send, keeping what was sent and what came back. The answer
// is judged exactly as Send judges it.
func (e *Endpoint) SendCaptured(cl *http.Client, req *http.Request) (int, Call, error) {
	c := Call{Method: req.Method, URL: maskURL(req.URL), ReqHeaders: maskHeaders(req.Header, req.Host)}
	c.ReqBody, c.ReqCut = requestBody(req)
	c.SentAt = time.Now()
	resp, err := cl.Do(req)
	if err != nil {
		c.Took = time.Since(c.SentAt)
		c.Result, c.Err = ResultNoAnswer, cut(err.Error(), errorCap)
		return 0, c, err
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, respReadCap))
	resp.Body.Close()
	c.Took = time.Since(c.SentAt)
	c.Status = resp.StatusCode
	c.RespHeaders = maskHeaders(resp.Header, "")
	c.RespBody, c.RespCut = keep(b, RespBodyCap)
	err = e.Judge(resp.StatusCode, string(b))
	if err != nil {
		c.Result, c.Err = ResultRefused, cut(err.Error(), errorCap)
	} else {
		c.Result = ResultAccepted
	}
	return resp.StatusCode, c, err
}

// The masking and cutting are the vendor adapter's, one copy for customer
// webhooks and vendor calls (httpadapter/capture.go).
func requestBody(req *http.Request) (string, bool)  { return httpadapter.RequestBody(req) }
func keep(b []byte, max int) (string, bool)         { return httpadapter.Keep(b, max) }
func cut(s string, max int) string                  { return httpadapter.Cut(s, max) }
func maskHeaders(h http.Header, host string) string { return httpadapter.MaskHeaders(h, host) }
func maskURL(u *url.URL) string                     { return httpadapter.MaskURL(u) }
func maskBody(b []byte) []byte                      { return httpadapter.MaskBody(b) }
