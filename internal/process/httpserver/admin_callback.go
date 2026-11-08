package httpserver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/callbackmap"
	"github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/lib/encoding"
)

// handleCallbackTry is a DLR / MO dialect's "Try it" (config/0125; one model
// with a client's dialect since 2026-10-08): an operator pastes a sample of
// what the vendor sends and sees what THIS binary's reader would take out of
// it and what it would answer — the same internal/callbackmap the dlr and mo
// roles run, so the screen can never promise a reading the gateway does not
// make. Stores nothing, reaches nothing, applies nothing.
//
// It takes the dialect as it is stored: request_map (the field list, or the
// older object), request_format, request_wrapper, response_map and
// keep_unmapped. The answer says, like a client dialect's dry run:
//
//	items[]      one per callback in the sample: values (our field -> value),
//	             read (per field: their name, where the dialect looks, where
//	             the value was found, what was sent and what we take it as),
//	             and state / done_at (a receipt) or encoding (a message)
//	problems[]   what would stop a callback being used, in words
//	outcome      how the sample itself would be answered: OK or UNREADABLE
//	reply        that answer
//	responses    the answer for every outcome: OK, STORE_UNAVAILABLE,
//	             UNREADABLE (reply_ok and reply_fail repeat the first two)
func (p *Process) handleCallbackTry(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, 405, map[string]string{"error": "POST only"})
		return
	}
	var req struct {
		Kind           string            `json:"kind"`
		RequestMap     json.RawMessage   `json:"request_map"`
		ResponseMap    json.RawMessage   `json:"response_map"`
		RequestFormat  *string           `json:"request_format"`
		RequestWrapper *string           `json:"request_wrapper"`
		KeepUnmapped   json.RawMessage   `json:"keep_unmapped"`
		Method         string            `json:"method"`
		ContentType    string            `json:"content_type"`
		Query          string            `json:"query"`
		Headers        map[string]string `json:"headers"`
		Body           string            `json:"body"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	opt := callbackmap.Options{}
	if req.RequestFormat != nil {
		opt.Format = *req.RequestFormat
	}
	if req.RequestWrapper != nil {
		opt.Wrapper = *req.RequestWrapper
	}
	keep := false
	switch strings.TrimSpace(string(req.KeepUnmapped)) {
	case "true", "1", `"true"`, `"1"`:
		keep = true
	}
	q, err := callbackmap.ParseRequestWith(string(req.RequestMap), req.Kind, opt)
	if err != nil {
		writeJSON(w, 200, map[string]any{"valid": false, "error": err.Error()})
		return
	}
	rp, err := callbackmap.ParseReplyWith(string(req.ResponseMap), keep)
	if err != nil {
		writeJSON(w, 200, map[string]any{"valid": false, "error": err.Error()})
		return
	}
	if err := callbackmap.CheckSize(q, rp); err != nil {
		writeJSON(w, 200, map[string]any{"valid": false, "error": err.Error()})
		return
	}
	out := map[string]any{"valid": true}
	if (q != nil && q.Legacy) || rp.Legacy() {
		out["older_shape"] = true
	}
	// How the sample itself would be answered.
	sampleOutcome := callbackmap.Outcome{Count: 1, MessageID: "01EXAMPLEID0000000000000000"}
	if q == nil && req.Kind == callbackmap.KindDLR {
		// A receipt door with no dialect reads with the default reading
		// (issue 19, 2026-10-27): tried as the dlr role reads it.
		q = callbackmap.DefaultDLR()
		out["reading"] = "default"
		out["note"] = "no dialect: the door reads receipts with the default reading — the usual names for the message id, status, error code and number"
	}
	if q == nil {
		out["note"] = "no request mapping: this door reads callbacks the old way"
	} else {
		method := strings.ToUpper(strings.TrimSpace(req.Method))
		if method == "" {
			method = http.MethodPost
		}
		target := "/" + req.Kind + "/sample"
		if qs := strings.TrimPrefix(strings.TrimSpace(req.Query), "?"); qs != "" {
			target += "?" + qs
		}
		sample := httptest.NewRequest(method, target, strings.NewReader(req.Body))
		if req.ContentType != "" {
			sample.Header.Set("Content-Type", req.ContentType)
		}
		for k, v := range req.Headers {
			sample.Header.Set(k, v)
		}
		items, err := q.ReadTraced(sample, []byte(req.Body))
		if err != nil {
			out["error"] = err.Error()
		}
		shown := make([]map[string]any, 0, len(items))
		var problems []string
		readable := 0
		sampleOutcome.Count = len(items)
		if req.Kind == callbackmap.KindDLR {
			sampleOutcome.MessageID = ""
		}
		for i, it := range items {
			row := map[string]any{"values": it.Values, "read": it.Read}
			switch req.Kind {
			case callbackmap.KindDLR:
				// What the receipt would be recorded as — the dlr role's own
				// reading of the word, through this dialect's words first.
				st := dlr.ParseStat(it.Values["status"], q.StatusMap)
				row["state"] = string(st)
				if it.Values["message_id"] == "" {
					problems = append(problems, itemWord(i, len(items))+"no message id — it would be kept as an orphan")
				} else {
					readable++
					sampleOutcome.MessageID = it.Values["message_id"]
				}
				if st == "" {
					problems = append(problems, itemWord(i, len(items))+"status "+strconv.Quote(it.Values["status"])+" is not a word the gateway knows — add it to the status field's values")
				}
				// The delivery time as it would be stored, so the operator can
				// see the format is read right before a vendor relies on it.
				if t, ok, terr := q.DoneAt(it.Values); terr != nil {
					problems = append(problems, itemWord(i, len(items))+"delivery time "+strconv.Quote(it.Values["done_at"])+" could not be read: "+terr.Error())
				} else if ok {
					row["done_at"] = t.Format(time.RFC3339)
				}
			case callbackmap.KindMO:
				if enc, _, err := encoding.Plan(it.Values["text"], ""); err == nil {
					row["encoding"] = enc
				}
				if it.Values["from"] == "" || it.Values["to"] == "" {
					problems = append(problems, itemWord(i, len(items))+"no sender or receiver — it would be dropped")
				} else {
					readable++
					sampleOutcome.VendorMessageID = it.Values["message_id"]
				}
			}
			shown = append(shown, row)
		}
		if readable == 0 {
			sampleOutcome.Reason = callbackmap.ReasonUnreadable
		}
		out["items"] = shown
		out["problems"] = problems
	}
	out["outcome"] = "OK"
	if sampleOutcome.Reason != "" {
		out["outcome"] = sampleOutcome.Reason
	}
	out["reply"] = rp.Render(sampleOutcome)
	// And every answer this dialect can give, on fixed sample values.
	fixed := callbackmap.Outcome{MessageID: "01EXAMPLEID0000000000000000", VendorMessageID: "vendor-ref-1", Count: 1}
	responses := map[string]any{"OK": rp.Render(fixed)}
	for _, reason := range callbackmap.Reasons() {
		if reason == callbackmap.ReasonAny {
			continue
		}
		o := fixed
		o.Reason = reason
		responses[reason] = rp.Render(o)
	}
	out["responses"] = responses
	out["reply_ok"] = responses["OK"]
	out["reply_fail"] = responses[callbackmap.ReasonStore]
	writeJSON(w, 200, out)
}

func itemWord(i, n int) string {
	if n <= 1 {
		return ""
	}
	return "item " + strconv.Itoa(i+1) + ": "
}
