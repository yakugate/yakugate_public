package pipeline

import (
	"encoding/json"
	"net/http"
	"strings"
)

// safeHeaders is the ALLOW-list of request headers that may be persisted with a
// rejected message. It is an allow-list, never a deny-list: a deny-list silently
// starts leaking the moment a client or a proxy introduces a new header name.
//
// The reference gateway's rejected log stored the full header map verbatim,
// which meant live customer JWTs (whatever header carried them) sat in the database
// indefinitely and replayably — see saleem-hadal-db-review.md §6.0. Nothing that
// can authenticate a request is allowed through here.
var safeHeaders = map[string]bool{
	"content-type":    true,
	"content-length":  true,
	"user-agent":      true,
	"accept":          true,
	"accept-encoding": true,
	"x-request-id":    true,
	"x-forwarded-for": true,
	"referer":         true,
}

// SanitizeHeaders reduces a request's headers to the persistable subset. The
// result is a plain map suitable for JSON storage; multi-valued headers keep
// only their first value, which is all diagnostics need.
func SanitizeHeaders(h http.Header) map[string]string {
	if len(h) == 0 {
		return nil
	}
	out := make(map[string]string, len(safeHeaders))
	for k, v := range h {
		if len(v) == 0 {
			continue
		}
		if safeHeaders[strings.ToLower(k)] {
			out[strings.ToLower(k)] = v[0]
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// RequestInfoJSON renders the sanitized headers plus basic request context as
// the JSON blob stored on a rejected record. Returns "" when there is nothing
// worth storing, so callers can persist SQL NULL.
func RequestInfoJSON(h http.Header, method, path string) string {
	info := map[string]any{}
	if hdr := SanitizeHeaders(h); hdr != nil {
		info["headers"] = hdr
	}
	if method != "" {
		info["method"] = method
	}
	if path != "" {
		info["path"] = path
	}
	if len(info) == 0 {
		return ""
	}
	b, err := json.Marshal(info)
	if err != nil {
		return ""
	}
	return string(b)
}
