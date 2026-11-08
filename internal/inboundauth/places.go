package inboundauth

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/yakugate/yakugate/internal/callbackmap"
)

// Places says where in a vendor's callback each credential is — the
// auth_config of the door's dlr/mo dialect (config/0128; it was the door's own
// credential_places for a day, config/0127). PO 2026-09-28: "they can send any
// value at any place of http, header, body, query".
//
//	{"token":"header:X-Api-Key", "jwt":"query:access_token",
//	 "username":"body:auth.user", "password":"body:auth.pass"}
//
// A place is written:
//
//	header:NAME   a request header
//	query:NAME    a query-string parameter
//	body:PATH     a field of the body: JSON / XML / form, a dot path as in a
//	              mapping ("auth.user", "creds.0.key", "@attr" in XML)
//	path          the URL segment after the vendor: /dlr/<cid>/<token> (token only)
//	any[:NAME]    header NAME, then query NAME, then the URL path (token only;
//	              NAME defaults to yaku-sms-key) — the reading before places
//	bearer        Authorization: Bearer (JWT only)
//	basic         HTTP Basic auth (username and password, together)
//
// A credential with no place keeps its old reading: the token by token_source
// and token_header, a JWT from Authorization: Bearer or the token header, a
// username and password from Basic auth or the query's username/password.
type Places struct {
	Token    string `json:"token,omitempty"`
	JWT      string `json:"jwt,omitempty"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

// Empty reports whether no place is set.
func (p Places) Empty() bool { return p == Places{} }

var placeName = regexp.MustCompile(`^[A-Za-z0-9!#$%&'*+.^_` + "`" + `|~-]{1,64}$`)

// CheckPlace refuses a place this credential cannot be read from. The Admin
// API applies the same rules (services/inboundPlaces.js) — change together.
func CheckPlace(cred, place string) error {
	place = strings.TrimSpace(place)
	if place == "" {
		return nil
	}
	kind, name, hasName := strings.Cut(place, ":")
	kind = strings.ToLower(kind)
	switch kind {
	case "header", "query":
		if !hasName || !placeName.MatchString(name) {
			return fmt.Errorf("%s: %q needs a name, like %s:X-Api-Key", cred, place, kind)
		}
		return nil
	case "body":
		if !hasName || strings.TrimSpace(name) == "" || len(name) > 128 || strings.Count(name, ".") > 5 {
			return fmt.Errorf("%s: %q needs a field, like body:auth.key", cred, place)
		}
		return nil
	case "path":
		if cred != "token" {
			return fmt.Errorf("%s: only a token can be read from the URL path", cred)
		}
		return nil
	case "any":
		if cred != "token" {
			return fmt.Errorf("%s: \"any\" is for a token only", cred)
		}
		if hasName && !placeName.MatchString(name) {
			return fmt.Errorf("%s: %q is not a header name", cred, name)
		}
		return nil
	case "bearer":
		if cred != "jwt" || hasName {
			return fmt.Errorf("%s: \"bearer\" is for a JWT only", cred)
		}
		return nil
	case "basic":
		if (cred != "username" && cred != "password") || hasName {
			return fmt.Errorf("%s: \"basic\" is for a username and password only", cred)
		}
		return nil
	}
	return fmt.Errorf("%s: %q is not a place (header:NAME, query:NAME, body:FIELD%s)", cred, place, extraPlaces(cred))
}

func extraPlaces(cred string) string {
	switch cred {
	case "token":
		return ", path or any"
	case "jwt":
		return " or bearer"
	case "username", "password":
		return " or basic"
	}
	return ""
}

// Check refuses a set of places that cannot work together.
func (p Places) Check() error {
	for _, c := range []struct{ cred, place string }{
		{"token", p.Token}, {"jwt", p.JWT}, {"username", p.Username}, {"password", p.Password},
	} {
		if err := CheckPlace(c.cred, c.place); err != nil {
			return err
		}
	}
	// Basic auth carries both, so it is both or neither.
	if (strings.EqualFold(p.Username, "basic")) != (strings.EqualFold(p.Password, "basic")) {
		return fmt.Errorf("username and password: Basic auth carries both — set both to basic, or neither")
	}
	return nil
}

// requestValues reads values out of one callback, decoding the body at most
// once and only if a place asks for it.
type requestValues struct {
	r       *http.Request
	body    []byte
	doc     any
	decoded bool
}

func newRequestValues(r *http.Request, body []byte) *requestValues {
	return &requestValues{r: r, body: body}
}

// at reads a header:, query: or body: place. Other places are "" here; the
// callers handle path / any / bearer / basic.
func (v *requestValues) at(place string) string {
	kind, name, _ := strings.Cut(strings.TrimSpace(place), ":")
	switch strings.ToLower(kind) {
	case "header":
		return strings.TrimSpace(v.r.Header.Get(name))
	case "query":
		return queryValue(v.r, name)
	case "body":
		if !v.decoded {
			v.doc, v.decoded = callbackmap.Decode(v.r.Header.Get("Content-Type"), v.body), true
		}
		return strings.TrimSpace(callbackmap.Lookup(v.doc, name))
	}
	return ""
}

// presentedToken reads the callback token from where this door says it is.
func presentedToken(v *requestValues, e Endpoint, urlToken string) string {
	place := strings.TrimSpace(e.Places.Token)
	if place == "" {
		// The reading before places: token_source + token_header.
		place = e.TokenSource
		if place == "" {
			place = "any"
		}
		name := e.TokenHeader
		if name == "" {
			name = DefaultTokenHeader
		}
		switch place {
		case "header", "query":
			place += ":" + name
		case "any":
			place = "any:" + name
		}
	}
	kind, name, _ := strings.Cut(place, ":")
	switch strings.ToLower(kind) {
	case "path":
		return urlToken
	case "any":
		if name == "" {
			name = DefaultTokenHeader
		}
		// The header first, then the query string, then the path — the path
		// last because a URL secret is the weakest of the three and should
		// never win over one the vendor took the trouble to send properly.
		if s := strings.TrimSpace(v.r.Header.Get(name)); s != "" {
			return s
		}
		if s := queryValue(v.r, name); s != "" {
			return s
		}
		return urlToken
	}
	return v.at(place)
}

// presentedJWT reads a JWT from where this door says it is. A header value
// that starts "Bearer " is taken without that word, so header:Authorization
// works as the vendor writes it.
func presentedJWT(v *requestValues, e Endpoint) string {
	place := strings.TrimSpace(e.Places.JWT)
	if place == "" {
		return bearer(v.r, e)
	}
	if strings.EqualFold(place, "bearer") {
		if h := v.r.Header.Get("Authorization"); strings.HasPrefix(strings.ToLower(h), "bearer ") {
			return strings.TrimSpace(h[7:])
		}
		return ""
	}
	s := v.at(place)
	if len(s) > 7 && strings.EqualFold(s[:7], "bearer ") {
		s = strings.TrimSpace(s[7:])
	}
	return s
}

// userAndPassword reads the pair from where this door says they are.
func userAndPassword(v *requestValues, e Endpoint) (user, pass string, ok bool) {
	pu, pp := strings.TrimSpace(e.Places.Username), strings.TrimSpace(e.Places.Password)
	if pu == "" && pp == "" {
		return basicOrFields(v.r)
	}
	if strings.EqualFold(pu, "basic") {
		return v.r.BasicAuth()
	}
	if pu == "" {
		pu = "query:username"
	}
	if pp == "" {
		pp = "query:password"
	}
	user, pass = v.at(pu), v.at(pp)
	return user, pass, user != "" || pass != ""
}

func queryValue(r *http.Request, name string) string {
	for k, vs := range r.URL.Query() {
		if strings.EqualFold(k, name) && len(vs) > 0 {
			return strings.TrimSpace(vs[0])
		}
	}
	return ""
}

// bearer reads a JWT from Authorization: Bearer, then the door's token header
// — the reading before places.
func bearer(r *http.Request, e Endpoint) string {
	if v := r.Header.Get("Authorization"); strings.HasPrefix(strings.ToLower(v), "bearer ") {
		return strings.TrimSpace(v[7:])
	}
	name := e.TokenHeader
	if name == "" {
		name = DefaultTokenHeader
	}
	v := strings.TrimSpace(r.Header.Get(name))
	if strings.Count(v, ".") == 2 { // only a JWT-shaped value, so a static token is not mistaken for one
		return v
	}
	return ""
}

// basicOrFields reads username/password from Basic auth or from the query
// string — the reading before places.
func basicOrFields(r *http.Request) (user, pass string, ok bool) {
	if u, p, okBasic := r.BasicAuth(); okBasic {
		return u, p, true
	}
	u, p := queryValue(r, "username"), queryValue(r, "password")
	if u != "" || p != "" {
		return u, p, true
	}
	return "", "", false
}
