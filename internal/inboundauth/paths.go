package inboundauth

import (
	"fmt"
	"regexp"
	"strings"
)

// A DOOR'S OWN PATH (issue 20, Saleem 2026-10-23: "dialects for MO and DLR
// should also have the custom endpoints feature, and the DLR and MO servers
// should also support it").
//
// A vendor that can only post to a URL of its own shape (/callback/status,
// /api/receipts.php …) could not be served: the doors answered /dlr/<cid> and
// /mo/<cid> only, and every vendor's URL showed our cid. Now each door may
// carry a path of its own (inbound_endpoints.custom_path, config/0152) — on
// the DOOR, not the dialect: a dialect is shared by many vendors, and a path
// must name one vendor's door. The default path stays.
//
// The rule, written twice — here (the console's door, internal/provision, and
// what the dlr and mo roles load) and in the Admin API
// (p13-backend/src/services/inboundPlaces.js cleanCustomPath) — CHANGE
// TOGETHER; the shared cases are sqa/e2e/shared/door-path-vectors.json:
//
//	"/" then parts of letters, digits and . _ ~ - between single slashes;
//	no "." or ".." part; a slash at the end is dropped; at most 190
//	characters and 10 parts;
//	not under a path the gateway keeps for itself (ReservedPathParts):
//	its doors' defaults, the http-server's pages, the health checks;
//	unique across every door of both kinds, and never one door's path under
//	another's (a vendor that sends its token in the path calls
//	<path>/<token>, so /acme and /acme/r could not be told apart);
//	not an account's custom submit path (the http-server's own paths when
//	the roles share an address).
//
// Letter case is kept as written and ignored when two paths are compared, as
// the database's unique key compares them.
//
// A DIALECT'S OWN URL (27 Oct, Saleem: "we also had decided with PO that
// dialects for mo and dlr will also have custom urls, similar to MT one").
// A dlr or mo dialect's sms_service_dialects.url may be a path of its own,
// under the same rule, and unique against every other path the gateway
// answers (PathsClash): the dialect URLs of every kind, the doors' own paths
// and the accounts' submit paths. As for an MT dialect, the path picks the
// dialect, and the caller's credentials pick who it is: the doors of that
// kind reading with that dialect are the candidates, and the one the
// credentials name is judged as on its own path (Guard.Identify). A door's
// own path stays beside it.

// MaxPathLength and MaxPathParts bound a custom path.
const (
	MaxPathLength = 190
	MaxPathParts  = 10
)

// ReservedPathParts are the first parts a custom path may not have: the
// doors' default paths, the http-server's own (sms, messages, admin, status,
// metrics) and the health checks every role answers.
func ReservedPathParts() []string {
	return []string{"dlr", "mo", "sms", "messages", "admin", "status", "metrics", "healthz", "readyz"}
}

// Why a custom path is refused, for the shared cases.
const (
	PathBadForm  = "form"
	PathReserved = "reserved"
	PathTooLong  = "too_long"
	PathTooDeep  = "too_deep"
)

// PathError is a refused custom path: Reason is one of the Path* codes.
type PathError struct {
	Reason string
	Text   string
}

func (e *PathError) Error() string { return e.Text }

var pathPart = regexp.MustCompile(`^[A-Za-z0-9._~-]+$`)

// CleanCustomPath reads a door's custom path as it is to be stored: trimmed,
// without a slash at the end. "" = none. A path that breaks the rule is a
// *PathError.
func CleanCustomPath(raw string) (string, error) {
	p := strings.TrimSpace(raw)
	if p == "" {
		return "", nil
	}
	form := &PathError{Reason: PathBadForm, Text: fmt.Sprintf(
		`%q: a path is "/" followed by letters, digits and . _ ~ - between single slashes (for example /acme/receipts)`, p)}
	if !strings.HasPrefix(p, "/") {
		return "", form
	}
	p = "/" + strings.TrimRight(strings.TrimLeft(p, "/"), "/")
	if strings.HasPrefix(strings.TrimSpace(raw), "//") || p == "/" {
		return "", form
	}
	if len(p) > MaxPathLength {
		return "", &PathError{Reason: PathTooLong, Text: fmt.Sprintf("a path is at most %d characters; this one has %d", MaxPathLength, len(p))}
	}
	parts := strings.Split(p[1:], "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." || !pathPart.MatchString(part) {
			return "", form
		}
	}
	if len(parts) > MaxPathParts {
		return "", &PathError{Reason: PathTooDeep, Text: fmt.Sprintf("a path has at most %d parts; this one has %d", MaxPathParts, len(parts))}
	}
	first := strings.ToLower(parts[0])
	for _, r := range ReservedPathParts() {
		if first == r {
			return "", &PathError{Reason: PathReserved, Text: fmt.Sprintf(
				"%q starts with /%s, which the gateway keeps for itself (its own doors, pages and health checks) — choose another first part", p, r)}
		}
	}
	return p, nil
}

// PathKey is a path as two are compared: lower case.
func PathKey(p string) string { return strings.ToLower(p) }

// NativeSubmitPath is the client API's shared default url
// (sms_service_dialects.url). A dlr or mo dialect that holds it, or holds
// nothing, has no URL of its own.
const NativeSubmitPath = "/sms/v4/send"

// What a path belongs to, for PathsClash.
const (
	PathOfDoor = "door" // an inbound door's own path (inbound_endpoints.custom_path)
	PathOfMT   = "mt"   // an account's own submit path (an MT dialect's url)
	PathOfDLR  = "dlr"  // a DLR dialect's own URL
	PathOfMO   = "mo"   // an MO dialect's own URL
)

// PathsClash reports whether two paths cannot both be kept (issue 20, the
// follow-up of 27 Oct: a DLR or MO dialect has a URL of its own, as an MT
// dialect has). Two accounts' submit paths clash only when they are the same
// — the http-server matches a submit path whole, as it always has. Any other
// pair also clashes when one sits under the other: a vendor that sends its
// token in the path calls <path>/<token>, so /acme and /acme/r could not be
// told apart. Letter case is ignored; a slash at the end does not count.
// Written twice — services/inboundPlaces.js pathsClash — with the shared
// cases' "clash" section.
func PathsClash(kindA, a, kindB, b string) bool {
	a, b = strings.TrimRight(strings.TrimSpace(a), "/"), strings.TrimRight(strings.TrimSpace(b), "/")
	if a == "" || b == "" || PathKey(a) == PathKey(NativeSubmitPath) || PathKey(b) == PathKey(NativeSubmitPath) {
		return false
	}
	if kindA == PathOfMT && kindB == PathOfMT {
		return PathKey(a) == PathKey(b)
	}
	return PathsNest(a, b)
}

// DialectURL reads a dlr or mo dialect's stored url as its own path: "" when
// it has none (the shared default, or nothing), else the path as both doors
// clean it.
func DialectURL(stored string) (string, error) {
	s := strings.TrimSpace(stored)
	if s == "" || PathKey(strings.TrimRight(s, "/")) == PathKey(NativeSubmitPath) {
		return "", nil
	}
	return CleanCustomPath(s)
}

// PathsNest reports whether two cleaned paths are the same, or one sits under
// the other ("/acme" and "/acme/receipts"), whatever their letter case.
func PathsNest(a, b string) bool {
	a, b = PathKey(a), PathKey(b)
	if a == "" || b == "" {
		return false
	}
	return a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}
