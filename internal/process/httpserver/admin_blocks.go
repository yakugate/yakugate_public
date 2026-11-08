package httpserver

// GET /admin/blocks — who is locked out right now (2026-10-09, Saleem).
//
// POST /admin/unblock has lifted a block since 2026-08-09, but only for
// somebody who already knew the login name or the address: nothing listed
// what was blocked. So the first an operator heard of a block was a customer
// saying their sends answered 423 or their bind was refused. This is the list
// the portal's Login Blocks screen shows.
//
// What is listed: every login and address held by the identity ladder, the
// per-address ladder and the guessing guard — see internal/lockout. Under the
// Redis gate those are one list for every process on that Redis, the SMPP
// server's bind blocks included ("shared": true). Without Redis each process
// keeps its own, and this answers for this http-server only ("shared":
// false); a block the smpp-server holds is then not listed here.
//
// Not listed: the vendor callback doors' own ladders (the dlr and mo roles
// keep those in memory), and a block imposed by a process older than the
// list (lockout/blocked.go) — at most an hour's gap after an upgrade.
//
// A row never carries a token. A token ladder's id is the token that was
// tried; the row gives its kind, a reference to hand back to /admin/unblock,
// and — for a JWT — who the token SAYS it is, read without checking it.
//
// Query: ?limit=N (default 500, at most 2000). The blocks with the longest
// time left come first; "more" says the list was cut.
//
// Response: {"shared", "deployment_id", "blocks": [{ref, kind, reason, value,
// hint, level, retry_after_sec, until}], "more", "limits"}.
//   kind    login (HTTP username) | smpp (system_id) | token | address | other
//   reason  for an address: failures (too many failed logins from it) or
//           guessing (several different logins failed from it)

import (
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/lockout"
)

const (
	blocksDefaultLimit = 500
	blocksMaxLimit     = 2000
	// blocksRefScan is how far a reference is looked for. Far more than a
	// gateway holds blocked outside an attack; during one, the address or the
	// login name still lifts a block that this did not reach.
	blocksRefScan = 5000
)

// blockLimits is what makes a block, so the screen can say it in numbers.
type blockLimits struct {
	LoginFails     int   `json:"login_fails"`     // failed tries of one login inside the window
	AddressFails   int   `json:"address_fails"`   // failed tries from one address inside the window
	GuessingLogins int   `json:"guessing_logins"` // different logins failed from one address inside the window
	WindowSec      int   `json:"window_sec"`
	RungsSec       []int `json:"rungs_sec"` // how long a block lasts: first, then each time it is earned again
}

func newBlockLimits(loginFails, addressFails, guessingLogins int) blockLimits {
	l := blockLimits{LoginFails: loginFails, AddressFails: addressFails, GuessingLogins: guessingLogins,
		WindowSec: int(lockout.Window().Seconds())}
	for _, r := range lockout.Rungs() {
		l.RungsSec = append(l.RungsSec, int(r.Seconds()))
	}
	return l
}

type blockRow struct {
	Ref           string `json:"ref"`
	Kind          string `json:"kind"`
	Reason        string `json:"reason,omitempty"`
	Value         string `json:"value,omitempty"`
	Hint          string `json:"hint,omitempty"`
	Level         int    `json:"level"`
	RetryAfterSec int    `json:"retry_after_sec"`
	Until         string `json:"until"`
}

// listers keeps the ladders that can say who they hold.
func listers(ladders ...any) []lockout.Lister {
	var out []lockout.Lister
	for _, l := range ladders {
		if ls, ok := l.(lockout.Lister); ok {
			out = append(out, ls)
		}
	}
	return out
}

// describeBlock says what a ladder id is, in the words an operator uses. The
// prefixes are the ones internal/auth and the SMPP server lock under; the
// guessing guard keys by the bare address. A form nobody here knows is
// "other" and shows nothing of itself.
func describeBlock(id string) (kind, reason, value, hint string) {
	switch {
	case strings.HasPrefix(id, "u:"):
		return "login", "", id[2:], ""
	case strings.HasPrefix(id, "smpp:"):
		return "smpp", "", id[5:], ""
	case strings.HasPrefix(id, "ip:"):
		return "address", "failures", id[3:], ""
	case strings.HasPrefix(id, "tok:"):
		return "token", "", "", ""
	case strings.HasPrefix(id, "t:"):
		return "token", "", "", tokenSubject(id[2:])
	}
	if net.ParseIP(id) != nil {
		return "address", "guessing", id, ""
	}
	return "other", "", "", ""
}

// blockLabel is the one-line name of a block for the answer and the logs:
// "login:alice", "address:203.0.113.7", "token:#1a2b3c4d".
func blockLabel(id string) string {
	kind, _, value, _ := describeBlock(id)
	if value == "" {
		value = "#" + lockout.Ref(id)[:8]
	}
	return kind + ":" + value
}

// tokenSubject reads who a sign-in token SAYS it is: its "sub" claim, read
// without checking the signature. A hint for the operator, never proof. ""
// when the text is not a JWT, names nobody, or is too long to be worth it.
func tokenSubject(tok string) string {
	parts := strings.Split(tok, ".")
	if len(parts) != 3 || len(parts[1]) > 4096 {
		return ""
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		return ""
	}
	var claims struct {
		Sub any `json:"sub"`
	}
	if json.Unmarshal(raw, &claims) != nil {
		return ""
	}
	sub, _ := claims.Sub.(string)
	var b strings.Builder
	n := 0
	for _, r := range sub {
		if !unicode.IsPrint(r) {
			continue
		}
		b.WriteRune(r)
		if n++; n == 64 {
			break
		}
	}
	return strings.TrimSpace(b.String())
}

// blocksNow reads every list this process has, longest time left first. In
// memory the three lists hold different kinds of id; under Redis there is one
// list. Either way an id is reported once.
func (p *Process) blocksNow(limit int) ([]lockout.Block, bool, error) {
	seen := map[string]bool{}
	var all []lockout.Block
	more := false
	for _, l := range p.blockLists {
		got, m, err := l.Blocked(limit)
		if err != nil {
			return nil, false, err
		}
		more = more || m
		for _, b := range got {
			if !seen[b.ID] {
				seen[b.ID] = true
				all = append(all, b)
			}
		}
	}
	sort.SliceStable(all, func(i, j int) bool {
		if all[i].Remaining != all[j].Remaining {
			return all[i].Remaining > all[j].Remaining
		}
		return all[i].ID < all[j].ID
	})
	if len(all) > limit {
		all, more = all[:limit], true
	}
	return all, more, nil
}

// blockByRef finds the id a reference names among what is blocked now.
func (p *Process) blockByRef(ref string) (id string, found bool, err error) {
	all, _, err := p.blocksNow(blocksRefScan)
	if err != nil {
		return "", false, err
	}
	for _, b := range all {
		if lockout.Ref(b.ID) == ref {
			return b.ID, true, nil
		}
	}
	return "", false, nil
}

// clearBlock lifts exactly one listed block, in the ladder that holds its
// kind of id.
func (p *Process) clearBlock(id string) {
	switch {
	case strings.HasPrefix(id, "ip:"):
		if p.ipUnblock != nil {
			p.ipUnblock.Clear(id)
		}
	case net.ParseIP(id) != nil:
		if p.sprayUnblock != nil {
			p.sprayUnblock.Clear(id)
		}
	default:
		if p.idUnblock != nil {
			p.idUnblock.Clear(id)
		}
	}
}

func (p *Process) handleBlocks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, 405, map[string]string{"error": "GET only"})
		return
	}
	limit := blocksDefaultLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			writeJSON(w, 400, map[string]string{"error": "limit must be a whole number above 0"})
			return
		}
		limit = min(n, blocksMaxLimit)
	}
	all, more, err := p.blocksNow(limit)
	if err != nil {
		// Never an empty list in place of one that could not be read: "nobody
		// is blocked" is an answer an operator acts on.
		writeJSON(w, 503, map[string]string{"error": "the list of blocks could not be read: " + err.Error()})
		return
	}
	now := time.Now().UTC()
	rows := make([]blockRow, 0, len(all))
	for _, b := range all {
		kind, reason, value, hint := describeBlock(b.ID)
		left := int((b.Remaining + time.Second - 1) / time.Second) // a block with half a second left is not over
		rows = append(rows, blockRow{
			Ref: lockout.Ref(b.ID), Kind: kind, Reason: reason, Value: value, Hint: hint,
			Level: b.Level, RetryAfterSec: left, Until: now.Add(b.Remaining).Format(time.RFC3339),
		})
	}
	writeJSON(w, 200, map[string]any{
		"shared":        p.blocksShared,
		"deployment_id": deployment.ID(),
		"blocks":        rows,
		"more":          more,
		"limits":        p.blockLimits,
	})
}
