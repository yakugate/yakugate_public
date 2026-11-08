// Reading an account's JSON columns: SMPP config, number and sender lists,
// IP list, quiet hours, multipart mode.

package refdata

import (
	"encoding/json"
	"log"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/msisdn"
)

// parseSMPPConfig fills the account's SMPP bind fields from smpp_config JSON. [SCOPE P2]
func parseSMPPConfig(s dal.NullString, a *Account) {
	if !s.Valid || s.String == "" {
		return
	}
	var cfg struct {
		SystemID     string   `json:"system_id"`
		PasswordHash string   `json:"password_hash"`
		AllowedModes []string `json:"allowed_modes"`
		MaxBinds     *int     `json:"max_binds"`
		// EnquireLinkSec/IdleTimeoutSec (PO 2026-07-27, [SCOPE P2-KEEPALIVE]):
		// per-account override of the server-initiated keepalive interval and
		// the inactivity expiry; absent/0 = the process default (30s/90s).
		EnquireLinkSec *int `json:"enquire_link_sec"`
		IdleTimeoutSec *int `json:"idle_timeout_sec"`
		// Window (PO 2026-09-04): submit_sm in flight per bind before
		// ESME_RTHROTTLED. Absent/0 = the process default.
		Window *int `json:"window_size"`
		// DeliverWindow (Saleem, 28 Oct 2026): deliver_sm — receipts and MO —
		// one bind may have out without an answer. Absent/0 = the default.
		DeliverWindow *int `json:"deliver_window"`
		// MultipartMode (PO 2026-09-17): how a long inbound message or
		// receipt reaches this client — "udh" parts (default), or "tlv":
		// one deliver_sm carrying message_payload, used only on a bind
		// that declared SMPP 3.4 or later.
		MultipartMode string `json:"multipart_mode"`
	}
	if json.Unmarshal([]byte(s.String), &cfg) != nil {
		return
	}
	a.SMPPMultipartMode = MultipartModeOf(cfg.MultipartMode)
	// The system_id IS the username (PO 2026-08-25, made real 2026-09-03).
	//
	// Until now this read cfg.SystemID — a free-text JSON field with no
	// constraint anywhere — and treated it as an OVERRIDE that beat the
	// username. That was a tenancy hazard: account A storing
	// system_id="bob" took the key from account B whose username actually IS
	// "bob", so B could not bind at all and anyone binding as "bob" was served
	// A's routes, rate cards, balance and blocklist. Two accounts storing the
	// same override collided silently, last-loaded winning, unstable across
	// restarts. Nothing logged either case.
	//
	// Deriving it from the username instead makes a collision IMPOSSIBLE rather
	// than merely detected: username already carries a UNIQUE key
	// (uq_ssc_username), so the bind identity inherits that guarantee and there
	// is no second identity field left to disagree with the first.
	// (PO 2026-09-03: "that's not possible to have two similar usernames, they
	// must be unique".)
	a.SMPPSystemID = a.Username
	// A stored value that DISAGREES is now ignored — say so, because for such an
	// account the bind identity changes with this release and a silent change of
	// who can bind is exactly the kind of thing nobody connects to a deploy.
	if cfg.SystemID != "" && cfg.SystemID != a.Username {
		log.Printf("[refdata] account %d (%s) stores smpp_config.system_id=%q — IGNORED, the username is the bind identity now; that client must bind as %q",
			a.ID, a.Username, cfg.SystemID, a.Username)
	}
	a.SMPPPasswordHash = cfg.PasswordHash
	a.SMPPAllowedModes = cfg.AllowedModes
	// DEC-2/D-065 direction: absent -> default 10; explicit 0 -> unlimited.
	if cfg.MaxBinds == nil {
		a.SMPPMaxBinds = 10
	} else {
		a.SMPPMaxBinds = *cfg.MaxBinds
	}
	if cfg.EnquireLinkSec != nil {
		a.SMPPEnquireLinkSec = *cfg.EnquireLinkSec
	}
	if cfg.Window != nil {
		a.SMPPWindow = *cfg.Window
	}
	if cfg.DeliverWindow != nil {
		a.SMPPDeliverWindow = *cfg.DeliverWindow
	}
	if cfg.IdleTimeoutSec != nil {
		a.SMPPIdleTimeoutSec = *cfg.IdleTimeoutSec
	}
}

// parseNumberSet parses a JSON array of MSISDNs into a lookup set.
// parseStringList reads a JSON array of plain strings. Deliberately NOT
// parseNumberSet: that one runs every entry through msisdn.Clean, which would
// strip a sender ID like "MYBANK" to nothing. Sender IDs are alphanumeric and
// are compared exactly, the same way stage 12 compares the account's own
// allowlist.
func parseStringList(s dal.NullString) []string {
	if !s.Valid || s.String == "" {
		return nil
	}
	var arr []string
	if err := json.Unmarshal([]byte(s.String), &arr); err != nil {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, v := range arr {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func parseNumberSet(s dal.NullString) map[string]struct{} {
	if !s.Valid || s.String == "" {
		return nil
	}
	var arr []string
	if err := json.Unmarshal([]byte(s.String), &arr); err != nil {
		return nil
	}
	set := make(map[string]struct{}, len(arr))
	for _, n := range arr {
		// The whitelist stage compares against msisdn.Clean'd destinations —
		// an entry stored as "+9230..." could never match, silently locking
		// the account out of that number (chunk-5 ruling 5.5, 2026-08-03).
		if n = msisdn.Clean(n); n != "" {
			set[n] = struct{}{}
		}
	}
	return set
}

func parseSenderIDs(s dal.NullString) (masks []string, allowAll bool) {
	if !s.Valid || s.String == "" {
		return nil, true
	}
	var arr []string
	if err := json.Unmarshal([]byte(s.String), &arr); err != nil {
		return nil, true
	}
	for _, m := range arr {
		if m == "*" {
			return nil, true
		}
		masks = append(masks, m)
	}
	return masks, len(masks) == 0
}

func parseIPList(s dal.NullString) []string {
	if !s.Valid || s.String == "" {
		return nil
	}
	var out []string
	if err := json.Unmarshal([]byte(s.String), &out); err != nil {
		return nil
	}
	return out
}

// parseQuietRange parses 'HH:MM-HH:MM' into minutes-of-day; -1,-1 = unset/invalid.
func parseQuietRange(s dal.NullString) (int, int) {
	if !s.Valid || s.String == "" {
		return -1, -1
	}
	parts := strings.SplitN(s.String, "-", 2)
	if len(parts) != 2 {
		return -1, -1
	}
	toMin := func(hm string) (int, bool) {
		hmp := strings.SplitN(strings.TrimSpace(hm), ":", 2)
		if len(hmp) != 2 {
			return 0, false
		}
		h, e1 := strconv.Atoi(hmp[0])
		m, e2 := strconv.Atoi(hmp[1])
		if e1 != nil || e2 != nil || h < 0 || h > 23 || m < 0 || m > 59 {
			return 0, false
		}
		return h*60 + m, true
	}
	start, ok1 := toMin(parts[0])
	end, ok2 := toMin(parts[1])
	if !ok1 || !ok2 {
		return -1, -1
	}
	return start, end
}

// MultipartModeOf normalises a multipart setting: "tlv", "sar" and "payload"
// all mean the message_payload TLV; anything else is UDH parts.
func MultipartModeOf(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "tlv", "sar", "payload", "message_payload":
		return "tlv"
	}
	return "udh"
}
