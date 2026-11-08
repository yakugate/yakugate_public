package provision

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/senderid"
)

// An account's allowed senders. [SCOPE P14-ACCT]
//
// sms_service_config.allowed_sender_ids is a JSON array of senders the
// account may send with. The engine (refdata parseSenderIDs) reads an EMPTY
// list as "any sender", and so is "*"; any other list allows exactly its
// entries plus the account's approved sender IDs (sender_ids rows, merged in
// by mergeWorkflowSenders). Compared exactly, case and all.
//
// The Admin API's rules for the field (accountRoutes.js bodySchema: an array
// of strings; services/defaultSender.js on every write that sends it): the
// account's default sender must still be one of its senders after the write,
// judged on the row as it will be — so taking the default out of the list is
// refused, and so is emptying the list while the default is neither "*"-ed
// nor approved (an empty list allows every sender at the gateway, but the
// Admin API does not count it as naming the default, and this door may not
// be looser). Every entry must be a sender the gateway would accept at all
// (one it refuses can never match a message, so it would only mislead
// whoever reads the list), and "*" beside other senders is refused (they
// would do nothing) — the Admin API's cleanAllowedSenders since 2026-10-02
// (services/defaultSender.js, with the default limits 11 and 15; this side
// reads YAKU_SENDER_MAX_*). Change together.

// SenderLimits are the gateway's own sender lengths — the validate stage's
// YAKU_SENDER_MAX_ALPHA / YAKU_SENDER_MAX_NUMERIC (11 and 15 by default).
func SenderLimits() (alpha, numeric int) {
	return envInt("YAKU_SENDER_MAX_ALPHA", senderid.DefaultMaxAlpha), envInt("YAKU_SENDER_MAX_NUMERIC", senderid.DefaultMaxNumeric)
}

func envInt(name string, def int) int {
	if n, err := strconv.Atoi(strings.TrimSpace(os.Getenv(name))); err == nil && n > 0 {
		return n
	}
	return def
}

// ParseSenderList reads the console's entry for an account's allowed
// senders: senders separated by commas, "*" alone for any sender, empty for
// any sender. A sender typed twice is kept once. The result is what is
// stored: [] for empty, ["*"], or the senders in the order given.
func ParseSenderList(v string) ([]string, error) {
	alpha, numeric := SenderLimits()
	out := []string{}
	seen := map[string]bool{}
	star := false
	for _, part := range strings.Split(v, ",") {
		s := strings.TrimSpace(part)
		if s == "" {
			continue
		}
		if s == "*" {
			star = true
			continue
		}
		if why := senderid.Problem(s, alpha, numeric); why != "" {
			return nil, refusef("%q cannot be an allowed sender: %s. A sender is a number of up to %d digits (a + first is allowed), or a name of up to %d letters A-Z, digits, spaces and . - _",
				s, why, numeric, alpha)
		}
		if seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	if star {
		if len(out) > 0 {
			return nil, refusef(`"*" allows every sender, so %s beside it would do nothing. Give "*" on its own, or the senders without it`, strings.Join(out, ", "))
		}
		return []string{"*"}, nil
	}
	return out, nil
}

// AllowsAnySender says whether a stored list lets the account send with any
// sender, as the engine reads it.
func AllowsAnySender(list []string) bool {
	if len(list) == 0 {
		return true
	}
	for _, v := range list {
		if v == "*" {
			return true
		}
	}
	return false
}

// SenderListWords says what a list means, in a few words.
func SenderListWords(list []string) string {
	switch {
	case len(list) == 0:
		return "any sender (the list is empty)"
	case AllowsAnySender(list):
		return `any sender ("*")`
	}
	return strings.Join(list, ", ")
}

// ApprovedSenders are the account's approved sender IDs (sender_ids,
// config/0062), which count as its senders beside the list. A store without
// that table has none, as the Admin API reads it (defaultSender.js).
func (s Store) ApprovedSenders(ctx context.Context, id int64) ([]string, error) {
	out, err := s.DB.ApprovedSenderRows(ctx, id)
	if err != nil {
		var re dal.RowsError
		if errors.As(err, &re) {
			return nil, re.Err
		}
		if s.DB.MissingTable(err) { // no such table
			return nil, nil
		}
		return nil, fmt.Errorf("read account %d's approved sender IDs: %w", id, err)
	}
	return out, nil
}

// setAllowedSenders replaces an account's allowed senders, after the
// default-sender rule, and audits it as the Admin API does (accounts.update).
func (s Store) setAllowedSenders(ctx context.Context, cur AccountDetail, value string) error {
	list, err := ParseSenderList(value)
	if err != nil {
		return err
	}
	if err := s.assertDefaultKept(ctx, cur.ID, cur.DefaultSender, list); err != nil {
		return err
	}
	b, _ := json.Marshal(list)
	if err := s.DB.SetAccountAllowedSenders(ctx, cur.ID, string(b)); err != nil {
		return fmt.Errorf("update account %d: %w", cur.ID, err)
	}
	before := cur.AllowedSenders
	if before == nil {
		before = []string{}
	}
	company := cur.Company // "—" when the account has none
	if cur.CompanyID == 0 {
		company = ""
	}
	return s.audit(ctx, auditEntry{
		Action: "accounts.update", Entity: "account", Feature: "accounts", EntityID: cur.ID,
		AccountID: cur.ID, CompanyID: cur.CompanyID, CompanyName: company,
		Label:   cur.Username,
		Summary: "Updated account " + cur.Username + " from the console",
		Detail: map[string]any{
			"fields": []string{"allowed_sender_ids"},
			"before": map[string]any{"allowed_sender_ids": before},
			"after":  map[string]any{"allowed_sender_ids": list},
		},
	})
}

// assertDefaultKept is services/defaultSender.js on a write that changes the
// allowed senders and not the default: the default, when there is one, must
// still be a sender the gateway can send with, and still one of the
// account's senders — "*" in the new list, an entry of it, or an approved
// sender ID.
func (s Store) assertDefaultKept(ctx context.Context, id int64, def string, list []string) error {
	def = strings.TrimSpace(def)
	if def == "" {
		return nil
	}
	if !wellFormedSender(def) {
		return refusef("this account's default sender %q is not a sender the gateway can send with: up to 15 digits, or up to 11 letters, digits, spaces and . - _. Choose another default, or clear it, first", def)
	}
	for _, v := range list {
		if v == "*" || v == def {
			return nil
		}
	}
	approved, err := s.ApprovedSenders(ctx, id)
	if err != nil {
		return err
	}
	for _, v := range approved {
		if v == def {
			return nil
		}
	}
	if len(list) == 0 {
		return refusef(`%q is this account's default sender — choose another default, or clear it, before removing it from the allowed senders. An empty list allows every sender, but does not name the default: give "*" to allow every sender and keep it`, def)
	}
	return refusef("%q is this account's default sender — choose another default, or clear it, before removing it from the allowed senders", def)
}
