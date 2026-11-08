package provision

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/pushretry"
)

// Where an account's receipts and incoming messages go (D-137, 2026-10-15).
//
// client_dlr and mo_queue are each one queue drained by two legs, the SMPP
// bind and the HTTP poster, and <kind>_channel decides which of them owns an
// attempt:
//
//	bind -> the bind only          url  -> the poster only
//	both -> the bind first, the poster once it has failed <kind>_bind_attempts
//
// The two kinds are set apart: MO may go to a webhook while the receipts stay
// on the bind (Saleem 2026-10-15: "what if i want mo to be only sent to
// http"). Two settings that quietly deliver nothing are refused, as the Admin
// API refuses them (accountRoutes.js assertPushChannel — change together):
//
//	url or both with no active webhook of that kind for the account (the
//	     one it names, dlr_webhook_id / mo_webhook_id, config/0174);
//	both with the bind attempts at or above that webhook's own retry budget,
//	     which gives the job up before the webhook ever gets a turn.
//
// The account's HTTP switch plays no part: it says whether the client may
// SUBMIT over HTTP, and a webhook is the client's own address, which an
// SMPP-only client may have (Saleem 2026-10-15).

// The three channels. "both" is the column default and comes first, so the
// console offers it first.
const (
	PushBoth = "both"
	PushBind = "bind"
	PushURL  = "url"
)

// PushChannels are the values <kind>_channel takes.
var PushChannels = []string{PushBoth, PushBind, PushURL}

// maxBindAttempts is the Admin API's schema limit (1 to 1000).
const maxBindAttempts = 1000

var pushKindWords = map[string]string{"dlr": "DLR", "mo": "MO"}

// pushKey splits one of the four settings into its kind and part.
func pushKey(key string) (kind string, channel bool, ok bool) {
	switch key {
	case "dlr_channel":
		return "dlr", true, true
	case "mo_channel":
		return "mo", true, true
	case "dlr_bind_attempts":
		return "dlr", false, true
	case "mo_bind_attempts":
		return "mo", false, true
	}
	return "", false, false
}

// setPushChannel changes one of the four settings, judged with the other half
// of its pair as stored, and writes the accounts.update audit row the portal's
// stale-form check reads (staleSave.js), as SetAccountDisabled does.
func (s Store) setPushChannel(ctx context.Context, cur AccountDetail, key, value string) error {
	kind, isChannel, _ := pushKey(key)
	channel, attempts := cur.DLRChannel, cur.DLRBindAttempts
	if kind == "mo" {
		channel, attempts = cur.MOChannel, cur.MOBindAttempts
	}
	value = strings.ToLower(strings.TrimSpace(value))
	var before, after any
	if isChannel {
		if !slices.Contains(PushChannels, value) {
			return refusef("%q: %s go to the bind, the url (the account's %s webhook) or both", value, pushThings(kind), pushKindWords[kind])
		}
		if value == channel {
			return nil
		}
		before, after, channel = channel, value, value
	} else {
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 || n > maxBindAttempts {
			return refusef("%q: the bind attempts are a whole number from 1 to %d", value, maxBindAttempts)
		}
		if n == attempts {
			return nil
		}
		before, after, attempts = attempts, n, n
	}
	if err := s.assertPushChannel(ctx, cur.ID, kind, channel, attempts); err != nil {
		return err
	}
	// key is one of the four column names above, never typed by anyone.
	if err := s.DB.SetAccountPushSetting(ctx, cur.ID, key, after); err != nil {
		return fmt.Errorf("update account %d: %w", cur.ID, err)
	}
	return s.audit(ctx, auditEntry{
		Action: "accounts.update", Entity: "account", Feature: "accounts", EntityID: cur.ID,
		AccountID: cur.ID, CompanyID: cur.CompanyID, CompanyName: cur.Company,
		Label:   cur.Username,
		Summary: "Updated account " + cur.Username + " from the console",
		Detail:  map[string]any{"fields": []string{key}},
		Before:  map[string]any{key: before},
		After:   map[string]any{key: after},
	})
}

// assertPushChannel refuses a channel that would deliver nothing.
func (s Store) assertPushChannel(ctx context.Context, accountID int64, kind, channel string, attempts int) error {
	if channel == PushBind {
		return nil // needs no webhook and no budget
	}
	retry, err := s.DB.AccountWebhookRetry(ctx, accountID, kind)
	return judgePushChannel(kind, channel, attempts, retry, err)
}

// judgePushChannel is assertPushChannel on the webhook the account names (or
// would name): retry is its retry policy, err the read of it (ErrNoRows =
// none, or not switched on). A bind channel needs nothing.
func judgePushChannel(kind, channel string, attempts int, retry string, err error) error {
	if channel == PushBind {
		return nil
	}
	word := pushKindWords[kind]
	if errors.Is(err, dal.ErrNoRows) {
		return refusef("%s_channel %q needs an active %s webhook for this account: add one under HTTP calls first", kind, channel, word)
	}
	if err != nil {
		return fmt.Errorf("read the account's %s webhook: %w", word, err)
	}
	if channel != PushBoth {
		return nil
	}
	if total := PushBudget(retry); attempts >= total {
		return refusef("%s_bind_attempts (%d) must be less than the %s webhook's total retry attempts (%d): otherwise the job is given up before the webhook ever gets a turn",
			kind, attempts, word, total)
	}
	return nil
}

// PushBudget is a webhook's total attempts as the Admin API reads them: its
// retry policy's max_attempts when that is a whole number above 0, else the
// gateway's default (YAKU_PUSH_MAX_ATTEMPTS, 30). A malformed policy falls
// back to the default, as the engine's does.
func PushBudget(retryJSON string) int {
	total := pushretry.Defaults().MaxAttempts
	if total <= 0 {
		total = 30 // the Admin API's `Number(env) || 30`: 0 is not a budget
	}
	var r struct {
		MaxAttempts *float64 `json:"max_attempts"`
	}
	if strings.TrimSpace(retryJSON) == "" || json.Unmarshal([]byte(retryJSON), &r) != nil || r.MaxAttempts == nil {
		return total
	}
	if n := *r.MaxAttempts; n > 0 && n == math.Trunc(n) && n <= math.MaxInt32 {
		return int(n)
	}
	return total
}

// PushWords says in a sentence where one kind goes.
func PushWords(kind, channel string, attempts int) string {
	word := pushKindWords[kind]
	switch channel {
	case PushBind:
		return "the SMPP bind only"
	case PushURL:
		return "the " + word + " webhook only"
	}
	return fmt.Sprintf("the SMPP bind, then the %s webhook after %d failed tries", word, attempts)
}

func pushThings(kind string) string {
	if kind == "mo" {
		return "incoming messages"
	}
	return "receipts"
}
