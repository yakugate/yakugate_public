package mo

// Honouring STOP (PO ruling 2026-09-09: opt-in per customer, gateway-managed
// or customer-managed).
//
// The gateway already refused blocklisted numbers at accept, and
// blocklist.source already had an 'unsub' value. Nothing wrote it — so a
// subscriber replying STOP was routed to the customer and forgotten, and the
// enforcement half sat there looking as though opt-outs were handled.
//
// What this does NOT do: swallow the message. The customer receives the STOP
// either way. Even when we act on it they need to see it, or their own
// subscriber records end up disagreeing with ours — and for a customer running
// their own compliance, ours is the copy that should be invisible.

import (
	"context"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
)

// Default keyword sets. Deliberately the words carriers and regulators expect
// rather than a long list: a false positive here silently stops a subscriber's
// messages, so an ordinary word that happens to appear alone must not qualify.
var (
	defaultOptOut = []string{"stop", "stopall", "unsubscribe", "end", "quit", "cancel", "optout", "opt-out"}
	defaultOptIn  = []string{"start", "unstop", "subscribe", "optin", "opt-in"}
)

// OptOutIntent is what an inbound message asks for.
type OptOutIntent int

const (
	IntentNone OptOutIntent = iota
	IntentOptOut
	IntentOptIn
)

// ReadIntent decides whether a message body is an opt-out, an opt-in, or
// ordinary traffic.
//
// The whole message must BE the keyword, give or take punctuation and case.
// "STOP" opts out; "please stop sending these at 6am" does not, and neither
// does "we stop at nothing". Matching a keyword anywhere in the text would
// unsubscribe people for using an ordinary English word, and the failure is
// silent — they simply stop receiving messages and nobody learns why.
func ReadIntent(body, optOutCSV, optInCSV string) OptOutIntent {
	word := strings.ToLower(strings.TrimSpace(body))
	word = strings.Trim(word, ".,!?;:'\"()[]{}")
	if word == "" {
		return IntentNone
	}
	if matchesAny(word, optOutCSV, defaultOptOut) {
		return IntentOptOut
	}
	if matchesAny(word, optInCSV, defaultOptIn) {
		return IntentOptIn
	}
	return IntentNone
}

func matchesAny(word, csv string, fallback []string) bool {
	list := fallback
	if s := strings.TrimSpace(csv); s != "" {
		list = strings.Split(s, ",")
	}
	for _, k := range list {
		if word == strings.ToLower(strings.TrimSpace(k)) {
			return true
		}
	}
	return false
}

// RecordOptOut adds the sender to this account's blocklist as an unsubscribe.
//
// Scoped to the ACCOUNT, never global: a subscriber who stops one customer's
// messages has not asked to stop everybody's, and blocking them gateway-wide
// would cut off their bank's one-time codes because they unsubscribed from a
// shop.
//
// Idempotent by the table's own unique key, so a subscriber sending STOP three
// times leaves one row and the reason from the first.
//
// The write is the config store's (dal.MORules.OptOut: one upsert of a
// blocklist row, source 'unsub', reason "opted out by SMS to <sender>").
func RecordOptOut(ctx context.Context, rules dal.MORules, accountID int64, msisdn, sender string) error {
	if rules == nil || msisdn == "" {
		return nil
	}
	return rules.OptOut(ctx, accountID, msisdn, sender)
}

// RecordOptIn undoes an opt-out for this account.
//
// Disables the row rather than deleting it, so the history survives: "this
// number opted out in March and back in in May" is exactly what someone will
// need when a subscriber complains, and a DELETE erases the evidence that we
// honoured the original request.
//
// Only rows we created are touched. A number a human blocked manually, or one
// blocked for abuse, stays blocked — a START from the handset must not be able
// to lift a block somebody put there deliberately.
//
// The write is the config store's (dal.MORules.OptIn: is_disabled = 1 on the
// account's 'unsub' row for the number).
func RecordOptIn(ctx context.Context, rules dal.MORules, accountID int64, msisdn string) error {
	if rules == nil || msisdn == "" {
		return nil
	}
	return rules.OptIn(ctx, accountID, msisdn)
}
