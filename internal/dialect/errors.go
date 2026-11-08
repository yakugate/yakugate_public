package dialect

// Every refusal says what went wrong in words (Saleem 2026-10-02: "each error
// must contain message"). A refusal reason is "CODE: detail"; many stages
// refuse with the bare CODE, which left the response's `error` empty — a
// client sent in quiet hours got status QUIET_HOURS and nothing else. When a
// refusal carries no detail of its own, `error` now says the code's message
// from this table.
//
// The words are what the CLIENT may know: nothing here tells one reason for a
// block from another (a trap number reads exactly like any blocked number),
// names a spam rule, or says whether a username exists.
// TestEveryRefusalCodeHasAMessage fails when the engine gains a refusal code
// with no line here.
var refusalText = map[string]string{
	// The request itself
	"INVALID_REQUEST":    "The request is missing a required field or has one in the wrong format.",
	"INVALID_JSON":       "The request body is not valid JSON.",
	"NO_RECIPIENTS":      "No recipient was given.",
	"REQUEST_TOO_LARGE":  "The request is too large.",
	"INVALID_MSISDN":     "The destination number is not valid. Send it in international format, with the country code.",
	"INVALID_SENDER":     "The sender ID is not valid: use letters, digits, space and . - _ (up to 11), or a number of up to 15 digits.",
	"MESSAGE_TOO_LONG":   "The text needs more parts than this gateway allows in one message.",
	"ENCODING_MISMATCH":  "The text has characters the requested encoding cannot carry.",
	"NOT_FOUND":          "Not found.",
	"METHOD_NOT_ALLOWED": "This method is not allowed here.",

	// Who is sending
	"AUTH_FAILED":           "The credentials were not accepted.",
	"AUTH_LOCKED":           "Too many failed logins: this login is locked for a while. Try again later.",
	"IP_LOCKED":             "Too many failed logins from this address. Try again later.",
	"IP_THROTTLED":          "Too many failed logins from this address. Try again later.",
	"ACCOUNT_DISABLED":      "This account is disabled.",
	"ACCOUNT_SUSPENDED":     "This account is suspended.",
	"IP_NOT_ALLOWED":        "Requests from this IP address are not allowed for this account.",
	"RESTRICTED_DEPLOYMENT": "This account is not served by this gateway.",

	// What may be sent, and where
	"QUIET_HOURS":         "Sending is paused during this account's quiet hours. Try again after they end.",
	"DESTINATION_BLOCKED": "The destination number is blocked.",
	"NOT_WHITELISTED":     "This account may send only to approved numbers, and this one is not on the list.",
	"COUNTRY_BLOCKED":     "Sending to this country is not allowed for this account.",
	"TEMPLATE_MISMATCH":   "The text does not match an approved template for this account.",
	"SPAM_KEYWORD":        "The message was refused by the content rules.",
	"SENDER_NOT_ALLOWED":  "This sender ID is not allowed for this account.",
	"HLR_UNRESOLVED":      "The destination's network could not be found.",
	"NO_ROUTE":            "No route is set up for this destination.",
	"LOSS_PREVENTED":      "The message cannot be sent to this destination at the current price.",

	// How much may be sent
	"INSUFFICIENT_BALANCE": "The balance is too low to send this message.",
	"QUOTA_EXHAUSTED":      "The sending quota for this period is used up.",
	"QUOTA_EXCEEDED":       "The sending quota for this period is used up.",
	"RATE_LIMIT_EXCEEDED":  "Too many messages per second. Slow down and try again.",
	"DESTINATION_CAP":      "Too many messages to this number in this period. Try again later.",
	"DUPLICATE_MESSAGE":    "The same message was already sent to this number recently.",

	// Our side
	"ENCRYPTION_UNAVAILABLE": "The message cannot be stored securely right now. Try again later.",
	"QUEUE_UNAVAILABLE":      "The gateway cannot take messages right now. Try again later.",
	"SERVICE_UNAVAILABLE":    "The gateway is not ready. Try again shortly.",
	"INTERNAL_ERROR":         "Something went wrong on our side. Try again later.",
}

// RefusalText is the message for a refusal code, for a refusal that carries
// no detail of its own. A code with no line here still gets words, never "".
func RefusalText(code string) string {
	if s, ok := refusalText[code]; ok {
		return s
	}
	if code == "" {
		return "The message was not accepted."
	}
	return "The message was not accepted (" + code + ")."
}

// errorText is what `error` says for one result: the refusal's own detail,
// else its code's message; "" for an accepted message.
func errorText(r Result) string {
	if r.Detail != "" || r.Accepted {
		return r.Detail
	}
	return RefusalText(r.Reason)
}
