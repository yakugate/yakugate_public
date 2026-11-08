// Package ratematch is the one rule for which row of a rate card prices a
// number. Billing (internal/billing.Rater: what is reserved at acceptance and
// what the billing role charges) and the reference cache (internal/refdata:
// the price LCR compares and the loss check weighs) both choose through it,
// so the row a rule is routed by is the row the customer is charged by
// (review of QA BILL-004 / EP-017 / RR-001, 2026-10-01 — the two had been
// written twice and had already drifted once: the cache ignored the "Any
// destination" row).
package ratematch

import (
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/netcode"
)

// Key is what the rule reads from one rate row. An empty match field matches
// every number; a row with all three empty is the card's "Any destination"
// row.
type Key struct {
	MCC, MNC string
	Prefix   string    // destination_country_code: a dialling-code prefix of the number
	From     time.Time // effective_from; zero = always
	ID       int64
	// Since and Until are the life of this VERSION of the rate (valid_from,
	// valid_to); zero = open on that side. Only billing sets them, when it
	// prices a message by the rates as they stood when the message was
	// accepted and so holds older versions too. Left zero they change nothing.
	Since, Until time.Time
}

// Best returns the index of the row that prices a number, and false when no
// row applies:
//
//   - every non-empty match field must match (the MCC; the MNC, "01" = "001";
//     the prefix of the number) — a row naming something else is skipped;
//   - a row whose effective_from is still ahead of now is skipped, and so is
//     a version that had not begun at now (Since) or had already been
//     replaced or removed by then (Until);
//   - more matched fields win (the "Any destination" row matches with none,
//     so it prices only what nothing more specific does);
//   - then the longer prefix, then the later effective_from, then the lower
//     id, so the choice never depends on the order the rows were read in.
func Best[T any](rows []T, key func(*T) Key, mcc, mnc, dest string, now time.Time) (int, bool) {
	var best Key
	at, bestScore := -1, -1
	for i := range rows {
		k := key(&rows[i])
		if !k.From.IsZero() && k.From.After(now) {
			continue
		}
		if (!k.Since.IsZero() && k.Since.After(now)) || (!k.Until.IsZero() && !k.Until.After(now)) {
			continue
		}
		score := 0
		if k.MCC != "" {
			if k.MCC != mcc {
				continue
			}
			score++
		}
		if k.MNC != "" {
			if !netcode.SameMNC(k.MNC, mnc) {
				continue
			}
			score++
		}
		if k.Prefix != "" {
			if !strings.HasPrefix(dest, k.Prefix) {
				continue
			}
			score++
		}
		better := at < 0 || score > bestScore ||
			(score == bestScore && len(k.Prefix) > len(best.Prefix)) ||
			(score == bestScore && len(k.Prefix) == len(best.Prefix) && k.From.After(best.From)) ||
			(score == bestScore && len(k.Prefix) == len(best.Prefix) && k.From.Equal(best.From) && k.ID < best.ID)
		if better {
			best, bestScore, at = k, score, i
		}
	}
	return at, at >= 0
}
