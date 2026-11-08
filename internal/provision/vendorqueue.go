package provision

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/queues"
)

// A vendor's queues (issue 22 of the design audit; Saleem and the PO, 26 Oct
// 2026; config/0161). A vendor's outgoing messages wait in a common queue
// and one queue per short code, and its rate is shared between the queues
// that have work:
//
//	queue_split  fair      each queue with work an equal share (default)
//	             priority  priority 1 from all the vendor's queues first,
//	                       then 2, and so on
//	short_codes  the vendor's short codes: a message whose sender is one
//	             of them waits in that short code's queue
//
// ONE RULE AT BOTH DOORS ("one database, two doors"): the Admin API's twin is
// p13-backend/src/services/vendorQueue.js, and both read the shared cases in
// sqa/e2e/shared/vendor-queue-vectors.json. Change together.

// QueueSplits are the ways a vendor's rate is shared between its queues.
func QueueSplits() []string { return []string{"fair", "priority"} }

// DefaultQueueSplit is what a vendor has until one is chosen (the column's
// default).
const DefaultQueueSplit = "fair"

// MaxShortCodes is the most short codes one vendor may list.
const MaxShortCodes = 50

// shortCodeRE is what a short code may be: it becomes part of a queue's name
// (queues.Sub reads it the same way) and a MySQL value.
var shortCodeRE = regexp.MustCompile(`^[A-Za-z0-9+_-]{1,16}$`)

// CleanQueueSplit is the rule for vendors.queue_split: trimmed, any case,
// stored lower case, one of fair and priority. Empty is "not given" and comes
// back as "" (a new vendor then has fair). Anything else is refused, naming
// the field.
func CleanQueueSplit(s string) (string, error) {
	w := strings.ToLower(strings.TrimSpace(s))
	if w == "" {
		return "", nil
	}
	if !oneOf(w, QueueSplits()) {
		return "", refusef("queue_split: %q is not a way to share the rate — use fair (each queue with work an equal share) "+
			"or priority (priority 1 from all the vendor's queues first, then 2, and so on)", s)
	}
	return w, nil
}

// CleanShortCodes is the rule for vendors.short_codes: each entry trimmed and
// 1 to 16 characters of A–Z a–z 0–9 + _ -; the word common (any case) is
// refused, because it names the common queue; a repeat is dropped (the first
// kept, the order kept); at most MaxShortCodes. An empty list is none, which
// comes back as nil and is stored as NULL. A refusal names the field and the
// entry.
func CleanShortCodes(v []string) ([]string, error) {
	var out []string
	seen := map[string]bool{}
	for _, raw := range v {
		sc := strings.TrimSpace(raw)
		if !shortCodeRE.MatchString(sc) {
			return nil, refusef("short_codes: %q is not a short code — each is 1 to 16 characters of A–Z, a–z, 0–9, + _ and -", raw)
		}
		if strings.EqualFold(sc, queues.Common) {
			return nil, refusef("short_codes: %q cannot be a short code — it is the name of the vendor's common queue", raw)
		}
		if seen[sc] {
			continue
		}
		seen[sc] = true
		out = append(out, sc)
	}
	if len(out) > MaxShortCodes {
		return nil, refusef("short_codes: at most %d short codes — this list has %d", MaxShortCodes, len(out))
	}
	return out, nil
}

// ShortCodesFromText reads the console's typed list: short codes separated by
// commas, semicolons or spaces. It only splits; CleanShortCodes judges.
func ShortCodesFromText(s string) []string {
	return strings.FieldsFunc(s, func(r rune) bool {
		return r == ',' || r == ';' || r == ' ' || r == '\t' || r == '\n' || r == '\r'
	})
}

// ShortCodesWords is a vendor's short codes as the console shows them.
func ShortCodesWords(v []string) string {
	if len(v) == 0 {
		return "none — every message waits in the common queue"
	}
	return strings.Join(v, ", ")
}

// QueueSplitWords is a split mode in words.
func QueueSplitWords(mode string) string {
	if mode == "priority" {
		return "priority — priority 1 from all its queues first, then 2, and so on"
	}
	return "fair — each of its queues with work gets an equal share"
}

// hasVendorQueueColumns reports whether the config store has both columns of
// config/0161. A store the gateway has not upgraded yet has neither, and
// every vendor there is fair with no short codes.
func (s Store) hasVendorQueueColumns(ctx context.Context) bool {
	n, err := s.DB.VendorQueueColumns(ctx)
	return err == nil && n == 2
}

// vendorQueueSettings reads one vendor's split mode and short codes. A store
// without the columns answers the defaults, with ok=false.
func (s Store) vendorQueueSettings(ctx context.Context, id int64) (split string, codes []string, ok bool) {
	if !s.hasVendorQueueColumns(ctx) {
		return DefaultQueueSplit, nil, false
	}
	split, raw, err := s.DB.VendorQueueSettings(ctx, id)
	if err != nil {
		return DefaultQueueSplit, nil, false
	}
	if raw != "" && raw != "null" {
		_ = json.Unmarshal([]byte(raw), &codes)
	}
	return split, codes, true
}

// setVendorQueueSetting writes one of the two, by the shared rule. Short codes
// come as the console's text: "none" (any case) removes them all.
func (s Store) setVendorQueueSetting(ctx context.Context, id int64, key, value string) error {
	if !s.hasVendorQueueColumns(ctx) {
		return refusef("the gateway has to be upgraded before a vendor's %s can be set (config/0161): start the new build once, then try again", key)
	}
	switch key {
	case "queue_split":
		w, err := CleanQueueSplit(value)
		if err != nil {
			return err
		}
		if w == "" {
			return refusef("queue_split: choose fair or priority")
		}
		if err := s.DB.SetVendorQueueSplit(ctx, id, w); err != nil {
			return fmt.Errorf("update vendor %d: %w", id, err)
		}
	case "short_codes":
		var list []string
		switch t := strings.TrimSpace(value); {
		case t == "":
			return refusef("short_codes: give the short codes, separated by commas (e.g. 8585, 3344), or none to remove them all")
		case strings.EqualFold(t, "none"):
		default:
			list = ShortCodesFromText(t)
		}
		codes, err := CleanShortCodes(list)
		if err != nil {
			return err
		}
		var stored dal.NullString // NULL for none, as the Admin API stores it
		if len(codes) > 0 {
			b, _ := json.Marshal(codes)
			stored = dal.NullString{String: string(b), Valid: true}
		}
		if err := s.DB.SetVendorShortCodes(ctx, id, stored); err != nil {
			return fmt.Errorf("update vendor %d: %w", id, err)
		}
	}
	return nil
}
