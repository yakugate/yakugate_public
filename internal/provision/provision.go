// Package provision is the rulebook for writing configuration — accounts,
// vendors and routing rules — into the config store. [SCOPE P14]
//
// # WHY THIS EXISTS
//
// The gateway runs without the Admin API (PO 2026-09-18). It could not be
// CONFIGURED without it: every /admin endpoint the engine serves is a tool or
// a read, so a gateway-only install could start and then do nothing, because
// the only account it had was the seeded one. Adding a customer meant writing
// SQL by hand.
//
// # THE RULES HAVE TO COME WITH THE DOOR
//
// The portal's door has a lock on it: the Admin API refuses a vendor whose
// send call states no success rule, a cid that would change after creation, a
// buy rate card used as a customer's price. A console that wrote rows straight
// into MySQL would be a door with no lock, and the customer with no portal is
// exactly the one with no second chance to catch the mistake.
//
// This box shows what that costs. `testvendor` was saved before the success
// rule existed; the vendor role has refused to load it ever since, and 703
// messages sat in its queue, accepted and never sent, with every client told
// "200 ACCEPTED".
//
// So the checks live here, in Go, beside the engine that has to honour them —
// and the console, a future flag-driven command and (in time) the Admin API
// itself all ask the same code the same question.
//
// # WHAT THIS PACKAGE IS NOT
//
// It is not a second schema. It writes the same tables the portal writes, in
// the same shapes; one database, two doors. Nothing here belongs on the hot
// path: it runs from a command an operator typed.
package provision

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
)

// ErrRefused is what a caller shows the operator: a rule said no, and the
// message says which and why. Anything else from this package is a fault —
// the store was unreachable, a query broke — and is reported as such.
var ErrRefused = errors.New("refused")

// refusef builds an ErrRefused carrying a sentence written for a person who
// is looking at a terminal, not a stack trace.
func refusef(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrRefused, fmt.Sprintf(format, a...))
}

// Refused reports whether err is a rule refusing, rather than something broken.
func Refused(err error) bool { return errors.Is(err, ErrRefused) }

// Reason strips the wrapper so a caller can print the sentence on its own.
func Reason(err error) string {
	s := err.Error()
	if i := strings.Index(s, "refused: "); i >= 0 {
		return s[i+len("refused: "):]
	}
	return s
}

// Store is the config store this package writes to, plus the queue store,
// which a vendor needs because creating one creates its queue table.
//
// Two handles rather than one: the config and queue stores are separate
// databases in every deployment shape the gateway supports, and a vendor
// write touches both.
type Store struct {
	// DB is the config store (internal/dal): every read and write of this
	// package goes through it.
	DB dal.ConfigStore
	// Queues is the queue store's vendor tables; nil when the queue store
	// is not open (or not MySQL).
	Queues dal.VendorQueueTables
	// Live is the live balance counters, when Redis is on. A balance change
	// must reach them too, or the reserver's write-back erases the change
	// (see AdjustBalance). nil = Redis is not configured.
	Live dal.LiveBalances
}

// Ping fails early and in plain words, so the console can say which store is
// unreachable before an operator has typed anything.
func (s Store) Ping(ctx context.Context) error {
	if s.DB == nil {
		return errors.New("no config store: set YAKU_CONFIG_DSN")
	}
	if err := s.DB.Ping(ctx); err != nil {
		return fmt.Errorf("config store unreachable: %w", err)
	}
	if s.Queues != nil {
		if err := s.Queues.Ping(ctx); err != nil {
			return fmt.Errorf("queue store unreachable: %w", err)
		}
	}
	return nil
}

// Field is one editable setting, described so a caller can render a menu for
// it and check an answer without knowing what the setting means.
//
// Held here rather than in the console because WHAT may be changed, and what
// counts as a good value, is a rule — the same rule a flag-driven command and
// the Admin API have to follow. The console only draws it.
type Field struct {
	Key     string   // the name used with Update
	Label   string   // what an operator sees
	Hint    string   // why it matters, or what the values mean
	Kind    string   // text | int | bool | choice | json
	Choices []string // for Kind == "choice"
}

// FixedField explains why something cannot be changed, so a menu can say so
// instead of silently leaving it out — an operator looking for a setting that
// is not there assumes the console is incomplete.
type FixedField struct {
	Key   string // optional: the key its current value is read by (CompanyValue)
	Label string
	Why   string
}
