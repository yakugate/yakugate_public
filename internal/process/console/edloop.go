package console

import (
	"bufio"
	"fmt"
	"sort"
	"strings"

	"github.com/yakugate/yakugate/internal/provision"
)

// The review-and-save loop every rule editor runs (Saleem 2026-09-30: "if
// any error, don't lose the data, ask the same again"). [SCOPE P14-ROUTE]
//
// The draft is shown with its parts; the operator saves it or changes a
// part. A refused save keeps everything: it says why, suggests the part the
// refusal is about, asks that part again, and saves again. The draft is
// dropped only when the operator chooses to discard it, and says so twice;
// a closed input ends the flow at once, without looping.

// edPart is one part of a draft that can be asked again.
type edPart struct {
	key   string // its menu key ("2".."9", "10"…)
	label string
	now   string      // what it is now, in a few words
	redo  func() bool // asks it again; false = backed out (or the input closed)
	hint  []string    // words in a refusal that point at this part
	rank  int         // the order hints are tried in (lower first)
}

// edLoop is one editor: its header, its parts and how it saves.
type edLoop struct {
	header    func()
	parts     func() []edPart
	saveText  string // "Create rule", "Save rule"
	leaveText string // "Discard this rule", "Leave without saving"
	save      func() error
	// leave says whether to leave (true) when the operator chooses 0; it
	// asks first when something would be lost.
	leave func() bool
	// unchanged: an edit with nothing to save (nil = always something).
	unchanged func() bool
	// snapshot keeps the draft before a part is asked again; calling what it
	// returns puts it back.
	snapshot func() func()
	// extra are menu lines that are not parts (a test): no retry after them.
	extra []edPart
	// refused is a refusal the draft already had before the editor opened
	// (a copy the rules turned down): the editor opens on the part it is
	// about, and saves again once that part is answered.
	refused error
}

// errDraft is a refusal the console makes itself, before the store sees the
// draft ("choose where these messages go").
func errDraft(format string, a ...any) error {
	return fmt.Errorf("%w: %s", provision.ErrRefused, fmt.Sprintf(format, a...))
}

// runEditor runs the loop. It returns true when the draft was saved.
func (p *Process) runEditor(sc *bufio.Scanner, l edLoop) bool {
	def := "1"
	pending, retry := false, false
	if l.refused != nil {
		def, retry = suggestKey(l.refused, l.parts()), true
	}
	for {
		if pending {
			pending = false
			err := l.save()
			if err == nil {
				return true
			}
			p.refusal(err)
			p.printf("\n  Nothing is lost. Change the part that was refused and it is tried again.\n")
			def, retry = suggestKey(err, l.parts()), true
			continue
		}
		l.header()
		parts := l.parts()
		opts := []opt{wideItem("1", l.saveText)}
		for _, pt := range parts {
			opts = append(opts, wideItem(pt.key, fmt.Sprintf("%-24s %s", pt.label, pt.now)))
		}
		for _, x := range l.extra {
			opts = append(opts, wideItem(x.key, x.label))
		}
		opts = append(opts, wideItem("0", l.leaveText))
		p.printf("\n")
		choice, ok := p.choose(sc, "Choose", def, opts)
		if !ok {
			return false // the input closed: nothing more can be asked
		}
		afterRefusal := retry
		def, retry = "1", false
		switch choice {
		case "1":
			if l.unchanged != nil && l.unchanged() {
				p.printf("  Nothing changed, so nothing was saved.\n")
				return false
			}
			pending = true
			continue
		case "0":
			if l.leave() || p.ended {
				return false
			}
			continue
		}
		var found *edPart
		for i := range parts {
			if parts[i].key == choice {
				found = &parts[i]
			}
		}
		if found == nil {
			for i := range l.extra {
				if l.extra[i].key == choice {
					l.extra[i].redo()
					found = &l.extra[i]
				}
			}
			if found == nil {
				p.errf("  %q is not one of the choices.\n", choice)
			}
			if p.ended {
				return false
			}
			continue
		}
		restore := l.snapshot()
		if !found.redo() {
			if p.ended {
				return false
			}
			restore() // left half-way: keep what was there
			p.warnf("  Kept as it was.\n")
			continue
		}
		if afterRefusal {
			pending = true // the refused part was asked again: try again
		}
	}
}

// suggestKey is the part a refusal is about, by the words it uses; "1"
// (save again) when none fits. Parts with a lower rank are tried first, so
// the most telling words win ("a vendor's buy price" is about Where to
// send, not about cards).
func suggestKey(err error, parts []edPart) string {
	low := strings.ToLower(provision.Reason(err))
	order := append([]edPart(nil), parts...)
	sort.SliceStable(order, func(i, j int) bool { return order[i].rank < order[j].rank })
	for _, pt := range order {
		for _, h := range pt.hint {
			if strings.Contains(low, h) {
				return pt.key
			}
		}
	}
	return "1"
}
