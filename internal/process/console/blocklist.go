package console

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/provision"
)

// Blocked numbers. [SCOPE P14-BLOCK] The rulebook is provision/blocklist.go,
// the Admin API's rules for the blocklist table; this file only asks.

func (p *Process) blocklistMenu(ctx context.Context, sc *bufio.Scanner) {
	for {
		_, total, _ := p.store.ListBlocks(ctx, provision.BlockFilter{Limit: 1})
		p.printf("\n  Blocked numbers (%d)\n", total)
		p.printf("  A message to a blocked number is refused (403 DESTINATION_BLOCKED).\n\n")
		choice, ok := p.choose(sc, "Choose", "", []opt{
			item("1", "Block a number"),
			item("2", "Block many numbers — typed, or from a file"),
			item("3", "Is a number blocked?"),
			item("4", "List — the newest, or by number"),
			item("0", "Back"),
		})
		if !ok {
			return
		}
		switch choice {
		case "1":
			p.addBlock(ctx, sc)
		case "2":
			p.addBlocks(ctx, sc)
		case "3":
			p.checkBlocked(ctx, sc)
		case "4":
			p.listBlocks(ctx, sc)
		case "0", "":
			return
		default:
			p.errf("  %q is not one of the choices.\n", choice)
		}
	}
}

// askBlockTerms asks everything but the number: scope, sender, source,
// reason, end. ok=false means abandon.
func (p *Process) askBlockTerms(ctx context.Context, sc *bufio.Scanner) (provision.Block, bool) {
	var b provision.Block
	p.printf("\n  Blocked for whom?\n")
	scope, ok := p.choose(sc, "Choose", "1", []opt{
		item("1", "Every account"),
		item("2", "Every account of one company"),
		item("3", "One account"),
	})
	if !ok || scope == "0" {
		return b, false
	}
	switch scope {
	case "2":
		companies, err := p.store.ListCompanies(ctx, 200)
		if err != nil {
			p.refusal(err)
			return b, false
		}
		p.printf("\n  Companies\n")
		id, ok := p.chooseID(sc, "Company id", companyRows(companies))
		if !ok || id == 0 {
			return b, false
		}
		b.CompanyID = id
	case "3":
		accounts, err := p.store.ListAccounts(ctx, 500)
		if err != nil {
			p.refusal(err)
			return b, false
		}
		p.printf("\n  Accounts\n")
		id, ok := p.chooseID(sc, "Account id", accountRows(accounts))
		if !ok || id == 0 {
			return b, false
		}
		b.AccountID = id
	}

	p.printf("\n  Only from one sender? Exactly as the sender is written, e.g. MyBank.\n")
	sender, ok := p.ask(sc, "Sender (Enter = every sender)")
	if !ok || sender == "0" {
		return b, false
	}
	b.Sender = strings.TrimSpace(sender)

	p.printf("\n  Why is it blocked?\n")
	sources := provision.BlockSources()
	if b.AccountID != 0 || b.CompanyID != 0 {
		sources = sources[:3] // a trap number must block every account
	}
	for _, s := range sources {
		p.printf("    %-9s %s\n", s, provision.BlockSourceWhat(s))
	}
	if b.Source, ok = p.askChoice(sc, "Source", sources, "manual", ""); !ok {
		return b, false
	}
	reason, ok := p.ask(sc, "Note, e.g. the complaint ticket (Enter = none)")
	if !ok || reason == "0" {
		return b, false
	}
	b.Reason = strings.TrimSpace(reason)

	p.printf("\n  Until when? Enter on its own means for good.\n")
	until, ok := p.askWhen(sc, "Ends (YYYY-MM-DD, or YYYY-MM-DD HH:MM, UTC)")
	if !ok {
		return b, false
	}
	b.Expires = until
	return b, true
}

func (p *Process) addBlock(ctx context.Context, sc *bufio.Scanner) {
	p.printf("\n  Block a number. +, spaces and brackets are fine. 0 to abandon.\n")
	var num string
	for {
		v, ok := p.ask(sc, "Number")
		if !ok || v == "0" || v == "" {
			return
		}
		n, err := provision.CleanNumber(v)
		if err != nil {
			p.refusal(err)
			continue
		}
		if n != v {
			p.printf("  Stored as %s — the form the gateway matches.\n", n)
		}
		num = n
		break
	}
	b, ok := p.askBlockTerms(ctx, sc)
	if !ok {
		return
	}
	b.Number = num
	id, err := p.store.AddBlock(ctx, b)
	if err != nil {
		p.refusal(err)
		if id == 0 {
			return
		}
	}
	p.okf("\n  Blocked %s (entry #%d). A running gateway refuses it within 30 seconds.\n", num, id)
}

func (p *Process) addBlocks(ctx context.Context, sc *bufio.Scanner) {
	p.printf("\n  Block many numbers. Give a file — one number per line; anything after a\n")
	p.printf("  comma on a line is ignored, as in the portal's import — or press Enter to\n")
	p.printf("  type them, separated by commas.\n")
	path, ok := p.ask(sc, "File (Enter = type them)")
	if !ok || path == "0" {
		return
	}
	var lines []string
	if path = strings.TrimSpace(path); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			p.errf("  Cannot read %s: %v\n", path, err)
			return
		}
		for _, ln := range strings.Split(string(data), "\n") {
			first, _, _ := strings.Cut(ln, ",")
			first, _, _ = strings.Cut(first, ";")
			first, _, _ = strings.Cut(first, "\t")
			lines = append(lines, strings.TrimSpace(first))
		}
	} else {
		typed, ok := p.ask(sc, "Numbers")
		if !ok || typed == "0" || strings.TrimSpace(typed) == "" {
			return
		}
		for _, t := range strings.FieldsFunc(typed, func(r rune) bool { return r == ',' || r == ';' }) {
			lines = append(lines, strings.TrimSpace(t))
		}
	}
	tmpl, ok := p.askBlockTerms(ctx, sc)
	if !ok {
		return
	}
	res, err := p.store.AddBlocks(ctx, lines, tmpl)
	if err != nil {
		for i, b := range res.Bad {
			if i == 10 {
				p.printf("    … and %d more\n", len(res.Bad)-10)
				break
			}
			p.errf("    %s\n", b)
		}
		p.refusal(err)
		if res.Added == 0 {
			return
		}
	}
	p.okf("\n  Blocked %d number(s); %d were already blocked for this scope and were left.\n",
		res.Added, res.AlreadyBlocked)
	p.printf("  A running gateway refuses them within 30 seconds.\n")
}

func (p *Process) checkBlocked(ctx context.Context, sc *bufio.Scanner) {
	v, ok := p.ask(sc, "Number")
	if !ok || v == "0" || v == "" {
		return
	}
	blocks, err := p.store.BlocksFor(ctx, v)
	if err != nil {
		p.refusal(err)
		return
	}
	now := time.Now()
	active := 0
	for _, b := range blocks {
		if b.Active(now) {
			active++
		}
	}
	n, _ := provision.CleanNumber(v)
	switch {
	case len(blocks) == 0:
		p.printf("\n  %s is not blocked for anyone.\n", n)
		return
	case active == 0:
		p.warnf("\n  %s is NOT blocked now — every entry for it is off or has ended:\n", n)
	default:
		p.errf("\n  %s is blocked by %d entr%s:\n", n, active, plural(active, "y", "ies"))
	}
	p.printBlocks(blocks)
	if active > 1 {
		p.warnf("\n  It stays blocked until every one of them is removed or switched off.\n")
	}
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

func (p *Process) printBlocks(blocks []provision.Block) {
	now := time.Now()
	p.printf("   %-6s %-15s %-24s %-10s %-9s %s\n", "id", "number", "for", "sender", "source", "state")
	for _, b := range blocks {
		p.printf("   %-6d %-15s %-24s %-10s %-9s %s\n", b.ID, b.Number, trim(b.Scope, 24),
			trim(orDefault(b.Sender, "any"), 10), b.Source, blockState(b, now))
	}
}

func blockState(b provision.Block, now time.Time) string {
	switch {
	case b.Disabled:
		return "off"
	case !b.Expires.IsZero() && !b.Expires.After(now):
		return "ended " + b.Expires.UTC().Format("2006-01-02")
	case !b.Expires.IsZero():
		return "until " + b.Expires.UTC().Format("2006-01-02 15:04")
	}
	return "blocked"
}

func (p *Process) listBlocks(ctx context.Context, sc *bufio.Scanner) {
	start, ok := p.ask(sc, "Numbers starting with (Enter = the newest 50)")
	if !ok || start == "0" {
		return
	}
	blocks, total, err := p.store.ListBlocks(ctx, provision.BlockFilter{Number: strings.TrimSpace(start)})
	if err != nil {
		p.refusal(err)
		return
	}
	if len(blocks) == 0 {
		p.printf("\n  None.\n")
		return
	}
	p.printf("\n  %d shown of %d, newest first. All times UTC.\n\n", len(blocks), total)
	now := time.Now()
	list := []opt{}
	for _, b := range blocks {
		list = append(list, row(b.ID, fmt.Sprintf("   %-6d %-15s %-24s %-10s %-9s %s", b.ID, b.Number,
			trim(b.Scope, 24), trim(orDefault(b.Sender, "any"), 10), b.Source, blockState(b, now)), ""))
	}
	p.printf("   %-6s %-15s %-24s %-10s %-9s %s\n", "id", "number", "for", "sender", "source", "state")
	id, ok := p.chooseID(sc, "Open an entry by id (0 to go back)", list)
	if !ok || id == 0 {
		return
	}
	p.blockMenu(ctx, sc, id)
}

func (p *Process) blockMenu(ctx context.Context, sc *bufio.Scanner, id int64) {
	for {
		b, err := p.store.GetBlock(ctx, id)
		if err != nil {
			p.refusal(err)
			return
		}
		p.printf("\n  Entry #%d — %s, blocked for %s\n", b.ID, b.Number, b.Scope)
		p.printf("   %-10s %s\n", "Sender", orDefault(b.Sender, "any sender"))
		p.printf("   %-10s %s — %s\n", "Source", b.Source, provision.BlockSourceWhat(b.Source))
		p.printf("   %-10s %s\n", "Note", orDash(b.Reason))
		p.printf("   %-10s %s\n", "State", blockState(b, time.Now()))
		p.printf("   %-10s %s\n\n", "Added", orDash(b.Created))
		toggle := "Switch off — kept, but no longer enforced"
		if b.Disabled {
			toggle = "Switch on"
		}
		choice, ok := p.choose(sc, "Choose", "", []opt{
			item("1", toggle),
			item("2", "Change when it ends"),
			item("3", "Delete — the number can be sent to again"),
			item("0", "Back"),
		})
		if !ok {
			return
		}
		switch choice {
		case "1":
			if err := p.store.SetBlockDisabled(ctx, id, !b.Disabled); err != nil {
				p.refusal(err)
				continue
			}
			p.okf("  Done. A running gateway acts on it within 30 seconds.\n")
		case "2":
			p.printf("\n  Enter on its own means for good.\n")
			until, ok := p.askWhen(sc, "Ends (YYYY-MM-DD, or YYYY-MM-DD HH:MM, UTC)")
			if !ok {
				continue
			}
			if err := p.store.SetBlockExpiry(ctx, id, until); err != nil {
				p.refusal(err)
				continue
			}
			p.okf("  Done.\n")
		case "3":
			if !p.confirm(sc, b.Number) {
				p.printf("  Left alone.\n")
				continue
			}
			if err := p.store.DeleteBlock(ctx, id); err != nil {
				p.refusal(err)
				continue
			}
			p.okf("  Deleted entry #%d. Check the number again: another entry may still block it.\n", id)
			return
		case "0", "":
			return
		default:
			p.errf("  %q is not one of the choices.\n", choice)
		}
	}
}
