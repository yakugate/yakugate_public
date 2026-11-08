package console

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"unicode/utf8"

	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/provision"
)

// Run is the console. [SCOPE P14-01]
//
// INPUT   an operator at a terminal, choosing numbered options.
// LOGIC   each choice either shows something from the config store or asks the
//
//	questions one setting needs, checking every answer as it is given.
//
// OUTPUT  rows in the config store — the same rows the portal writes. A
//
//	running gateway picks them up within 30 seconds; nothing restarts.
//
// Every refusal is a sentence, not a code: this is the only surface a
// gateway-only install has, so "why not" has to be readable where it happens.
func (p *Process) Run(ctx context.Context) error {
	// A test hands the stores in directly and drives the menus with scripted
	// answers; a real run opens its own. Without this seam the screens could
	// only ever be checked by a person typing, which is how a menu quietly
	// stops working.
	sc := bufio.NewScanner(p.in)
	sc.Buffer(make([]byte, 4096), 1<<20)
	p.pickMode()
	// The console password (auth.go), before anything is shown — and before
	// the database setup below, so nobody rewrites the settings without it.
	if p.requireLogin && !p.login(sc) {
		return nil
	}
	if p.store.DB == nil {
		defer p.close()
		err := p.connect()
		if err == nil {
			err = p.store.Ping(ctx)
		}
		// A console that cannot reach its databases opens on the setup
		// screen instead of exiting (Saleem 2026-10-02): settings are what
		// the console is for. Only a non-MySQL config store is beyond it.
		if err != nil {
			if p.cfg.ConfigBackend != config.BackendMySQL {
				return err
			}
			if !p.setupStores(ctx, sc, err) {
				p.bye()
				return nil
			}
		}
	} else if err := p.store.Ping(ctx); err != nil {
		return err
	}

	p.printf("%s", p.banner())
	for {
		choice, ok := p.choose(sc, "Choose", "", p.mainMenu(ctx))
		if !ok {
			p.bye()
			return nil // the terminal closed: leaving is the right answer
		}
		switch choice {
		case "1":
			p.accountsMenu(ctx, sc)
		case "2":
			p.vendorsMenu(ctx, sc)
		case "3":
			p.routesMenu(ctx, sc)
		case "4":
			p.endpointsMenu(ctx, sc)
		case "5":
			p.dialectsMenu(ctx, sc)
		case "6":
			p.rateCardsMenu(ctx, sc)
		case "7":
			p.companiesMenu(ctx, sc)
		case "8":
			p.spamMenu(ctx, sc)
		case "9":
			p.blocklistMenu(ctx, sc)
		case "10":
			p.operatorsMenu(ctx, sc)
		case "g", "G":
			// Processes, Monitor and Settings in one place (Saleem 2026-09-29).
			if !p.systemMenu(ctx, sc) {
				p.bye()
				return nil // Ctrl-C inside the live monitor leaves the console
			}
		case "m", "M":
			// Live, full screen and refreshing on a terminal (PO 2026-09-21:
			// "like htop") — in numbered menus too (Saleem 2026-09-29: the
			// view should not depend on how menus are chosen); the static
			// screen only for piped input.
			if p.tty != nil {
				if !p.liveMonitor(ctx, sc) {
					p.bye()
					return nil // Ctrl-C inside the monitor leaves the console
				}
				break
			}
			p.monitoringMenu(ctx, sc)
		case "s", "S":
			p.settingsMenu(sc)
		case "c", "C":
			if p.tty == nil {
				p.errf("  Cursor menus need a terminal; this input is not one.\n\n")
				break
			}
			p.cursor = !p.cursor
			if p.cursor {
				p.printf("\n  Cursor menus: ↑↓ to move, Enter to choose, Esc to go back.\n")
			} else {
				p.printf("\n  Numbered menus: type the number, then Enter.\n")
			}
		case "0", "q", "quit", "exit":
			p.bye()
			return nil
		case "":
			// Enter on its own redraws, which is what people press when lost.
		default:
			p.errf("  %q is not one of the choices.\n\n", choice)
		}
	}
}

// salesLine is the offer under the logo. On a real terminal it stands out —
// bold yellow, the address underlined — so it reads as an offer, not as more
// of the banner; piped output (scripts, tests, a log) gets the plain text,
// and NO_COLOR (no-color.org) turns the colour off. (PO request 2026-09-21.)
func (p *Process) salesLine() string {
	const lead, addr = "Prefer a web portal to this console? Contact ", "sales@yakugate.com"
	if p.tty == nil || os.Getenv("NO_COLOR") != "" {
		return "  " + lead + addr
	}
	return "  \x1b[1;33m" + lead + "\x1b[4m" + addr + escReset
}

// creditLine is the team's signature under the banner (Saleem 2026-09-29):
// a soft rule either side, the heart in bold red, the team name in bold
// magenta. Piped output and NO_COLOR get the same words plain.
func (p *Process) creditLine() string {
	const (
		rule  = "──────"
		star  = "✦"
		heart = "❤"
	)
	if p.tty == nil || os.Getenv("NO_COLOR") != "" {
		return "  " + rule + " " + star + "  Developed with " + heart + " by Team YakuGate  " + star + " " + rule
	}
	const (
		dim     = "\x1b[2m"
		yellow  = "\x1b[33m"
		redBold = "\x1b[1;31m"
		magBold = "\x1b[1;35m"
		italic  = "\x1b[3m"
	)
	return "  " + dim + rule + escReset + " " + yellow + star + escReset +
		"  " + italic + "Developed with " + escReset + redBold + heart + escReset +
		italic + " by " + escReset + magBold + "Team YakuGate" + escReset +
		"  " + yellow + star + escReset + " " + dim + rule + escReset
}

func (p *Process) mainMenu(ctx context.Context) []opt {
	accounts := p.count(ctx, "accounts")
	companies := p.count(ctx, "companies")
	vendors := p.count(ctx, "vendors")
	calls := p.count(ctx, "calls")
	// A plan is an account's outgoing rules (Saleem 2026-09-30).
	plans := p.count(ctx, "plans")
	dialects := p.count(ctx, "dialects")
	cards := p.count(ctx, "cards")
	spam := p.count(ctx, "spam")
	blocked := p.count(ctx, "blocked")
	operators := p.count(ctx, "operators")
	p.printf("\n%s\n", p.headerLine())
	p.printf("  %s\n\n", p.storeLine())
	// Every entry: its name and count, then what it is for, the descriptions
	// all starting in one column (PO 2026-09-21).
	// Three columns (PO 2026-09-21): the name, how many there are (right-
	// aligned, in its own column; blank where there is nothing to count),
	// and what it is for.
	type entry struct{ key, name, count, what string }
	entries := []entry{
		{"1", "Accounts", accounts, "the customer logins that send messages"},
		{"2", "Vendors", vendors, "the carriers messages are sent through"},
		{"3", "Routing plans", plans, "each account's rules: which vendor a message goes to"},
		{"4", "HTTP calls", calls, "vendor calls and customer webhooks"},
		{"5", "Dialects", dialects, "a customer's own field names"},
		{"6", "Rate cards", cards, "what is charged, and what it costs"},
		{"7", "Companies", companies, "who owns accounts, balances and limits"},
		{"8", "Spam rules", spam, "words a message may not contain"},
		{"9", "Blocked numbers", blocked, "numbers nobody may send to"},
		{"10", "Operators", operators, "which network a number belongs to"},

		// One entry for running the gateway itself (Saleem 2026-09-29); m and
		// s still open the monitor and the settings straight from here.
		{"g", "Gateway", "", "processes, live monitor and settings (yakugate.config)"},
	}
	// The switch between the two ways of choosing, only where both work.
	if p.tty != nil {
		what := "switch to numbered menus (type the number, then Enter)"
		if !p.cursor {
			what = "switch to cursor menus (arrow keys and Enter)"
		}
		entries = append(entries, entry{"c", "Menus", "", what})
	}
	entries = append(entries, entry{"0", "Quit", "", "leave the console"})
	nameW, countW := 0, 0
	for i, e := range entries {
		if n, err := strconv.Atoi(e.count); err == nil {
			entries[i].count = provision.FormatCount(n) // 1424 → 1,424
		}
		nameW = max(nameW, utf8.RuneCountInString(e.name))
		countW = max(countW, utf8.RuneCountInString(entries[i].count))
	}
	opts := make([]opt, 0, len(entries))
	for _, e := range entries {
		opts = append(opts, wideItem(e.key, fmt.Sprintf("%-*s  %*s   %s", nameW, e.name, countW, e.count, e.what)))
	}
	return opts
}

var errNotImplemented = errors.New("not built yet")

var _ = errNotImplemented // vendors and routes land on this menu next

// bye is said on every way out of the main menu: 0, q, Ctrl-C, Esc, or the
// end of piped input (PO 2026-09-21).
func (p *Process) bye() {
	p.printf("\n  Bye.\n\n")
}
