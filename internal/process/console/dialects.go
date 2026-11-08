package console

import (
	"bufio"
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/provision"
)

// The dialects screen: a customer's own field names, so they can move to this
// gateway without changing their code. [SCOPE P14-DIALECT]
//
// Built by asking "what does this customer call the destination number?" for
// each value in turn — never by describing a mapping in JSON.

func (p *Process) dialectsMenu(ctx context.Context, sc *bufio.Scanner) {
	for {
		p.printf("\n  Dialects\n")
		p.printf("  A customer's own field names and reply shape, so their existing code\n")
		p.printf("  keeps working. Everything downstream still sees our own fields.\n")
		// A vendor's DLR / MO dialect has the same model since 2026-10-08,
		// but this screen does not ask its words, and provision refuses to
		// write one (provision.ValidateDialect) — so say where they are made.
		p.printf("  These are the dialects of a client's API (MT). How a vendor's receipts\n")
		p.printf("  and incoming messages are read (DLR and MO dialects) is set on the\n")
		p.printf("  portal's Dialects screen; their own URL can be set here too (4).\n\n")
		opts := []opt{}
		opts = append(opts, item("1", "Add a dialect"))
		opts = append(opts, item("2", "List dialects"))
		opts = append(opts, item("3", "Give an account a dialect"))
		opts = append(opts, item("4", "A vendor dialect's own URL (DLR / MO)"))
		opts = append(opts, item("0", "Back"))
		choice, ok := p.choose(sc, "Choose", "", opts)
		if !ok {
			return
		}
		switch choice {
		case "1":
			p.addDialect(ctx, sc)
		case "2":
			p.listDialects(ctx, sc)
		case "3":
			p.assignDialect(ctx, sc)
		case "4":
			p.vendorDialectURL(ctx, sc)
		case "0", "":
			return
		default:
			p.errf("  %q is not one of the choices.\n", choice)
		}
	}
}

func (p *Process) addDialect(ctx context.Context, sc *bufio.Scanner) {
	var d provision.Dialect
	var ok bool

	p.printf("\n  Add a dialect.\n\n")
	if d.Name, ok = p.ask(sc, "A name for it, e.g. \"Jazz XML\""); !ok || d.Name == "0" || d.Name == "" {
		return
	}
	p.printf("\n  Does this customer post to their own address? Their old gateway's path,\n")
	p.printf("  like /api/v1/send. Enter on its own to use ours (/sms/v4/send).\n")
	if d.URL, ok = p.ask(sc, "Their submit path"); !ok {
		return
	}
	if d.RequestFormat, ok = p.askChoice(sc, "How do their requests arrive", provision.RequestFormats(), "json",
		"query = in the web address; form/json/xml = in the body"); !ok {
		return
	}

	// ── what they send us ──
	p.printf("\n  What does this customer call each value they send?\n")
	p.printf("  Enter on its own to skip one they do not send.\n\n")
	for _, f := range provision.RequestFields() {
		theirs, ok := p.askName(sc, "   "+f.Key+" — "+f.What+"\n   they call it (Enter to skip, 0 to abandon)")
		if !ok {
			return
		}
		if strings.TrimSpace(theirs) == "" {
			continue
		}
		m := provision.DialectMapping{Ours: f.Key, Theirs: strings.TrimSpace(theirs)}
		// A value that also needs translating, e.g. their "1" means ucs2.
		if f.Key == "encoding" || f.Key == "priority" {
			if p.askYes(sc, "   do their values differ from ours too", false) {
				m.Values = map[string]string{}
				for {
					their, _ := p.ask(sc, "     their value (Enter when done)")
					if strings.TrimSpace(their) == "" {
						break
					}
					our, _ := p.ask(sc, "     means")
					if strings.TrimSpace(our) != "" {
						m.Values[their] = our
					}
				}
			}
		}
		d.Request = append(d.Request, m)
		p.printf("   ✓ %s ← %s\n\n", f.Key, m.Theirs)
	}

	// ── what we send back ──
	if d.ResponseFormat, ok = p.askChoice(sc, "How should replies be written", provision.ResponseFormats(), "json", ""); !ok {
		return
	}
	p.printf("\n  What should each value in the reply be called?\n")
	p.printf("  Enter on its own to leave one out of the reply.\n\n")
	for _, f := range provision.ResponseFields() {
		theirs, ok := p.askName(sc, "   "+f.Key+" — "+f.What+"\n   call it (Enter to leave out, 0 to abandon)")
		if !ok {
			return
		}
		if strings.TrimSpace(theirs) == "" {
			continue
		}
		d.Response = append(d.Response, provision.DialectMapping{Ours: f.Key, Theirs: strings.TrimSpace(theirs)})
		p.printf("   ✓ %s → %s\n\n", f.Key, strings.TrimSpace(theirs))
	}

	if err := provision.ValidateDialect(d); err != nil {
		p.refusal(err)
		return
	}
	id, err := p.store.CreateDialect(ctx, d)
	if err != nil {
		p.refusal(err)
		return
	}
	p.okf("\n  Created dialect #%d %q with %d incoming and %d outgoing names.\n",
		id, d.Name, len(d.Request), len(d.Response))
	p.warnf("  It does nothing until an account is given it — menu option 3.\n")
}

func (p *Process) listDialects(ctx context.Context, sc *bufio.Scanner) {
	rows, err := p.store.ListDialects(ctx, 200)
	if err != nil {
		p.errf("  %v\n", err)
		return
	}
	if len(rows) == 0 {
		p.printf("\n  No dialects yet. Every account uses our own field names.\n")
		return
	}
	p.printf("\n   %-6s %-28s %-22s %-8s %-9s %s\n", "id", "name", "submit path", "format", "mappings", "accounts")
	list := []opt{}
	for _, r := range rows {
		list = append(list, row(r.ID, fmt.Sprintf("   %-6d %-28s %-22s %-8s %-9d %d",
			r.ID, trim(r.Name, 28), trim(r.URL, 22), r.Format, r.Mappings, r.Accounts), ""))
	}
	// Opens the row, as every other list does (bug 5).
	id, ok := p.chooseID(sc, "Open a dialect by id (0 to go back)", list)
	if !ok || id == 0 {
		return
	}
	var d provision.DialectRow
	for _, r := range rows {
		if r.ID == id {
			d = r
		}
	}
	if d.ID == 0 {
		p.errf("  There is no dialect #%d in this list.\n", id)
		return
	}
	p.printf("\n  Dialect #%d %q\n", d.ID, d.Name)
	p.printf("   %-12s %s\n", "Submit path", orDash(d.URL))
	p.printf("   %-12s %s\n", "Format", orDash(d.Format))
	p.printf("   %-12s %d\n", "Mappings", d.Mappings)
	p.printf("   %-12s %d\n\n", "Accounts", d.Accounts)
	choice, ok := p.choose(sc, "Choose", "", []opt{
		item("1", "Delete — permanent"),
		item("0", "Back"),
	})
	if !ok || choice != "1" {
		return
	}
	if d.Accounts > 0 {
		p.errf("\n  %d account(s) still use this dialect, so it cannot be deleted. Give them\n", d.Accounts)
		p.errf("  another dialect first (menu option 3).\n")
		return
	}
	if !p.confirm(sc, strconv.FormatInt(id, 10)) {
		p.printf("  Left alone.\n")
		return
	}
	if err := p.store.DeleteDialect(ctx, id); err != nil {
		p.refusal(err)
		return
	}
	p.okf("  Deleted dialect #%d.\n", id)
}

func (p *Process) assignDialect(ctx context.Context, sc *bufio.Scanner) {
	accounts, err := p.store.ListAccounts(ctx, 200)
	if err != nil {
		p.errf("  %v\n", err)
		return
	}
	p.printf("\n  Which account?\n")
	aid, ok := p.chooseID(sc, "Account id", accountRows(accounts))
	if !ok || aid == 0 {
		return
	}
	dialects, err := p.store.ListDialects(ctx, 200)
	if err != nil {
		p.errf("  %v\n", err)
		return
	}
	p.printf("\n  Which dialect?\n")
	shapes := []opt{}
	for _, d := range dialects {
		shapes = append(shapes, row(d.ID, fmt.Sprintf("   %4d  %-28s %s", d.ID, trim(d.Name, 28), trim(d.URL, 24)), ""))
	}
	shapes = append(shapes, opt{Key: "0", Text: "      0  none — our own field names"})
	did, back, ok := p.chooseWithZero(sc, "Dialect id", shapes)
	if !ok || back {
		return
	}
	if err := p.store.AssignDialect(ctx, aid, did); err != nil {
		p.refusal(err)
		return
	}
	p.okf("  Done. The account uses it within 30 seconds.\n")
}

// vendorDialectURL gives a DLR or MO dialect its own URL, or takes it away
// (27 Oct, Saleem: "dialects for mo and dlr will also have custom urls,
// similar to MT one"). Every vendor whose door reads with the dialect may post
// to it; the credentials it sends say which door it is. The rule is
// provision.SetVendorDialectURL, the Admin API's twin.
func (p *Process) vendorDialectURL(ctx context.Context, sc *bufio.Scanner) {
	list, err := p.store.ListVendorDialects(ctx)
	if err != nil {
		p.refusal(err)
		return
	}
	if len(list) == 0 {
		p.printf("\n  There is no DLR or MO dialect yet. They are made on the portal's Dialects screen.\n")
		return
	}
	p.printf("\n  A DLR or MO dialect may have a URL of its own. Each vendor whose door reads\n")
	p.printf("  with it may post there; the token, username or JWT it sends says which door\n")
	p.printf("  it is, and a caller whose credentials name none of them is refused (403).\n")
	p.printf("  Each door still answers on /dlr/<vendor> or /mo/<vendor> and its own path.\n")
	opts := []opt{}
	for i, d := range list {
		own := d.URL
		if own == "" {
			own = "none"
		}
		opts = append(opts, item(strconv.Itoa(i+1), fmt.Sprintf("%-3s #%d %s   doors: %d   its URL: %s",
			strings.ToUpper(d.Kind), d.ID, d.Name, d.Doors, own)))
	}
	opts = append(opts, item("0", "Back"))
	choice, ok := p.choose(sc, "Which dialect", "", opts)
	n, _ := strconv.Atoi(choice)
	if !ok || n < 1 || n > len(list) {
		return
	}
	d := list[n-1]
	val, ok := p.ask(sc, "  Its own URL, e.g. /acme/receipts (Enter keeps it, none takes it away)")
	if !ok || val == "" || val == "0" {
		p.printf("  Nothing changed.\n")
		return
	}
	if strings.EqualFold(val, "none") {
		val = ""
	}
	got, err := p.store.SetVendorDialectURL(ctx, d.ID, val)
	if err != nil {
		p.refusal(err)
		return
	}
	if got == "" {
		p.okf("  The %s dialect %q has no URL of its own now.\n", strings.ToUpper(d.Kind), d.Name)
		return
	}
	p.okf("  Vendors whose %s door reads with %q may post to %s. A running gateway picks it up within 30 seconds.\n",
		strings.ToUpper(d.Kind), d.Name, got)
	if d.Doors == 0 {
		p.warnf("  No door reads with this dialect yet, so the URL answers 404 until one does.\n")
	}
}
