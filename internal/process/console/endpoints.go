package console

import (
	"bufio"
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/provision"
)

// The HTTP calls screen: a vendor's send, poll and login, and a customer's
// receipt and incoming-message webhooks. [SCOPE P14-ENDPOINT]
//
// NOTHING HERE ASKS FOR JSON. Every piece is a question with its own
// vocabulary offered alongside it, because the shape of an adapter profile is
// something an operator could only have learned from the portal — and this
// console exists for the installs that have no portal (PO 2026-09-18).

func (p *Process) endpointsMenu(ctx context.Context, sc *bufio.Scanner) {
	for {
		p.printf("\n  HTTP calls\n")
		p.printf("  Every call a vendor makes, and every webhook we push to a customer.\n\n")
		opts := []opt{}
		opts = append(opts, item("1", "Add a vendor call      (send, poll, login, receipt callback)"))
		opts = append(opts, item("2", "Add a customer webhook (receipts, incoming messages)"))
		opts = append(opts, item("3", "List calls"))
		opts = append(opts, item("0", "Back"))
		choice, ok := p.choose(sc, "Choose", "", opts)
		if !ok {
			return
		}
		switch choice {
		case "1":
			p.addVendorCall(ctx, sc)
		case "2":
			p.addCustomerWebhook(ctx, sc)
		case "3":
			p.listEndpoints(ctx, sc)
		case "0", "":
			return
		default:
			p.errf("  %q is not one of the choices.\n", choice)
		}
	}
}

// addVendorCall walks the whole call: where it goes, what the carrier calls
// each field, how we know it worked, and how to read the message id back.
func (p *Process) addVendorCall(ctx context.Context, sc *bufio.Scanner) {
	vendors, err := p.store.ListVendors(ctx, 200)
	if err != nil {
		p.errf("  %v\n", err)
		return
	}
	p.printf("\n  Which vendor is this call for?\n")
	list := []opt{}
	for _, v := range vendors {
		if v.Transport == "http" || v.Transport == "soap" {
			list = append(list, row(v.ID, fmt.Sprintf("   %4d  %-16s %s", v.ID, trim(v.CID, 16), v.Company), ""))
		}
	}
	// A call need not be made for a vendor (2026-10-12): one made for nobody
	// is picked later, by the vendor (its send call or poll call setting).
	list = append(list, opt{Key: "0", Text: "      0  no vendor yet — a vendor picks it later"})
	vid, back, ok := p.chooseWithZero(sc, "Vendor id", list)
	if !ok || back {
		return
	}
	p.buildVendorCall(ctx, sc, vid, "")
}

// buildVendorCall asks for one call. role empty = ask which kind; otherwise it
// is already decided, which is how creating an HTTP vendor walks straight into
// building its send call without the operator having to find this menu.
func (p *Process) buildVendorCall(ctx context.Context, sc *bufio.Scanner, vid int64, role string) int64 {
	if role == "" {
		p.printf("\n  What is this call for?\n")
		roles := provision.VendorRoles()
		ro := []opt{}
		for i, r := range roles {
			ro = append(ro, item(strconv.Itoa(i+1), fmt.Sprintf("%-8s %s", r.Name, r.What)))
		}
		pick, ok := p.choose(sc, "Choose", "1", ro)
		n, _ := strconv.ParseInt(pick, 10, 64)
		if !ok || n < 1 || int(n) > len(roles) {
			return 0
		}
		role = roles[n-1].Name
	}
	// Said before the questions, not after them: a call this vendor cannot
	// have (an SMPP vendor's, a SOAP vendor's send) is refused here.
	if err := p.store.EndpointVendorProblem(ctx, vid, role); err != nil {
		p.refusal(err)
		return 0
	}
	var ok bool

	var e provision.Endpoint
	e.Type, e.Name, e.VendorID = "vendor", role, vid

	if role == "webhook" {
		// Retired (issue 19, 2026-10-27): the vendor's receipt door reads its
		// receipts — set it up, and its dialect, in the portal.
		p.refusal(provision.ValidateEndpoint(e))
		return 0
	}

	if e.URL, ok = p.askName(sc, "URL the carrier gave you"); !ok {
		return 0
	}
	if e.Method, ok = p.askChoice(sc, "Method", []string{"GET", "POST", "PUT"}, "GET", ""); !ok {
		return 0
	}
	if e.Encoding, ok = p.askChoice(sc, "How are the fields carried", provision.Encodings(), "query",
		"query = in the web address; form/json/xml = in the body"); !ok {
		return 0
	}
	ms, ok := p.askInt(sc, "Give up after (milliseconds)", 15000)
	if !ok {
		return 0
	}
	e.TimeoutMS = int(ms)

	// ── the fields the carrier expects ──
	p.printf("\n  Now the fields this carrier expects. For each one: the name IT uses,\n")
	p.printf("  and what we put in it.\n")
	sources := provision.SourcesFor(role)
	for {
		alias, ok := p.askName(sc, "\n  Carrier's field name (Enter when there are no more, 0 to abandon)")
		if !ok {
			return 0
		}
		if alias == "" {
			break
		}
		var pr provision.Param
		pr.Alias = alias
		if len(sources) > 0 {
			p.printf("  What goes in %q?\n", alias)
			what := []opt{}
			for i, s := range sources {
				what = append(what, opt{Key: strconv.Itoa(i + 1), Text: fmt.Sprintf("   %2d  %-12s %s", i+1, s.Key, s.What)})
			}
			what = append(what, opt{Key: "0", Text: "    0  a fixed value (a password, an account name, a constant)"})
			pick, back, ok := p.chooseWithZeroTight(sc, "Choose", what)
			if !ok || back {
				return 0
			}
			if pick > 0 && int(pick) <= len(sources) {
				pr.Source = sources[pick-1].Key
			}
		}
		if pr.Source == "" {
			p.printf("  A fixed value. For a credential use {{secret:env://NAME}} so the value\n")
			p.printf("  itself stays out of the database.\n")
			if pr.DefaultValue, ok = p.ask(sc, "Value"); !ok {
				return 0
			}
		}
		e.Params = append(e.Params, pr)
		p.okf("  Added %s\n", describeParam(pr))
	}

	// ── how the text is written ──
	if role == "send" {
		p.printf("\n  How does this carrier want the text? plain = as it is (UTF-8);\n")
		p.printf("  ucs2_hex = Urdu/Arabic/emoji messages as hex (0633…), others plain;\n")
		p.printf("  hex = every message as hex.\n")
		if e.TextFormat, ok = p.askChoice(sc, "Text", provision.TextFormats(), "plain", ""); !ok {
			return 0
		}
	}

	// ── how we know it worked ──
	if role == "send" {
		p.printf("\n  How do we know the carrier accepted the message?\n")
		p.printf("  Without this, an answer carrying an error reads as success and the\n")
		p.printf("  message is never sent again. This is the check testvendor fails.\n")
		codes, ok := p.ask(sc, "Status codes that mean accepted, comma separated [200]")
		if !ok {
			return 0
		}
		if strings.TrimSpace(codes) == "" {
			codes = "200"
		}
		for _, c := range strings.Split(codes, ",") {
			if n, err := strconv.Atoi(strings.TrimSpace(c)); err == nil {
				e.Response.OKStatus = append(e.Response.OKStatus, n)
			}
		}
		if p.askYes(sc, "Does a value in the answer also have to be right (e.g. status=OK)", false) {
			src, _ := p.ask(sc, "  The carrier's name for that value")
			val, _ := p.ask(sc, "  The value that means accepted")
			if src != "" && val != "" {
				e.Response.OKContains = append(e.Response.OKContains,
					provision.SuccessIdentifier{Source: src, Value: val})
			}
		}
	}

	// ── reading values out of the answer ──
	if role == "send" || role == "poll" || role == "login" {
		if e.Response.Type, ok = p.askChoice(sc, "The answer is", provision.ResponseTypes(), "xml",
			"a plain-text answer (e.g. OK:12345) is read as xml, with a pattern that has ( ) around the value: OK:(\\d+)"); !ok {
			return 0
		}
		want := map[string][2]string{
			"send":  {"operator_id", "the carrier's own message id, so its receipt can be matched later"},
			"poll":  {"status", "what the carrier says happened to the message"},
			"login": {"session_id", "the session the send call will carry"},
		}[role]
		p.printf("\n  We need to read %s out of the answer: %s\n", want[0], want[1])
		src, ok := p.ask(sc, "  The carrier's name for it (Enter to skip)")
		if !ok {
			return 0
		}
		if src != "" {
			e.Response.Params = append(e.Response.Params,
				provision.ResponseParam{Alias: want[0], Source: src})
		}
	}

	if err := provision.ValidateEndpoint(e); err != nil {
		p.refusal(err)
		return 0
	}
	id, err := p.store.CreateEndpoint(ctx, e)
	if err != nil {
		p.refusal(err)
		return 0
	}
	p.okf("\n  Created call #%d (%s) for %s.\n", id, role, ownerName(vid))
	p.offerAttach(ctx, sc, vid, role, id)
	return id
}

// offerAttach points the vendor at a call just made — its send or poll slot;
// the vendor's other calls are kept.
func (p *Process) offerAttach(ctx context.Context, sc *bufio.Scanner, vid int64, role string, id int64) {
	if vid <= 0 || (role != "send" && role != "poll") {
		return
	}
	if !p.askYes(sc, fmt.Sprintf("Point the vendor's %s at this call now", map[string]string{
		"send": "send call", "poll": "status poll"}[role]), true) {
		return
	}
	if err := p.store.AttachEndpoints(ctx, vid, map[string]int64{role: id}); err != nil {
		p.refusal(err)
		return
	}
	p.okf("  Done. The vendor will load it within 30 seconds.\n")
}

// addCustomerWebhook points a customer's receipts or incoming messages at
// their own URL.
func (p *Process) addCustomerWebhook(ctx context.Context, sc *bufio.Scanner) {
	accounts, err := p.store.ListAccounts(ctx, 200)
	if err != nil {
		p.errf("  %v\n", err)
		return
	}
	p.printf("\n  Which customer?\n")
	aid, ok := p.chooseID(sc, "Account id", accountRows(accounts))
	if !ok || aid == 0 {
		return
	}

	kinds := provision.AccountWebhookTypes()
	p.printf("\n  What should we push?\n")
	ko := []opt{}
	for i, k := range kinds {
		ko = append(ko, item(strconv.Itoa(i+1), fmt.Sprintf("%-4s %s", k.Name, k.What)))
	}
	kpick, ok := p.choose(sc, "Choose", "1", ko)
	n, _ := strconv.ParseInt(kpick, 10, 64)
	if !ok || n < 1 || int(n) > len(kinds) {
		return
	}
	kind := kinds[n-1].Name

	var e provision.Endpoint
	e.Type, e.Name, e.AccountID = kind, kind, aid
	if e.URL, ok = p.ask(sc, "The customer's URL"); !ok || e.URL == "0" {
		return
	}
	if e.Method, ok = p.askChoice(sc, "Method", []string{"POST", "GET"}, "POST", ""); !ok {
		return
	}
	if e.Encoding, ok = p.askChoice(sc, "How are the fields carried", provision.Encodings(), "json", ""); !ok {
		return
	}

	p.printf("\n  Which values should we send, and what does the customer call each one?\n")
	p.printf("  Enter on its own when there are no more.\n")
	sources := provision.SourcesFor(kind)
	for {
		p.printf("\n")
		vals := []opt{}
		for i, s := range sources {
			vals = append(vals, opt{Key: strconv.Itoa(i + 1), Text: fmt.Sprintf("   %2d  %-12s %s", i+1, s.Key, s.What)})
		}
		if p.cursor && p.tty != nil {
			vals = append(vals, opt{Key: "0", Text: "    ✓  done — no more values"})
		}
		pk, ok := p.chooseTight(sc, "Which value (0 when finished)", "0", vals)
		pick, _ := strconv.ParseInt(pk, 10, 64)
		if !ok || pick == 0 {
			break
		}
		if pick < 1 || int(pick) > len(sources) {
			p.errf("  There is no value %d.\n", pick)
			continue
		}
		src := sources[pick-1].Key
		alias, ok := p.ask(sc, "  The customer's name for it ["+src+"]")
		if !ok {
			return
		}
		if alias == "" {
			alias = src
		}
		e.Params = append(e.Params, provision.Param{Alias: alias, Source: src})
		p.okf("  Added %s\n", describeParam(e.Params[len(e.Params)-1]))
	}

	if err := provision.ValidateEndpoint(e); err != nil {
		p.refusal(err)
		return
	}
	id, err := p.store.CreateEndpoint(ctx, e)
	if err != nil {
		p.refusal(err)
		return
	}
	// The account now names it (config/0174); other accounts may be pointed
	// at it too, from their own menu (Accounts → an account → Webhooks).
	p.okf("\n  Created %s webhook #%d; account #%d uses it now. A running gateway uses it within 30 seconds.\n", kind, id, aid)
}

func (p *Process) listEndpoints(ctx context.Context, sc *bufio.Scanner) {
	rows, err := p.store.ListEndpoints(ctx, "", 200)
	if err != nil {
		p.errf("  %v\n", err)
		return
	}
	if len(rows) == 0 {
		p.printf("\n  No calls configured yet.\n")
		return
	}
	p.printf("\n   %-6s %-8s %-16s %-16s %-6s %-7s %s\n",
		"id", "type", "role", "owner", "fields", "active", "URL")
	list := []opt{}
	for _, r := range rows {
		act := "yes"
		if !r.Active {
			act = "NO"
		}
		note := ""
		if r.Trouble != "" {
			note = "          ^ " + r.Trouble
		}
		// Every column is cut to its width (bug 17): a long name used to
		// push the rest of the row out of line.
		list = append(list, row(r.ID, fmt.Sprintf("   %-6d %-8s %-16s %-16s %-6d %-7s %s",
			r.ID, trim(r.Type, 8), trim(r.Name, 16), trim(r.Owner, 16), r.Fields, act, trim(r.URL, 40)), note))
	}
	// Opens the row, as every other list does (bug 5): this prompt used to be
	// "Delete a call by id", so an id typed to look at a call deleted it.
	id, ok := p.chooseID(sc, "Open a call by id (0 to go back)", list)
	if !ok || id == 0 {
		return
	}
	for _, r := range rows {
		if r.ID == id {
			p.endpointMenu(ctx, sc, r)
			return
		}
	}
	p.errf("  There is no call #%d in this list.\n", id)
}

// endpointMenu is one call: what it is, and deleting it.
func (p *Process) endpointMenu(ctx context.Context, sc *bufio.Scanner, r provision.EndpointRow) {
	p.printf("\n  Call #%d — %s %s for %s\n", r.ID, r.Type, r.Name, r.Owner)
	p.printf("   %-8s %s\n", "URL", orDash(r.URL))
	p.printf("   %-8s %d\n", "Fields", r.Fields)
	p.printf("   %-8s %s\n\n", "Active", yesNo(r.Active))
	if r.Trouble != "" {
		p.errf("  %s\n\n", r.Trouble)
	}
	choice, ok := p.choose(sc, "Choose", "", []opt{
		item("1", "Delete — permanent"),
		item("0", "Back"),
	})
	if !ok || choice != "1" {
		return
	}
	p.warnf("\n  Deleting call #%d stops whatever uses it: a vendor loses this call, a\n", r.ID)
	p.warnf("  customer loses this webhook.\n")
	if !p.confirm(sc, strconv.FormatInt(r.ID, 10)) {
		p.printf("  Left alone.\n")
		return
	}
	if err := p.store.DeleteEndpoint(ctx, r.ID); err != nil {
		p.refusal(err)
		return
	}
	p.okf("  Deleted call #%d.\n", r.ID)
}

func describeParam(pr provision.Param) string {
	if pr.Source != "" {
		return pr.Alias + " ← " + pr.Source
	}
	v := pr.DefaultValue
	if strings.Contains(v, "{{secret:") {
		v = "a secret reference"
	}
	return pr.Alias + " = " + v
}

func ownerName(vendorID int64) string {
	if vendorID == 0 {
		return "no vendor yet (a vendor picks it later)"
	}
	return "vendor #" + strconv.FormatInt(vendorID, 10)
}
