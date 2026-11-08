package console

import (
	"bufio"
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/provision"
)

// The Vendors menu: adding, listing, showing and changing a vendor, and its
// door paths.

func (p *Process) vendorsMenu(ctx context.Context, sc *bufio.Scanner) {
	for {
		p.printf("\n  Vendors\n")
		opts := []opt{}
		opts = append(opts, item("1", "Add a vendor"))
		opts = append(opts, item("2", "List vendors"))
		opts = append(opts, item("0", "Back"))
		choice, ok := p.choose(sc, "Choose", "", opts)
		if !ok {
			return
		}
		switch choice {
		case "1":
			p.addVendor(ctx, sc)
		case "2":
			p.listVendors(ctx, sc)
		case "0", "":
			return
		default:
			p.errf("  %q is not one of the choices.\n", choice)
		}
	}
}

// addVendor asks what the transport needs and nothing else: an SMPP vendor is
// a handful of connection settings, an HTTP one is a profile. [SCOPE P14-VENDOR]
func (p *Process) addVendor(ctx context.Context, sc *bufio.Scanner) {
	p.printf("\n  Add a vendor. Enter on its own takes the default; 0 to abandon.\n\n")

	companies, err := p.store.ListCompanies(ctx, 200)
	if err != nil {
		p.errf("  %v\n", err)
		return
	}
	if len(companies) == 0 {
		p.printf("  There are no companies yet, and every vendor must belong to one.\n")
		return
	}
	p.printf("  Companies\n")
	var v provision.Vendor
	cid, ok := p.chooseID(sc, "Company id", companyRows(companies))
	if !ok || cid == 0 {
		return
	}
	v.CompanyID = cid

	p.printf("  The cid names the vendor's queue table (queue_<cid>) and its webhook path,\n")
	p.printf("  so it is fixed for life: lower-case letters, digits and underscores.\n")
	name, ok := p.ask(sc, "cid")
	if !ok || name == "0" || name == "" {
		return
	}
	v.CID = name

	v.Name, ok = p.ask(sc, "Display name (editable later)")
	if !ok {
		return
	}

	v.Transport, ok = p.askChoice(sc, "Transport", provision.Transports(), "smpp",
		"smpp = bind to a carrier; http/soap = call an API; blackhole = a simulator that never transmits")
	if !ok {
		return
	}

	switch v.Transport {
	case "smpp":
		if v.Host, ok = p.ask(sc, "Host"); !ok || v.Host == "0" {
			return
		}
		port, ok2 := p.askInt(sc, "Port", 2775)
		if !ok2 {
			return
		}
		v.Port = int(port)
		if v.SystemID, ok = p.ask(sc, "System id (the name the carrier knows this bind by)"); !ok {
			return
		}
		p.printf("  The password may be the value, or a reference the gateway resolves at\n")
		p.printf("  load: env://NAME or file:///path. A reference keeps it out of the database.\n")
		if v.Password, ok = p.ask(sc, "Password or reference"); !ok {
			return
		}
		sessions, ok2 := p.askInt(sc, "Concurrent binds", 1)
		if !ok2 {
			return
		}
		v.Sessions = int(sessions)

	case "http", "soap":
		// No JSON. An HTTP vendor is made first, then walked straight into
		// building its send call with the guided questions — asking someone to
		// paste an adapter profile assumes they know a shape they could only
		// have learned from the portal (PO 2026-09-18).
		p.printf("\n  An HTTP vendor is driven by the calls it makes. We will create the\n")
		p.printf("  vendor, then build its send call together — field by field.\n")
	}

	if err := provision.ValidateVendor(v); err != nil {
		p.refusal(err)
		return
	}
	// Which gateways claim its queue and hold its bind (config/0122), and its
	// rate on each (config/0124). Asked here, not left blank, because a
	// vendor nobody has placed is one no routing rule from a placed account
	// may point at.
	deps, ok := p.askVendorDeployments(sc)
	if !ok {
		return
	}
	v.Deployments = deps
	id, err := p.store.CreateVendor(ctx, v)
	if err != nil {
		p.refusal(err)
		return
	}
	p.okf("\n  Created vendor #%d %q.\n", id, v.CID)
	p.printf("  Its queue table queue_%s is ready. A running gateway picks it up within 30s.\n", v.CID)
	if v.Transport != "smpp" {
		p.warnf("  Its receipts and incoming messages are refused until its inbound doors are\n")
		p.warnf("  set up — in the portal, Vendors → Inbounds.\n")
	}

	// Straight into the send call: without one this vendor cannot carry
	// anything, and leaving an operator to find another menu is how a vendor
	// ends up looking configured and silently doing nothing.
	if v.Transport == "http" || v.Transport == "soap" {
		p.warnf("\n  It cannot send anything yet. Let us build its send call now.\n")
		if p.askYes(sc, "Build the send call", true) {
			if call := p.buildVendorCall(ctx, sc, id, "send"); call == 0 {
				p.warnf("\n  No send call yet. The vendor will show as \"will not load\" until it has\n")
				p.warnf("  one: HTTP calls → Add a vendor call → send.\n")
			}
		} else {
			p.printf("  HTTP calls → Add a vendor call → send, when you are ready.\n")
		}
	}
	if v.Transport == "smpp" && v.Password != "" && !provision.IsSecretRef(v.Password) {
		p.warnf("\n  Note: the password is stored as the value itself. env://NAME or\n")
		p.warnf("  file:///path would keep it out of the database.\n")
	}
	p.warnf("\n  It has no routing rule yet, so nothing will be sent to it.\n")
}

// vendorListMax is how many vendors the list shows. A vendor can be opened
// only from this list, and it showed the first 200: on an install with more
// (the dev box reached 202) every newer vendor could not be opened at all.
const vendorListMax = 1000

func (p *Process) listVendors(ctx context.Context, sc *bufio.Scanner) {
	rows, err := p.store.ListVendors(ctx, vendorListMax)
	if err != nil {
		p.errf("  %v\n", err)
		return
	}
	if len(rows) == 0 {
		p.printf("\n  No vendors yet.\n")
		return
	}
	p.printf("\n   %-6s %-16s %-20s %-10s %-9s %-7s %s\n",
		"id", "cid", "company", "transport", "status", "queued", "will it load?")
	list := make([]opt, 0, len(rows))
	for _, r := range rows {
		state := r.Trouble
		if state == "" {
			state = "yes"
		} else {
			state = "NO — " + state
		}
		list = append(list, row(r.ID, fmt.Sprintf("   %-6d %-16s %-20s %-10s %-9s %-7d %s",
			r.ID, trim(r.CID, 16), trim(r.Company, 20), r.Transport, r.Status, r.Queued, state), ""))
	}
	if len(rows) == vendorListMax {
		p.warnf("\n  Showing the first %d vendors; there are more.\n", vendorListMax)
	}
	id, ok := p.chooseID(sc, "Open a vendor by id (0 to go back)", list)
	if !ok || id == 0 {
		return
	}
	for _, r := range rows {
		if r.ID == id {
			p.vendorMenu(ctx, sc, r)
			return
		}
	}
	p.errf("  There is no vendor #%d in this list.\n", id)
}

func (p *Process) vendorMenu(ctx context.Context, sc *bufio.Scanner, r provision.VendorRow) {
	for {
		d, err := p.store.GetVendor(ctx, r.ID)
		if err != nil {
			p.refusal(err)
			return
		}
		p.printf("\n  Vendor #%d %q — %s, %s, %s\n", d.ID, d.CID, d.Company, d.Transport, d.Status)
		if d.Trouble != "" {
			p.errf("  This vendor CANNOT be loaded: %s\n", d.Trouble)
			p.errf("  The role skips it, so anything routed here queues and is never sent.\n")
		}
		if d.Queued > 0 {
			p.printf("  %d message(s) waiting in queue_%s.\n", d.Queued, d.CID)
		}
		opts := []opt{}
		opts = append(opts, item("1", "View everything"))
		opts = append(opts, item("2", "Change a setting"))
		if d.Status == "active" {
			opts = append(opts, item("3", "Pause — stop sending to it, keep the queue"))
		} else {
			opts = append(opts, item("3", "Activate"))
		}
		opts = append(opts, item("4", "Disable"))
		opts = append(opts, item("5", "Delete — permanent"))
		opts = append(opts, item("6", "Inbound doors: their own paths"))
		opts = append(opts, item("0", "Back"))
		choice, ok := p.choose(sc, "Choose", "", opts)
		if !ok {
			return
		}
		switch choice {
		case "1":
			p.viewVendor(d)
		case "2":
			// The two queue settings (issue 22) say what they are now first.
			p.editOneShowing(ctx, sc, provision.VendorFields(d.Transport), provision.VendorFixed(),
				func(key string) string {
					switch key {
					case "queue_split":
						return provision.QueueSplitWords(d.QueueSplit)
					case "short_codes":
						return provision.ShortCodesWords(d.ShortCodes)
					}
					return ""
				},
				func(key, val string) error { return p.store.UpdateVendor(ctx, d.ID, key, val) })
		case "3":
			want := "active"
			if d.Status == "active" {
				want = "paused"
			}
			if err := p.store.SetVendorStatus(ctx, d.ID, want); err != nil {
				p.refusal(err)
				return
			}
			p.okf("  Now %s. A running gateway acts on it within 30 seconds.\n", want)
		case "4":
			if err := p.store.SetVendorStatus(ctx, d.ID, "disabled"); err != nil {
				p.refusal(err)
				return
			}
			p.okf("  Now disabled.\n")
		case "5":
			p.warnf("\n  Deleting vendor %q drops its queue table as well.\n", d.CID)
			if !p.confirm(sc, d.CID) {
				p.printf("  Left alone.\n")
				continue
			}
			if err := p.store.DeleteVendor(ctx, d.ID); err != nil {
				p.refusal(err)
				continue
			}
			p.okf("  Deleted vendor #%d %q.\n", d.ID, d.CID)
			return
		case "6":
			p.doorPaths(ctx, sc, d)
		case "0", "":
			return
		default:
			p.errf("  %q is not one of the choices.\n", choice)
		}
	}
}

// doorPaths shows the paths a vendor's two inbound doors answer on and gives
// one a path of its own, or takes it away (issue 20; the rule is
// provision.SetDoorPath, the Admin API's twin). The doors' credentials and
// dialects are still set in the portal.
func (p *Process) doorPaths(ctx context.Context, sc *bufio.Scanner, d provision.VendorDetail) {
	paths, err := p.store.DoorPaths(ctx, d.ID)
	if err != nil {
		p.refusal(err)
		return
	}
	p.printf("\n  Each door answers on its default path and, when it has one, on a path of its own\n")
	p.printf("  (for a vendor that can only post to a URL of its own shape).\n")
	opts := []opt{}
	for i, dp := range paths {
		own := dp.Path
		if own == "" {
			own = "none"
		}
		opts = append(opts, item(strconv.Itoa(i+1), fmt.Sprintf("%-4s /%s/%s   its own path: %s", strings.ToUpper(dp.Kind), dp.Kind, d.CID, own)))
	}
	opts = append(opts, item("0", "Back"))
	choice, ok := p.choose(sc, "Which door", "", opts)
	n, _ := strconv.Atoi(choice)
	if !ok || n < 1 || n > len(paths) {
		return
	}
	dp := paths[n-1]
	val, ok := p.ask(sc, "  Its own path, e.g. /acme/receipts (Enter keeps it, none takes it away)")
	if !ok || val == "" || val == "0" {
		p.printf("  Nothing changed.\n")
		return
	}
	if strings.EqualFold(val, "none") {
		val = ""
	}
	got, err := p.store.SetDoorPath(ctx, d.ID, dp.Kind, val)
	if err != nil {
		p.refusal(err)
		return
	}
	if got == "" {
		p.okf("  The %s door answers on /%s/%s only.\n", strings.ToUpper(dp.Kind), dp.Kind, d.CID)
	} else {
		p.okf("  The %s door answers on %s too. A running gateway picks it up within 30 seconds.\n", strings.ToUpper(dp.Kind), got)
	}
	p.printf("  Its credentials and dialect are set in the portal, Vendors → Inbounds.\n")
}

func (p *Process) viewVendor(d provision.VendorDetail) {
	loads := "yes"
	if d.Trouble != "" {
		loads = "NO — " + d.Trouble
	}
	inbound := "none — its callbacks are refused 403 (portal: Vendors → Inbounds)"
	switch {
	case d.HasInbound:
		inbound = "a door has a credential (portal: Vendors → Inbounds)"
	case d.Transport == "smpp":
		// An SMPP carrier sends its receipts down the bind, not to our
		// webhook, so it needs no inbound token (bug 9, 2026-09-21).
		inbound = "not needed — receipts arrive over the SMPP bind"
	}
	p.printf("\n  Vendor #%d\n", d.ID)
	rows := [][2]string{
		{"cid", d.CID + "  (fixed: it names queue_" + d.CID + ")"},
		{"Display name", orDash(d.Name)},
		{"Company", fmt.Sprintf("%s (#%d)", d.Company, d.CompanyID)},
		{"Transport", d.Transport},
		{"Status", d.Status},
		{"Will it load?", loads},
		{"Waiting in its queue", fmt.Sprintf("%d", d.Queued)},
		{"Routing rules to it", fmt.Sprintf("%d", d.Routes)},
		{"Inbound doors", inbound},
		{"Queue split", provision.QueueSplitWords(d.QueueSplit)},
		{"Short codes", provision.ShortCodesWords(d.ShortCodes)},
	}
	if !d.QueueColumns {
		rows[len(rows)-2][1] += "  (this store has no queue settings yet: the gateway has to be upgraded)"
	}
	switch d.Transport {
	case "smpp":
		rows = append(rows,
			[2]string{"Host", fmt.Sprintf("%s:%d", orDash(d.Host), d.Port)},
			[2]string{"System id", orDash(d.SystemID)},
			[2]string{"Password", d.PasswordShown},
			[2]string{"Concurrent binds", binds(d.Sessions)},
			[2]string{"Encodings sent as", d.Wire.Describe()})
		for _, pr := range d.WireProblems {
			rows = append(rows, [2]string{"", "ignored: " + pr})
		}
	case "http", "soap":
		rows = append(rows, [2]string{"Adapter profile", trim(d.AdapterConfig, 60)})
	}
	for _, kv := range rows {
		p.printf("   %-24s %s\n", kv[0], kv[1])
	}
}

func throughput(n int) string {
	if n == 0 {
		return "unlimited"
	}
	return strconv.Itoa(n) + " segments/sec"
}

// vendorRows is the "which vendor?" list every picker draws.
func vendorRows(vendors []provision.VendorRow) []opt {
	out := make([]opt, 0, len(vendors))
	for _, v := range vendors {
		out = append(out, row(v.ID, fmt.Sprintf("   %4d  %-16s %-10s %s", v.ID, trim(v.CID, 16), v.Transport, trim(v.Company, 20)), ""))
	}
	return out
}

// binds is the number of binds the smpp-vendor role opens: it treats 0 (not
// set) as 1, so the screen says 1 too (bug 10, 2026-09-21).
func binds(n int) string {
	if n < 1 {
		return "1 (default)"
	}
	return strconv.Itoa(n)
}
