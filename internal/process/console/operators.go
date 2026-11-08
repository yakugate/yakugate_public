package console

import (
	"bufio"
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/hlr"
	"github.com/yakugate/yakugate/internal/msisdn"
	"github.com/yakugate/yakugate/internal/provision"
)

// Operators: the mobile networks and the number ranges each was issued.
// [SCOPE P14-OPERATORS] The rulebook is provision/operator.go; this file only
// asks. The number lookup uses the gateway's own range index (hlr.Ranges),
// so it answers exactly as the gateway will.

func (p *Process) operatorsMenu(ctx context.Context, sc *bufio.Scanner) {
	for {
		nets, ranges, err := p.store.OperatorCounts(ctx)
		if err != nil {
			p.refusal(err)
			return
		}
		p.printf("\n  Operators — %s networks, %s number ranges\n", provision.FormatCount(nets), provision.FormatCount(ranges))
		p.printf("  Which network a number was issued to. The gateway picks up a change\n")
		p.printf("  within 5 minutes.\n\n")
		choice, ok := p.choose(sc, "Choose", "", []opt{
			item("1", "Look up a number"),
			item("2", "Find a network — by country, name or code"),
			item("3", "Add a network, or a number range to one"),
			item("0", "Back"),
		})
		if !ok {
			return
		}
		switch choice {
		case "1":
			p.lookUpNumber(ctx, sc)
		case "2":
			p.findNetworks(ctx, sc)
		case "3":
			p.addNetwork(ctx, sc)
		case "0", "":
			return
		default:
			p.errf("  %q is not one of the choices.\n", choice)
		}
	}
}

// lookUpNumber answers from the same range index the gateway loads.
func (p *Process) lookUpNumber(ctx context.Context, sc *bufio.Scanner) {
	if p.ranges == nil {
		p.printf("\n  Loading the number ranges (once per session)…\n")
		r := hlr.NewRanges(func(string, ...any) {})
		src := p.numberRanges() // the console's config store; none = refused
		if err := r.Load(ctx, src); err != nil {
			p.refusal(err)
			return
		}
		p.ranges = r
	}
	for {
		v, ok := p.ask(sc, "Number (Enter to go back)")
		if !ok || strings.TrimSpace(v) == "" || v == "0" {
			return
		}
		n := msisdn.Clean(v)
		if mcc, mnc, found := p.ranges.Range(n); found {
			net, err := p.store.GetNetwork(ctx, mcc, mnc)
			if err != nil {
				p.okf("  %s → network %s-%s\n", n, mcc, mnc)
				continue
			}
			p.okf("  %s → %s (%s-%s), %s\n", n, net.Name, mcc, mnc, net.Country)
			p.printf("    by the range it was issued in; a number moved to another network is\n")
			p.printf("    known from HLR data or a live lookup, not from here.\n")
			continue
		}
		if mcc, found := p.ranges.Country(n); found {
			p.warnf("  %s is in no range. By its calling code it is in country %s (MCC); the\n", n, mcc)
			p.warnf("  network is unknown.\n")
			continue
		}
		p.errf("  %s is in no range, and its calling code gives no single country.\n", n)
	}
}

func (p *Process) findNetworks(ctx context.Context, sc *bufio.Scanner) {
	q, ok := p.ask(sc, "Country, name, MCC or MCC+MNC (Enter = the first 50)")
	if !ok || q == "0" {
		return
	}
	nets, err := p.store.FindNetworks(ctx, q, 50)
	if err != nil {
		p.refusal(err)
		return
	}
	if len(nets) == 0 {
		p.printf("\n  No network matches %q.\n", q)
		return
	}
	p.printf("\n   %-4s %-8s %-28s %-20s %s\n", "#", "network", "name", "country", "ranges")
	list := []opt{}
	for i, n := range nets {
		list = append(list, row(int64(i+1), fmt.Sprintf("   %-4d %-8s %-28s %-20s %s",
			i+1, n.MCC+"-"+n.MNC, trim(n.Name, 28), trim(n.Country, 20), provision.FormatCount(n.Ranges)), ""))
	}
	if len(nets) == 50 {
		p.printf("  (the first 50 — type more of the name to narrow it)\n")
	}
	pick, ok := p.chooseID(sc, "Open a network by # (0 to go back)", list)
	if !ok || pick == 0 {
		return
	}
	if pick < 1 || int(pick) > len(nets) {
		p.errf("  There is no #%d in this list.\n", pick)
		return
	}
	p.networkMenu(ctx, sc, nets[pick-1].MCC, nets[pick-1].MNC)
}

func (p *Process) networkMenu(ctx context.Context, sc *bufio.Scanner, mcc, mnc string) {
	for {
		n, err := p.store.GetNetwork(ctx, mcc, mnc)
		if err != nil {
			p.refusal(err)
			return
		}
		p.printf("\n  Network %s-%s %q — %s", mcc, mnc, n.Name, n.Country)
		if n.Region != "" && n.Region != n.Country {
			p.printf(" (%s)", n.Region)
		}
		p.printf(", calling code %s, %s range(s)\n\n", n.CCC, provision.FormatCount(n.Ranges))
		choice, ok := p.choose(sc, "Choose", "", []opt{
			item("1", "Its number ranges"),
			item("2", "Add a number range"),
			item("3", "Change its name, country or region"),
			item("4", "Delete the whole network — every range"),
			item("0", "Back"),
		})
		if !ok {
			return
		}
		switch choice {
		case "1":
			p.networkRanges(ctx, sc, n)
		case "2":
			p.askRange(ctx, sc, n)
		case "3":
			field, ok := p.askChoice(sc, "Which", []string{"name", "country", "region"}, "name", "")
			if !ok {
				continue
			}
			v, ok := p.ask(sc, "New "+field)
			if !ok || v == "0" {
				continue
			}
			if err := p.store.RenameNetwork(ctx, mcc, mnc, field, v); err != nil {
				p.refusal(err)
				continue
			}
			p.okf("  Done, on all %s range(s).\n", provision.FormatCount(n.Ranges))
		case "4":
			routes, prices, err := p.store.NetworkInUse(ctx, mcc, mnc)
			if err != nil {
				p.refusal(err)
				continue
			}
			if routes > 0 || prices > 0 {
				p.errf("\n  %s is still in use: %d routing rule(s) match it and %d price(s) are set\n", n.Name, routes, prices)
				p.errf("  for it. Change those first.\n")
				continue
			}
			p.warnf("\n  Every one of its %s range(s) goes. Numbers in them will no longer be\n", provision.FormatCount(n.Ranges))
			p.warnf("  known to belong to %s.\n", n.Name)
			if !p.confirm(sc, mcc+mnc) {
				p.printf("  Left alone.\n")
				continue
			}
			gone, err := p.store.DeleteNetwork(ctx, mcc, mnc)
			if err != nil {
				p.refusal(err)
				continue
			}
			p.okf("  Deleted %s-%s: %d range(s).\n", mcc, mnc, gone)
			return
		case "0", "":
			return
		default:
			p.errf("  %q is not one of the choices.\n", choice)
		}
	}
}

func (p *Process) networkRanges(ctx context.Context, sc *bufio.Scanner, n provision.Network) {
	prefix, ok := p.ask(sc, "Ranges starting with (Enter = the first 50)")
	if !ok || prefix == "0" {
		return
	}
	prefix = msisdn.Clean(prefix)
	rs, total, err := p.store.NetworkRanges(ctx, n.MCC, n.MNC, prefix, 50)
	if err != nil {
		p.refusal(err)
		return
	}
	if len(rs) == 0 {
		p.printf("\n  None.\n")
		return
	}
	p.printf("\n  %d shown of %s.\n\n   %-9s %-17s %s\n", len(rs), provision.FormatCount(total), "id", "from", "to")
	list := []opt{}
	for _, r := range rs {
		list = append(list, row(r.ID, fmt.Sprintf("   %-9d %-17s %s", r.ID, r.From, r.To), ""))
	}
	id, ok := p.chooseID(sc, "Open a range by id (0 to go back)", list)
	if !ok || id == 0 {
		return
	}
	var r provision.NumberRange
	for _, x := range rs {
		if x.ID == id {
			r = x
		}
	}
	if r.ID == 0 {
		p.errf("  There is no range #%d in this list.\n", id)
		return
	}
	p.printf("\n  Range #%d: %s to %s (%s)\n\n", r.ID, r.From, r.To, n.Name)
	choice, ok := p.choose(sc, "Choose", "", []opt{item("1", "Delete — permanent"), item("0", "Back")})
	if !ok || choice != "1" {
		return
	}
	if !p.confirm(sc, strconv.FormatInt(r.ID, 10)) {
		p.printf("  Left alone.\n")
		return
	}
	if err := p.store.DeleteRange(ctx, r.ID); err != nil {
		p.refusal(err)
		return
	}
	p.okf("  Deleted range #%d. The gateway stops using it within 5 minutes.\n", r.ID)
}

// addNetwork asks for a network's codes; an existing one goes straight to
// adding a range to it.
func (p *Process) addNetwork(ctx context.Context, sc *bufio.Scanner) {
	p.printf("\n  Add a network, or a range to one. 0 to abandon.\n")
	mcc, ok := p.ask(sc, "MCC — the country's network code, e.g. 410")
	if !ok || mcc == "0" || mcc == "" {
		return
	}
	mnc, ok := p.ask(sc, "MNC — the network's code, e.g. 01")
	if !ok || mnc == "" {
		return
	}
	// Any spelling of the code (01 / 001) is the same network to the gateway.
	if n, found, err := p.store.SameNetwork(ctx, mcc, mnc); err != nil {
		p.refusal(err)
		return
	} else if found {
		p.printf("  %s-%s is %q (%s): adding a range to it.\n", n.MCC, n.MNC, n.Name, n.Country)
		p.askRange(ctx, sc, n)
		return
	}
	n := provision.Network{MCC: mcc, MNC: mnc}
	if n.CCC, ok = p.ask(sc, "Calling code, e.g. 92"); !ok || n.CCC == "0" {
		return
	}
	if n.Name, ok = p.askName(sc, "Network name"); !ok {
		return
	}
	if n.Country, ok = p.askName(sc, "Country"); !ok {
		return
	}
	if n.Region, ok = p.ask(sc, "Region (Enter = none)"); !ok || n.Region == "0" {
		return
	}
	p.printf("  A network is known by its number ranges; give its first one.\n")
	p.askRange(ctx, sc, n)
}

// askRange asks for one range of network n and adds it.
func (p *Process) askRange(ctx context.Context, sc *bufio.Scanner, n provision.Network) {
	p.printf("\n  Both ends in full, with the calling code and the same number of digits,\n")
	p.printf("  e.g. 923000000000 to 923009999999.\n")
	from, ok := p.ask(sc, "From")
	if !ok || from == "0" || from == "" {
		return
	}
	to, ok := p.ask(sc, "To")
	if !ok || to == "0" || to == "" {
		return
	}
	r := provision.NumberRange{From: msisdn.Clean(from), To: msisdn.Clean(to), Network: n}
	warn, err := provision.ValidateRange(r)
	if err != nil {
		p.refusal(err)
		return
	}
	if warn != "" {
		p.warnf("  Careful: %s.\n", warn)
		if !p.askYes(sc, "Add it anyway", false) {
			p.printf("  Left alone.\n")
			return
		}
	}
	id, err := p.store.AddRange(ctx, r)
	if err != nil {
		p.refusal(err)
		if id == 0 {
			return
		}
	}
	p.okf("\n  Added range #%d to %s (%s-%s). The gateway uses it within 5 minutes.\n", id, n.Name, n.MCC, n.MNC)
}
