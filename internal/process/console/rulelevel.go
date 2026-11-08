package console

import (
	"fmt"
	"strings"

	"github.com/yakugate/yakugate/internal/sendplan"
)

// Changing one level of "Where to send", and a vendor's own buy or sell
// price for one rule.

// editLevel changes one level: add, remove or move a vendor, share by %,
// or remove a backup. Removing Main's last vendor while a backup exists
// makes Backup 1 the new Main.
func (e *ruleEditor) editLevel(i int) bool {
	p, d := e.p, e.d
	for {
		pl := d.plan
		if i >= len(pl.Levels) {
			return true
		}
		lv := &pl.Levels[i]
		name := sendplan.LevelName(i)
		p.printf("\n  %s: %s\n", name, e.levelWords(*lv))
		opts := []opt{item("1", "Add a vendor")}
		if len(lv.Vendors) > 0 {
			opts = append(opts, item("2", "Remove a vendor"))
		}
		if len(lv.Vendors) > 1 {
			share := "Share by % — off"
			if lv.Share {
				share = "Share by % — on (change the shares, or turn it off)"
			}
			opts = append(opts, item("3", "Move a vendor up — the order Direct tries them in"), item("4", share))
		}
		if i > 0 {
			opts = append(opts, item("5", "Remove "+name))
		}
		opts = append(opts, item("0", "Done"))
		c, ok := p.choose(e.sc, "Choose", "0", opts)
		if !ok {
			return false
		}
		switch c {
		case "0", "":
			return true
		case "1":
			before := len(lv.Vendors)
			picked := e.pickVendors(name, e.usedVendors(), lv)
			if len(lv.Vendors) > before && len(lv.Vendors) > 1 {
				if !d.wayAsked && !e.askWay() && p.ended {
					return false
				}
				if lv.Share {
					p.warnf("  A vendor was added, so the shares must be set again.\n")
					if !e.askShare(lv, name) && p.ended {
						return false
					}
				}
			}
			if !picked && p.ended {
				return false
			}
		case "2", "3":
			var list []opt
			for k, v := range lv.Vendors {
				if c == "3" && k == 0 {
					continue
				}
				list = append(list, row(v.VendorID, fmt.Sprintf("   %4d  %s", v.VendorID, e.vname(v.VendorID)), ""))
			}
			id, ok := p.chooseID(e.sc, "Vendor id", list)
			if !ok {
				return false
			}
			k := -1
			for x, v := range lv.Vendors {
				if v.VendorID == id {
					k = x
				}
			}
			if id == 0 || k < 0 || (c == "3" && k == 0) {
				continue
			}
			d.whereTouched = true
			if c == "3" {
				lv.Vendors[k-1], lv.Vendors[k] = lv.Vendors[k], lv.Vendors[k-1]
				continue
			}
			lv.Vendors = append(lv.Vendors[:k], lv.Vendors[k+1:]...)
			switch {
			case len(lv.Vendors) == 0 && i == 0 && len(pl.Levels) > 1:
				pl.Levels = pl.Levels[1:]
				p.warnf("  Main had no vendor left, so Backup 1 is now Main.\n")
				return true
			case len(lv.Vendors) == 0 && i > 0:
				pl.Levels = append(pl.Levels[:i], pl.Levels[i+1:]...)
				p.warnf("  %s had no vendor left, so it was removed.\n", name)
				return true
			case lv.Share && len(lv.Vendors) > 1:
				p.warnf("  A vendor was removed, so the shares must be set again.\n")
				if !e.askShare(lv, name) && p.ended {
					return false
				}
			case len(lv.Vendors) < 2:
				lv.Share = false
			}
		case "4":
			if len(lv.Vendors) > 1 && !e.askShare(lv, name) && p.ended {
				return false
			}
		case "5":
			if i > 0 {
				pl.Levels = append(pl.Levels[:i], pl.Levels[i+1:]...)
				d.whereTouched = true
				p.okf("  Removed %s.\n", name)
				return true
			}
		default:
			p.errf("  %q is not one of the choices.\n", c)
		}
	}
}

// editPrices gives a vendor its own buy or sell price for this rule only.
func (e *ruleEditor) editPrices() bool {
	p, d := e.p, e.d
	for {
		p.printf("\n  Prices for this rule, per vendor. Only this rule uses them.\n")
		var list []opt
		for _, lv := range d.plan.Levels {
			for _, v := range lv.Vendors {
				buy, sell := "its own card", "the rule's sell card"
				if v.BuyRateCardID != 0 {
					buy = e.cardName(v.BuyRateCardID)
				}
				if v.SellRateCardID != 0 {
					sell = e.cardName(v.SellRateCardID)
				}
				list = append(list, row(v.VendorID, fmt.Sprintf("   %4d  %-16s buy: %s · sell: %s", v.VendorID, trim(e.vname(v.VendorID), 16), buy, sell), ""))
			}
		}
		if len(list) == 0 {
			p.printf("   There is no vendor yet.\n")
			return true
		}
		id, ok := p.chooseID(e.sc, "Vendor id (0 = done)", list)
		if !ok {
			return false
		}
		if id == 0 {
			return true
		}
		var v *sendplan.Vendor
		for li := range d.plan.Levels {
			for vi := range d.plan.Levels[li].Vendors {
				if d.plan.Levels[li].Vendors[vi].VendorID == id {
					v = &d.plan.Levels[li].Vendors[vi]
				}
			}
		}
		if v == nil {
			p.errf("  Vendor #%d is not in this rule.\n", id)
			continue
		}
		p.printf("\n  Buy price: what %s charges us for this rule's messages.\n", e.vname(id))
		buy, ok := e.pickCard("buy", "MT", "its own card (the vendor's default)", v.BuyRateCardID)
		if !ok {
			if p.ended {
				return false
			}
			continue
		}
		p.printf("\n  Sell price: what %s pays when %s sends it.\n", d.acct, e.vname(id))
		sell, ok := e.pickCard("sell", "MT", "the rule's sell card", v.SellRateCardID)
		if !ok {
			if p.ended {
				return false
			}
			continue
		}
		v.BuyRateCardID, v.SellRateCardID = buy, sell
		d.whereTouched = true
	}
}

// pickCard offers the cards that fit a slot — the right side and traffic —
// and 0 for the default. ok=false: backed out, nothing changed.
func (e *ruleEditor) pickCard(kind, traffic, zero string, cur int64) (int64, bool) {
	p := e.p
	var list []opt
	for _, c := range e.rateCards() {
		if c.Kind != kind || (c.Type != "" && !strings.EqualFold(c.Type, traffic)) {
			continue
		}
		note := ""
		switch {
		case c.Disabled:
			note = "  (off)"
		case c.Trouble != "":
			note = "  ← " + c.Trouble
		}
		if c.ID == cur {
			note += "  ← now"
		}
		if e.view.Company != "" && c.Company != "" && c.Company != e.view.Company {
			note += "  (another company's)"
		}
		list = append(list, row(c.ID, fmt.Sprintf("   %4d  %-26s %-4s %s%s", c.ID, trim(c.Name, 26), c.Currency, trim(c.Company, 18), note), ""))
	}
	if len(list) == 0 {
		p.printf("   No %s cards for %s traffic yet — Rate cards → Add a card.\n", kind, traffic)
		return cur, true
	}
	list = append(list, opt{Key: "0", Text: "      0  " + zero})
	id, back, ok := p.chooseWithZero(e.sc, "Card id (0 = "+zero+")", list)
	if !ok || back {
		return cur, false
	}
	if id == 0 {
		return 0, true
	}
	for _, c := range e.rateCards() {
		if c.ID == id && c.Kind == kind {
			return id, true
		}
	}
	p.errf("  Card #%d is not one of the choices.\n", id)
	return cur, false
}
