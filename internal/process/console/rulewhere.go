package console

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/provision"
	"github.com/yakugate/yakugate/internal/sendplan"
)

// The rule editor's "Where to send" part: Main and its backups, the vendors
// on each, the way of choosing, and shares by %.

// fitting lists the vendors a rule of this account may name, saying once
// which were left out and why.
func (e *ruleEditor) fitting() []provision.VendorRow {
	if e.fit == nil {
		e.fit = e.p.vendorsThatFit(e.ctx, e.vendors, e.d.account, "MT")
		if e.fit == nil {
			e.fit = []provision.VendorRow{}
		}
	}
	return e.fit
}

func (e *ruleEditor) usedVendors() map[int64]bool {
	used := map[int64]bool{}
	if e.d.plan != nil {
		for _, lv := range e.d.plan.Levels {
			for _, v := range lv.Vendors {
				used[v.VendorID] = true
			}
		}
	}
	return used
}

func vendorLine(v provision.VendorRow) opt {
	note := ""
	if v.Trouble != "" {
		note = "   ← WILL NOT LOAD: " + v.Trouble
	} else if v.Status != "active" {
		note = "   ← " + v.Status
	}
	return row(v.ID, fmt.Sprintf("   %4d  %-16s %-10s %s%s", v.ID, trim(v.CID, 16), v.Transport, trim(v.Company, 18), note), "")
}

// pickVendors adds vendors to one level until "no more". A vendor already
// in the rule is not offered again: a vendor sits in one level only.
func (e *ruleEditor) pickVendors(level string, used map[int64]bool, lv *sendplan.Level) bool {
	p := e.p
	for {
		var list []opt
		for _, v := range e.fitting() {
			if !used[v.ID] {
				list = append(list, vendorLine(v))
			}
		}
		if len(list) == 0 {
			if len(lv.Vendors) == 0 {
				p.errf("  No vendor is left for %s: every vendor that runs where %s runs is in this rule already.\n", level, e.d.acct)
				return false
			}
			return true
		}
		p.printf("\n  Vendors of %s (%d so far).\n", level, len(lv.Vendors))
		var vid int64
		if len(lv.Vendors) == 0 {
			id, ok := p.chooseID(e.sc, "Vendor id", list)
			if !ok || id == 0 {
				return false
			}
			vid = id
		} else {
			list = append(list, opt{Key: "0", Text: "      0  no more vendors in " + level})
			id, back, ok := p.chooseWithZero(e.sc, "Another vendor for "+level, list)
			if !ok {
				return false
			}
			if back || id == 0 {
				return true
			}
			vid = id
		}
		var found *provision.VendorRow
		for i, v := range e.fitting() {
			if v.ID == vid && !used[vid] {
				found = &e.fitting()[i]
			}
		}
		if found == nil {
			p.errf("  Vendor #%d is not one of the choices.\n", vid)
			continue
		}
		if found.Trouble != "" {
			p.warnf("\n  Careful: that vendor cannot be loaded (%s).\n", found.Trouble)
			yes, ok := p.askYesOK(e.sc, "Use it anyway", false)
			if !ok {
				return false
			}
			if !yes {
				continue
			}
		}
		used[vid] = true
		lv.Vendors = append(lv.Vendors, sendplan.Vendor{VendorID: vid})
		e.d.whereTouched = true
	}
}

func (e *ruleEditor) whereLinearOrEdit() bool {
	pl := e.d.plan
	if pl == nil || (len(pl.Levels) == 0 && len(pl.Exceptions) == 0) {
		return e.whereLinear()
	}
	return e.editWhere()
}

// whereLinear asks Main's vendors, the way (once, when a level first has
// two vendors to choose between), shares, backups and per-vendor prices.
func (e *ruleEditor) whereLinear() bool {
	p, d := e.p, e.d
	if len(e.fitting()) == 0 {
		p.errf("  No vendor runs where %s runs. Place one on its deployments under Vendors first.\n", d.acct)
		return false
	}
	p.printf("\n  Where to send. Main is tried first; a backup is used only when no vendor\n")
	p.printf("  of the level above can send.\n")
	plan := &sendplan.Plan{}
	d.plan, d.whereTouched = plan, true
	used := map[int64]bool{}
	for level := 0; ; level++ {
		plan.Levels = append(plan.Levels, sendplan.Level{})
		name := sendplan.LevelName(level)
		if !e.pickVendors(name, used, &plan.Levels[level]) {
			if len(plan.Levels[level].Vendors) == 0 {
				plan.Levels = plan.Levels[:level]
			}
			return false
		}
		if len(plan.Levels[level].Vendors) > 1 {
			if !d.wayAsked && !e.askWay() {
				return false
			}
			if !e.askShare(&plan.Levels[level], name) {
				return false
			}
		}
		if len(used) >= len(e.fitting()) {
			break
		}
		yes, ok := p.askYesOK(e.sc, "Add "+sendplan.LevelName(level+1)+" — used only when no vendor of "+name+" can send", false)
		if !ok {
			return false
		}
		if !yes {
			break
		}
	}
	if plan.Method == "" {
		plan.Method = sendplan.Direct
	}
	yes, ok := p.askYesOK(e.sc, "Give a vendor its own buy or sell price for this rule", false)
	if !ok {
		return false
	}
	if yes {
		return e.editPrices()
	}
	return true
}

// askWay asks the rule's way of choosing, once for Main and every backup
// (Saleem 2026-09-30: "rule wise not option wise").
func (e *ruleEditor) askWay() bool {
	p, pl := e.p, e.d.plan
	p.printf("\n  How should this rule choose among the vendors of a level?\n")
	p.printf("  Main and every backup choose the same way.\n")
	opts := []opt{}
	def := "1"
	for i, m := range sendplan.Methods {
		opts = append(opts, item(strconv.Itoa(i+1), fmt.Sprintf("%-15s %s", sendplan.MethodName(m), sendplan.MethodWhat(m))))
		if m == pl.Method {
			def = strconv.Itoa(i + 1)
		}
	}
	k, ok := p.choose(e.sc, "Choose", def, opts)
	n, _ := strconv.Atoi(k)
	if !ok || n < 1 || n > len(sendplan.Methods) {
		return false
	}
	pl.Method = sendplan.Methods[n-1]
	e.d.wayAsked, e.d.whereTouched = true, true
	for _, lv := range pl.Levels {
		if lv.Share && len(lv.Vendors) > 1 {
			if w := sendplan.ShareWarning(pl.Method); w != "" {
				p.warnf("  %s\n", w)
			}
			break
		}
	}
	return true
}

func evenPcts(n int) []int {
	out := make([]int, n)
	if n == 0 {
		return out
	}
	base := 100 / n
	for i := range out {
		out[i] = base
		if i < 100-base*n {
			out[i]++
		}
	}
	return out
}

// askShare asks whether a level shares its traffic by %, and the shares.
// On a way that orders by a score it warns first, in the contract's words.
func (e *ruleEditor) askShare(lv *sendplan.Level, name string) bool {
	p := e.p
	yes, ok := p.askYesOK(e.sc, "Share "+name+"'s traffic by % between them", lv.Share)
	if !ok {
		return false
	}
	e.d.whereTouched = true
	if !yes {
		lv.Share = false
		for i := range lv.Vendors {
			lv.Vendors[i].Pct = 0
		}
		return true
	}
	if w := sendplan.ShareWarning(e.d.plan.Method); w != "" {
		p.warnf("  %s\n", w)
	}
	even := evenPcts(len(lv.Vendors))
	pcts := make([]int, len(lv.Vendors))
	for {
		total := 0
		for i, v := range lv.Vendors {
			def := even[i]
			if lv.Share && v.Pct > 0 {
				def = v.Pct
			}
			pct, ok := p.askInt(e.sc, fmt.Sprintf("Share of %s (%%)", e.vname(v.VendorID)), def)
			if !ok {
				return false
			}
			pcts[i] = int(pct)
			total += int(pct)
		}
		if total == 100 {
			break
		}
		p.errf("  The shares add up to %d%%; they must add up to 100%%.\n", total)
	}
	for i := range lv.Vendors {
		lv.Vendors[i].Pct = pcts[i]
	}
	lv.Share = true
	return true
}

func (e *ruleEditor) levelWords(lv sendplan.Level) string {
	method := e.d.plan.Method
	if method == "" {
		method = e.d.plan.MethodFor(lv)
	}
	s := e.names.LevelPlain(lv, method)
	var own []string
	for _, v := range lv.Vendors {
		if v.BuyRateCardID != 0 || v.SellRateCardID != 0 {
			own = append(own, e.vname(v.VendorID))
		}
	}
	if len(own) > 0 {
		s += " (own price: " + strings.Join(own, ", ") + ")"
	}
	return s
}

// editWhere changes a plan: the way, each level's vendors, their order and
// shares, backups, and per-vendor prices.
func (e *ruleEditor) editWhere() bool {
	p, d := e.p, e.d
	for {
		pl := d.plan
		method := pl.Method
		if method == "" && len(pl.Levels) > 0 {
			method = pl.MethodFor(pl.Levels[0])
		}
		p.printf("\n  Where to send — chooses by %s: %s.\n", sendplan.MethodName(method), sendplan.MethodWhat(method))
		for i, lv := range pl.Levels {
			p.printf("    %-9s %s\n", sendplan.LevelName(i)+":", e.levelWords(lv))
		}
		for code, lvs := range pl.Exceptions {
			if len(lvs) > 0 {
				p.printf("    Network %s: %s (kept from before; network tabs are not changed here)\n", code, e.names.LevelPlain(lvs[0], method))
			}
		}
		opts := []opt{wideItem("1", "How it chooses — "+sendplan.MethodName(method))}
		levelKey := map[string]int{}
		k := 2
		for i := range pl.Levels {
			key := strconv.Itoa(k)
			levelKey[key] = i
			opts = append(opts, wideItem(key, sendplan.LevelName(i)+" — its vendors, their order, share by %"))
			k++
		}
		addKey := strconv.Itoa(k)
		opts = append(opts, wideItem(addKey, "Add "+sendplan.LevelName(len(pl.Levels))+" — used only when no vendor of the level above can send"))
		priceKey := strconv.Itoa(k + 1)
		opts = append(opts, wideItem(priceKey, "Prices for this rule, per vendor"))
		opts = append(opts, wideItem("0", "Done"))
		c, ok := p.choose(e.sc, "Choose", "0", opts)
		if !ok {
			return false
		}
		switch {
		case c == "0" || c == "":
			return true
		case c == "1":
			if !e.askWay() && p.ended {
				return false
			}
		case c == addKey:
			lv := sendplan.Level{}
			name := sendplan.LevelName(len(pl.Levels))
			picked := e.pickVendors(name, e.usedVendors(), &lv)
			if len(lv.Vendors) > 0 {
				pl.Levels = append(pl.Levels, lv)
				d.whereTouched = true
				if len(lv.Vendors) > 1 {
					if !d.wayAsked && !e.askWay() && p.ended {
						return false
					}
					if !e.askShare(&pl.Levels[len(pl.Levels)-1], name) && p.ended {
						return false
					}
				}
			}
			if !picked && p.ended {
				return false
			}
		case c == priceKey:
			if !e.editPrices() && p.ended {
				return false
			}
		default:
			i, ok := levelKey[c]
			if !ok {
				p.errf("  %q is not one of the choices.\n", c)
				continue
			}
			if !e.editLevel(i) && p.ended {
				return false
			}
		}
	}
}
