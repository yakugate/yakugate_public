package console

import (
	"bufio"
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/yakugate/yakugate/internal/monitor"
)

// The live monitor (PO 2026-09-21: "something like htop, properly, with auto
// refresh and a manual one as well"). [SCOPE P14-MONITOR]
//
// Full screen on the terminal's alternate buffer, so the menu is back exactly
// as it was on leaving. It redraws every `every` (5 s to start) from
// monitor.Summarize — the same summary as the HTML health page — and between
// redraws waits for a key:
//
//	r, space   refresh now          p   pause / resume the auto refresh
//	+  -       refresh less / more often (1 to 30 s)
//	l          Live for a minute: ask the gateway itself at every refresh
//	t          go to a time         [ ]  one interval back / on   n  back to now
//	e  d       events, days         u  s  queues, MySQL & Redis (each opens one)
//	g          which gateway        q, Esc, 0  back to the menu   Ctrl-C  leave
//
// (2026-10-05: the portal's monitoring, in the console too.)
//
// Piped input, a test, or a terminal without cursor keys gets the static
// screen (monitoringMenu) instead.

// timedKeys is a key source that can wait for a key only so long.
type timedKeys interface {
	keySource
	nextWithin(d time.Duration) (key string, got bool, err error)
}

type topState struct {
	every  time.Duration
	paused bool
	last   monitor.Overview
	err    error
}

// liveMonitor runs the full-screen view. It returns false when the person
// asked to leave the console altogether (Ctrl-C).
func (p *Process) liveMonitor(ctx context.Context, sc *bufio.Scanner) bool {
	keys, _ := p.keys.(timedKeys)
	if keys == nil {
		tk := &ttyKeys{f: p.tty}
		keys = tk
	}
	if err := keys.begin(); err != nil {
		p.monitoringMenu(ctx, sc)
		return true
	}
	defer keys.end()
	p.printf("\x1b[?1049h\x1b[?25l") // alternate screen, cursor hidden
	defer p.printf("\x1b[?25h\x1b[?1049l")

	st := &topState{every: 5 * time.Second} // PO 2026-09-21: 5 s to start
	read := func() {
		o, err := p.overview(ctx)
		st.last, st.err = o, err
	}
	// A line-based screen (events, days, queues, stores, a time): leave the
	// full screen for it and come back.
	sub := func(f func()) {
		p.printf("\x1b[?25h\x1b[?1049l")
		keys.end()
		f()
		_ = keys.begin()
		p.printf("\x1b[?1049h\x1b[?25l")
	}
	goTo := func(t time.Time, ok bool) {
		if ok {
			p.mon.at, p.mon.liveUntil = t, time.Time{}
		}
	}
	read()
	for {
		p.drawTop(st)
		wait := st.every
		switch {
		case p.mon.live():
			wait = liveEvery
		case st.paused || !p.mon.at.IsZero():
			wait = time.Hour // a past time does not change
		}
		deadline := time.Now().Add(wait)
	waiting:
		for {
			left := time.Until(deadline)
			if left <= 0 {
				read()
				break
			}
			k, got, err := keys.nextWithin(min(left, time.Second))
			if err != nil {
				return true
			}
			if !got {
				continue
			}
			switch k {
			case "q", "Q", "0", "\x1b":
				return true
			case "\x03", "\x04":
				return false
			case "r", "R", " ", "\r":
				read()
			case "p", "P":
				st.paused = !st.paused
			case "+", "=":
				st.every = min(st.every+time.Second, 30*time.Second)
			case "-", "_":
				st.every = max(st.every-time.Second, time.Second)
			case "l", "L":
				if p.mon.live() {
					p.mon.liveUntil = time.Time{}
				} else {
					p.mon.at, p.mon.liveUntil = time.Time{}, time.Now().Add(liveFor)
				}
				read()
			case "t", "T":
				sub(func() { goTo(p.askTime(sc)) })
				read()
			case "[", "]":
				if p.mon.at.IsZero() {
					continue
				}
				step := time.Duration(max(p.mon.interval, 5)) * time.Second
				if k == "[" {
					p.mon.at = p.mon.at.Add(-step)
				} else if next := p.mon.at.Add(step); next.Before(time.Now()) {
					p.mon.at = next
				}
				read()
			case "n", "N":
				p.mon.at = time.Time{}
				read()
			case "e", "E":
				sub(func() { goTo(p.eventsScreen(ctx, sc)) })
				read()
			case "d", "D":
				sub(func() { goTo(p.daysScreen(ctx, sc)) })
				read()
			case "u", "U":
				sub(func() { p.queuesScreen(ctx, sc) })
				read()
			case "s", "S":
				sub(func() { p.storesScreen(ctx, sc) })
				read()
			case "g", "G":
				p.nextGateway()
				read()
			default:
				continue // not ours: keep waiting, no redraw
			}
			break waiting
		}
		if ctx.Err() != nil {
			return true
		}
	}
}

// ── drawing ─────────────────────────────────────────────────────────────────

const (
	topInv   = "\x1b[7m"
	topBold  = "\x1b[1m"
	topHead  = "\x1b[30;42m" // black on green, the table headings
	topReset = "\x1b[0m"
)

// cell pads or cuts plain text to exactly w columns.
func cell(s string, w int) string {
	n := utf8.RuneCountInString(s)
	if n > w {
		r := []rune(s)
		if w <= 1 {
			return string(r[:w])
		}
		return string(r[:w-1]) + "…"
	}
	return s + strings.Repeat(" ", w-n)
}

// rcell right-aligns plain text in w columns.
func rcell(s string, w int) string {
	n := utf8.RuneCountInString(s)
	if n >= w {
		return cell(s, w)
	}
	return strings.Repeat(" ", w-n) + s
}

func (p *Process) paintIf(code, s string) string {
	if !p.colorOn() {
		return s
	}
	return code + s + topReset
}

// meter is an htop bar: "CPU [||||||      23.4%]", coloured by how full.
func (p *Process) meter(label string, pct float64, w int) string {
	inner := max(w-len(label)-3, 10)
	fill := int(pct / 100 * float64(inner))
	fill = min(max(fill, 0), inner)
	txt := fmt.Sprintf("%.1f%%", pct)
	bar := strings.Repeat("|", fill) + strings.Repeat(" ", inner-fill)
	// the figure sits at the right end of the bar, as in htop
	bar = bar[:inner-len(txt)] + txt
	colour := colGreen
	switch {
	case pct >= 90:
		colour = colRed
	case pct >= 70:
		colour = colYellow
	}
	return topBold + label + topReset + " [" + p.paintIf(colour, bar[:min(fill, inner-len(txt))]) +
		bar[min(fill, inner-len(txt)):] + "]"
}

func mib(b int64) string {
	switch {
	case b <= 0:
		return "—"
	case b >= 1<<30:
		return fmt.Sprintf("%.1fG", float64(b)/(1<<30))
	}
	return fmt.Sprintf("%dM", b>>20)
}

func perS(f float64) string {
	if f == 0 {
		return "·"
	}
	if f >= 100 {
		return fmt.Sprintf("%.0f", f)
	}
	return fmt.Sprintf("%.1f", f)
}

func (p *Process) drawTop(st *topState) {
	w, h := p.termSize()
	var lines []string
	add := func(s string) { lines = append(lines, s) }
	o := st.last

	// Title bar.
	mode := fmt.Sprintf("every %ds", int(st.every/time.Second))
	switch {
	case p.mon.live():
		mode = fmt.Sprintf("LIVE %ds left", int(time.Until(p.mon.liveUntil).Seconds()))
	case !p.mon.at.IsZero():
		mode = "PAST"
	case st.paused:
		mode = "PAUSED"
	}
	mode += " · " + p.mon.gatewayLabel()
	host := "—"
	if o.Host != nil {
		host = o.Host.Name
		if len(o.Hosts) > 1 {
			host += fmt.Sprintf(" (busiest of %d)", len(o.Hosts))
		}
	}
	// What changes and what the keys act on first; the host last, where a
	// narrow window cuts.
	src := o.Source
	if p.mon.live() {
		src = "" // the summary says how the ask went
	}
	title := fmt.Sprintf(" Yakugate monitor · %s · %s UTC · %s · %s", mode, o.Time.Format("15:04:05"), src, host)
	title = strings.Replace(title, " ·  · ", " · ", 1)
	add(topInv + cell(title, w) + topReset)
	if st.err != nil {
		add(p.paintIf(colRed, cell(" "+st.err.Error(), w)))
	}

	// Meters on the left, the summary on the right.
	left := max(min(w/2-2, 60), 30)
	var meters []string
	if hl := o.Host; hl != nil {
		meters = []string{p.meter("CPU", hl.CPUPct, left), p.meter("Mem", hl.MemPct, left), p.meter("Dsk", hl.DiskPct, left)}
	}
	notReady, waiting, msgs, oldest := 0, 0, int64(0), int64(0)
	for _, r := range o.Roles {
		if !r.Ready {
			notReady++
		}
	}
	for _, q := range o.Queues {
		if q.Depth > 0 {
			waiting++
			msgs += q.Depth
			oldest = max(oldest, q.OldestSec)
		}
	}
	t := o.Traffic
	summary := []string{
		fmt.Sprintf("Roles: %d running, %d not ready", len(o.Roles), notReady),
		fmt.Sprintf("In %s/s · out %s/s · receipts %s/s · refused %s/s",
			perS(t.Accepted.PerS), perS(t.Sent.PerS), perS(t.Receipts.PerS), perS(t.Rejected.PerS)),
		fmt.Sprintf("Queues: %d with %d waiting%s", waiting, msgs, map[bool]string{true: ", oldest " + monitor.Ago(time.Duration(oldest)*time.Second), false: ""}[msgs > 0]),
		p.smppBillingLine(o),
	}
	if p.mon.live() {
		summary = append([]string{"Live: " + p.mon.liveNote}, summary...)
	} else if !p.mon.at.IsZero() {
		summary = append([]string{o.Source}, summary...)
	}
	// Every gateway and machine, when there are several (2026-10-05).
	if len(o.Deployments) > 1 {
		var parts []string
		for _, d := range o.Deployments {
			parts = append(parts, fmt.Sprintf("%s %d/%d ready", d.ID, d.Ready, d.Roles))
		}
		summary = append(summary, "Gateways: "+strings.Join(parts, " · "))
	}
	if len(o.Hosts) > 1 {
		var parts []string
		for _, h := range o.Hosts {
			parts = append(parts, fmt.Sprintf("%s cpu %.0f%% mem %.0f%%", h.Name, h.CPUPct, h.MemPct))
		}
		summary = append(summary, "Machines: "+strings.Join(parts, " · "))
	}
	// MySQL and Redis (D-129): one line, each store's health and round trip.
	if len(o.Stores) > 0 {
		var parts []string
		for _, st := range o.Stores {
			kind := map[string]string{"mysql": "MySQL", "redis": "Redis"}[st.Kind]
			parts = append(parts, fmt.Sprintf("%s %s %s %.1f ms", kind, st.Name, st.Health, st.PingMS))
		}
		summary = append(summary, "Stores: "+strings.Join(parts, " · "))
	}
	if hl := o.Host; hl != nil {
		summary[0] += fmt.Sprintf(" · load %.2f", hl.Load1)
	}
	for i := 0; i < max(len(meters), len(summary)); i++ {
		l := strings.Repeat(" ", left+2)
		if i < len(meters) {
			l = " " + meters[i] + " "
		}
		r := ""
		if i < len(summary) {
			r = cell(summary[i], max(w-left-4, 10))
		}
		add(l + " " + r)
	}
	add("")

	// What needs attention.
	if len(o.Problems) == 0 {
		add(" " + p.paintIf(colGreen, "✓ nothing needs attention"))
	} else {
		shown := min(len(o.Problems), 5)
		for _, pr := range o.Problems[:shown] {
			add(" " + p.paintIf(colRed, cell("✗ "+pr, w-2)))
		}
		if more := len(o.Problems) - shown; more > 0 {
			add(fmt.Sprintf("   … and %d more", more))
		}
	}
	add("")

	// The roles, like htop's process list.
	head := " " + cell("ROLE", 15) + cell("STATE", 11) + rcell("UP", 8) + rcell("SEEN", 7) + rcell("PID", 9) +
		rcell("MEM", 8) + rcell("GOR", 6) + rcell("IN/s", 8) + rcell("OUT/s", 8) + "  HOST"
	add(p.paintIf(topHead, cell(head, w)))
	// Leave room below for the queues: their heading, two rows, a blank
	// line and the key bar. Roles that do not fit are counted, not dropped
	// silently — not-ready ones first, so a problem is never the one cut.
	roles := append([]monitor.RoleLine(nil), o.Roles...)
	sort.SliceStable(roles, func(i, j int) bool { return !roles[i].Ready && roles[j].Ready })
	roomRoles := h - len(lines) - 5
	cut := 0
	if roomRoles < len(roles) {
		roomRoles = max(roomRoles-1, 1)
		cut = len(roles) - roomRoles
		roles = roles[:roomRoles]
	}
	for _, r := range roles {
		pid := r.Instance
		if r.Host != "" && strings.HasPrefix(pid, r.Host+"-") {
			pid = strings.TrimPrefix(pid, r.Host+"-")
		}
		state := "ready"
		if !r.Ready {
			state = "NOT READY"
		}
		row := " " + cell(r.Role, 15) + cell(state, 11) + rcell(monitor.Ago(r.Uptime), 8) + rcell(monitor.Ago(r.Seen), 7) +
			rcell(pid, 9) + rcell(mib(r.RSSBytes), 8) + rcell(fmt.Sprint(r.Goroutines), 6) +
			rcell(perS(r.InPerS), 8) + rcell(perS(r.OutPS), 8) + "  " + r.Host
		row = cell(row, w)
		if !r.Ready {
			row = p.paintIf(colRed, row)
		}
		add(row)
	}
	if cut > 0 {
		add(fmt.Sprintf("   … and %d more role(s); enlarge the window to see them", cut))
	}
	add("")

	// Queues, as many as fit above the key bar.
	// Every queue moving or holding something: vendor queues, then customers'
	// receipts and MO (D-127).
	add(p.paintIf(topHead, cell(" "+cell("QUEUE", 24)+rcell("WAITING", 10)+rcell("READY", 9)+rcell("HELD", 8)+
		rcell("IN/S", 9)+rcell("OUT/S", 9)+rcell("OLDEST", 12), w)))
	room := h - len(lines) - 1
	shownQ := 0
	all := append(append([]monitor.QueueLine{}, o.Queues...), o.CustomerQueues...)
	for _, q := range all {
		if q.Depth == 0 && q.InPerS == 0 && q.OutPerS == 0 {
			continue
		}
		if shownQ == room-1 && room > 0 {
			add("   … more queues than fit; enlarge the window")
			break
		}
		old := "—"
		if q.Depth > 0 {
			old = monitor.Ago(time.Duration(q.OldestSec) * time.Second)
		}
		row := " " + cell(queueName(q), 24) + rcell(fmt.Sprint(q.Depth), 10) + rcell(fmt.Sprint(q.Ready), 9) + rcell(fmt.Sprint(q.Held), 8) +
			rcell(perS(q.InPerS), 9) + rcell(perS(q.OutPerS), 9) + rcell(old, 12)
		if q.OldestSec > 300 {
			row = p.paintIf(colYellow, row)
		}
		add(row)
		shownQ++
	}
	if shownQ == 0 {
		add("   every queue is empty and idle")
	}

	// Pad to the bottom and draw the key bar, htop style.
	for len(lines) < h-1 {
		add("")
	}
	if len(lines) > h-1 {
		lines = lines[:h-1]
	}
	keys := []struct{ k, what string }{{"r", "Refresh"}, {"p", map[bool]string{true: "Resume", false: "Pause"}[st.paused]},
		{"l", map[bool]string{true: "NoLive", false: "Live"}[p.mon.live()]}, {"t", "Time"}, {"e", "Events"}, {"d", "Days"},
		{"u", "Queues"}, {"s", "Stores"}, {"g", "Gateway"}, {"q", "Back"}}
	if !p.mon.at.IsZero() {
		keys = append([]struct{ k, what string }{{"[", "Earlier"}, {"]", "Later"}, {"n", "Now"}}, keys[2:]...)
	}
	var bar strings.Builder
	used := 0
	for _, k := range keys {
		if used+1+7+1 > w {
			break
		}
		bar.WriteString(topBold + k.k + topReset + topInv + cell(k.what, 7) + topReset + " ")
		used += 1 + 7 + 1
	}
	if used < w {
		bar.WriteString(topInv + strings.Repeat(" ", w-used) + topReset)
	}
	var b strings.Builder
	b.WriteString("\x1b[H")
	for _, l := range lines {
		b.WriteString(l + "\x1b[K\r\n")
	}
	b.WriteString(bar.String() + "\x1b[K\x1b[J")
	p.printf("%s", b.String())
}

func (p *Process) smppBillingLine(o monitor.Overview) string {
	var parts []string
	if s := o.SMPP; s.VendorsTotal > 0 {
		parts = append(parts, fmt.Sprintf("SMPP %d/%d bound", s.VendorsBound, s.VendorsTotal))
	}
	if b := o.Billing; b != nil {
		if b.Waiting == 0 && b.Delayed == 0 {
			parts = append(parts, "billing queue empty")
		} else {
			parts = append(parts, fmt.Sprintf("billing queue %d waiting (oldest %ds)", b.Waiting+b.Delayed, b.OldestSec))
		}
	}
	return strings.Join(parts, " · ")
}

// queueName is how a queue reads on screen: the vendor queue's name, or whose
// receipts or incoming messages wait.
func queueName(q monitor.QueueLine) string {
	switch q.Kind {
	case "dlr":
		return "receipts #" + q.Vendor
	case "mo":
		return "MO #" + q.Vendor
	}
	if q.Vendor == "_reroute" {
		return "re-route inbox"
	}
	return q.Vendor
}
