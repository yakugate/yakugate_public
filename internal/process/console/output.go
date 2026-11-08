package console

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/yakugate/yakugate/internal/platform/version"
	"github.com/yakugate/yakugate/internal/provision"
)

// What the console prints: the banner and logo, the header lines, refusals,
// and the small words every screen uses.

// logo is shown once, when the console opens — not on every redraw, where it
// would push the menu off a small terminal. 54 columns, so it fits any 80-wide
// screen. Block characters are UTF-8: every terminal the gateway's supported
// hosts ship with draws them.
const logo = `
  █   █   ███   █   █  █   █   ████   ███   █████  █████
   █ █   █   █  █  █   █   █  █      █   █    █    █
    █    █████  ███    █   █  █  ██  █████    █    ████
    █    █   █  █  █   █   █  █   █  █   █    █    █
    █    █   █  █   █   ███    ███   █   █    █    █████

  SMS gateway · console · https://yakugate.com`

func (p *Process) printf(format string, a ...any) { fmt.Fprintf(p.out, format, a...) }

// refusal separates "a rule said no" from "something is broken", because they
// need different things from the person reading.
func (p *Process) refusal(err error) {
	if provision.Refused(err) {
		p.errf("\n  Not saved: %s\n", provision.Reason(err))
		return
	}
	p.errf("\n  Failed: %v\n", err)
}

// count is one of the main menu's counts (dal.ConsoleViews.MenuCount names
// them), "?" when it cannot be read.
func (p *Process) count(ctx context.Context, what string) string {
	if p.store.DB == nil {
		return "?"
	}
	n, err := p.store.DB.MenuCount(ctx, what)
	if err != nil {
		return "?"
	}
	return strconv.Itoa(n)
}

func (p *Process) storeLine() string {
	ok := p.paint(colGreen, "✓")
	cfg := ok + " config store ok"
	q := ok + " queue store ok"
	if p.store.Queues == nil {
		q = p.paint(colYellow, "!") + " queue store not configured"
	}
	return cfg + "   " + q
}

// headerLine is the line above every main menu: the product in the brand's
// two colours ("Yaku" light/navy, "gate" teal),
// the build dimmed (Saleem 2026-09-29: "more beautified").
func (p *Process) headerLine() string {
	if !p.colorOn() {
		return "  ● Yakugate " + version.String() + " — console"
	}
	yaku := brandLight
	if lightBackground() {
		yaku = brandNavy
	}
	return "  " + p.fg(brandTeal, false) + "●" + escReset + " " + p.fg(yaku, true) + "Yaku" +
		p.fg(brandTeal, true) + "gate" + escReset + " " + version.String() + " \x1b[2m— console\x1b[0m"
}

// The brand's colours, from the logo itself (p13/public/yakugate-logo.png,
// sampled 2026-09-29): "yaku" navy #0E1A2B, "gate" and its underline teal
// #0891B2, the "A2P SMS GATEWAY" line grey #5B6673. Navy cannot be read on a
// dark terminal, so there "yaku" is drawn light slate — the same contrast the
// logo has on white. Exact colours where the terminal takes 24-bit colour
// (COLORTERM), the nearest of 256 elsewhere.
type brandColor struct {
	r, g, b int
	near    int // the nearest xterm-256 colour
}

var (
	brandNavy  = brandColor{14, 26, 43, 235}
	brandLight = brandColor{226, 232, 240, 254}
	brandTeal  = brandColor{8, 145, 178, 31}
	brandGrey  = brandColor{91, 102, 115, 245}
)

// logoSplit is the column where "YAKU" ends and "GATE" begins in the logo.
const logoSplit = 29

func (p *Process) fg(c brandColor, bold bool) string {
	b := ""
	if bold {
		b = "1;"
	}
	if ct := strings.ToLower(os.Getenv("COLORTERM")); ct == "truecolor" || ct == "24bit" {
		return fmt.Sprintf("\x1b[%s38;2;%d;%d;%dm", b, c.r, c.g, c.b)
	}
	return fmt.Sprintf("\x1b[%s38;5;%dm", b, c.near)
}

// lightBackground reads COLORFGBG ("15;0" = light text on black), which many
// terminals set; without it the terminal is taken as dark, as most are.
func lightBackground() bool {
	v := os.Getenv("COLORFGBG")
	i := strings.LastIndex(v, ";")
	if i < 0 {
		return false
	}
	switch v[i+1:] {
	case "7", "15":
		return true
	}
	return false
}

// banner is the opening screen, shown once: the logo framed, in the brand's
// colours with its teal bar, the logo's own tagline, then the offer and the
// team's line. The frame is 64 columns, so it fits any 80-column terminal;
// piped output gets the same frame, uncoloured.
func (p *Process) banner() string {
	const inner = 62
	col := p.colorOn()
	paint := func(code, s string) string {
		if !col {
			return s
		}
		return code + s + escReset
	}
	pad := func(s string) string {
		n := utf8.RuneCountInString(s)
		if n >= inner {
			return s
		}
		return s + strings.Repeat(" ", inner-n)
	}
	center := func(s string) string {
		n := utf8.RuneCountInString(s)
		left := (inner - n) / 2
		if left < 0 {
			left = 0
		}
		return pad(strings.Repeat(" ", left) + s)
	}
	frame := func(code string) string { return paint("\x1b[38;5;240m", code) }
	var b strings.Builder
	b.WriteString("\n  " + frame("╭"+strings.Repeat("─", inner)+"╮") + "\n")
	blank := "  " + frame("│") + strings.Repeat(" ", inner) + frame("│") + "\n"
	b.WriteString(blank)
	yaku := brandLight
	if lightBackground() {
		yaku = brandNavy
	}
	rows := strings.Split(strings.Trim(logo, "\n"), "\n")
	widest := 0
	for _, row := range rows {
		row = strings.TrimRight(row, " ")
		if !strings.Contains(row, "█") {
			continue // the logo's own tagline line; the frame carries its own
		}
		widest = max(widest, utf8.RuneCountInString(row))
		left, right := row, ""
		if r := []rune(row); len(r) > logoSplit {
			left, right = string(r[:logoSplit]), string(r[logoSplit:])
		}
		line := pad("  " + left + right)
		if col {
			line = p.fg(yaku, true) + "  " + left + p.fg(brandTeal, true) + right + escReset +
				strings.Repeat(" ", inner-utf8.RuneCountInString("  "+left+right))
		}
		b.WriteString("  " + frame("│") + line + frame("│") + "\n")
	}
	// The teal bar under "gate", as on the logo.
	barFrom, barTo := 2+logoSplit+1, 2+widest+1
	bar := strings.Repeat(" ", barFrom) + strings.Repeat("━", barTo-barFrom)
	if col {
		bar = strings.Repeat(" ", barFrom) + p.fg(brandTeal, true) + strings.Repeat("━", barTo-barFrom) + escReset +
			strings.Repeat(" ", inner-barTo)
	} else {
		bar = pad(bar)
	}
	b.WriteString("  " + frame("│") + bar + frame("│") + "\n")
	// The logo's own line, spaced as it is there.
	tag := center("A 2 P   S M S   G A T E W A Y")
	if col {
		tag = p.fg(brandGrey, false) + tag + escReset
	}
	b.WriteString("  " + frame("│") + tag + frame("│") + "\n")
	b.WriteString(blank)
	sub := center("console  ·  https://yakugate.com")
	if col {
		sub = "\x1b[2m" + sub + escReset
	}
	b.WriteString("  " + frame("│") + sub + frame("│") + "\n")
	b.WriteString(blank)
	b.WriteString("  " + frame("╰"+strings.Repeat("─", inner)+"╯") + "\n\n")
	b.WriteString(p.salesLine() + "\n")
	// The team's line centred under the frame (its words are 54 columns).
	credit := strings.TrimLeft(p.creditLine(), " ")
	b.WriteString(strings.Repeat(" ", 2+(inner+2-54)/2) + credit + "\n")
	return b.String()
}

// trim shortens a name to n CHARACTERS, not bytes.
//
// Bytes would be wrong twice over: a company name may be Urdu or carry an
// accent, and slicing mid-character produces broken text; and Go pads a column
// (%-28s) by characters, so a byte-trimmed name would also sit out of line —
// in a list an operator reads to pick an id.
func trim(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	if n <= 1 {
		return string(r[:n])
	}
	return string(r[:n-1]) + "…"
}

func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

func orDash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "—"
	}
	return s
}

func orDefault(s, def string) string {
	if strings.TrimSpace(s) == "" {
		return def
	}
	return s
}
