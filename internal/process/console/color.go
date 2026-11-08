package console

import (
	"fmt"
	"os"
	"strings"
)

// Colour for the three kinds of message an operator must not miss
// (PO request 2026-09-21): green when something was done, red when it was
// refused or failed, yellow when it needs care. [SCOPE P14-COLOR]
//
// Only on a real terminal. Piped output, scripts and the tests get exactly
// the plain text they always did, and NO_COLOR (no-color.org) turns colour
// off for anyone who prefers it. Explanations, headings and menus stay
// uncoloured, so colour always means one of the three.

const (
	colGreen  = "\x1b[32m"
	colRed    = "\x1b[31m"
	colYellow = "\x1b[33m"
	// colCyan is for one row of the Gateway processes table only: a role
	// that runs on another deployment — neither running here, nor a
	// failure, nor stopped (2026-10-01).
	colCyan = "\x1b[36m"
)

// colorOn reports whether this console may use colour.
func (p *Process) colorOn() bool {
	return p.tty != nil && os.Getenv("NO_COLOR") == ""
}

// paint colours the text of each line, leaving its indent and the line
// breaks outside the colour, so a message spanning lines colours each one.
func (p *Process) paint(code, s string) string {
	if !p.colorOn() {
		return s
	}
	lines := strings.Split(s, "\n")
	for i, ln := range lines {
		body := strings.TrimLeft(ln, " ")
		if body == "" {
			continue
		}
		lines[i] = ln[:len(ln)-len(body)] + code + body + escReset
	}
	return strings.Join(lines, "\n")
}

// okf prints a success: something was saved, deleted or done.
func (p *Process) okf(format string, a ...any) {
	p.printf("%s", p.paint(colGreen, fmt.Sprintf(format, a...)))
}

// errf prints a refusal or a failure, or an answer that was not accepted.
func (p *Process) errf(format string, a ...any) {
	p.printf("%s", p.paint(colRed, fmt.Sprintf(format, a...)))
}

// warnf prints something to be careful about before or after acting.
func (p *Process) warnf(format string, a ...any) {
	p.printf("%s", p.paint(colYellow, fmt.Sprintf(format, a...)))
}
