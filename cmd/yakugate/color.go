package main

import (
	"io"
	"os"
	"strings"

	"golang.org/x/term"
)

// Colour for the maintenance commands' messages (PO request 2026-09-21),
// matching the console: green = done, red = refused or failed, yellow = be
// careful. Only when that stream is a real terminal; piped output, scripts
// and tests get the plain text, and NO_COLOR (no-color.org) or TERM=dumb
// turns it off.

const (
	cGreen  = "\x1b[32m"
	cRed    = "\x1b[31m"
	cYellow = "\x1b[33m"
	cReset  = "\x1b[0m"
)

func colorOn(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok || os.Getenv("NO_COLOR") != "" || os.Getenv("TERM") == "dumb" {
		return false
	}
	return term.IsTerminal(int(f.Fd()))
}

// paint colours each line's text for w, leaving indents and line breaks out.
func paint(w io.Writer, code, s string) string {
	if !colorOn(w) {
		return s
	}
	lines := strings.Split(s, "\n")
	for i, ln := range lines {
		body := strings.TrimLeft(ln, " ")
		if body == "" {
			continue
		}
		lines[i] = ln[:len(ln)-len(body)] + code + body + cReset
	}
	return strings.Join(lines, "\n")
}

func green(w io.Writer, s string) string  { return paint(w, cGreen, s) }
func red(w io.Writer, s string) string    { return paint(w, cRed, s) }
func yellow(w io.Writer, s string) string { return paint(w, cYellow, s) }
