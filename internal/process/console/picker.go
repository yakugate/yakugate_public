package console

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/term"
)

// Two ways to choose, one set of screens. [SCOPE P14-CURSOR]
//
// CURSOR mode (a terminal): the screen draws the same lines it always did, and
// the operator moves a highlight with ↑/↓ and presses Enter. The numbers are
// still shown and still work — press 3 and item 3 is chosen at once — and in a
// long list, typing finds a row. Esc goes back.
//
// NUMBERED mode: exactly the console as it was — the lines, then "Choose:",
// and a typed answer. It is what a piped script, a test, or a terminal that
// cannot do cursor keys gets, and what an operator gets who prefers it
// (YAKU_CONSOLE_MENUS=numbers, or the switch on the main menu). The PO will
// pick the default later (2026-09-19); both stay.
//
// Every screen asks through choose(), so the two modes cannot drift apart:
// numbered output is byte-for-byte what the screens printed before this file
// existed, which is why the 69 menu tests did not need to change.

// opt is one line a person can choose.
type opt struct {
	Key  string // what choose returns; "0" is back, by convention
	Text string // the line as drawn, already formatted by the screen
	Note string // an optional line under it (a warning), drawn dim
	Skip bool   // drawn, never chosen: a line that explains, like "cannot be changed"
	// Keyed marks a menu line whose key is typed digit by digit (wideItem):
	// a menu with one is picked by its keys even when a key has two digits.
	// Without it, a list of two-digit ids stays a searchable list.
	Keyed bool
}

// row is one line of a list keyed by id: "   12     name …".
func row(id int64, text, note string) opt {
	return opt{Key: strconv.FormatInt(id, 10), Text: text, Note: note}
}

// item makes a menu line in the house format: "   3  List rules".
func item(key, label string) opt { return opt{Key: key, Text: "   " + key + "  " + label} }

// wideItem is item with the key right-aligned in two places, for a menu that
// has a two-digit key (the main menu's "10"), so the labels still line up.
func wideItem(key, label string) opt {
	return opt{Key: key, Text: fmt.Sprintf("   %2s  %s", key, label), Keyed: true}
}

// choose shows opts and returns the chosen key. def is the answer Enter gives
// in numbered mode ("" = none, and the prompt then takes free text as before);
// a numeric def makes the numbered prompt read a number, as askInt did.
// ok=false means the terminal closed, or Ctrl-C.
func (p *Process) choose(sc *bufio.Scanner, prompt, def string, opts []opt) (string, bool) {
	return p.chooseAs(sc, prompt, def, opts, false)
}

// chooseTight is choose for the few lists that have always put their prompt
// straight under the last line, with no blank line between.
func (p *Process) chooseTight(sc *bufio.Scanner, prompt, def string, opts []opt) (string, bool) {
	return p.chooseAs(sc, prompt, def, opts, true)
}

func (p *Process) chooseAs(sc *bufio.Scanner, prompt, def string, opts []opt, tight bool) (string, bool) {
	if p.cursor && p.tty != nil {
		return p.chooseCursor(sc, prompt, def, opts)
	}
	for _, o := range opts {
		p.printf("%s\n", o.Text)
		if o.Note != "" {
			p.printf("%s\n", o.Note)
		}
	}
	if !tight {
		p.printf("\n")
	}
	if def == "" {
		return p.ask(sc, prompt)
	}
	d, err := strconv.Atoi(def)
	if err != nil {
		v, ok := p.ask(sc, fmt.Sprintf("%s [%s]", prompt, def))
		if ok && v == "" {
			v = def
		}
		return v, ok
	}
	n, ok := p.askInt(sc, prompt, d)
	return strconv.FormatInt(n, 10), ok
}

// chooseID is choose for a list of rows keyed by id: the "Open … by id"
// prompts. 0 = back.
func (p *Process) chooseID(sc *bufio.Scanner, prompt string, rows []opt) (int64, bool) {
	if p.cursor && p.tty != nil {
		rows = append(rows, opt{Key: "0", Text: "   ← Back"})
	}
	k, ok := p.choose(sc, prompt, "0", rows)
	if !ok {
		return 0, false
	}
	n, _ := strconv.ParseInt(k, 10, 64)
	return n, true
}

// keyBack is the cursor-mode Back row of a list where 0 is a real choice
// ("every account", "clear it"), so Esc can never quietly pick it.
const keyBack = "back"

// chooseWithZero is chooseID for a list where 0 MEANS something. back=true
// when the operator backed out (cursor mode only: in numbered mode 0 has
// always been that choice, and closing the terminal the only way out).
func (p *Process) chooseWithZero(sc *bufio.Scanner, prompt string, rows []opt) (id int64, back, ok bool) {
	return p.chooseWithZeroAs(sc, prompt, rows, false)
}

func (p *Process) chooseWithZeroTight(sc *bufio.Scanner, prompt string, rows []opt) (id int64, back, ok bool) {
	return p.chooseWithZeroAs(sc, prompt, rows, true)
}

func (p *Process) chooseWithZeroAs(sc *bufio.Scanner, prompt string, rows []opt, tight bool) (id int64, back, ok bool) {
	if p.cursor && p.tty != nil {
		rows = append(rows, opt{Key: keyBack, Text: "   ← Back"})
	}
	k, ok := p.chooseAs(sc, prompt, "0", rows, tight)
	if !ok {
		return 0, false, false
	}
	if k == keyBack {
		return 0, true, true
	}
	n, _ := strconv.ParseInt(k, 10, 64)
	return n, false, true
}

// ── the cursor ─────────────────────────────────────────────────────────────

const (
	escClearLine = "\x1b[2K"
	escClearDown = "\x1b[J"
	escHide      = "\x1b[?25l"
	escShow      = "\x1b[?25h"
	escReverse   = "\x1b[7m"
	escDim       = "\x1b[2m"
	escReset     = "\x1b[0m"
)

// keySource reads key presses. The terminal one puts stdin in raw mode for
// the length of one choice; a test hands in scripted keys.
type keySource interface {
	begin() error
	next() (string, error)
	end()
}

type ttyKeys struct {
	f       *os.File
	state   *term.State
	pending string // bytes read but not yet handed out as keys
}

func (k *ttyKeys) begin() error {
	st, err := term.MakeRaw(int(k.f.Fd()))
	if err != nil {
		return err
	}
	k.state = st
	return nil
}

// next returns one key press. A terminal sends an arrow key's bytes in one
// write, which is how a lone Esc is told apart from the start of an arrow. But
// one read can also hold several keys — fast typing, a paste, "m" and Enter —
// so a read is split into keys, and the rest kept for the next call.
func (k *ttyKeys) next() (string, error) {
	if k.pending == "" {
		buf := make([]byte, 64)
		n, err := k.f.Read(buf)
		if err != nil {
			return "", err
		}
		k.pending = string(buf[:n])
	}
	key := splitKey(k.pending)
	k.pending = k.pending[len(key):]
	return key, nil
}

// splitKey takes the first key off the front of s: an escape sequence
// (ESC [ … final byte, or ESC O x), a lone ESC, or one character.
func splitKey(s string) string {
	if s == "" {
		return ""
	}
	if s[0] == 0x1b {
		if len(s) >= 3 && s[1] == 'O' {
			return s[:3]
		}
		if len(s) >= 2 && s[1] == '[' {
			for i := 2; i < len(s); i++ {
				if c := s[i]; c >= 0x40 && c <= 0x7e { // the sequence's final byte
					return s[:i+1]
				}
			}
			return s
		}
		return s[:1]
	}
	_, size := utf8.DecodeRuneInString(s)
	return s[:size]
}

func (k *ttyKeys) end() {
	if k.state != nil {
		_ = term.Restore(int(k.f.Fd()), k.state)
		k.state = nil
	}
}

// screen size, with a safe answer when the terminal will not say.
func (p *Process) termSize() (w, h int) {
	w, h = 80, 24
	if f, ok := p.out.(*os.File); ok {
		if cw, ch, err := term.GetSize(int(f.Fd())); err == nil && cw > 20 && ch > 8 {
			w, h = cw, ch
		}
	}
	return w, h
}

func (p *Process) chooseCursor(sc *bufio.Scanner, prompt, def string, opts []opt) (string, bool) {
	keys := p.keys
	if keys == nil {
		keys = &ttyKeys{f: p.tty}
	}
	if len(opts) == 0 {
		return "", true
	}
	// Single-character keys choose at once; a list of ids is searched instead.
	// A menu whose keys are all one or two characters (the main menu, with
	// "10 Operators", PO 2026-09-21) is keyed too: the typed characters build
	// the key, and it is chosen as soon as no longer key could follow — "1"
	// waits (it may be 10), "10" or "2" choose at once, Enter takes the
	// highlighted "1".
	instant, short, marked := true, true, false
	for _, o := range opts {
		n := utf8.RuneCountInString(o.Key)
		if n != 1 {
			instant = false
		}
		if n > 2 {
			short = false
		}
		marked = marked || o.Keyed
	}
	keyed := !instant && short && marked
	cur := 0
	for cur < len(opts)-1 && opts[cur].Skip {
		cur++
	}
	for i, o := range opts {
		if def != "" && def != "0" && o.Key == def { // never start on Back
			cur = i
		}
	}
	if err := keys.begin(); err != nil {
		// No raw mode after all: fall back to typing, for this choice.
		p.cursor = false
		defer func() { p.cursor = true }()
		return p.choose(sc, prompt, def, opts)
	}
	defer keys.end()

	var search string
	drawn := 0
	top := 0
	draw := func() {
		w, h := p.termSize()
		room := h - 6
		if room < 5 {
			room = 5
		}
		height := func(i int) int {
			if opts[i].Note != "" {
				return 2
			}
			return 1
		}
		// Keep the highlight on screen.
		if cur < top {
			top = cur
		}
		for {
			used := 0
			for i := top; i <= cur; i++ {
				used += height(i)
			}
			if used <= room || top == cur {
				break
			}
			top++
		}
		var b strings.Builder
		if drawn > 0 {
			fmt.Fprintf(&b, "\x1b[%dA", drawn)
		}
		b.WriteString("\r" + escClearDown)
		lines := 0
		if top > 0 {
			b.WriteString(escDim + fmt.Sprintf("   ↑ %d more", top) + escReset + "\r\n")
			lines++
		}
		used, last := 0, top
		for i := top; i < len(opts) && used+height(i) <= room; i++ {
			text := fit(opts[i].Text, w-1)
			if i == cur {
				r := []rune(text)
				if len(r) > 1 && r[1] == ' ' {
					r[1] = '›'
				}
				text = string(r)
				pad := w - 1 - utf8.RuneCountInString(text)
				if pad > 0 {
					text += strings.Repeat(" ", pad)
				}
				b.WriteString(escReverse + text + escReset + "\r\n")
			} else {
				b.WriteString(text + "\r\n")
			}
			lines++
			if opts[i].Note != "" {
				b.WriteString(escDim + fit(opts[i].Note, w-1) + escReset + "\r\n")
				lines++
			}
			used += height(i)
			last = i
		}
		if rest := len(opts) - 1 - last; rest > 0 {
			b.WriteString(escDim + fmt.Sprintf("   ↓ %d more", rest) + escReset + "\r\n")
			lines++
		}
		help := "   ↑↓ move · Enter choose · Esc back"
		if instant {
			help += " · or press its number"
		} else if keyed {
			help += " · or type its number"
			if search != "" {
				help += ": " + search
			}
		} else {
			help += " · type to find"
			if search != "" {
				help += ": " + search
			}
		}
		b.WriteString("\r\n" + escDim + fit(help, w-1) + escReset + "\r\n")
		lines += 2
		p.printf("%s", b.String())
		drawn = lines
	}
	clear := func() {
		if drawn > 0 {
			p.printf("\x1b[%dA\r%s", drawn, escClearDown)
		}
	}
	chosen := func(o opt) (string, bool) {
		clear()
		p.printf("%s%s\r\n", "  › ", strings.TrimSpace(firstLine(o.Text)))
		return o.Key, true
	}
	back := func() (string, bool) {
		for _, o := range opts {
			if o.Key == keyBack {
				clear()
				return keyBack, true
			}
		}
		for _, o := range opts {
			if o.Key == "0" {
				clear()
				return "0", true
			}
		}
		clear()
		return "", false // a choice with no Back: Esc abandons it
	}
	find := func(from int) {
		q := strings.ToLower(search)
		for n := 0; n < len(opts); n++ {
			i := (from + n) % len(opts)
			if !opts[i].Skip && strings.Contains(strings.ToLower(opts[i].Text), q) {
				cur = i
				return
			}
		}
	}

	p.printf("%s", escHide)
	defer p.printf("%s", escShow)
	draw()
	for {
		k, err := keys.next()
		if err != nil {
			clear()
			p.ended = true
			return "", false
		}
		switch k {
		case "\x1b[A", "\x1bOA", "\x10": // up, Ctrl-P
			cur = step(opts, cur, -1)
		case "\x1b[B", "\x1bOB", "\x0e": // down, Ctrl-N
			cur = step(opts, cur, +1)
		case "\x1b[H", "\x1b[1~", "\x1bOH":
			cur = step(opts, -1, +1)
		case "\x1b[F", "\x1b[4~", "\x1bOF":
			cur = step(opts, len(opts), -1)
		case "\x1b[5~": // page up
			cur -= 10
			if cur < 0 {
				cur = 0
			}
		case "\x1b[6~": // page down
			cur += 10
			if cur > len(opts)-1 {
				cur = len(opts) - 1
			}
		case "\r", "\n":
			if opts[cur].Skip {
				break
			}
			return chosen(opts[cur])
		case "\x03", "\x04": // Ctrl-C, Ctrl-D: leave the console
			clear()
			p.ended = true
			return "", false
		case "\x1b", "\x1b[D":
			if search != "" {
				search = ""
				break
			}
			return back()
		case "\x7f", "\x08": // backspace
			if search != "" {
				_, size := utf8.DecodeLastRuneInString(search)
				search = search[:len(search)-size]
				break
			}
			return back()
		default:
			r, _ := utf8.DecodeRuneInString(k)
			if len(k) == 0 || !unicode.IsPrint(r) || strings.HasPrefix(k, "\x1b") {
				break
			}
			if keyed {
				if cur2, done, ok := keyedPick(opts, search+k); ok {
					cur, search = cur2, search+k
					if done {
						return chosen(opts[cur])
					}
				} else if cur2, done, ok := keyedPick(opts, k); ok {
					cur, search = cur2, k // not a continuation: start a new key
					if done {
						return chosen(opts[cur])
					}
				}
				break
			}
			if instant {
				if (k == "j" || k == "k") && !hasKey(opts, k) {
					if k == "j" {
						cur = step(opts, cur, +1)
					} else {
						cur = step(opts, cur, -1)
					}
					break
				}
				for _, o := range opts {
					if strings.EqualFold(o.Key, k) && !o.Skip {
						return chosen(o)
					}
				}
				break
			}
			search += k
			find(cur)
		}
		draw()
	}
}

// keyedPick finds typed in a keyed menu: the line whose key it is (or the
// first whose key starts with it), and whether it can be chosen now — it is a
// whole key and no longer key starts with it. ok=false: nothing matches.
func keyedPick(opts []opt, typed string) (cur int, done, ok bool) {
	exact, first, longer := -1, -1, false
	for i, o := range opts {
		if o.Skip {
			continue
		}
		switch {
		case strings.EqualFold(o.Key, typed):
			exact = i
		case len(o.Key) > len(typed) && strings.EqualFold(o.Key[:len(typed)], typed):
			longer = true
			if first < 0 {
				first = i
			}
		}
	}
	switch {
	case exact >= 0:
		return exact, !longer, true
	case first >= 0:
		return first, false, true
	}
	return 0, false, false
}

// step moves the highlight one choosable line, stepping over lines that only
// explain.
func step(opts []opt, cur, dir int) int {
	for i := cur + dir; i >= 0 && i < len(opts); i += dir {
		if !opts[i].Skip {
			return i
		}
	}
	// Nothing choosable that way: stay put, and never off the list.
	if cur < 0 {
		return 0
	}
	if cur >= len(opts) {
		return len(opts) - 1
	}
	return cur
}

func hasKey(opts []opt, k string) bool {
	for _, o := range opts {
		if o.Key == k {
			return true
		}
	}
	return false
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// fit cuts a line to the screen width, so it never wraps: a wrapped line
// would throw off the redraw, which counts lines to move back up.
func fit(s string, w int) string {
	if w < 4 || utf8.RuneCountInString(s) <= w {
		return s
	}
	r := []rune(s)
	return string(r[:w-1]) + "…"
}

// pickMode decides how choices are made. Cursor keys need a real terminal on
// both ends; anything else — a pipe, a file, a test — gets numbered menus, so
// scripts written against the console keep working unchanged.
// YAKU_CONSOLE_MENUS=numbers (in yakugate.config or the environment) asks for the
// numbered menus on a terminal too.
func (p *Process) pickMode() {
	in, ok1 := p.in.(*os.File)
	out, ok2 := p.out.(*os.File)
	if !ok1 || !ok2 || !term.IsTerminal(int(in.Fd())) || !term.IsTerminal(int(out.Fd())) {
		return
	}
	p.tty = in
	p.cursor = !strings.EqualFold(strings.TrimSpace(os.Getenv("YAKU_CONSOLE_MENUS")), "numbers")
}

// menusLabel is the main menu's switch between the two modes.
func (p *Process) menusLabel() string {
	if p.cursor {
		return "Switch to numbered menus (type the number, then Enter)"
	}
	return "Switch to cursor menus (arrow keys and Enter)"
}
