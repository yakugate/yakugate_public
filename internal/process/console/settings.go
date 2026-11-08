package console

import (
	"bufio"
	"fmt"
	"os"

	"github.com/yakugate/yakugate/internal/platform/config"
)

// Settings: yakugate.config from the console (2026-09-21). [SCOPE P14-SETTINGS]
//
// The same editor as `yakugate config` (internal/platform/config/edit.go):
// only the chosen line changes, comments stay, the old file is kept as .bak,
// and a name the gateway does not read is refused. A new name the gateway
// does not know yet is `yakugate config set --new` only — a menu is the wrong
// place to invent settings.
//
// Unlike everything else in the console, a change here does NOT reach a
// running gateway by itself: each role reads the file once, at start. Every
// screen says so.

// settingsPath is the file this console was started with.
func (p *Process) settingsPath() string {
	if p.settingsFile != "" {
		return p.settingsFile
	}
	return config.LoadedFile()
}

func (p *Process) settingsMenu(sc *bufio.Scanner) {
	path := p.settingsPath()
	if path == "" {
		p.printf("\n  This console was started without a settings file (environment only), so\n")
		p.printf("  there is no file to change. Make one with:\n")
		p.printf("    yakugate --config ./%s config set NAME=VALUE\n", config.FileName)
		return
	}
	for {
		p.printf("\n  Settings — %s\n", path)
		p.printf("  Ports, databases and keys. Each role reads this file once, at start:\n")
		p.printf("  a change here needs the roles restarted.\n\n")
		choice, ok := p.choose(sc, "Choose", "", []opt{
			item("1", "List the settings in the file"),
			item("2", "Change a setting"),
			item("3", "Unset a setting — the default applies again"),
			// A setting the gateway reads but the file has never named was
			// invisible here: option 1 lists only what is IN the file, and
			// option 2 asks you to type a name you would have to know
			// already. YAKU_DEPLOYMENT_ID is exactly that case — nothing in
			// a fresh file, and the first thing a second gateway needs
			// (PO 2026-09-24).
			item("4", "Browse every setting the gateway reads"),
			item("0", "Back"),
		})
		if !ok {
			return
		}
		switch choice {
		case "1":
			p.listSettings(path)
		case "2":
			p.changeSetting(sc, path)
		case "3":
			p.unsetSetting(sc, path)
		case "4":
			p.browseSettings(sc, path)
		case "0", "":
			return
		default:
			p.errf("  %q is not one of the choices.\n", choice)
		}
	}
}

func (p *Process) listSettings(path string) {
	settings, err := config.ReadSettings(path)
	if err != nil {
		p.errf("\n  %v\n", err)
	}
	if len(settings) == 0 {
		p.printf("\n  No settings in the file; every one is at its default.\n")
		return
	}
	p.printf("\n   %-5s %-30s %s\n", "line", "setting", "value")
	for _, s := range settings {
		v := orDash(config.Shown(s.Name, s.Value))
		note := ""
		if s.Shadowed {
			note = "   ← ignored: an earlier line wins"
		} else if ev, ok := os.LookupEnv(s.Name); ok && ev != s.Value {
			note = "   ← the environment overrides it"
		}
		p.printf("   %-5d %-30s %s%s\n", s.Line, trim(s.Name, 30), trim(v, 44), note)
		if d := config.Describe(s.Name); d != "" {
			p.printf("         %s\n", d)
		}
	}
	p.printf("\n  Passwords and keys are hidden; `yakugate config get NAME --show-secrets`\n")
	p.printf("  shows one.\n")
}

// browseSettings lists EVERY setting the gateway reads — not only the ones
// the file happens to name — and lets one be picked and changed.
//
// config.KnownNames() has always been there (it is what refuses a typo); the
// console simply never offered it, so a setting at its default could only be
// changed by somebody who already knew its exact name.
func (p *Process) browseSettings(sc *bufio.Scanner, path string) {
	names := config.KnownNames()
	opts := make([]opt, 0, len(names)+1)
	for _, n := range names {
		v, _, had := current(path, n)
		note := "default"
		if had {
			note = orDash(config.Shown(n, v))
		}
		// The name is the KEY (so picking one returns it); the row adds
		// what it is set to and, in a few words, what it does (Saleem
		// 2026-10-01) — typing searches all three, so "port" finds every
		// listener.
		opts = append(opts, opt{Key: n, Text: fmt.Sprintf("   %-32s %-16s %s", n, trim(note, 16), config.Describe(n))})
	}
	opts = append(opts, item("0", "Back"))
	p.printf("\n  Every setting the gateway reads. Type to search (a name or a word\n")
	p.printf("  like \"port\"); one shown as %s is not in the file and is using\n", "default")
	p.printf("  its built-in value.\n\n")
	p.printf("   %-32s %-16s %s\n", "setting", "now", "what it does")
	name, ok := p.choose(sc, "Setting", "", opts)
	if !ok || name == "" || name == "0" {
		return
	}
	p.changeNamed(sc, path, name)
}

// current is a setting's live value in the file, and whether it has one.
func current(path, name string) (string, int, bool) {
	settings, _ := config.ReadSettings(path)
	for _, s := range settings {
		if s.Name == name && !s.Shadowed {
			return s.Value, s.Line, true
		}
	}
	return "", 0, false
}

// knownOrInFile is the console's name check: a name the gateway reads, or one
// already in the file. Anything else is refused with the likely meaning.
func (p *Process) knownOrInFile(path, name string) bool {
	if config.Known(name) {
		return true
	}
	if _, _, ok := current(path, name); ok {
		return true
	}
	msg := fmt.Sprintf("  The gateway does not read a setting called %s.", name)
	if s := config.Suggest(name); s != "" {
		msg += fmt.Sprintf(" Did you mean %s?", s)
	}
	p.printf("%s\n", msg)
	return false
}

func (p *Process) changeSetting(sc *bufio.Scanner, path string) {
	p.printf("\n  Which setting? Its full name, e.g. YAKU_API_ADDR. 0 to go back.\n")
	name, ok := p.askName(sc, "Setting")
	if !ok || name == "" || !p.knownOrInFile(path, name) {
		return
	}
	p.changeNamed(sc, path, name)
}

// changeNamed edits one setting whose name is already settled — typed into
// "Change a setting", or picked out of the browser above.
func (p *Process) changeNamed(sc *bufio.Scanner, path, name string) {
	if d := config.Describe(name); d != "" {
		p.printf("\n  %s — %s\n", name, d)
	}
	old, line, had := current(path, name)
	if had {
		p.printf("  Now: %s  (line %d)\n", orDash(config.Shown(name, old)), line)
	} else {
		p.printf("  Not set in the file: the gateway uses its default.\n")
	}
	p.printf("\n  Enter on its own leaves it as it is. Here 0 is a value, not \"back\".\n")
	v, ok := p.ask(sc, "New value")
	if !ok || v == "" {
		p.printf("  Left alone.\n")
		return
	}
	if err := config.CheckValue(name, v); err != nil {
		p.errf("\n  Not saved: %v\n", err)
		return
	}
	p.printf("\n  %s: %s → %s\n", name, orDash(config.Shown(name, old)), config.Shown(name, v))
	if !p.askYes(sc, "Save it", false) {
		p.printf("  Left alone.\n")
		return
	}
	p.applySettings(path, []config.Change{{Name: name, Value: v}})
}

func (p *Process) unsetSetting(sc *bufio.Scanner, path string) {
	p.printf("\n  Which setting? 0 to go back.\n")
	name, ok := p.askName(sc, "Setting")
	if !ok || name == "" {
		return
	}
	old, line, had := current(path, name)
	if !had {
		p.printf("  %s is not set in the file.\n", name)
		return
	}
	p.printf("  Now: %s  (line %d). The line is commented out, not deleted.\n", orDash(config.Shown(name, old)), line)
	if !p.askYes(sc, "Unset it", false) {
		p.printf("  Left alone.\n")
		return
	}
	p.applySettings(path, []config.Change{{Name: name, Unset: true}})
}

func (p *Process) applySettings(path string, changes []config.Change) {
	outs, err := config.ApplyChanges(path, changes, config.EditOptions{})
	if err != nil {
		p.errf("\n  Not saved: %v\n", err)
		return
	}
	for _, o := range outs {
		switch o.How {
		case "unchanged", "not set":
			p.printf("\n  %s was already that — nothing changed.\n", o.Name)
			return
		case "unset":
			p.okf("\n  Unset %s.\n", o.Name)
		default:
			p.okf("\n  Saved %s at line %d.\n", o.Name, o.Line)
		}
	}
	p.printf("  The file before the change is %s.bak.\n", path)
	p.warnf("  Restart the roles for it to take effect — including this console.\n")
}
