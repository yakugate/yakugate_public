package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/yakugate/yakugate/internal/platform/config"
)

// `yakugate config` shows and changes yakugate.config (2026-09-21).
//
//	yakugate [--config FILE] config [list] [--all] [--show-secrets]
//	yakugate [--config FILE] config get NAME [--show-secrets]
//	yakugate [--config FILE] config set NAME=VALUE... [--new]
//	yakugate [--config FILE] config unset NAME...
//	yakugate [--config FILE] config path
//
// It works on the same file a role would read (config.FindFile), and never
// loads it into the environment: what the environment holds is shown beside
// the file, because the environment wins at start and an operator changing
// the file needs to know when that change will not count.
//
// The editing rules live in internal/platform/config/edit.go. Running roles
// are not touched: each reads yakugate.config once, at start.
const cmdConfig = "config"

type configTool struct {
	cfgPath     string
	action      string // list, get, set, unset, path
	args        []string
	all         bool
	showSecrets bool
	allowNew    bool
}

// parseConfigTool returns nil when the command word is not "config". The
// command word is the first argument that is not an option or an option's
// value — never just any argument spelled "config", so `config set
// X=migrate` or a role named in a value cannot be mistaken for a command.
func parseConfigTool(args []string) (*configTool, error) {
	var t configTool
	word, i := commandWord(args)
	if word != cmdConfig {
		return nil, nil
	}
	// A settings option before the command still counts.
	for k := 0; k < i; k++ {
		if path, ok, _ := configOption(args, &k); ok {
			t.cfgPath = path
		}
	}
	for i++; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--all":
			t.all = true
		case a == "--show-secrets":
			t.showSecrets = true
		case a == "--new":
			t.allowNew = true
		case a == "-c" || a == "--config" || a == "-config":
			if i+1 >= len(args) || args[i+1] == "" {
				return nil, fmt.Errorf("%s needs a file path", a)
			}
			i++
			t.cfgPath = args[i]
		case strings.HasPrefix(a, "--config=") || strings.HasPrefix(a, "-config="):
			t.cfgPath = a[strings.IndexByte(a, '=')+1:]
		case strings.HasPrefix(a, "-") && t.action != "set":
			return nil, fmt.Errorf("config: unknown option %q", a)
		case t.action == "":
			switch a {
			case "list", "get", "set", "unset", "path":
				t.action = a
			default:
				return nil, fmt.Errorf("config: %q is not list, get, set, unset or path", a)
			}
		default:
			t.args = append(t.args, a)
		}
	}
	if t.action == "" {
		t.action = "list"
	}
	switch t.action {
	case "list", "path":
		if len(t.args) > 0 {
			return nil, fmt.Errorf("config %s takes no names", t.action)
		}
	case "get":
		if len(t.args) != 1 {
			return nil, errors.New("config get takes one setting name")
		}
	case "unset":
		if len(t.args) == 0 {
			return nil, errors.New("config unset needs at least one setting name")
		}
	case "set":
		if len(t.args) == 0 {
			return nil, errors.New("config set needs NAME=VALUE")
		}
		for _, a := range t.args {
			if !strings.Contains(a, "=") {
				return nil, fmt.Errorf("config set: %q is not NAME=VALUE (write NAME= for an empty value)", a)
			}
		}
	}
	if t.action != "set" && t.allowNew {
		return nil, errors.New("--new goes with config set")
	}
	return &t, nil
}

func (t *configTool) run() int { return t.runTo(os.Stdout, os.Stderr) }

func (t *configTool) runTo(out, errw io.Writer) int {
	path := config.FindFile(t.cfgPath)
	fail := func(f string, a ...any) int {
		fmt.Fprintln(errw, red(errw, fmt.Sprintf("yakugate: config: "+f, a...)))
		return 1
	}
	if t.action == "path" {
		if path == "" {
			fmt.Fprintln(errw, red(errw, noFileHelp()))
			return 1
		}
		fmt.Fprintln(out, path)
		return 0
	}

	exists := path != "" && fileExists(path)
	var settings []config.Setting
	if exists {
		var err error
		if settings, err = config.ReadSettings(path); err != nil {
			// Still show what can be shown: this is how an operator finds
			// the bad line.
			fmt.Fprintln(errw, red(errw, fmt.Sprintf("yakugate: config: %v", err)))
			if t.action != "list" {
				return 1
			}
		}
	}

	switch t.action {
	case "list":
		if path == "" {
			fmt.Fprintln(errw, red(errw, noFileHelp()))
		} else if !exists {
			fmt.Fprintln(errw, red(errw, fmt.Sprintf("yakugate: config: %s does not exist", path)))
		}
		t.list(out, path, exists, settings)
		return 0

	case "get":
		name := t.args[0]
		show := func(v string) string {
			if t.showSecrets {
				return v
			}
			return config.Shown(name, v)
		}
		inFile, fileLine := "", 0
		for _, s := range settings {
			if s.Name == name && !s.Shadowed {
				inFile, fileLine = s.Value, s.Line
			}
		}
		if ev, ok := os.LookupEnv(name); ok {
			fmt.Fprintln(out, show(ev))
			if fileLine > 0 && ev != inFile {
				fmt.Fprintln(errw, yellow(errw, fmt.Sprintf("(from the environment, which wins; the file says %s on line %d)", show(inFile), fileLine)))
			} else {
				fmt.Fprintln(errw, "(from the environment)")
			}
			return 0
		}
		if fileLine == 0 {
			msg := fmt.Sprintf("%s is not set", name)
			if !config.Known(name) {
				msg = fmt.Sprintf("the gateway does not read a setting called %s", name)
				if s := config.Suggest(name); s != "" {
					msg += fmt.Sprintf(" — did you mean %s?", s)
				}
			} else {
				msg += "; the gateway uses its default"
				if d := config.Describe(name); d != "" {
					msg += ". " + d
				}
			}
			return fail("%s", msg)
		}
		fmt.Fprintln(out, show(inFile))
		return 0

	case "set", "unset":
		if path == "" {
			fmt.Fprintln(errw, red(errw, noFileHelp()))
			fmt.Fprintln(errw, "  To make a new one: yakugate --config ./"+config.FileName+" config set NAME=VALUE")
			return 1
		}
		if t.action == "unset" && !exists {
			return fail("%s does not exist", path)
		}
		var changes []config.Change
		for _, a := range t.args {
			if t.action == "unset" {
				changes = append(changes, config.Change{Name: a, Unset: true})
				continue
			}
			k, v, _ := strings.Cut(a, "=")
			changes = append(changes, config.Change{Name: strings.TrimSpace(k), Value: v})
		}
		outs, err := config.ApplyChanges(path, changes, config.EditOptions{AllowNew: t.allowNew})
		if err != nil {
			return fail("%v", err)
		}
		t.report(out, path, outs)
		return 0
	}
	return fail("unknown action %q", t.action)
}

func (t *configTool) list(out io.Writer, path string, exists bool, settings []config.Setting) {
	show := func(name, v string) string {
		switch {
		case v == "":
			return "(empty)"
		case t.showSecrets:
			return v
		}
		return config.Shown(name, v)
	}
	type row struct{ name, value, from string }
	var rows []row
	inFile := map[string]bool{}
	for _, s := range settings {
		if s.Shadowed {
			rows = append(rows, row{s.Name, show(s.Name, s.Value), fmt.Sprintf("line %d — IGNORED, line %d comes first", s.Line, firstLine(settings, s.Name))})
			continue
		}
		inFile[s.Name] = true
		from := fmt.Sprintf("line %d", s.Line)
		if ev, ok := os.LookupEnv(s.Name); ok && ev != s.Value {
			rows = append(rows, row{s.Name, show(s.Name, ev), fmt.Sprintf("environment — wins over line %d (%s)", s.Line, show(s.Name, s.Value))})
			continue
		}
		rows = append(rows, row{s.Name, show(s.Name, s.Value), from})
	}
	var envOnly []string
	for _, kv := range os.Environ() {
		k, _, _ := strings.Cut(kv, "=")
		if strings.HasPrefix(k, "YAKU_") && !inFile[k] {
			envOnly = append(envOnly, k)
		}
	}
	sort.Strings(envOnly)
	for _, k := range envOnly {
		rows = append(rows, row{k, show(k, os.Getenv(k)), "environment"})
	}
	if t.all {
		for _, k := range config.KnownNames() {
			if _, ok := os.LookupEnv(k); ok || inFile[k] {
				continue
			}
			// What it does, in a line: the sample file lists only the
			// settings an install decides, so this is where the rest are read.
			what := "not set — the default"
			if d := config.Describe(k); d != "" {
				what = "not set — " + d
			}
			rows = append(rows, row{k, "—", what})
		}
	}

	if path != "" && exists {
		fmt.Fprintf(out, "Settings file: %s\n\n", path)
	}
	if len(rows) == 0 {
		fmt.Fprintln(out, "No settings.")
		return
	}
	w := len("SETTING")
	for _, r := range rows {
		w = max(w, len(r.name))
	}
	vw := len("VALUE")
	for _, r := range rows {
		vw = max(vw, min(len([]rune(r.value)), 48))
	}
	fmt.Fprintf(out, "  %-*s  %-*s  %s\n", w, "SETTING", vw, "VALUE", "WHERE")
	for _, r := range rows {
		v := r.value
		if rs := []rune(v); len(rs) > 48 {
			v = string(rs[:47]) + "…"
		}
		from := r.from
		if strings.Contains(from, "IGNORED") || strings.HasPrefix(from, "environment —") {
			from = yellow(out, from)
		}
		fmt.Fprintf(out, "  %-*s  %-*s  %s\n", w, r.name, vw, v, from) // fmt pads by rune, not byte
	}
	if !t.all {
		fmt.Fprintf(out, "\n%d settings the gateway reads are not set here; --all lists them with the rest, and what each does.\n", countUnset(inFile))
	}
}

func (t *configTool) report(out io.Writer, path string, outs []config.Outcome) {
	show := func(name, v string) string {
		if v == "" {
			return "(empty)"
		}
		return config.Shown(name, v)
	}
	changed := false
	for _, o := range outs {
		switch o.How {
		case "changed":
			fmt.Fprintln(out, green(out, fmt.Sprintf("  %s: %s → %s  (line %d)", o.Name, show(o.Name, o.Old), show(o.Name, o.New), o.Line)))
		case "uncommented":
			fmt.Fprintln(out, green(out, fmt.Sprintf("  %s: set to %s  (line %d, where it was commented out)", o.Name, show(o.Name, o.New), o.Line)))
		case "added":
			fmt.Fprintln(out, green(out, fmt.Sprintf("  %s: set to %s  (added at line %d)", o.Name, show(o.Name, o.New), o.Line)))
		case "unset":
			fmt.Fprintln(out, green(out, fmt.Sprintf("  %s: unset (was %s; the line is commented out, the default applies)", o.Name, show(o.Name, o.Old))))
		case "unchanged":
			fmt.Fprintf(out, "  %s: already %s — nothing to do\n", o.Name, show(o.Name, o.New))
		case "not set":
			fmt.Fprintf(out, "  %s: was not set — nothing to do\n", o.Name)
		}
		if o.How != "unchanged" && o.How != "not set" {
			changed = true
		}
		if _, ok := os.LookupEnv(o.Name); ok {
			fmt.Fprintln(out, yellow(out, fmt.Sprintf("    Note: %s is also set in the environment, which wins over the file.", o.Name)))
		}
	}
	if !changed {
		return
	}
	fmt.Fprintln(out, green(out, fmt.Sprintf("\nSaved %s (the file before this change is %s.bak).", path, path)))
	fmt.Fprintln(out, yellow(out, "Running roles keep their old settings until restarted: each reads the file once, at start."))
}

func firstLine(settings []config.Setting, name string) int {
	for _, s := range settings {
		if s.Name == name {
			return s.Line
		}
	}
	return 0
}

func countUnset(inFile map[string]bool) int {
	n := 0
	for _, k := range config.KnownNames() {
		if _, ok := os.LookupEnv(k); !ok && !inFile[k] {
			n++
		}
	}
	return n
}

func fileExists(p string) bool {
	info, err := os.Stat(p)
	return err == nil && !info.IsDir()
}

func noFileHelp() string {
	var b strings.Builder
	b.WriteString("yakugate: config: no settings file found. Looked for " + config.FileName + " in:\n")
	for _, p := range config.DefaultPaths() {
		b.WriteString("    " + p + "\n")
	}
	b.WriteString("  Name one with --config FILE.")
	return b.String()
}
