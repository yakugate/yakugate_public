package main

import "strings"

// commandWord is the command a command line names: the first argument that is
// not the settings option (--config FILE, -config FILE, -c FILE,
// --config=FILE, -config=FILE) or its value. at is its index, -1 if none.
//
// Every maintenance command is picked by this, by POSITION (2026-09-21, bug
// 13). The migrate and backup parsers used to take their word from anywhere
// in the line, so a value that happened to spell it — a file called
// "migrate", `config set YAKU_ENV=migrate` — ran the wrong command.
func commandWord(args []string) (word string, at int) {
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "-c" || a == "--config" || a == "-config":
			i++ // its value is never the command
		case strings.HasPrefix(a, "--config=") || strings.HasPrefix(a, "-config="):
		default:
			return a, i
		}
	}
	return "", -1
}

// without is args minus the element at i.
func without(args []string, i int) []string {
	out := make([]string, 0, len(args))
	out = append(out, args[:i]...)
	return append(out, args[i+1:]...)
}

// configOption reads a settings option at args[*i] in any of its forms,
// advancing *i past a separate value. ok=false: not a settings option.
func configOption(args []string, i *int) (path string, ok bool, err error) {
	a := args[*i]
	switch {
	case a == "-c" || a == "--config" || a == "-config":
		if *i+1 >= len(args) || args[*i+1] == "" {
			return "", true, errNeedsFile(a)
		}
		*i++
		return args[*i], true, nil
	case strings.HasPrefix(a, "--config=") || strings.HasPrefix(a, "-config="):
		v := a[strings.IndexByte(a, '=')+1:]
		if v == "" {
			return "", true, errNeedsFile(a)
		}
		return v, true, nil
	}
	return "", false, nil
}

type errNeedsFile string

func (e errNeedsFile) Error() string { return string(e) + " needs a file path" }
