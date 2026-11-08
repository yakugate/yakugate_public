package config

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
)

// FileName is the gateway's settings file (yakugate.config since 2026-10-02,
// Saleem; sms.config before, .env before that). It holds the same NAME=value
// lines throughout, so every setting keeps its YAKU_* name.
const FileName = "yakugate.config"

// OldFileName is the name the settings file had from 2026-09-14 to
// 2026-10-02, when the gateway went live with it. It is still read, in each
// place the gateway looks, when there is no yakugate.config there — a live
// install keeps starting untouched, and is told to rename it.
const OldFileName = "sms.config"

// LegacyFileName is the oldest name, still read when no settings file is
// found anywhere, so a deployment that has not renamed its file keeps
// starting.
const LegacyFileName = ".env"

// fileSearched is set once LoadFile has run, so Load does not search again.
var fileSearched bool

// loadedFile is the settings file actually read, or "" when there was none.
// A process that fails to reach a store needs to be able to say WHY in the
// operator's terms — "there is no settings file" beats a driver reporting an
// empty username (2026-09-18).
var loadedFile string

// LoadedFile is the settings file in use, or "" if the gateway is running on
// environment variables alone.
func LoadedFile() string { return loadedFile }

// originalEnv is the environment as it was before LoadFile put the file's
// lines into it.
var originalEnv []string

// OriginalEnviron is the process environment as it was BEFORE the settings
// file was read (2026-09-29). A process started from here — a role under
// `yakugate`, or the gateway started from the console — gets this, not the
// current environment: it reads the file itself, so a change saved to
// sms.config reaches it, which it could not if the file's old values came
// down as environment variables (those win over the file). Before LoadFile
// has run it is the current environment.
func OriginalEnviron() []string {
	if originalEnv == nil {
		return os.Environ()
	}
	return append([]string(nil), originalEnv...)
}

// DefaultPaths lists where the gateway looks for yakugate.config when it is
// not given one, in order. The first file that exists is used; the rest are
// not read. In each place the old name, sms.config, is tried right after it
// (searchPaths).
//
//  1. the folder the gateway was started in
//  2. the folder the gateway binary is in
//  3. the user's config folder (~/.config/yakugate on Linux)
//  4. the system folder (/etc/yakugate; %ProgramData%\yakugate on Windows)
func DefaultPaths() []string {
	var ps []string
	add := func(p string) {
		if p == "" {
			return
		}
		if abs, err := filepath.Abs(p); err == nil {
			p = abs
		}
		for _, have := range ps {
			if have == p {
				return
			}
		}
		ps = append(ps, p)
	}
	add(FileName)
	// Then upwards from the start folder, and upwards from the binary's own
	// folder (2026-09-18). Without this, running ./bin/yakugate from inside
	// bin/ — the most natural thing to do, and where an operator's shell
	// usually is — found no settings at all and the gateway then tried to
	// reach MySQL as user "" with no password. The file that was obviously
	// meant sat one folder up.
	//
	// Bounded and additive: at most a few levels, and it can only find a file
	// where before there was none, so no existing deployment changes.
	addAncestors := func(start string) {
		dir := start
		for i := 0; i < 6; i++ {
			parent := filepath.Dir(dir)
			if parent == dir { // reached the filesystem root
				return
			}
			dir = parent
			add(filepath.Join(dir, FileName))
		}
	}
	if wd, err := os.Getwd(); err == nil {
		addAncestors(wd)
	}
	if exe, err := os.Executable(); err == nil {
		if real, err := filepath.EvalSymlinks(exe); err == nil {
			exe = real
		}
		add(filepath.Join(filepath.Dir(exe), FileName))
		addAncestors(filepath.Dir(exe))
	}
	if dir, err := os.UserConfigDir(); err == nil {
		add(filepath.Join(dir, "yakugate", FileName))
	}
	if runtime.GOOS == "windows" {
		if pd := os.Getenv("ProgramData"); pd != "" {
			add(filepath.Join(pd, "yakugate", FileName))
		}
	} else {
		add(filepath.Join("/etc", "yakugate", FileName))
	}
	return ps
}

// searchPaths is DefaultPaths with the old name tried in each place, right
// after the new one: yakugate.config wins where both are, and a place's old
// file still beats a new one in a later place, so adding a yakugate.config
// under /etc never quietly takes over from a working folder's sms.config.
func searchPaths() []string {
	var out []string
	for _, p := range DefaultPaths() {
		out = append(out, p, filepath.Join(filepath.Dir(p), OldFileName))
	}
	return out
}

// LoadFile finds the settings file and copies its values into the process
// environment, where every part of the gateway reads them.
//
// explicit is the path given with --config. When it is set the file must
// exist: a typed path that is wrong has to stop the start, not quietly fall
// back to some other file. When it is empty the DefaultPaths are tried in
// order, then the old .env in the working folder.
//
// A variable already set in the environment is never overwritten, so an
// operator can override any one line for a single run.
//
// It returns the path it read, or "" when there was no file at all (the
// gateway then runs on environment variables alone, as a container would).
func LoadFile(explicit string) (string, error) {
	if originalEnv == nil {
		originalEnv = os.Environ()
	}
	fileSearched = true
	if explicit != "" {
		p := explicit
		if abs, err := filepath.Abs(p); err == nil {
			p = abs
		}
		if err := readInto(p); err != nil {
			return "", err
		}
		loadedFile = p
		return p, nil
	}
	for _, p := range searchPaths() {
		if !isFile(p) {
			continue
		}
		if err := readInto(p); err != nil {
			return "", err
		}
		loadedFile = p
		return p, nil
	}
	if isFile(LegacyFileName) {
		p := LegacyFileName
		if abs, err := filepath.Abs(p); err == nil {
			p = abs
		}
		if err := readInto(p); err != nil {
			return "", err
		}
		loadedFile = p
		return p, nil
	}
	return "", nil
}

// IsLegacyFile reports whether path has an old name (sms.config or .env), so
// the caller can suggest renaming it to yakugate.config.
func IsLegacyFile(path string) bool {
	b := filepath.Base(path)
	return b == OldFileName || b == LegacyFileName
}

func isFile(p string) bool {
	info, err := os.Stat(p)
	return err == nil && !info.IsDir()
}

var nameRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// readInto reads NAME=value lines into the process environment, for names not
// already set. Blank lines and lines starting with # are skipped. A line that
// is not NAME=value is an error naming the line, because a setting that is
// silently ignored is worse than a gateway that will not start.
func readInto(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("config file %s: %w", path, err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	var bad []string
	n := 0
	for sc.Scan() {
		n++
		k, v, ok, problem := parseLine(sc.Text(), n)
		if problem != "" {
			bad = append(bad, problem)
			continue
		}
		if !ok {
			continue
		}
		if _, set := os.LookupEnv(k); !set {
			_ = os.Setenv(k, v)
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("config file %s: %w", path, err)
	}
	if len(bad) > 0 {
		return fmt.Errorf("config file %s: %w", path, errors.New(strings.Join(bad, "; ")))
	}
	return nil
}

// ReadFile reads a settings file into a map WITHOUT touching the process
// environment, with the loader's own line reading (parseLine), and refuses
// it as LoadFile would. For looking at a file other than the one this
// process runs on — the console checks the file it is about to start the
// gateway with, and reads the control address from it (2026-09-29).
func ReadFile(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("config file %s: %w", path, err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	out := map[string]string{}
	var bad []string
	n := 0
	for sc.Scan() {
		n++
		k, v, ok, problem := parseLine(sc.Text(), n)
		if problem != "" {
			bad = append(bad, problem)
			continue
		}
		if ok {
			out[k] = v
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("config file %s: %w", path, err)
	}
	if len(bad) > 0 {
		return nil, fmt.Errorf("config file %s: %s", path, strings.Join(bad, "; "))
	}
	return out, nil
}

// parseLine reads one line of the settings file, numbered n from 1. ok is
// false for a blank line or a comment; problem is set for a line the gateway
// refuses. It is the ONE reading of a line: the loader uses it, and so does
// `yakugate config` (edit.go) to check that what it writes reads back the
// same — an editor with its own idea of the format would, sooner or later,
// write a line the gateway reads differently.
func parseLine(raw string, n int) (key, value string, ok bool, problem string) {
	line := strings.TrimSpace(raw)
	if n == 1 {
		line = strings.TrimPrefix(line, "\ufeff") // a BOM from a Windows editor
	}
	if line == "" || strings.HasPrefix(line, "#") {
		return "", "", false, ""
	}
	i := strings.IndexByte(line, '=')
	if i < 0 {
		return "", "", false, fmt.Sprintf("line %d: expected NAME=value", n)
	}
	k := strings.TrimSpace(line[:i])
	if !nameRE.MatchString(k) {
		return "", "", false, fmt.Sprintf("line %d: %q is not a valid setting name", n, k)
	}
	v := strings.TrimSpace(line[i+1:])
	// GW-10: strip an inline comment — a # preceded by whitespace ends the
	// value (`ADDR=host:6379  # note`). A # INSIDE an unbroken token (e.g.
	// a password) is kept; quote the value to keep ` #` literally.
	if q := strings.IndexAny(v, "\"'"); q != 0 { // unquoted value
		if h := strings.Index(v, " #"); h >= 0 {
			v = strings.TrimSpace(v[:h])
		} else if h := strings.Index(v, "\t#"); h >= 0 {
			v = strings.TrimSpace(v[:h])
		}
	}
	v = strings.Trim(v, "\"'")
	return k, v, true, ""
}
