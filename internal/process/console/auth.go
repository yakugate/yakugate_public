package console

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"golang.org/x/crypto/bcrypt"

	"github.com/yakugate/yakugate/internal/platform/config"
)

// The console's password (2026-09-29, Saleem: "secure console with password,
// which can be changed later, initial with default password YakuAdmin@123,
// also forget option, with 3 questions, easy ones").
//
// Kept in its own file, hashed (Saleem: not the database, not yakugate.config):
// console-auth.json beside the settings file, readable by its owner only —
// the password as a bcrypt hash, three security questions with their answers
// hashed the same way, and the count of wrong tries. No file means the
// default password, which works — with a warning every time — until it is
// changed (Saleem: "warn only"). There is no reset command (Saleem: "no way
// back"); whoever can delete the file on the server gets the default back,
// as with any lock kept on the server itself.

// DefaultConsolePassword is the password of a console that has never had one
// set. It is public: the console says so until it is changed.
const DefaultConsolePassword = "YakuAdmin@123"

// AuthFileName is the file the console password is kept in.
const AuthFileName = "console-auth.json"

// Five wrong tries — password or answers — lock the console for 15 minutes.
const (
	maxTries = 5
	lockFor  = 15 * time.Minute
)

// securityQuestions are the questions a person picks three of. Easy ones on
// purpose (Saleem): things a person remembers without writing down.
var securityQuestions = []string{
	"What is the name of the town where you were born?",
	"What was the name of your first school?",
	"What was the name of your first pet?",
	"What is your mother's first name?",
	"What was your childhood nickname?",
	"What is your favourite food?",
	"What is the name of your best friend from childhood?",
	"What was the first car you owned (make)?",
}

type authAnswer struct {
	Question   int    `json:"question"` // index into securityQuestions
	AnswerHash string `json:"answer_hash"`
}

type authFile struct {
	PasswordHash string       `json:"password_hash"`
	Questions    []authAnswer `json:"questions,omitempty"`
	FailedTries  int          `json:"failed_tries,omitempty"`
	LockedUntil  time.Time    `json:"locked_until,omitempty"`
	ChangedAt    time.Time    `json:"changed_at,omitempty"`
}

// authPath is where the console password is kept: beside the settings file,
// else in the user's config folder, else the working folder.
func authPath() string {
	if f := config.LoadedFile(); f != "" {
		return filepath.Join(filepath.Dir(f), AuthFileName)
	}
	if d, err := os.UserConfigDir(); err == nil {
		return filepath.Join(d, "yakugate", AuthFileName)
	}
	return AuthFileName
}

// loadAuth reads the file; a missing file is the default password.
func loadAuth(path string) (authFile, bool, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return authFile{}, false, nil
	}
	if err != nil {
		return authFile{}, false, fmt.Errorf("console password file %s: %w", path, err)
	}
	var a authFile
	if err := json.Unmarshal(b, &a); err != nil || a.PasswordHash == "" {
		// A damaged file never falls back to the default: that would turn
		// a broken file into an open door.
		return authFile{}, false, fmt.Errorf("console password file %s cannot be read — restore it from a backup", path)
	}
	return a, true, nil
}

// saveAuth writes the file for its owner only, replacing it in one step.
func saveAuth(path string, a authFile) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("folder for %s: %w", path, err)
	}
	b, _ := json.MarshalIndent(a, "", "  ")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return fmt.Errorf("save %s: %w", path, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("save %s: %w", path, err)
	}
	return nil
}

func hashSecret(s string) (string, error) {
	h, err := bcrypt.GenerateFromPassword([]byte(s), bcrypt.DefaultCost)
	return string(h), err
}

func secretMatches(hash, s string) bool {
	return hash != "" && bcrypt.CompareHashAndPassword([]byte(hash), []byte(s)) == nil
}

// normalAnswer makes an answer forgiving: case, spaces and punctuation at the
// ends do not count ("  Lahore. " = "lahore").
func normalAnswer(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.TrimFunc(s, func(r rune) bool { return unicode.IsPunct(r) || unicode.IsSpace(r) })
	return strings.Join(strings.Fields(s), " ")
}

// checkNewPassword is the rule for a password a person chooses.
func checkNewPassword(pw string) error {
	switch {
	case len(pw) < 8:
		return errors.New("a password is at least 8 characters")
	case len(pw) > 72:
		return errors.New("a password is at most 72 characters")
	case pw == DefaultConsolePassword:
		return errors.New("that is the default password everyone knows — choose another")
	}
	var letter, digit bool
	for _, r := range pw {
		letter = letter || unicode.IsLetter(r)
		digit = digit || unicode.IsDigit(r)
	}
	if !letter || !digit {
		return errors.New("a password needs at least one letter and one digit")
	}
	return nil
}

// readSecret reads a line without showing it on a terminal.
//
// On a terminal it reads key by key through the menus' own reader, not with
// term.ReadPassword. That one reads the terminal directly, and once the
// terminal is gone (an SSH session that ended while the console was stopped)
// every read answers 0 bytes and no error, which it reads again for ever: two
// consoles left at this prompt burned two cores of tw-1 for a day
// (2026-10-22). Through os.File a read of 0 bytes is the end of input.
func (p *Process) readSecret(sc *bufio.Scanner, prompt string) (string, bool) {
	p.printf("%s: ", prompt)
	if p.tty != nil {
		keys := p.keys
		if keys == nil {
			keys = &ttyKeys{f: p.tty}
		}
		s, ok := readHidden(keys)
		p.printf("\n")
		return s, ok
	}
	if !sc.Scan() {
		return "", false
	}
	return strings.TrimRight(sc.Text(), "\r"), true
}

// readHidden reads keys up to Enter, showing none of them. ok=false: the
// input ended, or Ctrl-C / Ctrl-D.
func readHidden(keys keySource) (string, bool) {
	if err := keys.begin(); err != nil {
		return "", false
	}
	defer keys.end()
	var s []rune
	for {
		k, err := keys.next()
		if err != nil {
			return "", false
		}
		switch k {
		case "\r", "\n":
			return string(s), true
		case "\x03", "\x04": // Ctrl-C, Ctrl-D
			return "", false
		case "\x7f", "\x08": // backspace
			if len(s) > 0 {
				s = s[:len(s)-1]
			}
		default:
			// an arrow or other control key is not part of a password
			if r, _ := utf8.DecodeRuneInString(k); unicode.IsPrint(r) {
				s = append(s, r)
			}
		}
	}
}

// login asks for the console password before anything else. true = in.
func (p *Process) login(sc *bufio.Scanner) bool {
	path := p.authFile
	for {
		a, exists, err := loadAuth(path)
		if err != nil {
			p.errf("  %v\n", err)
			return false
		}
		if left := time.Until(a.LockedUntil); left > 0 {
			p.errf("  The console is locked after %d wrong tries. Try again in %s.\n", maxTries, shortDuration(left))
			return false
		}
		pw, ok := p.readSecret(sc, "  Console password (or type forgot)")
		if !ok {
			return false
		}
		if strings.EqualFold(strings.TrimSpace(pw), "forgot") {
			if !exists || len(a.Questions) == 0 {
				p.errf("  No security questions have been set, so the password cannot be recovered this way.\n")
				continue
			}
			if p.forgotPassword(sc, path) {
				return true
			}
			continue
		}
		good := (!exists && pw == DefaultConsolePassword) || (exists && secretMatches(a.PasswordHash, pw))
		if good {
			if exists && a.FailedTries > 0 {
				a.FailedTries = 0
				_ = saveAuth(path, a)
			}
			// The default password is the default whatever the file says: a
			// wrong try with no file writes one (to count tries, with the
			// default's hash), and reading that as a chosen password forced
			// security questions on someone who had never set one (2026-10-02).
			// A chosen password can never equal the default (checkNewPassword).
			if pw == DefaultConsolePassword {
				p.warnf("\n  You are using the default console password, which is public. Change it:\n")
				p.warnf("  Gateway (g) → Console password.\n")
			} else if len(a.Questions) == 0 {
				// A chosen password must be recoverable (Saleem 2026-09-29:
				// "force user to set them"): the menu does not open until the
				// three questions are set.
				p.warnf("\n  Your console password has no security questions yet. Set them now — the console opens once they are.\n")
				p.setQuestions(sc, path, a, true)
				if b, _, err := loadAuth(path); err != nil || len(b.Questions) == 0 {
					p.errf("  The console stays closed until the three security questions are set.\n")
					return false
				}
			}
			return true
		}
		if err := p.failedTry(path, a, exists); err != nil {
			p.errf("  %v\n", err)
			return false
		}
		p.errf("  Wrong password.\n")
	}
}

// failedTry counts a wrong password or answer, and locks at the limit. The
// default password's tries are kept too, in a file made just for that.
func (p *Process) failedTry(path string, a authFile, exists bool) error {
	if !exists {
		h, err := hashSecret(DefaultConsolePassword)
		if err != nil {
			return err
		}
		a.PasswordHash = h
	}
	a.FailedTries++
	if a.FailedTries >= maxTries {
		a.FailedTries = 0
		a.LockedUntil = time.Now().Add(lockFor)
		if err := saveAuth(path, a); err != nil {
			return err
		}
		return fmt.Errorf("%d wrong tries — the console is locked for %s", maxTries, shortDuration(lockFor))
	}
	return saveAuth(path, a)
}

// forgotPassword asks the three questions; all right = a new password.
func (p *Process) forgotPassword(sc *bufio.Scanner, path string) bool {
	a, _, err := loadAuth(path)
	if err != nil {
		p.errf("  %v\n", err)
		return false
	}
	p.printf("\n  Answer your three security questions.\n")
	allRight := true
	for _, q := range a.Questions {
		if q.Question < 0 || q.Question >= len(securityQuestions) {
			allRight = false
			continue
		}
		ans, ok := p.ask(sc, "  "+securityQuestions[q.Question])
		if !ok {
			return false
		}
		if !secretMatches(q.AnswerHash, normalAnswer(ans)) {
			allRight = false // keep asking: which one was wrong is not said
		}
	}
	if !allRight {
		if err := p.failedTry(path, a, true); err != nil {
			p.errf("  %v\n", err)
			return false
		}
		p.errf("  One or more answers were wrong.\n")
		return false
	}
	p.okf("  Answers accepted.\n")
	return p.setConsolePassword(sc, path, a)
}

// setConsolePassword asks for a new console password twice and saves it.
func (p *Process) setConsolePassword(sc *bufio.Scanner, path string, a authFile) bool {
	for {
		pw, ok := p.readSecret(sc, "  New console password")
		if !ok {
			return false
		}
		if err := checkNewPassword(pw); err != nil {
			p.errf("  %v\n", err)
			continue
		}
		again, ok := p.readSecret(sc, "  The same again")
		if !ok {
			return false
		}
		if again != pw {
			p.errf("  The two did not match.\n")
			continue
		}
		h, err := hashSecret(pw)
		if err != nil {
			p.errf("  %v\n", err)
			return false
		}
		a.PasswordHash, a.FailedTries, a.LockedUntil, a.ChangedAt = h, 0, time.Time{}, time.Now().UTC()
		if err := saveAuth(path, a); err != nil {
			p.errf("  %v\n", err)
			return false
		}
		p.okf("  The console password is changed.\n")
		return true
	}
}

// consolePasswordMenu: change the password, or set the three questions.
// Both need the current password first.
func (p *Process) consolePasswordMenu(sc *bufio.Scanner) {
	path := p.authFile
	a, exists, err := loadAuth(path)
	if err != nil {
		p.errf("  %v\n", err)
		return
	}
	p.printf("\n  Console password — kept hashed in %s\n", path)
	switch {
	case !exists:
		p.warnf("  Still the default password, which is public.\n")
	case !a.ChangedAt.IsZero():
		p.printf("  Last changed %s ago.\n", shortDuration(time.Since(a.ChangedAt)))
	}
	if len(a.Questions) == 0 {
		p.warnf("  No security questions set: a forgotten password cannot be recovered.\n")
	} else {
		p.printf("  Security questions: set (%d).\n", len(a.Questions))
	}
	p.printf("\n")
	choice, ok := p.choose(sc, "Choose", "", []opt{
		item("1", "Change the password"),
		item("2", "Set the three security questions"),
		item("0", "Back"),
	})
	if !ok || (choice != "1" && choice != "2") {
		return
	}
	cur, ok := p.readSecret(sc, "  Current console password")
	if !ok {
		return
	}
	if !((!exists && cur == DefaultConsolePassword) || (exists && secretMatches(a.PasswordHash, cur))) {
		if err := p.failedTry(path, a, exists); err != nil {
			p.errf("  %v\n", err)
			return
		}
		p.errf("  Wrong password — nothing was changed.\n")
		return
	}
	if !exists {
		// First change from the default: the file starts here.
		h, _ := hashSecret(DefaultConsolePassword)
		a.PasswordHash = h
	}
	if choice == "1" {
		if !p.setConsolePassword(sc, path, a) {
			return
		}
		// A new password with no way to recover it: the questions are the
		// next step, not a reminder for later (Saleem 2026-09-29).
		if a, _, err = loadAuth(path); err == nil && len(a.Questions) == 0 {
			p.printf("\n  Now set your three security questions — they are how a forgotten password is recovered.\n")
			p.setQuestions(sc, path, a, true)
		}
		return
	}
	p.setQuestions(sc, path, a, false)
}

// setQuestions picks three different questions and their answers. required
// (right after a password change with none set) offers no Cancel.
func (p *Process) setQuestions(sc *bufio.Scanner, path string, a authFile, required bool) {
	var picked []authAnswer
	used := map[int]bool{}
	for len(picked) < 3 {
		p.printf("\n  Question %d of 3\n", len(picked)+1)
		opts := make([]opt, 0, len(securityQuestions)+1)
		for i, q := range securityQuestions {
			if used[i] {
				continue
			}
			opts = append(opts, item(fmt.Sprint(i+1), q))
		}
		if !required {
			opts = append(opts, item("0", "Cancel — keep the questions as they were"))
		}
		choice, ok := p.choose(sc, "Choose", "", opts)
		if !ok {
			if required {
				p.warnf("  The security questions were not set: a forgotten password cannot be recovered.\n")
			}
			return
		}
		if choice == "0" && !required {
			p.printf("  Nothing was changed.\n")
			return
		}
		var idx int
		if _, err := fmt.Sscan(choice, &idx); err != nil || idx < 1 || idx > len(securityQuestions) || used[idx-1] {
			p.errf("  %q is not one of the questions.\n", choice)
			continue
		}
		ans, ok := p.ask(sc, "  Your answer")
		if !ok {
			return
		}
		norm := normalAnswer(ans)
		if len(norm) < 2 {
			p.errf("  An answer is at least 2 letters.\n")
			continue
		}
		h, err := hashSecret(norm)
		if err != nil {
			p.errf("  %v\n", err)
			return
		}
		used[idx-1] = true
		picked = append(picked, authAnswer{Question: idx - 1, AnswerHash: h})
	}
	a.Questions = picked
	if err := saveAuth(path, a); err != nil {
		p.errf("  %v\n", err)
		return
	}
	p.okf("  The three security questions are set. Answers are not case-sensitive.\n")
}
