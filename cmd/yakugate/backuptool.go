package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/process/backup"
)

// The two backup maintenance commands (P8 S8-04/S8-05, PO 2026-09-17). They
// are not roles: they run once and exit.
//
//	yakugate backup-verify  <run-dir>
//	yakugate [--config FILE] backup-restore <run-dir> --db config|cold|hot
//	        [--table NAME]… [--into-nonempty] [--dry-run]
const (
	cmdBackupVerify  = "backup-verify"
	cmdBackupRestore = "backup-restore"
)

// backupTool is one parsed maintenance command.
type backupTool struct {
	cmd, runDir, cfgPath, db string
	tables                   []string
	intoNonEmpty, dryRun     bool
}

// parseBackupTool returns nil, nil when args do not name a backup command, so
// the caller carries on with the ordinary role parsing.
func parseBackupTool(args []string) (*backupTool, error) {
	var t backupTool
	word, at := commandWord(args)
	if word != cmdBackupVerify && word != cmdBackupRestore {
		return nil, nil
	}
	t.cmd = word
	rest := without(args, at)
	value := func(i *int, name string) (string, error) {
		if k := strings.IndexByte(rest[*i], '='); k >= 0 {
			if v := rest[*i][k+1:]; v != "" {
				return v, nil
			}
		} else if *i+1 < len(rest) && rest[*i+1] != "" {
			*i++
			return rest[*i], nil
		}
		return "", fmt.Errorf("%s needs a value", name)
	}
	for i := 0; i < len(rest); i++ {
		a := rest[i]
		name := a
		if k := strings.IndexByte(a, '='); k >= 0 {
			name = a[:k]
		}
		var err error
		switch name {
		case "-c", "--config", "-config":
			t.cfgPath, err = value(&i, name)
		case "--db", "-db":
			t.db, err = value(&i, name)
		case "--table", "-table":
			var v string
			if v, err = value(&i, name); err == nil {
				t.tables = append(t.tables, v)
			}
		case "--into-nonempty":
			t.intoNonEmpty = true
		case "--dry-run":
			t.dryRun = true
		default:
			switch {
			case strings.HasPrefix(a, "-"):
				err = fmt.Errorf("unknown option %q", a)
			case t.runDir != "":
				err = fmt.Errorf("one run directory at a time: got %q and %q", t.runDir, a)
			default:
				t.runDir = a
			}
		}
		if err != nil {
			return nil, err
		}
	}
	if t.runDir == "" {
		return nil, fmt.Errorf("%s needs the run directory (<backup root>/<timestamp>)", t.cmd)
	}
	if t.cmd == cmdBackupVerify {
		if t.db != "" || len(t.tables) > 0 || t.intoNonEmpty || t.dryRun {
			return nil, fmt.Errorf("%s takes only the run directory", t.cmd)
		}
		return &t, nil
	}
	switch t.db {
	case "config", "cold", "hot":
	case "":
		return nil, fmt.Errorf("%s needs --db config|cold|hot", t.cmd)
	default:
		return nil, fmt.Errorf("--db %q: want config, cold or hot", t.db)
	}
	return &t, nil
}

// run executes the command and returns the process exit code: 0 good,
// 1 failed or refused, (2 is a usage error, given by main).
func (t *backupTool) run() int {
	if t.cmd == cmdBackupVerify {
		return verifyRun(t.runDir)
	}
	// The same settings the roles read: yakugate.config, environment on top.
	if _, err := config.LoadFile(t.cfgPath); err != nil {
		fmt.Fprintln(os.Stderr, red(os.Stderr, "yakugate: "+err.Error()))
		return 1
	}
	cfg := config.Load()
	dsn := map[string]string{"config": cfg.ConfigDSN, "cold": cfg.ColdDSN, "hot": cfg.HotDSN}[t.db]
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	res, err := backup.Restore(ctx, backup.RestoreOptions{
		RunDir: t.runDir, DB: t.db, DSN: dsn, Tables: t.tables,
		IntoNonEmpty: t.intoNonEmpty, DryRun: t.dryRun,
		Logf: func(f string, a ...any) { fmt.Printf(f+"\n", a...) },
	})
	if res != nil {
		var rows int64
		for _, tb := range res.Tables {
			rows += tb.Rows
		}
		verb := "restored"
		if t.dryRun && err == nil {
			verb = "dry run, nothing written: would restore"
		}
		// The database NAME only — a DSN carries a password.
		fmt.Printf("%s %d tables, %d rows into database %q", verb, len(res.Tables), rows, res.Target)
		if len(res.Triggers) > 0 {
			fmt.Printf("; triggers created: %s", strings.Join(res.Triggers, ", "))
		}
		fmt.Println()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, red(os.Stderr, "yakugate: backup-restore: "+err.Error()))
		return 1
	}
	return 0
}

func verifyRun(runDir string) int {
	man, rep, err := backup.Verify(runDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, red(os.Stderr, "yakugate: backup-verify: "+err.Error()))
		return 1
	}
	fmt.Printf("run %s, gateway %s, %s to %s\n", man.Run, man.GatewayVersion,
		man.StartedAt.Format("2006-01-02T15:04:05Z"), man.FinishedAt.Format("2006-01-02T15:04:05Z"))
	for _, d := range man.DatabasesFailed {
		fmt.Println(yellow(os.Stdout, fmt.Sprintf("note: database %s could not be dumped in this run", d)))
	}
	for _, x := range man.ExcludedTables {
		fmt.Println(yellow(os.Stdout, fmt.Sprintf("note: table %s was left out of this run", x)))
	}
	for _, p := range rep.Missing {
		fmt.Println(red(os.Stdout, "MISSING  "+p))
	}
	for _, p := range rep.Extra {
		fmt.Println(red(os.Stdout, "EXTRA    "+p))
	}
	for _, m := range rep.Mismatch {
		fmt.Println(red(os.Stdout, fmt.Sprintf("CHANGED  %s (%s)", m.Path, m.Reason)))
	}
	if rep.OK() {
		fmt.Println(green(os.Stdout, rep.String()))
	} else {
		fmt.Println(red(os.Stdout, rep.String()))
	}
	if !rep.OK() {
		return 1
	}
	// Files that match their manifest are not yet a backup (2026-10-18,
	// go-live P8 B1/E3): a run that left tables or a whole database out —
	// even every database, 0 files — passed with exit 0. Its files can still
	// be restored; the run is not whole, and the exit code says so.
	if !man.Whole() {
		fmt.Println(red(os.Stdout, fmt.Sprintf("NOT WHOLE: %d table(s) left out, %d database(s) not dumped — this run is not a whole backup",
			len(man.ExcludedTables), len(man.DatabasesFailed))))
		return 1
	}
	return 0
}
