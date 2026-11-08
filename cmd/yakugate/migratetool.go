package main

import (
	"context"
	"fmt"
	"os"

	"github.com/yakugate/yakugate/internal/platform/bootstrap"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// `yakugate migrate` builds or upgrades the MySQL stores and exits (2026-09-18).
//
//	yakugate [--config FILE] migrate [--seed]
//
// The engine has always applied its migrations at start, but only inside the
// http-server role, which then runs forever. So there was no way to prepare a
// database without starting a gateway on it — a test rig, a CI job or a fresh
// deployment either started the role and killed it, or ran the SQL by hand.
//
// This is the same `bootstrap.Ensure` the role calls, with the same settings
// file: create the database, apply the embedded migrations, record them in
// schema_migrations. `--seed` turns on the sample data (`YAKU_AUTO_SEED`),
// which is off by default since PO 2026-09-17.
//
// The engine builds every store it needs on its own, with or without the
// Admin API (PO 2026-09-18). When both run, the ENGINE migrates first, then
// the Admin API — admin/0013 creates account_secrets with a foreign key to
// the engine's sms_service_config. (This comment used to say the opposite.)
const cmdMigrate = "migrate"

type migrateTool struct {
	cfgPath string
	seed    bool
}

// parseMigrateTool returns nil when the command line does not name this
// command, so the caller carries on with the ordinary role parsing. The
// settings option works in every form the roles take, before or after the
// command (bug 12: only "--config FILE" used to).
func parseMigrateTool(args []string) (*migrateTool, error) {
	word, at := commandWord(args)
	if word != cmdMigrate {
		return nil, nil
	}
	var t migrateTool
	rest := without(args, at)
	for i := 0; i < len(rest); i++ {
		if path, ok, err := configOption(rest, &i); ok {
			if err != nil {
				return nil, err
			}
			t.cfgPath = path
			continue
		}
		switch a := rest[i]; a {
		case "--seed":
			t.seed = true
		default:
			return nil, fmt.Errorf("migrate: unexpected argument %q", a)
		}
	}
	return &t, nil
}

func (t *migrateTool) run() int {
	if t.seed {
		// Ensure reads this itself; setting it here keeps "--seed" the one
		// place a caller has to think about.
		os.Setenv("YAKU_AUTO_SEED", "true")
	}
	if _, err := config.LoadFile(t.cfgPath); err != nil {
		fmt.Fprintln(os.Stderr, red(os.Stderr, "yakugate: migrate: "+err.Error()))
		return 1
	}
	cfg := config.Load()
	// One database type per install, each DSN of its type (sqlcheck.go). Not
	// whether the DAL serves every area: the schema is built first.
	if p := sqlProblem(cfg, false); p != "" {
		fmt.Fprintln(os.Stderr, red(os.Stderr, "yakugate: migrate: "+p))
		return 1
	}
	if err := bootstrap.Ensure(context.Background(), cfg); err != nil {
		fmt.Fprintln(os.Stderr, red(os.Stderr, "yakugate: migrate: "+err.Error()))
		return 1
	}
	// A role adds the keys that only make a screen fast in the background and
	// carries on. This command is the job itself: it waits for them, so it is
	// not cut off half way through a large table.
	bootstrap.WaitLateKeys()
	fmt.Println(green(os.Stdout, "yakugate: stores are up to date"))
	return 0
}
