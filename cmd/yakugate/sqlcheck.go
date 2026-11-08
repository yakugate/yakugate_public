package main

import (
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// sqlProblem says what is wrong with the SQL stores' settings, or "" (DAL
// plan, step 4):
//   - a store's type and its DSN disagree (cfg.SQLProblem);
//   - the stores are on two kinds of SQL database — one SQL database per
//     install (dal.CheckOneSQL);
//   - with serving, the install's SQL DAL does not serve every area yet
//     (dal.CheckServed: today, SQL Server), so a role would half run.
//
// A role, the whole gateway and its background start refuse to start on
// it; migrate checks the first two only (it builds the schema, which a SQL
// DAL may serve before its areas); the console only warns, so its setup
// screen can still put it right.
func sqlProblem(cfg config.Config, serving bool) string {
	if p := cfg.SQLProblem(); p != "" {
		return p
	}
	if err := dal.CheckOneSQL(cfg); err != nil {
		return err.Error() + " — every SQL store of an install is on one database type (Redis may go with either)"
	}
	if serving {
		if err := dal.CheckServed(cfg); err != nil {
			return err.Error()
		}
	}
	return ""
}
