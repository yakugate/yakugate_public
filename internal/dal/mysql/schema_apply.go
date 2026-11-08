package mysql

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	driver "github.com/go-sql-driver/mysql"

	"github.com/yakugate/yakugate/internal/dal"
)

// Running one migration file on MySQL — moved here unchanged on 30 Oct 2026
// from internal/platform/bootstrap/bootstrap.go (applyMigrations' inner loop
// and the helpers it used). Which files run, and in what order, is still the
// bootstrap's.

// ApplyMigration runs one migration file and records it as version.
func (s *schema) ApplyMigration(ctx context.Context, version, body string, applied map[string]bool, log dal.Printer) error {
	db := s.db
	// GW-07 (2026-08-27): statements run ONE AT A TIME with per-statement
	// progress markers (`<version>#<n>` rows), so a mid-file failure
	// resumes at the failing statement instead of replaying completed work
	// — the 0045 incident made a mid-file failure permanently
	// unreplayable. Markers are deleted once the file completes.
	stmts := splitSQLStatements(body)
	for i, stmt := range stmts {
		marker := fmt.Sprintf("%s#%d", version, i)
		if len(stmts) > 1 && applied[marker] {
			continue
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			// GW-08/GW-09 (2026-08-27): an ALTER against a table that does
			// not exist on THIS deployment — seed-created clones
			// (queue_testvendor) or another machine's daily partitions
			// (mtlog_demo_*) — is skipped with a log line: the family's
			// template row carries the change for every future clone, so
			// the absent clone loses nothing.
			if isUnknownTableErr(err) && adminOwnedTable(missingTable(err)) {
				// A GATEWAY-ONLY INSTALL (PO 2026-09-18). The engine's set
				// carries a few statements that touch tables belonging to
				// the Node Admin API — config/0047 copies the old
				// admin_users/admin_roles rows into users/roles, a
				// one-time move from 2026. An install that never ran the
				// Admin API has none of those tables and no such rows, so
				// there is nothing to do and nothing is lost.
				log.Printf("migration %s statement %d skipped — %s belongs to the Admin API and this install does not have it", version, i+1, missingTable(err))
			} else if isMissingTableErr(err) && isAlterStmt(stmt) {
				log.Printf("migration %s statement %d skipped — target table absent on this deployment", version, i+1)
			} else if isAlreadyThereErr(err) && isAlterStmt(stmt) {
				// An ALTER that adds a column or key which is already
				// present asked for an end state that already holds, so it
				// is satisfied, not failed (2026-09-18). This is what lets
				// config/0087a bring the Admin API's two audit_log shapes
				// into the engine's own set: every existing deployment
				// applied them as admin/0007 and admin/0008 years of
				// migrations ago, and a fresh one has neither.
				log.Printf("migration %s statement %d already satisfied — the column or key is present", version, i+1)
			} else if isReferencedTableErr(err) && isAlterStmt(stmt) {
				// THE ENGINE MUST BUILD EVERYTHING IT NEEDS ON ITS OWN
				// (PO 2026-09-18: "all necessary database that are
				// required for sms gateway must be created by gateway, so
				// that i may run gateway even without admin api").
				//
				// One statement stood in the way: config/0045 adds a
				// foreign key from sms_service_config.user_id to
				// `users`, a table the NODE Admin API creates. Until now
				// that stopped a new database dead and told the operator
				// to start the Admin API first.
				//
				// Nothing is lost by skipping it. The link is advisory —
				// portal display, validated by the Admin API at write
				// time — and the engine's own config/0065 DROPS this very
				// constraint a few migrations later (GW-06, PO
				// 2026-08-27). So on a gateway-only install the end state
				// is exactly what it would have been anyway: the column
				// and its index, with no constraint.
				//
				// The clause is removed and the REST of the statement is
				// run: in 0045 the same ALTER also adds dlr_config and
				// mo_config, and dropping the lot would leave the table
				// short two columns.
				missing := referencedTable(err)
				if rest, any := withoutForeignKeyTo(stmt, missing); any {
					if _, err2 := db.ExecContext(ctx, rest); err2 != nil {
						return fmt.Errorf("bootstrap: apply %s (statement %d) without its foreign key to %s: %w", version, i+1, missing, err2)
					}
					log.Printf("migration %s statement %d applied without its foreign key to %s — that table belongs to the Admin API and this install does not have it", version, i+1, missing)
				} else {
					log.Printf("migration %s statement %d skipped — it only adds a foreign key to %s, a table the Admin API owns", version, i+1, missing)
				}
			} else if isTriggerStmt(stmt) && isTriggerRightsErr(err) {
				// A MySQL with binary logging on refuses CREATE TRIGGER to a
				// login without SUPER (1419) unless the server trusts it
				// (log_bin_trust_function_creators). Some installs can change
				// neither (no admin login, a managed server). There
				// YAKU_MIGRATE_SKIP_TRIGGERS=true builds the rest without the
				// trigger (Saleem 2026-11-03, open for the PO); off, the start
				// stops and says the three ways out.
				if !skipTriggers() {
					return fmt.Errorf("bootstrap: apply %s (statement %d): %w — %s", version, i+1, err, triggerRightsHint)
				}
				name := triggerName(stmt)
				log.Printf("migration %s statement %d SKIPPED — MySQL lets this login change no trigger (%s) and YAKU_MIGRATE_SKIP_TRIGGERS is on: %s", version, i+1, name, triggerLoss(name))
			} else {
				return fmt.Errorf("bootstrap: apply %s (statement %d): %w", version, i+1, err)
			}
		}
		if len(stmts) > 1 {
			_, _ = db.ExecContext(ctx, stSchemaMark.SQL, marker)
		}
	}
	if _, err := db.ExecContext(ctx, stSchemaRecord.SQL, version); err != nil {
		return fmt.Errorf("bootstrap: record %s: %w", version, err)
	}
	_, _ = db.ExecContext(ctx, stSchemaForgetMarks.SQL, version+"#%")
	return nil
}

// splitSQLStatements breaks a migration file into executable statements. Naive
// semicolon splitting would shred the BEGIN...END trigger bodies two config
// migrations carry, so a statement opening with CREATE TRIGGER runs until its
// terminating standalone "END;" line. Comments and blank lines are preserved
// inside their statement (the server ignores them).
func splitSQLStatements(body string) []string {
	var out []string
	var cur []string
	inTrigger := false
	flush := func() {
		s := strings.TrimSpace(strings.Join(cur, "\n"))
		// A fragment that is only comments/whitespace is not a statement.
		if stripSQLComments(s) != "" {
			out = append(out, s)
		}
		cur = cur[:0]
	}
	for _, line := range strings.Split(body, "\n") {
		cur = append(cur, line)
		t := strings.TrimSpace(line)
		upper := strings.ToUpper(stripSQLComments(strings.Join(cur, "\n")))
		if !inTrigger && strings.HasPrefix(strings.TrimSpace(upper), "CREATE TRIGGER") {
			inTrigger = true
		}
		if inTrigger {
			if strings.EqualFold(t, "END;") || strings.EqualFold(t, "END ;") {
				inTrigger = false
				flush()
			}
			continue
		}
		if strings.HasSuffix(t, ";") && !strings.HasPrefix(t, "--") {
			flush()
		}
	}
	flush()
	return out
}

// stripSQLComments removes -- line comments (enough to classify fragments; the
// statements themselves are executed verbatim, comments included).
func stripSQLComments(s string) string {
	var b strings.Builder
	for _, line := range strings.Split(s, "\n") {
		if i := strings.Index(line, "--"); i >= 0 {
			line = line[:i]
		}
		b.WriteString(strings.TrimSpace(line))
	}
	return strings.TrimSpace(b.String())
}

// isAlreadyThereErr reports the "that is already how it is" family: 1060
// (ER_DUP_FIELDNAME) and 1061 (ER_DUP_KEYNAME) when an ALTER adds a column or
// key that exists, and 1091 (ER_CANT_DROP_FIELD_OR_KEY) when it drops one that
// is already gone. Only ever consulted for an ALTER or a CREATE INDEX, where
// it means the end state the statement asked for already holds.
func isAlreadyThereErr(err error) bool {
	var me *driver.MySQLError
	return errors.As(err, &me) && (me.Number == 1060 || me.Number == 1061 || me.Number == 1091)
}

// isUnknownTableErr reports the two ways MySQL says "no such table": 1146
// (ER_NO_SUCH_TABLE, reading or writing one) and 1051 (ER_BAD_TABLE_ERROR,
// dropping one). config/0047 both reads admin_users and drops it afterwards.
func isUnknownTableErr(err error) bool {
	var me *driver.MySQLError
	return errors.As(err, &me) && (me.Number == 1146 || me.Number == 1051)
}

// isMissingTableErr reports MySQL 1146 (ER_NO_SUCH_TABLE).
func isMissingTableErr(err error) bool {
	var me *driver.MySQLError
	return errors.As(err, &me) && me.Number == 1146
}

// isReferencedTableErr: MySQL 1824 "Failed to open the referenced table" — a
// foreign key to a table another runtime creates.
func isReferencedTableErr(err error) bool {
	var me *driver.MySQLError
	return errors.As(err, &me) && me.Number == 1824
}

// referencedTable pulls the table name out of MySQL 1824 ("Failed to open the
// referenced table 'users'") for the log line. Falls back to a plain word when
// the message does not carry one.
func referencedTable(err error) string {
	var me *driver.MySQLError
	if errors.As(err, &me) {
		if a := strings.Index(me.Message, "'"); a >= 0 {
			if b := strings.Index(me.Message[a+1:], "'"); b > 0 {
				return me.Message[a+1 : a+1+b]
			}
		}
	}
	return "another component's table"
}

// isAlterStmt reports whether a statement's first keyword is ALTER — the only
// class GW-08/09 tolerate against a missing table.
func isAlterStmt(stmt string) bool {
	up := strings.ToUpper(strings.TrimSpace(stripSQLComments(stmt)))
	// CREATE INDEX is an ALTER by another name, and the two tolerances above
	// (target table absent, end state already holds) apply to it identically.
	return strings.HasPrefix(up, "ALTER ") || strings.HasPrefix(up, "CREATE INDEX ") ||
		strings.HasPrefix(up, "CREATE UNIQUE INDEX ")
}

// withoutForeignKeyTo removes from an ALTER every clause that adds a foreign
// key pointing at `table`, and reports whether anything is left to run.
//
// A blunt "skip the statement" is not good enough here: config/0045 adds the
// foreign key to `users` in the SAME ALTER that adds dlr_config and mo_config,
// so skipping the lot would leave the table short two columns and the next
// statement would fail on them. Only the offending clause goes.
//
// The clause list is split on commas at the top level, so a clause carrying its
// own parentheses — FOREIGN KEY (user_id) REFERENCES users(user_id) — stays in
// one piece.
func withoutForeignKeyTo(stmt, table string) (string, bool) {
	m := alterHeadRe.FindStringSubmatch(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(stmt), ";")))
	if m == nil {
		return "", false
	}
	head, body := m[1], m[2]

	var clauses []string
	depth, start := 0, 0
	for i, r := range body {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				clauses = append(clauses, body[start:i])
				start = i + 1
			}
		}
	}
	clauses = append(clauses, body[start:])

	kept := make([]string, 0, len(clauses))
	for _, c := range clauses {
		if fkClauseRe.MatchString(c) && strings.Contains(strings.ToLower(c), strings.ToLower("references "+table)) {
			continue
		}
		if strings.TrimSpace(stripSQLComments(c)) == "" {
			continue
		}
		kept = append(kept, strings.TrimSpace(c))
	}
	if len(kept) == 0 {
		return "", false
	}
	return head + "\n  " + strings.Join(kept, ",\n  ") + ";", true
}

var (
	alterHeadRe = regexp.MustCompile(`(?is)^(ALTER\s+TABLE\s+` + "`?" + `\w+` + "`?" + `)\s+(.*)$`)
	// ADD [CONSTRAINT name] FOREIGN KEY ... REFERENCES ...
	fkClauseRe = regexp.MustCompile(`(?is)^\s*ADD\s+(CONSTRAINT\s+` + "`?" + `\w+` + "`?" + `\s+)?FOREIGN\s+KEY\b`)
)

// adminOwnedTable reports whether a table belongs to the Node Admin API rather
// than the engine. The engine must be able to build every store it needs on
// its own (PO 2026-09-18: "all necessary database that are required for sms
// gateway must be created by gateway, so that i may run gateway even without
// admin api"), so a statement that reaches for one of these on an install that
// has no Admin API is skipped rather than fatal.
//
// Kept as an explicit list, not a guess: naming the boundary is the point. The
// engine READS none of these at run time — they carry portal logins, roles and
// the historical admin_* rows config/0047 moved across.
func adminOwnedTable(name string) bool {
	switch strings.ToLower(name) {
	case "users", "roles", "admin_users", "admin_roles", "user_invites":
		return true
	}
	return false
}

// missingTable pulls the table name out of MySQL 1146 ("Table 'db.foo' doesn't
// exist"), without the database prefix.
func missingTable(err error) string {
	var me *driver.MySQLError
	if !errors.As(err, &me) {
		return ""
	}
	a := strings.Index(me.Message, "'")
	if a < 0 {
		return ""
	}
	b := strings.Index(me.Message[a+1:], "'")
	if b < 0 {
		return ""
	}
	full := me.Message[a+1 : a+1+b]
	if i := strings.LastIndex(full, "."); i >= 0 {
		return full[i+1:]
	}
	return full
}

// --- Triggers on a MySQL that will not let this login make them ---------------
//
// Three migration files touch a trigger: config/0021 and config/0023
// (trg_accounts_after_insert, on a table config/0042 drops again) and
// config/0088 (trg_audit_log_chain, the audit trail's hash chain). With
// binary logging on, MySQL refuses CREATE TRIGGER to a login without SUPER
// unless log_bin_trust_function_creators is set (Saleem 2026-11-03: production
// stopped at config/0021 with 1419 and had no admin login).

// triggerRightsHint is what the start says when a trigger is refused and the
// skip is off.
const triggerRightsHint = "MySQL lets this login create no trigger (binary logging is on and the login has no SUPER). " +
	"Either run `yakugate migrate` once with a login that has SUPER, " +
	"or set log_bin_trust_function_creators=1 on every MySQL server, " +
	"or set YAKU_MIGRATE_SKIP_TRIGGERS=true (audit_log then keeps no hash chain)"

var triggerStmtRe = regexp.MustCompile(`(?is)^(CREATE|DROP)\s+(DEFINER\s*=\s*\S+\s+)?TRIGGER\s+(IF\s+EXISTS\s+)?` + "`?" + `([A-Za-z0-9_$]+)`)

// isTriggerStmt reports whether a statement creates or drops a trigger.
func isTriggerStmt(stmt string) bool {
	return triggerStmtRe.MatchString(triggerHead(stmt))
}

// triggerHead is the statement without its comment lines, its lines joined by
// a space (stripSQLComments joins them with nothing, which glues a trigger's
// name to the AFTER on the next line).
func triggerHead(stmt string) string {
	var parts []string
	for _, line := range strings.Split(stmt, "\n") {
		if i := strings.Index(line, "--"); i >= 0 {
			line = line[:i]
		}
		if line = strings.TrimSpace(line); line != "" {
			parts = append(parts, line)
		}
	}
	return strings.Join(parts, " ")
}

// triggerName is the trigger a statement creates or drops ("" when none).
func triggerName(stmt string) string {
	m := triggerStmtRe.FindStringSubmatch(triggerHead(stmt))
	if m == nil {
		return ""
	}
	return m[4]
}

// isTriggerRightsErr reports MySQL refusing a statement for want of a right:
// 1419 (binary logging on, no SUPER), 1142 (no TRIGGER privilege on the table),
// 1227 (the statement needs SUPER or another admin right).
func isTriggerRightsErr(err error) bool {
	var me *driver.MySQLError
	return errors.As(err, &me) && (me.Number == 1419 || me.Number == 1142 || me.Number == 1227)
}

// skipTriggers is YAKU_MIGRATE_SKIP_TRIGGERS (default off).
func skipTriggers() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("YAKU_MIGRATE_SKIP_TRIGGERS")))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

// triggerLoss says what an install built without the trigger goes without.
func triggerLoss(name string) string {
	switch strings.ToLower(name) {
	case "trg_audit_log_chain":
		return "audit_log rows get no hash chain; /admin/audit/verify counts them as unprotected. " +
			"To add it later, a login with SUPER runs the DROP and CREATE TRIGGER of config/0088"
	case "trg_accounts_after_insert":
		return "nothing is lost; config/0042 drops the table it was on"
	default:
		return "the install goes without this trigger"
	}
}
