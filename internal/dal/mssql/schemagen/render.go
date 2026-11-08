package schemagen

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Limits of a SQL Server index key, in bytes.
const (
	ClusteredKeyMax    = 900
	NonClusteredKeyMax = 1700
)

// Options say what the base file records about where it came from.
type Options struct {
	Store     string // config, queue, hot, hlr
	MySQLHead string // the last migrations/<store>/mysql file the MySQL install ran
	AdminHead string // the last p13-backend migration (config only)
	// Skip names tables left out (schema_migrations: the runner makes it).
	Skip map[string]bool
}

// Notes are what the generator could not carry over as it was, for the
// report and the file's head.
type Notes struct {
	OnUpdate        []string // table.column whose ON UPDATE CURRENT_TIMESTAMP was dropped
	NullableUniques []string // table.index made a filtered unique index
	NonClusteredPKs []string // table whose primary key is wider than a clustered key may be
	WideKeys        []string // table.index wider than a nonclustered key may be
	Triggers        []string // triggers translated by hand
}

// Triggers holds the T-SQL of each MySQL trigger, by name: a trigger body is
// translated by hand, never by machine. The generator refuses a schema with
// a trigger not listed here.
var Triggers = map[string]string{
	// The audit chain (config/0049 and later): each row's hash covers the
	// previous row's, so a row changed or removed afterwards breaks the chain.
	// MySQL computes it BEFORE INSERT; SQL Server has no BEFORE trigger, so
	// this one runs AFTER INSERT and sets the two hashes on the rows just
	// written, oldest first, holding the head row (UPDLOCK) as MySQL's FOR
	// UPDATE did. The text hashed is the same: every column COALESCEd to ''
	// and joined with '|', as UTF-8, SHA-256 in lower-case hex. A reader that
	// checks the chain reads each column back as text the same way
	// (CONVERT(VARCHAR(19), created_at, 120) for the time). MySQL's CHAR drops
	// trailing spaces and SQL Server's keeps them, so the empty head of a new
	// chain is RTRIMmed back to ''. Checked: the same rows give the same
	// hashes on both (unicode text included).
	"trg_audit_log_chain": `CREATE TRIGGER [trg_audit_log_chain] ON [audit_log] AFTER INSERT AS
BEGIN
  SET NOCOUNT ON;
  IF NOT EXISTS (SELECT 1 FROM inserted) RETURN;
  DECLARE @prev VARCHAR(64), @id BIGINT, @text NVARCHAR(MAX), @hash VARCHAR(64);
  -- The text is hashed as UTF-8, as MySQL's utf8mb4 is. A variable takes the
  -- database's collation (code page 1252), so the text passes through a
  -- column of a UTF-8 collation to become UTF-8 bytes.
  DECLARE @utf8 TABLE (v VARCHAR(MAX) COLLATE Latin1_General_100_CI_AS_SC_UTF8);
  SELECT @prev = RTRIM(head_hash) FROM [audit_chain_head] WITH (UPDLOCK, HOLDLOCK) WHERE id = 1;
  SET @prev = COALESCE(@prev, '');
  DECLARE rows_in CURSOR LOCAL FAST_FORWARD FOR SELECT id FROM inserted ORDER BY id;
  OPEN rows_in;
  FETCH NEXT FROM rows_in INTO @id;
  WHILE @@FETCH_STATUS = 0
  BEGIN
    SELECT @text = CONCAT_WS(N'|',
        @prev,
        COALESCE(a.actor, N''), COALESCE(a.actor_name, N''), COALESCE(a.actor_email, N''),
        COALESCE(CONVERT(NVARCHAR(20), a.account_id), N''), COALESCE(CONVERT(NVARCHAR(20), a.company_id), N''),
        COALESCE(a.company_name, N''),
        COALESCE(a.action, N''), COALESCE(a.method, N''), COALESCE(a.route, N''),
        COALESCE(a.params, N''), COALESCE(a.query, N''), COALESCE(CONVERT(NVARCHAR(20), a.response_code), N''),
        COALESCE(a.summary, N''), COALESCE(a.outcome, N''), COALESCE(a.reason, N''),
        COALESCE(a.entity, N''), COALESCE(a.feature, N''), COALESCE(a.module, N''),
        COALESCE(a.entity_id, N''), COALESCE(a.target_label, N''),
        COALESCE(a.detail, N''), COALESCE(a.before_data, N''), COALESCE(a.after_data, N''),
        COALESCE(a.ip, N''), COALESCE(a.user_agent, N''), COALESCE(a.content_type, N''),
        COALESCE(CONVERT(NVARCHAR(19), a.created_at, 120), N''))
      FROM [audit_log] a WHERE a.id = @id;
    DELETE FROM @utf8;
    INSERT INTO @utf8 (v) VALUES (@text);
    SELECT @hash = LOWER(CONVERT(CHAR(64), HASHBYTES('SHA2_256', v), 2)) FROM @utf8;
    UPDATE [audit_log] SET prev_hash = @prev, row_hash = @hash WHERE id = @id;
    SET @prev = @hash;
    FETCH NEXT FROM rows_in INTO @id;
  END
  CLOSE rows_in;
  DEALLOCATE rows_in;
  UPDATE [audit_chain_head] SET head_hash = @prev WHERE id = 1;
END`,
}

// Render writes the base file of one store: every table with its columns,
// defaults, checks and keys; the rows a fresh install holds; the foreign
// keys; the triggers. Batches are split by GO lines.
func Render(ctx context.Context, db *sql.DB, s *Schema, o Options) (string, Notes, error) {
	var notes Notes
	var b strings.Builder
	eval := func(expr string) (string, error) { return EvalDefault(ctx, db, expr) }

	var tables []*Table
	for _, t := range s.Tables {
		if !o.Skip[t.Name] {
			tables = append(tables, t)
		}
	}
	if len(s.Views) > 0 {
		return "", notes, fmt.Errorf("views are not carried over: %v", s.Views)
	}
	for _, tr := range s.Triggers {
		if _, ok := Triggers[tr.Name]; !ok {
			return "", notes, fmt.Errorf("trigger %s on %s has no T-SQL translation (schemagen.Triggers)", tr.Name, tr.Table)
		}
	}

	var body strings.Builder
	widths, err := MapSchema(s)
	if err != nil {
		return "", notes, err
	}
	for _, t := range tables {
		ddl, err := tableDDL(t, widths[t.Name], eval, &notes)
		if err != nil {
			return "", notes, fmt.Errorf("%s: %w", t.Name, err)
		}
		body.WriteString(ddl)
		body.WriteString("GO\n\n")
	}
	for _, t := range tables {
		if t.Rows == 0 {
			if t.AutoIncrement > 1 {
				// Nothing inserted yet: the first id is the seed itself.
				fmt.Fprintf(&body, "DBCC CHECKIDENT ('%s', RESEED, %d) WITH NO_INFOMSGS;\nGO\n\n", t.Name, t.AutoIncrement)
			}
			continue
		}
		data, err := tableRows(ctx, db, t)
		if err != nil {
			return "", notes, fmt.Errorf("%s rows: %w", t.Name, err)
		}
		body.WriteString(data)
		body.WriteString("GO\n\n")
	}
	for _, t := range tables {
		for _, fk := range t.ForeignKeys {
			if o.Skip[fk.RefTable] {
				continue
			}
			fmt.Fprintf(&body, "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)%s%s;\n",
				Quote(t.Name), Quote(fk.Name), quoteList(fk.Columns), Quote(fk.RefTable), quoteList(fk.RefColumns),
				fkRule("DELETE", fk.OnDelete), fkRule("UPDATE", fk.OnUpdate))
		}
	}
	body.WriteString("GO\n\n")
	for _, tr := range s.Triggers {
		body.WriteString(Triggers[tr.Name])
		body.WriteString("\nGO\n\n")
		notes.Triggers = append(notes.Triggers, tr.Name+" ("+tr.Timing+" "+tr.Event+" on "+tr.Table+")")
	}

	fmt.Fprintf(&b, "-- The %s store on SQL Server 2022: its starting schema.\n", o.Store)
	b.WriteString("--\n-- GENERATED by scripts/mssql/genbase from a fresh MySQL install; do not edit by hand.\n")
	b.WriteString("-- It equals the MySQL schema at the head below. Every migration added after it is\n")
	b.WriteString("-- written for both databases: migrations/" + o.Store + "/mysql/NNNN_x.sql and its twin\n")
	b.WriteString("-- migrations/" + o.Store + "/mssql/NNNN_x.sql (internal/dal/mssql parity_twins_test.go).\n--\n")
	fmt.Fprintf(&b, "-- mysql-head: %s\n", o.MySQLHead)
	if o.AdminHead != "" {
		fmt.Fprintf(&b, "-- admin-head: %s\n", o.AdminHead)
	}
	b.WriteString("--\n-- Types: DATETIME(n) -> DATETIME2(n) (UTC, SYSUTCDATETIME()); JSON -> NVARCHAR(MAX) +\n")
	b.WriteString("-- ISJSON check; ENUM -> VARCHAR + IN check; text -> NVARCHAR, " + TextCollation + ";\n")
	b.WriteString("-- CHAR(26) ids -> " + BinCollation + "; AUTO_INCREMENT -> IDENTITY; UNSIGNED widened\n")
	b.WriteString("-- where SQL Server's signed type would be too small. VARCHAR(n), n <= 2000 ->\n")
	b.WriteString("-- NVARCHAR(2n): n characters of any kind (widths.go; not the text of a key too wide).\n")
	if len(notes.OnUpdate) > 0 {
		b.WriteString("--\n-- MySQL sets these on every UPDATE (ON UPDATE CURRENT_TIMESTAMP); SQL Server has no\n")
		b.WriteString("-- such clause, so the statement that updates the row sets them:\n")
		for _, line := range wrapList(notes.OnUpdate, 88) {
			b.WriteString("--   " + line + "\n")
		}
	}
	if len(notes.NullableUniques) > 0 {
		b.WriteString("--\n-- MySQL lets a unique key hold many NULLs; SQL Server one. These unique keys over\n")
		b.WriteString("-- a nullable column are filtered (WHERE col IS NOT NULL), which is MySQL's rule:\n")
		for _, line := range wrapList(notes.NullableUniques, 88) {
			b.WriteString("--   " + line + "\n")
		}
	}
	b.WriteString("\n")
	b.WriteString(body.String())
	return b.String(), notes, nil
}

func wrapList(items []string, width int) []string {
	var out []string
	cur := ""
	for _, it := range items {
		if cur != "" && len(cur)+2+len(it) > width {
			out = append(out, cur+",")
			cur = ""
		}
		if cur != "" {
			cur += ", "
		}
		cur += it
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

func quoteList(cols []string) string {
	q := make([]string, len(cols))
	for i, c := range cols {
		q[i] = Quote(c)
	}
	return strings.Join(q, ", ")
}

func fkRule(on, rule string) string {
	switch strings.ToUpper(rule) {
	case "CASCADE":
		return " ON " + on + " CASCADE"
	case "SET NULL":
		return " ON " + on + " SET NULL"
	}
	return "" // NO ACTION, RESTRICT: SQL Server's default
}

// tableDDL is one table's CREATE TABLE and CREATE INDEX statements.
// mapped is the table's columns as MapSchema maps them (the text widths rule).
func tableDDL(t *Table, mapped map[string]Mapped, eval func(string) (string, error), notes *Notes) (string, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE %s (\n", Quote(t.Name))
	var lines []string
	var checks []string
	for _, c := range t.Columns {
		m, ok := mapped[strings.ToLower(c.Name)]
		if !ok {
			return "", fmt.Errorf("column %s: not mapped", c.Name)
		}
		line := "  " + Quote(c.Name) + " " + m.Type
		if m.Collation != "" {
			line += " COLLATE " + m.Collation
		}
		if c.AutoIncrement() {
			// MySQL's next id, when it is not 1, is set by CHECKIDENT after
			// the rows (Render).
			line += " IDENTITY(1,1)"
		}
		if c.Nullable {
			line += " NULL"
		} else {
			line += " NOT NULL"
		}
		def, err := DefaultSQL(c, eval)
		if err != nil {
			return "", err
		}
		if def != "" && !c.AutoIncrement() {
			line += " CONSTRAINT " + Quote(ConstraintName("DF", t.Name, c.Name)) + " DEFAULT (" + def + ")"
		}
		if c.OnUpdateNow() {
			notes.OnUpdate = append(notes.OnUpdate, t.Name+"."+c.Name)
		}
		lines = append(lines, line)
		if m.Check != "" {
			checks = append(checks, "  CONSTRAINT "+Quote(ConstraintName("CK", t.Name, c.Name))+" CHECK ("+m.Check+")")
		}
	}
	if pk := t.Primary(); pk != nil {
		bytes, _ := keyBytes(*pk, mapped)
		kind := ""
		if bytes > ClusteredKeyMax {
			kind = " NONCLUSTERED"
			notes.NonClusteredPKs = append(notes.NonClusteredPKs, fmt.Sprintf("%s (%d bytes)", t.Name, bytes))
		}
		lines = append(lines, "  CONSTRAINT "+Quote(ConstraintName("PK", t.Name, ""))+" PRIMARY KEY"+kind+" ("+indexCols(*pk)+")")
	}
	for _, ck := range t.Checks {
		lines = append(lines, "  CONSTRAINT "+Quote(ck.Name)+" CHECK "+checkClause(ck.Clause))
	}
	lines = append(lines, checks...)
	b.WriteString(strings.Join(lines, ",\n"))
	b.WriteString("\n);\n")

	for _, ix := range t.Indexes {
		if ix.Name == "PRIMARY" {
			continue
		}
		if ix.Kind != "BTREE" {
			return "", fmt.Errorf("index %s is %s: not carried over", ix.Name, ix.Kind)
		}
		for _, c := range ix.Columns {
			if c.SubPart > 0 {
				return "", fmt.Errorf("index %s has a prefix part on %s: decide by hand", ix.Name, c.Name)
			}
		}
		bytes, err := keyBytes(ix, mapped)
		if err != nil {
			return "", fmt.Errorf("index %s: %w", ix.Name, err)
		}
		if bytes > NonClusteredKeyMax {
			notes.WideKeys = append(notes.WideKeys, fmt.Sprintf("%s.%s (%d bytes)", t.Name, ix.Name, bytes))
		}
		kw := "INDEX"
		where := ""
		if ix.Unique {
			kw = "UNIQUE INDEX"
			var nullable []string
			for _, c := range ix.Columns {
				if col := t.Column(c.Name); col != nil && col.Nullable {
					nullable = append(nullable, Quote(c.Name)+" IS NOT NULL")
				}
			}
			if len(nullable) > 0 {
				where = " WHERE " + strings.Join(nullable, " AND ")
				notes.NullableUniques = append(notes.NullableUniques, t.Name+"."+ix.Name)
			}
		}
		fmt.Fprintf(&b, "CREATE %s %s ON %s (%s)%s;\n", kw, Quote(ix.Name), Quote(t.Name), indexCols(ix), where)
	}
	return b.String(), nil
}

func indexCols(ix Index) string {
	q := make([]string, len(ix.Columns))
	for i, c := range ix.Columns {
		q[i] = Quote(c.Name)
		if c.Desc {
			q[i] += " DESC"
		}
	}
	return strings.Join(q, ", ")
}

// keyBytes is the widest an index's key can be.
func keyBytes(ix Index, mapped map[string]Mapped) (int, error) {
	n := 0
	for _, c := range ix.Columns {
		m, ok := mapped[strings.ToLower(c.Name)]
		if !ok {
			return 0, fmt.Errorf("no column %s", c.Name)
		}
		if m.KeyBytes < 0 {
			return 0, fmt.Errorf("column %s is %s, which cannot be in a key", c.Name, m.Type)
		}
		n += m.KeyBytes
	}
	return n, nil
}

var (
	backtickRe   = regexp.MustCompile("`([^`]+)`")
	introducerRe = regexp.MustCompile(`_utf8mb4\\?'`)
)

// checkClause turns a MySQL CHECK clause into T-SQL: names bracketed, the
// charset introducers taken off. The clauses in the schema are plain
// comparisons and IN lists.
func checkClause(c string) string {
	c = backtickRe.ReplaceAllString(c, "[$1]")
	c = introducerRe.ReplaceAllString(c, "'")
	c = strings.ReplaceAll(c, `\'`, `'`)
	if !strings.HasPrefix(c, "(") {
		c = "(" + c + ")"
	}
	return c
}

// tableRows is the INSERTs of a table's rows, in primary key order, 500 a
// statement, with IDENTITY_INSERT on when the table has an identity.
func tableRows(ctx context.Context, db *sql.DB, t *Table) (string, error) {
	sel := make([]string, len(t.Columns))
	ident := false
	for i, c := range t.Columns {
		switch {
		case IsBinary(c):
			sel[i] = "HEX(`" + c.Name + "`)"
		case c.DataType == "json":
			sel[i] = "CAST(`" + c.Name + "` AS CHAR)"
		default:
			sel[i] = "`" + c.Name + "`"
		}
		if c.AutoIncrement() {
			ident = true
		}
	}
	order := ""
	if pk := t.Primary(); pk != nil {
		var cols []string
		for _, c := range pk.Columns {
			cols = append(cols, "`"+c.Name+"`")
		}
		order = " ORDER BY " + strings.Join(cols, ", ")
	}
	rows, err := db.QueryContext(ctx, "SELECT "+strings.Join(sel, ", ")+" FROM `"+t.Name+"`"+order)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var values []string
	for rows.Next() {
		vals := make([]sql.NullString, len(t.Columns))
		ptrs := make([]any, len(vals))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return "", err
		}
		lits := make([]string, len(vals))
		for i, v := range vals {
			c := t.Columns[i]
			switch {
			case !v.Valid:
				lits[i] = "NULL"
			case IsBinary(c):
				lits[i] = "0x" + v.String
				if v.String == "" {
					lits[i] = "0x"
				}
			case IsNumeric(c):
				lits[i] = v.String
			default:
				lits[i] = Literal(v.String)
			}
		}
		values = append(values, "("+strings.Join(lits, ", ")+")")
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	cols := make([]string, len(t.Columns))
	for i, c := range t.Columns {
		cols[i] = Quote(c.Name)
	}
	var b strings.Builder
	if ident {
		fmt.Fprintf(&b, "SET IDENTITY_INSERT %s ON;\n", Quote(t.Name))
	}
	for start := 0; start < len(values); start += 500 {
		end := start + 500
		if end > len(values) {
			end = len(values)
		}
		fmt.Fprintf(&b, "INSERT INTO %s (%s) VALUES\n  %s;\n", Quote(t.Name), strings.Join(cols, ", "),
			strings.Join(values[start:end], ",\n  "))
	}
	if ident {
		fmt.Fprintf(&b, "SET IDENTITY_INSERT %s OFF;\n", Quote(t.Name))
		// MySQL's next id may be past the highest row (rows moved away).
		var max int64
		for _, c := range t.Columns {
			if c.AutoIncrement() {
				if err := db.QueryRowContext(ctx, "SELECT COALESCE(MAX(`"+c.Name+"`), 0) FROM `"+t.Name+"`").Scan(&max); err != nil {
					return "", err
				}
			}
		}
		if t.AutoIncrement > max+1 {
			fmt.Fprintf(&b, "DBCC CHECKIDENT ('%s', RESEED, %d) WITH NO_INFOMSGS;\n", t.Name, t.AutoIncrement-1)
		}
	}
	return b.String(), nil
}

// SortedTableNames lists a schema's table names.
func SortedTableNames(s *Schema) []string {
	out := make([]string, 0, len(s.Tables))
	for _, t := range s.Tables {
		out = append(out, t.Name)
	}
	sort.Strings(out)
	return out
}
