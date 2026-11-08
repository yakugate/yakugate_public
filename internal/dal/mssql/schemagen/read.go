// Package schemagen turns a fresh MySQL install into the SQL Server starting
// schema (migrations/<store>/mssql/0001_base.sql), and holds the one type
// mapping both the generator and the parity test use (plan of 29 Oct 2026,
// section 7, "SQL Server choices").
//
// It is a tool's package: the gateway never links it. The generator is
// scripts/mssql/genbase; the parity test is internal/dal/mssql/parity_test.go.
package schemagen

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

// Column is one MySQL column as information_schema has it.
type Column struct {
	Name       string
	DataType   string // int, varchar, json, enum, …
	ColumnType string // int unsigned, varchar(64), enum('a','b'), …
	Nullable   bool
	Default    sql.NullString // column_default (NULL = no default)
	Extra      string         // auto_increment, DEFAULT_GENERATED on update CURRENT_TIMESTAMP, …
	Collation  string
	CharLen    int64 // character_maximum_length
	Precision  int64 // numeric_precision
	Scale      int64 // numeric_scale
	TimePrec   int64 // datetime_precision
	Comment    string
}

// AutoIncrement reports whether the column is the table's AUTO_INCREMENT.
func (c Column) AutoIncrement() bool {
	return strings.Contains(strings.ToLower(c.Extra), "auto_increment")
}

// OnUpdateNow reports whether MySQL sets the column on every UPDATE (ON
// UPDATE CURRENT_TIMESTAMP). SQL Server has no such clause: the statement
// sets it.
func (c Column) OnUpdateNow() bool {
	return strings.Contains(strings.ToLower(c.Extra), "on update current_timestamp")
}

// Unsigned reports an UNSIGNED integer.
func (c Column) Unsigned() bool { return strings.Contains(strings.ToLower(c.ColumnType), "unsigned") }

// IndexColumn is one column of a key.
type IndexColumn struct {
	Name    string
	SubPart int64 // a prefix length (none in the schema today)
	Desc    bool
}

// Index is one MySQL key (PRIMARY included).
type Index struct {
	Name    string
	Unique  bool
	Kind    string // BTREE, FULLTEXT, …
	Columns []IndexColumn
}

// ForeignKey is one foreign key.
type ForeignKey struct {
	Name       string
	Columns    []string
	RefTable   string
	RefColumns []string
	OnDelete   string
	OnUpdate   string
}

// Check is one named CHECK constraint.
type Check struct {
	Name   string
	Clause string
}

// Trigger is one trigger, kept for the report (a MySQL trigger body is not
// translated by machine).
type Trigger struct {
	Name, Table, Timing, Event, Body string
}

// Table is one base table.
type Table struct {
	Name          string
	Columns       []Column
	Indexes       []Index // PRIMARY first, then by name
	ForeignKeys   []ForeignKey
	Checks        []Check
	AutoIncrement int64 // the next AUTO_INCREMENT value (0 = none)
	Rows          int64 // exact row count
}

// Primary is the table's primary key, or nil.
func (t *Table) Primary() *Index {
	for i := range t.Indexes {
		if t.Indexes[i].Name == "PRIMARY" {
			return &t.Indexes[i]
		}
	}
	return nil
}

// Column finds a column by name (any case).
func (t *Table) Column(name string) *Column {
	for i := range t.Columns {
		if strings.EqualFold(t.Columns[i].Name, name) {
			return &t.Columns[i]
		}
	}
	return nil
}

// Schema is one MySQL database: its base tables, triggers and views.
type Schema struct {
	Database string
	Tables   []*Table // by name
	Triggers []Trigger
	Views    []string
}

// Table finds a table by name.
func (s *Schema) Table(name string) *Table {
	for _, t := range s.Tables {
		if t.Name == name {
			return t
		}
	}
	return nil
}

// ReadMySQL reads the database db is connected to. Exact row counts are read
// too (a fresh install is small).
func ReadMySQL(ctx context.Context, db *sql.DB) (*Schema, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	// information_schema keeps AUTO_INCREMENT cached for a day by default.
	if _, err := conn.ExecContext(ctx, "SET SESSION information_schema_stats_expiry = 0"); err != nil {
		return nil, err
	}
	s := &Schema{}
	if err := conn.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&s.Database); err != nil {
		return nil, err
	}
	byName := map[string]*Table{}
	rows, err := conn.QueryContext(ctx, `SELECT table_name, table_type, COALESCE(auto_increment, 0)
		FROM information_schema.tables WHERE table_schema = DATABASE() ORDER BY table_name`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var name, kind string
		var ai int64
		if err := rows.Scan(&name, &kind, &ai); err != nil {
			rows.Close()
			return nil, err
		}
		if kind == "VIEW" {
			s.Views = append(s.Views, name)
			continue
		}
		t := &Table{Name: name, AutoIncrement: ai}
		s.Tables = append(s.Tables, t)
		byName[name] = t
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = conn.QueryContext(ctx, `SELECT table_name, column_name, data_type, column_type, is_nullable,
			column_default, extra, COALESCE(collation_name, ''), COALESCE(character_maximum_length, 0),
			COALESCE(numeric_precision, 0), COALESCE(numeric_scale, 0), COALESCE(datetime_precision, 0),
			column_comment
		FROM information_schema.columns WHERE table_schema = DATABASE()
		ORDER BY table_name, ordinal_position`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var table, nullable string
		var c Column
		if err := rows.Scan(&table, &c.Name, &c.DataType, &c.ColumnType, &nullable, &c.Default, &c.Extra,
			&c.Collation, &c.CharLen, &c.Precision, &c.Scale, &c.TimePrec, &c.Comment); err != nil {
			rows.Close()
			return nil, err
		}
		c.DataType = strings.ToLower(c.DataType)
		c.Nullable = nullable == "YES"
		if t := byName[table]; t != nil {
			t.Columns = append(t.Columns, c)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = conn.QueryContext(ctx, `SELECT table_name, index_name, non_unique, index_type, column_name,
			COALESCE(sub_part, 0), COALESCE(collation, 'A')
		FROM information_schema.statistics WHERE table_schema = DATABASE()
		ORDER BY table_name, index_name, seq_in_index`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var table, name, kind, coll string
		var col sql.NullString
		var nonUnique int
		var sub int64
		if err := rows.Scan(&table, &name, &nonUnique, &kind, &col, &sub, &coll); err != nil {
			rows.Close()
			return nil, err
		}
		t := byName[table]
		if t == nil {
			continue
		}
		if !col.Valid {
			rows.Close()
			return nil, fmt.Errorf("%s.%s: a functional key part cannot be carried to SQL Server by machine", table, name)
		}
		var ix *Index
		for i := range t.Indexes {
			if t.Indexes[i].Name == name {
				ix = &t.Indexes[i]
			}
		}
		if ix == nil {
			t.Indexes = append(t.Indexes, Index{Name: name, Unique: nonUnique == 0, Kind: kind})
			ix = &t.Indexes[len(t.Indexes)-1]
		}
		ix.Columns = append(ix.Columns, IndexColumn{Name: col.String, SubPart: sub, Desc: coll == "D"})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, t := range s.Tables {
		sort.SliceStable(t.Indexes, func(i, j int) bool {
			if (t.Indexes[i].Name == "PRIMARY") != (t.Indexes[j].Name == "PRIMARY") {
				return t.Indexes[i].Name == "PRIMARY"
			}
			return t.Indexes[i].Name < t.Indexes[j].Name
		})
	}

	rows, err = conn.QueryContext(ctx, `SELECT k.table_name, k.constraint_name, k.column_name,
			k.referenced_table_name, k.referenced_column_name, r.delete_rule, r.update_rule
		FROM information_schema.key_column_usage k
		JOIN information_schema.referential_constraints r
		  ON r.constraint_schema = k.constraint_schema AND r.constraint_name = k.constraint_name
		 AND r.table_name = k.table_name
		WHERE k.table_schema = DATABASE() AND k.referenced_table_name IS NOT NULL
		ORDER BY k.table_name, k.constraint_name, k.ordinal_position`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var table, name, col, ref, refCol, del, upd string
		if err := rows.Scan(&table, &name, &col, &ref, &refCol, &del, &upd); err != nil {
			rows.Close()
			return nil, err
		}
		t := byName[table]
		if t == nil {
			continue
		}
		var fk *ForeignKey
		for i := range t.ForeignKeys {
			if t.ForeignKeys[i].Name == name {
				fk = &t.ForeignKeys[i]
			}
		}
		if fk == nil {
			t.ForeignKeys = append(t.ForeignKeys, ForeignKey{Name: name, RefTable: ref, OnDelete: del, OnUpdate: upd})
			fk = &t.ForeignKeys[len(t.ForeignKeys)-1]
		}
		fk.Columns = append(fk.Columns, col)
		fk.RefColumns = append(fk.RefColumns, refCol)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = conn.QueryContext(ctx, `SELECT t.table_name, c.constraint_name, c.check_clause
		FROM information_schema.table_constraints t
		JOIN information_schema.check_constraints c
		  ON c.constraint_schema = t.constraint_schema AND c.constraint_name = t.constraint_name
		WHERE t.table_schema = DATABASE() AND t.constraint_type = 'CHECK'
		ORDER BY t.table_name, c.constraint_name`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var table string
		var c Check
		if err := rows.Scan(&table, &c.Name, &c.Clause); err != nil {
			rows.Close()
			return nil, err
		}
		if t := byName[table]; t != nil {
			t.Checks = append(t.Checks, c)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = conn.QueryContext(ctx, `SELECT trigger_name, event_object_table, action_timing,
			event_manipulation, action_statement
		FROM information_schema.triggers WHERE trigger_schema = DATABASE() ORDER BY trigger_name`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var tr Trigger
		if err := rows.Scan(&tr.Name, &tr.Table, &tr.Timing, &tr.Event, &tr.Body); err != nil {
			rows.Close()
			return nil, err
		}
		s.Triggers = append(s.Triggers, tr)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for _, t := range s.Tables {
		if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM `"+t.Name+"`").Scan(&t.Rows); err != nil {
			return nil, fmt.Errorf("count %s: %w", t.Name, err)
		}
	}
	return s, nil
}

// EvalDefault asks MySQL for the value of an expression default
// (DEFAULT (json_object(…))), as text.
func EvalDefault(ctx context.Context, db *sql.DB, expr string) (string, error) {
	// information_schema escapes the quotes of an expression default.
	e := strings.ReplaceAll(expr, `\'`, `'`)
	var v sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT CAST(("+e+") AS CHAR)").Scan(&v); err != nil {
		return "", fmt.Errorf("evaluate default %s: %w", expr, err)
	}
	return v.String, nil
}
