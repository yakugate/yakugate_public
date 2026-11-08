package mssql

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Tables made from a template at run time: the per-company, per-day record
// tables (mt_<co>_<day> … from tpl_day_*), the vendor queues (queue_<cid>
// from queue_template), the HLR shards (hlr_<first4> from hlr_template).
//
// MySQL makes them with CREATE TABLE … LIKE. SQL Server has no such thing,
// so the DDL is built from the template as the catalog has it (sys.columns,
// sys.default_constraints, sys.check_constraints, sys.indexes): the same
// columns, types, collations, defaults, checks, primary key and indexes —
// no foreign keys and no triggers, as LIKE copies none either — and an
// identity that starts again at 1. Constraint names are the database's, not
// the table's, so each is renamed for the new table.
//
// The DDL of a template is cached until the template changes (its
// modify_date), and a table is made under an application lock named for it,
// so two processes making the same day's table at once make it once.

var (
	stCloneTemplate = SchemaStmts.Add("schema.clone_template", `SELECT o.object_id, o.modify_date
		FROM sys.objects o WHERE o.object_id = OBJECT_ID(@p1) AND o.type = 'U'`, false)
	stCloneColumns = SchemaStmts.Add("schema.clone_columns", `SELECT c.name, t.name, c.max_length, c.precision, c.scale,
			COALESCE(c.collation_name, ''), c.is_nullable, c.is_identity, COALESCE(dc.name, ''), COALESCE(dc.definition, '')
		FROM sys.columns c
		JOIN sys.types t ON t.user_type_id = c.user_type_id
		LEFT JOIN sys.default_constraints dc ON dc.object_id = c.default_object_id
		WHERE c.object_id = @p1 ORDER BY c.column_id`, false)
	stCloneChecks = SchemaStmts.Add("schema.clone_checks", `SELECT name, definition FROM sys.check_constraints
		WHERE parent_object_id = @p1 ORDER BY name`, false)
	stCloneIndexes = SchemaStmts.Add("schema.clone_indexes", `SELECT i.index_id, i.name, i.is_unique, i.is_primary_key,
			i.type_desc, COALESCE(i.filter_definition, ''), c.name, ic.is_descending_key, ic.is_included_column
		FROM sys.indexes i
		JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
		JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
		WHERE i.object_id = @p1 AND i.type IN (1, 2) AND i.is_hypothetical = 0
		ORDER BY i.index_id, ic.is_included_column, ic.key_ordinal, ic.index_column_id`, false)
	stTableExists = SchemaStmts.Add("schema.table_exists", `SELECT CASE WHEN OBJECT_ID(@p1, N'U') IS NULL THEN 0 ELSE 1 END`, false)
	stAppLockTx   = SchemaStmts.Add("schema.applock_tx", `DECLARE @r INT;
		EXEC @r = sp_getapplock @Resource = @p1, @LockMode = 'Exclusive', @LockOwner = 'Transaction', @LockTimeout = @p2;
		SELECT @r`, false)
)

// column is one column as the catalog has it.
type tplColumn struct {
	name, typ          string
	maxLen             int
	precision, scale   int
	collation          string
	nullable, identity bool
	defName, defSQL    string
}

// typeSQL is the column's type as DDL writes it.
func (c tplColumn) typeSQL() string {
	switch c.typ {
	case "nvarchar", "nchar":
		if c.maxLen < 0 {
			return strings.ToUpper(c.typ) + "(MAX)"
		}
		return fmt.Sprintf("%s(%d)", strings.ToUpper(c.typ), c.maxLen/2)
	case "varchar", "char", "varbinary", "binary":
		if c.maxLen < 0 {
			return strings.ToUpper(c.typ) + "(MAX)"
		}
		return fmt.Sprintf("%s(%d)", strings.ToUpper(c.typ), c.maxLen)
	case "decimal", "numeric":
		return fmt.Sprintf("%s(%d,%d)", strings.ToUpper(c.typ), c.precision, c.scale)
	case "datetime2", "time", "datetimeoffset":
		return fmt.Sprintf("%s(%d)", strings.ToUpper(c.typ), c.scale)
	}
	return strings.ToUpper(c.typ)
}

// isText reports a character type (it carries a collation).
func (c tplColumn) isText() bool {
	switch c.typ {
	case "nvarchar", "nchar", "varchar", "char":
		return true
	}
	return false
}

// definition is the column's clause in CREATE TABLE or ALTER TABLE … ADD,
// its default constraint named for table.
func (c tplColumn) definition(template, table string) string {
	s := schemaQuote(c.name) + " " + c.typeSQL()
	if c.isText() && c.collation != "" {
		s += " COLLATE " + c.collation
	}
	if c.identity {
		s += " IDENTITY(1,1)"
	}
	if c.nullable {
		s += " NULL"
	} else {
		s += " NOT NULL"
	}
	if c.defSQL != "" {
		s += " CONSTRAINT " + schemaQuote(renameConstraint(c.defName, template, table)) + " DEFAULT " + c.defSQL
	}
	return s
}

// index is one index as the catalog has it.
type tplIndex struct {
	name                string
	unique, primary     bool
	clustered           bool
	filter              string
	keys, desc, include []string
}

// tableShape is a template's columns, checks and indexes.
type tableShape struct {
	columns []tplColumn
	checks  [][2]string // name, definition
	indexes []tplIndex
}

// readShape reads a table's shape by object id.
func readShape(ctx context.Context, q catalogQuerier, objectID int64) (*tableShape, error) {
	sh := &tableShape{}
	rows, err := q.QueryContext(ctx, stCloneColumns.SQL, objectID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var c tplColumn
		if err := rows.Scan(&c.name, &c.typ, &c.maxLen, &c.precision, &c.scale, &c.collation,
			&c.nullable, &c.identity, &c.defName, &c.defSQL); err != nil {
			rows.Close()
			return nil, err
		}
		sh.columns = append(sh.columns, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows, err = q.QueryContext(ctx, stCloneChecks.SQL, objectID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var ck [2]string
		if err := rows.Scan(&ck[0], &ck[1]); err != nil {
			rows.Close()
			return nil, err
		}
		sh.checks = append(sh.checks, ck)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows, err = q.QueryContext(ctx, stCloneIndexes.SQL, objectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	last := -1
	for rows.Next() {
		var (
			id                          int
			name, kind, filter, colName string
			unique, primary, desc, incl bool
		)
		if err := rows.Scan(&id, &name, &unique, &primary, &kind, &filter, &colName, &desc, &incl); err != nil {
			return nil, err
		}
		if id != last {
			sh.indexes = append(sh.indexes, tplIndex{name: name, unique: unique, primary: primary,
				clustered: kind == "CLUSTERED", filter: filter})
			last = id
		}
		ix := &sh.indexes[len(sh.indexes)-1]
		switch {
		case incl:
			ix.include = append(ix.include, colName)
		default:
			ix.keys = append(ix.keys, colName)
			d := ""
			if desc {
				d = " DESC"
			}
			ix.desc = append(ix.desc, d)
		}
	}
	return sh, rows.Err()
}

// createDDL is the CREATE TABLE and CREATE INDEX statements of table made
// from template's shape.
func (sh *tableShape) createDDL(template, table string) string {
	var lines []string
	for _, c := range sh.columns {
		lines = append(lines, "  "+c.definition(template, table))
	}
	for _, ix := range sh.indexes {
		if !ix.primary {
			continue
		}
		kind := " NONCLUSTERED"
		if ix.clustered {
			kind = " CLUSTERED"
		}
		lines = append(lines, "  CONSTRAINT "+schemaQuote(renameConstraint(ix.name, template, table))+" PRIMARY KEY"+kind+" ("+ix.keyList()+")")
	}
	for _, ck := range sh.checks {
		lines = append(lines, "  CONSTRAINT "+schemaQuote(renameConstraint(ck[0], template, table))+" CHECK "+ck[1])
	}
	var b strings.Builder
	b.WriteString("CREATE TABLE " + schemaQuote(table) + " (\n" + strings.Join(lines, ",\n") + "\n);\n")
	for _, ix := range sh.indexes {
		if ix.primary {
			continue
		}
		b.WriteString(ix.createSQL(table))
	}
	return b.String()
}

func (ix tplIndex) keyList() string {
	parts := make([]string, len(ix.keys))
	for i, k := range ix.keys {
		parts[i] = schemaQuote(k) + ix.desc[i]
	}
	return strings.Join(parts, ", ")
}

// createSQL is CREATE INDEX for one (non-primary) index on table.
func (ix tplIndex) createSQL(table string) string {
	s := "CREATE "
	if ix.unique {
		s += "UNIQUE "
	}
	if ix.clustered {
		s += "CLUSTERED "
	} else {
		s += "NONCLUSTERED "
	}
	s += "INDEX " + schemaQuote(ix.name) + " ON " + schemaQuote(table) + " (" + ix.keyList() + ")"
	if len(ix.include) > 0 {
		inc := make([]string, len(ix.include))
		for i, c := range ix.include {
			inc[i] = schemaQuote(c)
		}
		s += " INCLUDE (" + strings.Join(inc, ", ") + ")"
	}
	if ix.filter != "" {
		s += " WHERE " + ix.filter
	}
	return s + ";\n"
}

// renameConstraint gives a template's constraint a name of the new table's:
// the template's name inside it is replaced by the table's (PK_tpl_day_mt →
// PK_mt_12_20261030); a name without it gets the table's added. A name past
// SQL Server's 128 characters is cut and made unique with a hash.
func renameConstraint(name, template, table string) string {
	var n string
	if strings.Contains(name, template) {
		n = strings.Replace(name, template, table, 1)
	} else {
		n = name + "_" + table
	}
	if len(n) > 128 {
		sum := sha1.Sum([]byte(n))
		n = n[:111] + "_" + hex.EncodeToString(sum[:])[:16]
	}
	return n
}

// querier is a pool, a pinned connection or a transaction.
type catalogQuerier interface {
	QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row
	ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error)
}

// Cloner makes tables from their templates on one database (CreateLike),
// caching each template's DDL until the template changes.
type Cloner struct {
	db *sql.DB
	mu sync.Mutex
	// by template name: its shape as read at modify date `at`.
	cache map[string]cachedShape
}

type cachedShape struct {
	at    time.Time
	shape *tableShape
}

// NewCloner makes tables from templates on db.
func NewCloner(db *sql.DB) *Cloner {
	return &Cloner{db: db, cache: map[string]cachedShape{}}
}

// cloneLockWait is how long a maker waits for another making the same table.
const cloneLockWait = 30 * time.Second

// CreateLike makes table from template, as MySQL's CREATE TABLE IF NOT
// EXISTS table LIKE template: nothing happens when the table is there. The
// table is made in one transaction under an application lock named for it,
// so it appears whole or not at all, and once.
func (c *Cloner) CreateLike(ctx context.Context, template, table string) error {
	var exists int
	if err := c.db.QueryRowContext(ctx, stTableExists.SQL, table).Scan(&exists); err != nil {
		return fmt.Errorf("create %s like %s: %w", table, template, err)
	}
	if exists == 1 {
		return nil
	}
	sh, err := c.shape(ctx, template)
	if err != nil {
		return fmt.Errorf("create %s like %s: %w", table, template, err)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("create %s like %s: %w", table, template, err)
	}
	defer tx.Rollback()
	var got int
	if err := tx.QueryRowContext(ctx, stAppLockTx.SQL, "yaku_clone:"+table, int(cloneLockWait/time.Millisecond)).Scan(&got); err != nil {
		return fmt.Errorf("create %s like %s: lock: %w", table, template, err)
	}
	if got < 0 {
		return fmt.Errorf("create %s like %s: another process held the lock for %s", table, template, cloneLockWait)
	}
	if err := tx.QueryRowContext(ctx, stTableExists.SQL, table).Scan(&exists); err != nil {
		return fmt.Errorf("create %s like %s: %w", table, template, err)
	}
	if exists == 1 {
		return nil // made by the process that held the lock
	}
	if _, err := tx.ExecContext(ctx, sh.createDDL(template, table)); err != nil {
		return fmt.Errorf("create %s like %s: %w", table, template, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("create %s like %s: %w", table, template, err)
	}
	return nil
}

// shape is the template's shape, from the cache while the template has not
// changed since it was read.
func (c *Cloner) shape(ctx context.Context, template string) (*tableShape, error) {
	var id int64
	var at time.Time
	if err := c.db.QueryRowContext(ctx, stCloneTemplate.SQL, template).Scan(&id, &at); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("no template table %s", template)
		}
		return nil, err
	}
	c.mu.Lock()
	hit, ok := c.cache[template]
	c.mu.Unlock()
	if ok && hit.at.Equal(at) {
		return hit.shape, nil
	}
	sh, err := readShape(ctx, c.db, id)
	if err != nil {
		return nil, err
	}
	if len(sh.columns) == 0 {
		return nil, fmt.Errorf("template %s has no columns", template)
	}
	c.mu.Lock()
	c.cache[template] = cachedShape{at: at, shape: sh}
	c.mu.Unlock()
	return sh, nil
}
