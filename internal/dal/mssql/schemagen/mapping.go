package schemagen

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// The collations of the SQL Server schema (plan section 7): text compares
// as MySQL's utf8mb4 *_ci does (case-insensitive; supplementary characters
// kept whole), ids and binary-collated text byte for byte.
const (
	TextCollation = "Latin1_General_100_CI_AS_SC"
	BinCollation  = "Latin1_General_100_BIN2"
)

// Mapped is a MySQL column's SQL Server type.
type Mapped struct {
	Type      string // NVARCHAR(64), BIGINT, DATETIME2(3), …
	Collation string // text types only
	// Check is the column's own CHECK expression: an ENUM's list of values,
	// a JSON column's ISJSON. "" = none.
	Check string
	// KeyBytes is what one value costs in an index key; -1 = a MAX type,
	// which cannot be a key column.
	KeyBytes int
}

// Map gives a MySQL column's SQL Server type:
//
//	DATETIME(n), TIMESTAMP(n)   DATETIME2(n), UTC
//	JSON                        NVARCHAR(MAX) + CHECK (ISJSON(col, VALUE) = 1)
//	ENUM(…)                     VARCHAR(longest) + CHECK (col IN (…))
//	TINYINT(1), TINYINT UNSIGNED TINYINT; a signed TINYINT is SMALLINT (it may hold -1)
//	SMALLINT UNSIGNED           INT;  INT UNSIGNED BIGINT;  BIGINT UNSIGNED BIGINT
//	VARCHAR(n), TEXT…           NVARCHAR(n) / NVARCHAR(MAX), Latin1_General_100_CI_AS_SC
//	CHAR(26) (a ULID)           CHAR(26) COLLATE Latin1_General_100_BIN2
//	CHAR(n)                     CHAR(n) (codes, hashes and UUIDs: ASCII)
//	*_bin text                  the same type, Latin1_General_100_BIN2
//	BINARY, VARBINARY, BLOB…    BINARY(n), VARBINARY(n), VARBINARY(MAX)
func Map(c Column) (Mapped, error) {
	ct := strings.ToLower(c.ColumnType)
	bin := strings.HasSuffix(strings.ToLower(c.Collation), "_bin")
	textColl := TextCollation
	if bin {
		textColl = BinCollation
	}
	switch c.DataType {
	case "tinyint":
		if ct == "tinyint(1)" || c.Unsigned() {
			return Mapped{Type: "TINYINT", KeyBytes: 1}, nil
		}
		return Mapped{Type: "SMALLINT", KeyBytes: 2}, nil
	case "smallint":
		if c.Unsigned() {
			return Mapped{Type: "INT", KeyBytes: 4}, nil
		}
		return Mapped{Type: "SMALLINT", KeyBytes: 2}, nil
	case "mediumint":
		return Mapped{Type: "INT", KeyBytes: 4}, nil
	case "int":
		if c.Unsigned() {
			return Mapped{Type: "BIGINT", KeyBytes: 8}, nil
		}
		return Mapped{Type: "INT", KeyBytes: 4}, nil
	case "bigint":
		return Mapped{Type: "BIGINT", KeyBytes: 8}, nil
	case "decimal":
		return Mapped{Type: fmt.Sprintf("DECIMAL(%d,%d)", c.Precision, c.Scale), KeyBytes: decimalBytes(c.Precision)}, nil
	case "float":
		return Mapped{Type: "REAL", KeyBytes: 4}, nil
	case "double":
		return Mapped{Type: "FLOAT", KeyBytes: 8}, nil
	case "datetime", "timestamp":
		return Mapped{Type: fmt.Sprintf("DATETIME2(%d)", c.TimePrec), KeyBytes: datetime2Bytes(c.TimePrec)}, nil
	case "date":
		return Mapped{Type: "DATE", KeyBytes: 3}, nil
	case "time":
		return Mapped{Type: fmt.Sprintf("TIME(%d)", c.TimePrec), KeyBytes: 5}, nil
	case "char":
		if c.CharLen == 26 {
			return Mapped{Type: "CHAR(26)", Collation: BinCollation, KeyBytes: 26}, nil
		}
		return Mapped{Type: fmt.Sprintf("CHAR(%d)", c.CharLen), Collation: textColl, KeyBytes: int(c.CharLen)}, nil
	case "varchar":
		if c.CharLen > 4000 {
			return Mapped{Type: "NVARCHAR(MAX)", Collation: textColl, KeyBytes: -1}, nil
		}
		return Mapped{Type: fmt.Sprintf("NVARCHAR(%d)", c.CharLen), Collation: textColl, KeyBytes: int(2 * c.CharLen)}, nil
	case "tinytext":
		return Mapped{Type: "NVARCHAR(255)", Collation: textColl, KeyBytes: 510}, nil
	case "text", "mediumtext", "longtext":
		return Mapped{Type: "NVARCHAR(MAX)", Collation: textColl, KeyBytes: -1}, nil
	case "json":
		return Mapped{Type: "NVARCHAR(MAX)", Collation: TextCollation, KeyBytes: -1,
			Check: "ISJSON(" + Quote(c.Name) + ", VALUE) = 1"}, nil
	case "enum":
		vals, err := enumValues(c.ColumnType)
		if err != nil {
			return Mapped{}, fmt.Errorf("%s: %w", c.Name, err)
		}
		longest := 1
		lits := make([]string, len(vals))
		for i, v := range vals {
			if len(v) > longest {
				longest = len(v)
			}
			lits[i] = "'" + strings.ReplaceAll(v, "'", "''") + "'"
		}
		return Mapped{Type: fmt.Sprintf("VARCHAR(%d)", longest), Collation: TextCollation, KeyBytes: longest,
			Check: Quote(c.Name) + " IN (" + strings.Join(lits, ", ") + ")"}, nil
	case "binary":
		return Mapped{Type: fmt.Sprintf("BINARY(%d)", c.CharLen), KeyBytes: int(c.CharLen)}, nil
	case "varbinary":
		if c.CharLen > 8000 {
			return Mapped{Type: "VARBINARY(MAX)", KeyBytes: -1}, nil
		}
		return Mapped{Type: fmt.Sprintf("VARBINARY(%d)", c.CharLen), KeyBytes: int(c.CharLen)}, nil
	case "tinyblob":
		return Mapped{Type: "VARBINARY(255)", KeyBytes: 255}, nil
	case "blob", "mediumblob", "longblob":
		return Mapped{Type: "VARBINARY(MAX)", KeyBytes: -1}, nil
	case "bit":
		return Mapped{Type: "BIT", KeyBytes: 1}, nil
	}
	return Mapped{}, fmt.Errorf("%s: no SQL Server type for MySQL %s", c.Name, c.ColumnType)
}

// IsBinary reports a MySQL binary type (its values are written as 0x… and
// read back as hex).
func IsBinary(c Column) bool {
	switch c.DataType {
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
		return true
	}
	return false
}

// IsNumeric reports a MySQL number type (its values are written bare).
func IsNumeric(c Column) bool {
	switch c.DataType {
	case "tinyint", "smallint", "mediumint", "int", "bigint", "decimal", "float", "double", "bit":
		return true
	}
	return false
}

// IsText reports a MySQL type whose values are text (N'…').
func IsText(c Column) bool {
	return !IsNumeric(c) && !IsBinary(c)
}

func decimalBytes(p int64) int {
	switch {
	case p <= 9:
		return 5
	case p <= 19:
		return 9
	case p <= 28:
		return 13
	}
	return 17
}

func datetime2Bytes(p int64) int {
	switch {
	case p <= 2:
		return 6
	case p <= 4:
		return 7
	}
	return 8
}

var enumRe = regexp.MustCompile(`'((?:[^']|'')*)'`)

// enumValues lists enum('a','b')'s values.
func enumValues(columnType string) ([]string, error) {
	open := strings.Index(columnType, "(")
	if open < 0 {
		return nil, fmt.Errorf("enum without values: %s", columnType)
	}
	var out []string
	for _, m := range enumRe.FindAllStringSubmatch(columnType[open:], -1) {
		out = append(out, strings.ReplaceAll(m[1], "''", "'"))
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("enum without values: %s", columnType)
	}
	return out, nil
}

// Quote brackets a SQL Server name.
func Quote(name string) string { return "[" + strings.ReplaceAll(name, "]", "]]") + "]" }

// DefaultSQL is a column's DEFAULT in T-SQL, or "" for none. eval answers an
// expression default's value (MySQL computes it); it may be nil when the
// schema has none.
func DefaultSQL(c Column, eval func(expr string) (string, error)) (string, error) {
	if !c.Default.Valid {
		return "", nil
	}
	d := c.Default.String
	up := strings.ToUpper(strings.TrimSpace(d))
	if strings.HasPrefix(up, "CURRENT_TIMESTAMP") || strings.HasPrefix(up, "NOW(") {
		return "SYSUTCDATETIME()", nil
	}
	if strings.Contains(strings.ToUpper(c.Extra), "DEFAULT_GENERATED") {
		if eval == nil {
			return "", fmt.Errorf("%s: expression default %s needs MySQL to evaluate it", c.Name, d)
		}
		v, err := eval(d)
		if err != nil {
			return "", err
		}
		d = v
	}
	switch {
	case IsNumeric(c):
		if _, err := strconv.ParseFloat(d, 64); err != nil {
			if c.DataType == "bit" && strings.HasPrefix(d, "b'") {
				return strings.Trim(d[1:], "'"), nil
			}
			return "", fmt.Errorf("%s: numeric default %q", c.Name, d)
		}
		return d, nil
	case IsBinary(c):
		return "0x" + fmt.Sprintf("%X", d), nil
	}
	return Literal(d), nil
}

// Literal is a T-SQL unicode string literal.
func Literal(s string) string { return "N'" + strings.ReplaceAll(s, "'", "''") + "'" }

// ConstraintName is a constraint's name, unique in the database and at most
// 128 characters: <prefix>_<table>[_<column>].
func ConstraintName(prefix, table, column string) string {
	n := prefix + "_" + table
	if column != "" {
		n += "_" + column
	}
	if len(n) > 128 {
		n = n[:128]
	}
	return n
}
