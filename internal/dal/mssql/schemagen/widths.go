package schemagen

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Text widths (2 Nov 2026, Saleem: "fix these"). MySQL's VARCHAR(n) holds n
// characters; SQL Server's NVARCHAR(n) holds n UTF-16 units, so a character
// outside the BMP (an emoji) takes two and 32 of them did not fit a
// 32-character spam list name. On SQL Server every NVARCHAR(n) with n up to
// 2000 is NVARCHAR(2n): n characters of any kind. The app still checks
// lengths in characters, as before.
//
// Kept narrow: the text columns of an index whose widened key would pass SQL
// Server's key size (900 bytes clustered, 1700 otherwise) — they hold ASCII
// (key ids, tokens, migration names) — and, so a foreign key still joins
// columns of one length, the other side of a foreign key on such a column.
//
// The same rule is migrations/<store>/mssql/<n>_text_widths.sql, which widens
// an install made from a base file of before this day; MapSchema is what a
// fresh base is made with (Render) and what the parity test expects.

// WideTextMax is the widest NVARCHAR(n) that is doubled: NVARCHAR(4000) is
// SQL Server's widest before MAX.
const WideTextMax = 2000

var nvarcharRe = regexp.MustCompile(`^NVARCHAR\((\d+)\)$`)

// narrowChars is n of a mapped NVARCHAR(n) that the rule doubles, else 0.
func narrowChars(m Mapped) int {
	g := nvarcharRe.FindStringSubmatch(m.Type)
	if g == nil {
		return 0
	}
	n, _ := strconv.Atoi(g[1])
	if n < 1 || n > WideTextMax {
		return 0
	}
	return n
}

func widen(m Mapped) Mapped {
	n := narrowChars(m)
	if n == 0 {
		return m
	}
	m.Type = fmt.Sprintf("NVARCHAR(%d)", 2*n)
	m.KeyBytes = 4 * n
	return m
}

// MapSchema maps every column of every table (Map), then applies the text
// widths rule. The answer is by table name, then lower-cased column name.
func MapSchema(s *Schema) (map[string]map[string]Mapped, error) {
	narrow := map[string]map[string]Mapped{}
	out := map[string]map[string]Mapped{}
	for _, t := range s.Tables {
		narrow[t.Name] = map[string]Mapped{}
		out[t.Name] = map[string]Mapped{}
		for _, c := range t.Columns {
			m, err := Map(c)
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", t.Name, c.Name, err)
			}
			key := strings.ToLower(c.Name)
			narrow[t.Name][key] = m
			out[t.Name][key] = widen(m)
		}
	}
	keep := func(table, col string) bool {
		key := strings.ToLower(col)
		m, ok := narrow[table][key]
		if !ok || out[table][key].Type == m.Type {
			return false
		}
		out[table][key] = m
		return true
	}
	// Indexes whose widened key passes the limit keep their text narrow.
	for _, t := range s.Tables {
		for _, ix := range t.Indexes {
			limit := NonClusteredKeyMax
			if ix.Name == "PRIMARY" {
				if b, err := keyBytes(ix, narrow[t.Name]); err == nil && b <= ClusteredKeyMax {
					limit = ClusteredKeyMax // the primary key is clustered (Render)
				}
			}
			b, err := keyBytes(ix, out[t.Name])
			if err != nil || b <= limit {
				continue
			}
			for _, c := range ix.Columns {
				keep(t.Name, c.Name)
			}
		}
	}
	// Both sides of a foreign key alike, until nothing changes.
	for changed := true; changed; {
		changed = false
		for _, t := range s.Tables {
			for _, fk := range t.ForeignKeys {
				for i, col := range fk.Columns {
					if i >= len(fk.RefColumns) {
						break
					}
					ref := fk.RefColumns[i]
					child, okC := out[t.Name][strings.ToLower(col)]
					parent, okP := out[fk.RefTable][strings.ToLower(ref)]
					if !okC || !okP {
						continue
					}
					childNarrow := child.Type == narrow[t.Name][strings.ToLower(col)].Type && narrowChars(child) > 0
					parentNarrow := parent.Type == narrow[fk.RefTable][strings.ToLower(ref)].Type && narrowChars(parent) > 0
					if childNarrow && !parentNarrow && keep(fk.RefTable, ref) {
						changed = true
					}
					if parentNarrow && !childNarrow && keep(t.Name, col) {
						changed = true
					}
				}
			}
		}
	}
	return out, nil
}
