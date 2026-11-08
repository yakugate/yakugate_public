// Package stmt is the statement registry each SQL DAL keeps its queries in
// (plan of 29 Oct 2026, "every DAL query is efficient").
//
// A DAL names every statement once, at package level, and calls it by that
// name. The registry is what the efficiency checks read: the lint test
// (Lint) and the plan test (daltest, EXPLAIN on MySQL, the query plan on SQL
// Server) go over every registered statement, and each statement counts its
// calls, time and rows so the load test can list the slowest.
package stmt

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Stmt is one named statement of a DAL.
type Stmt struct {
	Name string
	SQL  string
	// Hot marks a statement on the message path: it must stay under 2 ms
	// p95 and may not sort or build a temp table (plan test).
	Hot bool

	calls atomic.Int64
	nanos atomic.Int64
	rows  atomic.Int64
}

// Observe counts one call of the statement.
func (s *Stmt) Observe(took time.Duration, rows int64) {
	s.calls.Add(1)
	s.nanos.Add(int64(took))
	s.rows.Add(rows)
}

// Stats is a statement's counts since start.
type Stats struct {
	Name  string        `json:"name"`
	Hot   bool          `json:"hot,omitempty"`
	Calls int64         `json:"calls"`
	Total time.Duration `json:"total_ns"`
	Rows  int64         `json:"rows"`
}

// Stats reads the statement's counts.
func (s *Stmt) Stats() Stats {
	return Stats{Name: s.Name, Hot: s.Hot, Calls: s.calls.Load(), Total: time.Duration(s.nanos.Load()), Rows: s.rows.Load()}
}

// Registry is one DAL's statements, by name.
type Registry struct {
	DB string // "mysql", "mssql"

	mu     sync.RWMutex
	byName map[string]*Stmt
}

// New makes an empty registry for one DAL.
func New(db string) *Registry {
	return &Registry{DB: db, byName: map[string]*Stmt{}}
}

// Add registers a statement and returns it. A name used twice is a
// programming error and panics at init.
func (r *Registry) Add(name, sql string, hot bool) *Stmt {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.byName[name]; dup {
		panic(fmt.Sprintf("stmt: %s statement %q registered twice", r.DB, name))
	}
	s := &Stmt{Name: name, SQL: strings.TrimSpace(sql), Hot: hot}
	r.byName[name] = s
	return s
}

// Get finds a statement by name.
func (r *Registry) Get(name string) (*Stmt, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.byName[name]
	return s, ok
}

// All returns the statements sorted by name.
func (r *Registry) All() []*Stmt {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Stmt, 0, len(r.byName))
	for _, s := range r.byName {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Slowest returns the statements with the most total time, at most n.
func (r *Registry) Slowest(n int) []Stats {
	all := r.All()
	out := make([]Stats, 0, len(all))
	for _, s := range all {
		if st := s.Stats(); st.Calls > 0 {
			out = append(out, st)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Total > out[j].Total })
	if n > 0 && len(out) > n {
		out = out[:n]
	}
	return out
}

var (
	selectStar = regexp.MustCompile(`(?is)\bselect\s+(distinct\s+)?(top\s*\(?\s*\S+\s*\)?\s+)?\*`)
	offset     = regexp.MustCompile(`(?i)\boffset\s+(\?|@\w+|\d+)`)
	isSelect   = regexp.MustCompile(`(?is)^\s*(with\b.*?\)\s*)?select\b`)
	hasLimit   = regexp.MustCompile(`(?i)\blimit\s+(\?|@\w+|\d+)|\btop\s*\(?\s*(\?|@\w+|\d+)|\bfetch\s+(first|next)\b`)
	hasWhere   = regexp.MustCompile(`(?i)\bwhere\b`)
	aggregate  = regexp.MustCompile(`(?is)^\s*select\s+(count|sum|min|max|avg|exists)\s*\(`)
)

// Lint checks one statement against the query rules a text can show: no
// SELECT *, no OFFSET paging, and no read that may return a whole table (a
// SELECT with neither a WHERE nor a LIMIT/TOP, unless it is an aggregate).
// What only the database can tell (an index used, a sort) is the plan test's.
func Lint(s *Stmt) []string {
	var out []string
	if selectStar.MatchString(s.SQL) {
		out = append(out, "SELECT * — name the columns")
	}
	if offset.MatchString(s.SQL) {
		out = append(out, "OFFSET paging — page by key (WHERE id > ? … LIMIT n)")
	}
	if isSelect.MatchString(s.SQL) && !hasLimit.MatchString(s.SQL) && !hasWhere.MatchString(s.SQL) && !aggregate.MatchString(s.SQL) {
		out = append(out, "a read with no WHERE and no LIMIT/TOP — bound it")
	}
	return out
}
