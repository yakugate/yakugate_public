package mssql

import "github.com/yakugate/yakugate/internal/dal/stmt"

// The SQL Server DAL's statement registries (plan of 29 Oct 2026, section 9:
// "each DAL keeps its statements as named constants"). Every statement is
// named once, at package level, in the registry of its area, so the lint
// test, the plan test and the timings can read them all. The shapes every
// area builds its statements from (Upsert, UpsertBatch, InsertBatch, Take,
// InsertID: helpers*.go) register what they build in the registry they are
// given.
//
// More registries join as the areas move in (one per area, as on MySQL:
// ConfigStmts, QueueStmts, …). An area makes its own with newRegistry in its
// own file, which adds it to the list the lint test reads.
var (
	// EngineStmts: the named leases and the register beside them
	// (locks.go), and the console's setup screen (console.go).
	EngineStmts = newRegistry()
)

var registries []*stmt.Registry

// newRegistry is an area's statement registry, listed for the lint test.
func newRegistry() *stmt.Registry {
	r := stmt.New("mssql")
	registries = append(registries, r)
	return r
}

// Registries lists the registries the lint test reads.
func Registries() []*stmt.Registry {
	return registries
}
