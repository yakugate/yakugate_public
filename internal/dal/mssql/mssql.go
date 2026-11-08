// Package mssql is the SQL Server 2022 DAL (plan of 29 Oct 2026, step 4).
//
// It is made like internal/dal/mysql: the same areas, each written for SQL
// Server (READ_COMMITTED_SNAPSHOT, UPDLOCK upserts, table-valued parameters,
// sp_getapplock, memory-optimized queue tables when the install has no
// Redis). It gets its areas one by one; until every area MySQL serves is
// served here, a setting that asks for SQL Server is refused at start with
// dal.ErrNotServed (dal.CheckServed), not half run.
package mssql

import (
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// areas holds each area file's openers. An area file sets its own fields
// with a package-level `var _ = serves(func(d *dal.Driver) { d.X = openX })`,
// so two areas never edit the same lines. Package-level variables are set
// before any init runs, so init below sees every area.
var areas []func(*dal.Driver)

func serves(set func(*dal.Driver)) bool {
	areas = append(areas, set)
	return true
}

func init() {
	d := dal.Driver{Name: config.BackendMSSQL, HLRShards: openHLRShards,
		// The groundwork of step 4: the named leases and the register
		// beside them (locks.go), the console's setup screen (console.go).
		Leases:      openLeases,
		ServerSetup: func() dal.ServerSetup { return serverSetup{} },
		// The schema runner (schema*.go): starting schemas, migrations,
		// clones, seeds and the HLR copy.
		Schema: openSchema,
	}
	for _, set := range areas {
		set(&d)
	}
	dal.Register(d)
}
