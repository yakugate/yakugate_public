// Package migrations embeds the per-store SQL migrations so the engine can
// apply its own schema at startup without any external files. Stores:
// config, queue, hot, hlr; one folder per database the engine builds itself:
// mysql, and mssql (SQL Server 2022, since step 4 of the DAL plan: each
// starts with 0001_base.sql, the MySQL schema at a recorded migration, and
// every later migration is written for both). mongo and redis are applied
// manually.
package migrations

import "embed"

// FS holds migrations/<store>/<database>/*.sql for the stores the bootstrap
// builds.
//
//go:embed config/mysql/*.sql queue/mysql/*.sql hot/mysql/*.sql hlr/mysql/*.sql
//go:embed config/mssql/*.sql queue/mssql/*.sql hot/mssql/*.sql hlr/mssql/*.sql
var FS embed.FS
