// Package scripts embeds per-store seed SQL so the engine can self-seed each
// store's own database on first boot.
package scripts

import _ "embed"

// SeedConfigSQL seeds the CONFIG store DB (accounts, vendors, routes, …).
//
//go:embed seed/config.sql
var SeedConfigSQL string

// SeedQueueSQL seeds the QUEUE store DB (api_tokens, queue table, hlr shard).
//
//go:embed seed/queue.sql
var SeedQueueSQL string

// SeedConfigMSSQL is SeedConfigSQL written for SQL Server (T-SQL, one batch:
// its variables live for the batch).
//
//go:embed mssql/seed_config.sql
var SeedConfigMSSQL string

// SeedQueueMSSQL is SeedQueueSQL written for SQL Server. Its
// "-- yaku:create-like" lines make a table from its template, which SQL
// Server has no statement for (the MSSQL DAL's Seed reads them).
//
//go:embed mssql/seed_queue.sql
var SeedQueueMSSQL string
