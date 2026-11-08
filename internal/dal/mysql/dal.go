package mysql

import (
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// The MySQL DAL (internal/dal, plan of 29 Oct 2026). It serves the queue
// family from the queue database (YAKU_QUEUE_DSN) when the queues are not in
// Redis. More areas join as they move in.
//
// areas holds each area file's openers. An area file sets its own fields
// with a package-level `var _ = serves(func(d *dal.Driver) { d.X = openX })`,
// so two areas never edit the same lines (30 Oct 2026, as on SQL Server; it
// was one Driver literal here). Package-level variables are set before any
// init runs, so init below sees every area.
var areas []func(*dal.Driver)

func serves(set func(*dal.Driver)) bool {
	areas = append(areas, set)
	return true
}

func init() {
	d := dal.Driver{Name: config.BackendMySQL}
	for _, set := range areas {
		set(&d)
	}
	dal.Register(d)
}

var (
	_ dal.Queues         = (*Store)(nil)
	_ dal.RecordStore    = (*Store)(nil)
	_ dal.DataKeys       = (*Store)(nil)
	_ dal.VendorSessions = (*Store)(nil)
	_ dal.HLRLookupOf    = (*ConfigStore)(nil)
)
