// The openers of the Store areas (mysql.go, records*.go), registered here.
package mysql

import (
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
)

var _ = serves(func(d *dal.Driver) {
	// The queue family, from the queue database (YAKU_QUEUE_DSN).
	d.Queues = func(cfg config.Config) (dal.Queues, error) {
		return New(cfg.QueueDSN)
	}
	// The record store (the hot store, YAKU_HOT_DSN) as the roles have
	// always opened it — New on the process's one pool — and what its
	// errors say; the config store's data keys (YAKU_CONFIG_DSN) and the
	// HLR store's vendor sessions the same way (records*.go, mysql.go).
	d.RecordStore = func(cfg config.Config) (dal.RecordStore, error) {
		return New(cfg.HotDSN)
	}
	d.RecordFault = recordFault
	d.DataKeys = func(cfg config.Config) (dal.DataKeys, error) {
		return New(cfg.ConfigDSN)
	}
	d.VendorSessions = func(dsn string) (dal.VendorSessions, error) {
		return New(dsn)
	}
})
