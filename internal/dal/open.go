package dal

import (
	"errors"
	"fmt"
	"strings"

	"github.com/yakugate/yakugate/internal/platform/config"
)

// SQL names the install's one SQL database: the config store's backend,
// mysql when it is not set. Every SQL store of an install is on it
// (CheckOneSQL).
func SQL(cfg config.Config) config.Backend {
	if b := norm(cfg.ConfigBackend); b != "" {
		return b
	}
	return config.BackendMySQL
}

// ConfigOnSQL reports whether the config store is named and is a SQL
// database a DAL serves (MySQL or SQL Server): what a role reads from the
// config store (reference data, audit trail, log levels, HLR settings) is
// there.
func ConfigOnSQL(cfg config.Config) bool {
	b := SQL(cfg)
	return cfg.ConfigDSN != "" && (b == config.BackendMySQL || b == config.BackendMSSQL)
}

// CheckOneSQL refuses settings that put the SQL stores on two kinds of SQL
// database (config on MySQL and records on SQL Server, say). The four
// supported setups never mix; one SQL engine per install keeps every DAL
// tested as it runs. Redis is not a SQL store and is not checked.
func CheckOneSQL(cfg config.Config) error {
	want := SQL(cfg)
	stores := []struct {
		name string
		b    config.Backend
	}{
		{"YAKU_HOT_BACKEND", cfg.HotBackend},
		{"YAKU_QUEUE_BACKEND", cfg.QueueBackend},
		{"YAKU_HLR_BACKEND", cfg.HLRBackend},
	}
	var bad []string
	for _, s := range stores {
		b := norm(s.b)
		if b == "" || b == config.BackendRedis || b == want {
			continue
		}
		bad = append(bad, fmt.Sprintf("%s=%s", s.name, b))
	}
	if len(bad) > 0 {
		return fmt.Errorf("one SQL database per install: the config store is %s, but %s", want, strings.Join(bad, ", "))
	}
	return nil
}

// QueuesOn says which DAL serves the queue family: Redis when the settings
// run the queues there (config.QueuesInRedis), else the database
// YAKU_QUEUE_BACKEND names — never another one in its place — and the
// install's SQL database when it names none.
func QueuesOn(cfg config.Config) config.Backend {
	if cfg.QueuesInRedis() {
		return config.BackendRedis
	}
	if b := norm(cfg.QueueBackend); b != "" && b != config.BackendRedis {
		return b
	}
	return SQL(cfg)
}

// OpenQueues opens the queue family on the DAL QueuesOn picks.
func OpenQueues(cfg config.Config) (Queues, config.Backend, error) {
	on := QueuesOn(cfg)
	d, ok := Lookup(on)
	if !ok {
		return nil, on, fmt.Errorf("dal: no %s DAL in this binary (have %v)", on, Registered())
	}
	if d.Queues == nil {
		return nil, on, fmt.Errorf("dal: queues on %s: %w", on, ErrNotServed)
	}
	q, err := d.Queues(cfg)
	if err != nil {
		return nil, on, fmt.Errorf("dal: queues on %s: %w", on, err)
	}
	return q, on, nil
}

// OpenSQLQueues opens the queue family on the install's SQL database even
// when the queues run in Redis: the queue database's own handle, which the
// vendor roles have always opened beside their queues (its pool, and its
// read instance's when one is set).
func OpenSQLQueues(cfg config.Config) (Queues, error) {
	on := SQL(cfg)
	d, ok := Lookup(on)
	if !ok {
		return nil, fmt.Errorf("dal: no %s DAL in this binary (have %v)", on, Registered())
	}
	if d.Queues == nil {
		return nil, fmt.Errorf("dal: queues on %s: %w", on, ErrNotServed)
	}
	q, err := d.Queues(cfg)
	if err != nil {
		return nil, fmt.Errorf("dal: queues on %s: %w", on, err)
	}
	return q, nil
}

// Set is what Open built for a role: one handle per area, each on the DAL
// picked for it. The areas join it as they move into the DAL.
type Set struct {
	Queues   Queues
	QueuesOn config.Backend
}

// Open builds the Set once at start.
func Open(cfg config.Config) (*Set, error) {
	if err := CheckOneSQL(cfg); err != nil {
		return nil, err
	}
	s := &Set{}
	var err error
	if s.Queues, s.QueuesOn, err = OpenQueues(cfg); err != nil {
		return nil, err
	}
	return s, nil
}

// Close closes every handle of the Set.
func (s *Set) Close() error {
	if s == nil {
		return nil
	}
	var errs []error
	if s.Queues != nil {
		errs = append(errs, s.Queues.Close())
	}
	return errors.Join(errs...)
}

func norm(b config.Backend) config.Backend {
	return config.Backend(strings.ToLower(strings.TrimSpace(string(b))))
}
