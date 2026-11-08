package concat

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// Where a long message's parts wait until they are all in (Saleem, 28 Oct
// 2026: "a configuration whether to store concat parts in mysql, redis or
// each gateway's own heap memory … for both mt and mo"). One setting each:
// YAKU_CONCAT_MT_STORE (a client's long message, smpp-server) and
// YAKU_CONCAT_MO_STORE (a vendor's long incoming message, smpp-vendor).
// Every part is written as it comes, never through the record queue.
const (
	StoreMySQL  = "mysql"  // concat_assembly in the queue database
	StoreRedis  = "redis"  // the MAIN Redis, shared by every gateway
	StoreMemory = "memory" // the process's own memory, shared by its sessions
)

// StoreSetting reads the setting name: mysql, redis or memory (any case).
// Unset is redis when a Redis is configured, else mysql. A word it does not
// know is an error: the role says so and does not start.
func StoreSetting(name string, redisSet bool) (string, error) {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	switch v {
	case "":
		if redisSet {
			return StoreRedis, nil
		}
		return StoreMySQL, nil
	case StoreMySQL, StoreMemory:
		return v, nil
	case StoreRedis:
		if !redisSet {
			return "", fmt.Errorf("%s=redis but no Redis is configured (YAKU_REDIS_ADDR)", name)
		}
		return v, nil
	}
	return "", fmt.Errorf("%s=%q: it is mysql, redis or memory", name, v)
}

// Open builds the buffer the store names: redis = the gateway's MAIN Redis
// (not its queue Redis), mysql = the queue database of the install's SQL
// database (MySQL, or SQL Server on an install on SQL Server: the setting's
// word is kept), each opened by the DAL (dal.OpenConcatBuffer). mo picks the
// incoming side (its own keys and group identities).
func Open(cfg config.Config, store string, mo bool, timeout time.Duration, logf func(string, ...any)) (Buffer, error) {
	switch store {
	case StoreRedis, StoreMySQL:
		on := dal.SQL(cfg)
		if store == StoreRedis {
			on = config.BackendRedis
		}
		buf, err := dal.OpenConcatBuffer(cfg, on, mo)
		if err != nil {
			return nil, fmt.Errorf("concat store %s: %w", store, err)
		}
		if mo {
			return NewMO(buf, timeout, logf), nil
		}
		return New(buf, timeout, logf), nil
	case StoreMemory:
		if mo {
			return NewMemoryMO(timeout, logf), nil
		}
		return NewMemory(timeout, logf), nil
	}
	return nil, fmt.Errorf("concat store %q: it is mysql, redis or memory", store)
}

// SetScope makes an MO buffer group parts under scope(vendor id) — the
// vendor's company (vendorgroup.Groups.Scope) — so a carrier's parts that come
// in on its TX row and on its RX row meet. An MT buffer is not changed.
func SetScope(b Buffer, scope func(int64) int64) {
	switch x := b.(type) {
	case *Assembler:
		x.scope = scope
	case *CountedAssembler:
		x.scope = scope
	case *Memory:
		x.scope = scope
	}
}

var _ Buffer = (*Memory)(nil)

// OpenFor is what a role calls: the setting read (YAKU_CONCAT_MO_STORE when mo,
// else YAKU_CONCAT_MT_STORE), the buffer opened, and one line in the log
// saying where the parts wait. For mysql the DAL opens the queue DSN when it
// is a MySQL one. A setting it cannot use is said loudly and the default is
// taken; a buffer that cannot be opened is nil (the role then takes each part
// alone, as before an assembly buffer existed) — said loudly too.
func OpenFor(cfg config.Config, mo bool, timeout time.Duration, logf func(string, ...any)) Buffer {
	name := "YAKU_CONCAT_MT_STORE"
	if mo {
		name = "YAKU_CONCAT_MO_STORE"
	}
	store, err := StoreSetting(name, cfg.RedisEnabled())
	if err != nil {
		store = StoreMySQL
		if cfg.RedisEnabled() {
			store = StoreRedis
		}
		logf("ERROR %v — using %s", err, store)
	}
	b, err := Open(cfg, store, mo, timeout, logf)
	if err != nil {
		logf("ERROR long-message parts cannot wait anywhere: %v — each part is taken alone", err)
		return nil
	}
	switch store {
	case StoreMemory:
		logf("%s=memory: long-message parts wait in this process's memory — lost if it restarts, not seen by another gateway", name)
	default:
		logf("%s=%s: long-message parts wait there", name, store)
	}
	return b
}
