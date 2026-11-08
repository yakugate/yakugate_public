package refdata

import (
	"os"
	"strconv"
	"time"
)

// DefaultRefreshEvery is how often a role that reads the config store
// directly (no Redis snapshot) looks for reference-data changes.
//
// It was five minutes (QA SMPP-004, 2026-09-29): a number a vendor's status
// code put on the blocklist kept being accepted and sent for up to five
// minutes. Since 2026-09-17 a look that finds nothing changed costs one
// COUNT/MAX query per config table (ChangeSignature) and reloads nothing, so
// the cache can look as often as the Redis snapshot is rebuilt: every 30 s,
// the refresh the docs promise.
const DefaultRefreshEvery = 30 * time.Second

// RefreshEvery is YAKU_CACHE_REFRESH_SEC when set to a positive number of
// seconds, else DefaultRefreshEvery.
func RefreshEvery() time.Duration {
	if n, err := strconv.Atoi(os.Getenv("YAKU_CACHE_REFRESH_SEC")); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	return DefaultRefreshEvery
}
