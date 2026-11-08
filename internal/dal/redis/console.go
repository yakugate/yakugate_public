package redis

import (
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// openLiveParts opens the console's live balances and live view on ONE
// client of the main Redis, as the console made it (30 Oct 2026, from
// internal/process/console definition.go): a top-up reaches the live
// counter, and Live on the monitoring screens asks the roles there.
func openLiveParts(cfg config.Config) (*dal.LiveParts, error) {
	c := FromConfig(cfg).Client()
	return &dal.LiveParts{Balances: NewLiveBalances(c), Status: NewLiveStatus(c), Release: c.Close}, nil
}
