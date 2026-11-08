package redis

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// The HTTP vendors' poll turns (dal.PollTurns), moved here unchanged on 30
// Oct 2026 from internal/process/httpvendor/pollturn.go, which keeps why one
// process polls a vendor at a time.

// PollTurnKey is the vendor's turn.
func PollTurnKey(vendorID int64) string { return "pollturn:" + strconv.FormatInt(vendorID, 10) }

// takeTurn takes the turn when it is free, or keeps it when it is ours.
var takeTurn = redis.NewScript(`
local held = redis.call('GET', KEYS[1])
if held == false or held == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
  return 1
end
return 0
`)

// PollTurns is the turns on one Redis client.
type PollTurns struct{ c *redis.Client }

var _ dal.PollTurns = (*PollTurns)(nil)

// NewPollTurns keeps the turns on a client.
func NewPollTurns(c *redis.Client) *PollTurns { return &PollTurns{c: c} }

// openPollTurns is the turns' opener: the main Redis, one client of their
// own, as the http-vendor role made it.
func openPollTurns(cfg config.Config) (dal.PollTurns, error) {
	return NewPollTurns(FromConfig(cfg).Client()), nil
}

// TakePollTurn runs the script: one round trip.
func (t *PollTurns) TakePollTurn(ctx context.Context, vendorID int64, me string, hold time.Duration) (bool, error) {
	n, err := takeTurn.Run(ctx, t.c, []string{PollTurnKey(vendorID)}, me, hold.Milliseconds()).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}
