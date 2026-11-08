package httpvendor

import (
	"context"
	"time"
)

// One process polls a vendor's statuses at a time (go-live test of 18 Oct
// 2026, P4 D7, old bug P4-13).
//
// The messages waiting for a polled vendor's status are one shared list (the
// receipt facts in the main Redis), and every http-vendor the vendor is
// placed on walked it: a vendor on two deployments was asked about every
// message twice, two calls in the same second, and each answer for a message
// already final was a charged receipt row. Now a process polls a vendor only
// while it holds the vendor's turn (pollturn:<vendor id> in the main Redis,
// dal.PollTurns; pollTurnFor): it takes a free turn, keeps its own by polling, and a turn
// whose holder stopped polling is free again pollTurnFor later. Any process
// may apply what a poll answers, also for a message another deployment sent:
// the client's receipt is filed for the message's owner. Deployments that
// share a queue store file it there directly; when each has its own queue
// Redis, it is parked in the main Redis and the owner moves it into its own
// queue (internal/handoff, issue 5 of the design audit). Until 26 Oct 2026
// this comment said the receipt always reached the owner's queue: with a
// queue Redis per box it was written into the polling box's, where the owner
// never looked (issue 10).
//
// Without Redis the waiting list is each deployment's own MySQL reading of
// open_msgs and nothing is shared to take a turn through: both still poll.

// pollTurnFor is how long a turn is held after the holder last polled.
const pollTurnFor = 15 * time.Second

// myPollTurn reports whether this process is to poll the vendor now. With no
// Redis it always is. A Redis that does not answer leaves the poll to go on
// (two polls are better than none).
func (p *Process) myPollTurn(ctx context.Context, vendorID int64) bool {
	if p.turns == nil {
		return true
	}
	mine, err := p.turns.TakePollTurn(ctx, vendorID, p.instance, pollTurnFor)
	if err != nil {
		return true
	}
	return mine
}
