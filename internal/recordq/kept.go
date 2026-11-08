package recordq

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/model"
)

// Tasks the queue did not take are held, not written elsewhere (Saleem, 28 Oct
// 2026: with the DB worker every hot-store log and summary goes through the
// queue, and when the queue Redis is away a role "holds it in memory, queues
// it later"). A role never writes such a row to MySQL itself.
//
// keep holds the tasks in the order they came and puts them again, marked
// Once, until Redis takes them. Once is how the worker knows a task may be on
// a list already (the answer that sent it here may have come after Redis took
// it): a kind that adds a row is then written the careful way, nothing that
// is there already. While anything is held, new tasks go behind it
// (putOrKeep), so one message's tasks keep their order.
//
// Memory only: a role stopped while it holds tasks loses them, and says so.

type keptTask struct {
	t     Task
	since time.Time
}

// keepRetryEvery is how often held tasks are put again (an atomic: a test
// shortens it while an earlier test's putter may still be reading it).
var keepRetryEvery atomic.Int64

func init() { keepRetryEvery.Store(int64(500 * time.Millisecond)) }

var (
	// keepSayEvery is how often a role that still holds tasks says so.
	keepSayEvery = 30 * time.Second
)

// keepMost bounds the tasks held in memory.
const keepMost = 200000

// putOrKeep puts tasks on the queue, or holds them when it cannot (any error:
// Redis away, the queue full). nil = queued or held; an error only when too
// many are held already. Behind anything held already, so order is kept.
func (c *Client) putOrKeep(ctx context.Context, tasks ...Task) error {
	if len(tasks) == 0 {
		return nil
	}
	if c.Kept() == 0 {
		err := c.putNow(ctx, tasks)
		if err == nil {
			return nil
		}
		return c.keep(tasks, err)
	}
	return c.keep(tasks, fmt.Errorf("tasks are held already"))
}

// keep holds tasks to be put again, marked Once.
func (c *Client) keep(tasks []Task, cause error) error {
	now := time.Now()
	c.keepMu.Lock()
	if len(c.kept)+len(tasks) > keepMost {
		c.keepMu.Unlock()
		return fmt.Errorf("%w; and %d record tasks are held already", cause, keepMost)
	}
	first := len(c.kept) == 0
	for _, t := range tasks {
		t.Once = true
		c.kept = append(c.kept, keptTask{t: t, since: now})
	}
	start := !c.keepRunning
	c.keepRunning = true
	c.keepMu.Unlock()
	if first {
		c.logf("record task(s) not taken by the record queue (%v): held in memory and put again until Redis takes them", cause)
	}
	if start {
		go c.putKept()
	}
	return nil
}

// Kept is how many record tasks this role holds to be put again.
func (c *Client) Kept() int {
	if c == nil {
		return 0
	}
	c.keepMu.Lock()
	defer c.keepMu.Unlock()
	return len(c.kept)
}

// KeepRejected holds a refusal's row whose put failed or did not say (it is a
// RejectKeeper): put again, written once.
func (c *Client) KeepRejected(m model.Message, cause error) error {
	t, err := task(KindReject, m.ID, m)
	if err != nil {
		return err
	}
	return c.keep([]Task{t}, cause)
}

// KeepRejectedBatch is KeepRejected for a batch: all held, or none when the
// role holds too many already.
func (c *Client) KeepRejectedBatch(ms []model.Message, cause error) error {
	tasks, err := rejectTasks(ms)
	if err != nil {
		return err
	}
	return c.keep(tasks, cause)
}

// KeptRejects is how many tasks wait to be put again (refusals among them).
func (c *Client) KeptRejects() int { return c.Kept() }

// putKept puts the held tasks again, in order, until none is left or the
// client's life ends.
func (c *Client) putKept() {
	life := c.ctx
	if life == nil {
		life = context.Background()
	}
	t := time.NewTicker(time.Duration(keepRetryEvery.Load()))
	defer t.Stop()
	said := time.Now()
	for {
		select {
		case <-life.Done():
			c.keepMu.Lock()
			n := len(c.kept)
			c.keepRunning = false
			c.keepMu.Unlock()
			if n > 0 {
				c.logf("ERROR stopping with %d record task(s) held in memory and not queued: their rows are lost unless an earlier put reached Redis", n)
			}
			return
		case <-t.C:
		}
		c.keepMu.Lock()
		held := append([]keptTask(nil), c.kept...)
		c.keepMu.Unlock()
		put := 0
		var lastErr error
		for put < len(held) {
			end := put + Batch
			if end > len(held) {
				end = len(held)
			}
			ts := make([]Task, 0, end-put)
			for _, k := range held[put:end] {
				ts = append(ts, k.t)
			}
			ctx, cancel := context.WithTimeout(life, 3*time.Second)
			// Not held back by the queue's limit: these are rows of what has
			// happened already; the limit is for what the vendor roles send.
			lastErr = c.putNow(ctx, ts)
			cancel()
			if lastErr != nil {
				break
			}
			put = end
		}
		c.keepMu.Lock()
		c.kept = c.kept[put:]
		left := len(c.kept)
		if left == 0 {
			c.keepRunning = false
		}
		c.keepMu.Unlock()
		if left == 0 {
			c.logf("%d held record task(s) put on the record queue", put)
			return
		}
		if time.Since(said) >= keepSayEvery {
			said = time.Now()
			c.logf("still holding %d record task(s) in memory, the oldest since %s: %v", left, held[put].since.UTC().Format(time.RFC3339), lastErr)
		}
	}
}
