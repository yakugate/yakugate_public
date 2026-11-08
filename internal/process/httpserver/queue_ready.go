package httpserver

import "github.com/yakugate/yakugate/internal/pipeline"

// newQueueHealth is the readiness watcher of the queue store (go-live test of
// 18 Oct 2026, P1 E3, bug P1-8): pipeline.QueueHealth, shared with the
// smpp-server since 19 Oct. A role that could build no queue store at all
// (unavailableQueue) is never ready.
func newQueueHealth(q any) *pipeline.QueueHealth {
	if _, ok := q.(unavailableQueue); ok {
		return pipeline.NoQueueStore()
	}
	return pipeline.NewQueueHealth(q)
}
