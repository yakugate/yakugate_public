package smppserver

import "github.com/yakugate/yakugate/internal/pipeline"

// queueHealthOf watches the queue store a submit is written to, for
// ReadyForTraffic (retest of 19 Oct 2026, hand-off C-H2): the smpp-server
// answered ready while its queue store was down and every submit was refused
// ESME_RMSGQFUL, as the http-server's /readyz did before 18 Oct. The same
// watcher as the http-server's (pipeline.QueueHealth). A role that could
// build no queue store at all (unavailableQueue) is never ready.
func queueHealthOf(q any) *pipeline.QueueHealth {
	if _, ok := q.(unavailableQueue); ok {
		return pipeline.NoQueueStore()
	}
	return pipeline.NewQueueHealth(q)
}
