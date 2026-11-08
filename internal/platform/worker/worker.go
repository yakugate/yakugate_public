// Package worker provides the scaffolding processes run on. Poll-style processes
// implement Worker (Step loop); server-style processes (httpserver, adminapi)
// implement Runnable (Run until ctx is cancelled). Run dispatches on which it is.
package worker

import (
	"context"
	"errors"
	"time"

	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/platform/version"
)

// Worker is a poll-style process: one input->logic->output cycle per Step.
type Worker interface {
	Name() string
	Step(ctx context.Context) error
}

// Stopper is an optional part of a process: Stop is called once, after the
// context is cancelled and before the process ends, and returns when the work
// that was in flight is finished (within the process's own time limit — the
// supervisor kills a role 20 s after asking it to stop). A poll-style process
// gets it when its loop ends; a server-style one when Run has returned
// because the context ended (since 2026-10-14: the dlr and mo roles' Run
// returned the moment their listener closed, and the process went away with
// customer pushes still out).
type Stopper interface {
	Stop()
}

// Paced is an optional part of a poll-style process: StepEvery is how often
// its Step runs, in place of the interval Run is given (500 ms for every
// role). The roles whose Step checks a queue take it from that queue's
// setting (internal/queuecheck; issue 7, 26 Oct 2026). 0 = the given one.
type Paced interface {
	StepEvery() time.Duration
}

// Runnable is a long-running server-style process.
type Runnable interface {
	Name() string
	Run(ctx context.Context) error
}

// Run drives a process until the context is cancelled. If the process implements
// Runnable it is run directly; otherwise its Step is looped on an interval.
func Run(ctx context.Context, w Worker, interval time.Duration) error {
	log := logging.New(w.Name())
	// An interactive process announces nothing: its output is the thing the
	// operator came to read, and three status lines above a menu are noise
	// (2026-09-18). A background role still says what it is and when it
	// started, which is how anyone reading a log file knows.
	quiet := isInteractive(w)
	if !quiet {
		log.Printf("yakugate %s", version.String())
	}
	ready := &processReady // one role per process: the admin listener reads it too
	ready.Store(false)
	serveHealth(ctx, w, ready, log)
	if r, ok := any(w).(Runnable); ok {
		if !quiet {
			log.Printf("starting (server)")
		}
		ready.Store(true)
		err := r.Run(ctx)
		// Only for a stop: a Run that failed by itself (its port taken) has
		// nothing in flight to wait for.
		if ctx.Err() != nil {
			if st, ok := any(w).(Stopper); ok {
				st.Stop()
			}
		}
		return err
	}
	if pw, ok := any(w).(Paced); ok {
		if d := pw.StepEvery(); d > 0 {
			interval = d
		}
	}
	log.Printf("starting (worker, every %s)", interval)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Printf("shutting down")
			// A role with work in flight finishes it before the process ends
			// (Stopper): calls a vendor has not answered yet, and what they
			// answer still to be recorded.
			if st, ok := any(w).(Stopper); ok {
				st.Stop()
			}
			return ctx.Err()
		case <-t.C:
			if err := w.Step(ctx); err != nil {
				var ex Exit
				if errors.As(err, &ex) {
					log.Printf("%v", err)
					return err
				}
				log.Printf("step error: %v", err)
			} else {
				ready.Store(true) // one step done without error: this role can do its job
			}
		}
	}
}

// Exit is a Step error that ends the process on purpose, with its own exit
// status, rather than one logged before the next Step. The one today is
// singleton.Elsewhere: a gateway-wide role the supervisor started found
// another deployment running it (2026-10-01). Run returns it; main exits
// with ExitCode.
type Exit interface {
	error
	ExitCode() int
}

// Interactive marks a process that talks to a person at a terminal rather than
// running unattended. The framework keeps quiet for one: no start-up banner,
// and no health listener, because there is nothing to watch and the process
// ends when the person does.
type Interactive interface{ Interactive() }

func isInteractive(w Worker) bool {
	_, ok := any(w).(Interactive)
	return ok
}
