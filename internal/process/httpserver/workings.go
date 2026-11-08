package httpserver

import "context"

// Step runs one input -> logic -> output cycle for the "http-server" process.
//
//	INPUT:  client HTTP request (payload + credentials/token)
//	LOGIC:  authenticate, validate, template + anti-abuse checks, resolve route + HLR (in-memory)
//	OUTPUT: enqueue to the selected vendor's queue; synchronous ack [Phase 1]
func (p *Process) Step(ctx context.Context) error {
	// Nothing to do here: http-server is a Runnable (server.go), so the
	// framework never polls Step. The request work lives in handler.go and
	// the pipeline. Kept so the process still satisfies worker.Worker.
	return nil
}
