package mo

import (
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// Delivered remembers the MO this process handed to its customer and could
// not yet record (go-live test of 18 Oct 2026: P6 E11, P14 A4).
//
// A hand-over the customer confirmed — its webhook answered, or its bind
// answered the deliver_sm — is finished by writing the record and taking the
// MO off mo_queue (Finalize). When the record could not be written (MySQL
// away, or the DB worker's record queue full) the MO used to be either handed
// over AGAIN when its lease ended (the mo role's webhook: 60 MO twice in a
// 12 s MySQL cut, every MO four times at the record queue limit) or acked
// with no record at all (smpp-server's bind: 59 MO on no log, not charged).
// Now it stays on the queue, noted here, and its next claim writes the record
// and acks it without handing it over again.
//
// In memory, per process: a process restarted in between hands it over once
// more, and another process's leg (the webhook after the bind's budget) does
// not know of it.
type Delivered struct {
	mu    sync.Mutex
	at    map[string]time.Time
	tries map[string]dal.ClientTry // the try the client took (D-140): written as it was, later
}

// DeliveredKeep is how long a handed-over MO is remembered: far longer than
// any record store is expected to be away.
const DeliveredKeep = 6 * time.Hour

// deliveredMax bounds the set; past it the oldest half is forgotten.
const deliveredMax = 200_000

// Note remembers id as handed over and not yet recorded.
func (d *Delivered) Note(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.at == nil {
		d.at = map[string]time.Time{}
	}
	now := time.Now()
	if len(d.at) >= deliveredMax {
		for k, t := range d.at {
			if now.Sub(t) > DeliveredKeep/2 || len(d.at) >= deliveredMax {
				delete(d.at, k)
				delete(d.tries, k)
			}
			if len(d.at) < deliveredMax/2 {
				break
			}
		}
	}
	d.at[id] = now
}

// Has reports whether id was handed over and is still to be recorded.
func (d *Delivered) Has(id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	t, ok := d.at[id]
	if ok && time.Since(t) > DeliveredKeep {
		delete(d.at, id)
		return false
	}
	return ok
}

// Forget drops id once it is recorded.
func (d *Delivered) Forget(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.at, id)
	delete(d.tries, id)
}

// NoteTry remembers id as handed over by the try t, still to be recorded:
// its next claim writes that same try (one accepted row, not two).
func (d *Delivered) NoteTry(id string, t dal.ClientTry) {
	d.Note(id)
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.tries == nil {
		d.tries = map[string]dal.ClientTry{}
	}
	d.tries[id] = t
}

// Try is the try id was handed over by (NoteTry); ok false when none is
// remembered.
func (d *Delivered) Try(id string) (dal.ClientTry, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	t, ok := d.tries[id]
	return t, ok
}
