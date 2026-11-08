package smppserver

// The sequence numbers this server sends with: one band for each stream.

// Outbound sequence-number bands [SCOPE P2-SEQ].
//
// Every stream this server ORIGINATES owns its own band of 2^28 sequence
// numbers. Client-initiated exchanges are not in here at all: the server
// echoes the client's own sequence_number, as SMPP requires, and allocates
// nothing.
//
//	0x40000000  unbind broadcast at shutdown  (one per connection, one-shot)
//	0x50000000  enquire_link keepalive        (per session)
//	0x60000000  MO deliver_sm pushes          (process-wide)
//	0x70000000  DLR deliver_sm pushes         (process-wide)
//
// The bands exist because the MO and DLR pushes register waiters in the SAME
// per-session map, keyed by sequence number, and session.expect OVERWRITES a
// duplicate key without a word. Before this, every counter ran from its own
// base all the way to 0x7FFFFFFF, so the MO counter climbed through
// 0x70000000 and into the DLR band after ~268M pushes — about three days at
// 1,000 MO/s. From then on the two streams could issue the same number, and a
// deliver_sm_resp would resolve the wrong waiter: a receipt acked that was
// never confirmed, or a job left unacked and redelivered as a duplicate.
//
// Each band now wraps within itself, so two streams can never meet however
// long a process runs.
const seqBand = 0x10000000 // 268,435,456 numbers per stream

const (
	seqUnbind      uint32 = 0x40000000
	seqEnquireLink uint32 = 0x50000000
	seqMO          uint32 = 0x60000000
	seqDLR         uint32 = 0x70000000
)

// wrapSeq advances an outbound sequence counter within its own band, wrapping
// back to base+1 at the band's end. SMPP sequence numbers are 31-bit, so the
// top band also stops short of 0x7FFFFFFF.
func wrapSeq(seq *uint32, base uint32) uint32 {
	limit := base + seqBand - 1
	if limit > 0x7FFFFFFE {
		limit = 0x7FFFFFFE
	}
	*seq++
	// <= base catches a counter that was never initialised to its own base.
	if *seq > limit || *seq <= base {
		*seq = base + 1
	}
	return *seq
}
