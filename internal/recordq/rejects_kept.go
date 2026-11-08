package recordq

import "github.com/yakugate/yakugate/internal/model"

// A refusal's row after an answer that does not say where its task is
// (go-live test of 18 Oct 2026, P14 E6: the queue Redis restarted while
// refusals came in; 16 "reject log write failed", and 4 of 600 refusals had
// no row).
//
// RejectsOrDirect writes a refusal's row to the record store itself only
// when its task certainly did not reach Redis (NotQueued). After an unclear
// error — the connection dropped or timed out once the transaction was sent
// — the task may be in Redis or not, and the row was left to it: when it was
// not, the refusal had no row. Now such a refusal is put again, marked Once,
// until Redis takes it. The worker writes a Once task the careful way
// (WriteRejectedBatchOnce: nothing that is there already), and a message's
// tasks ride one lane in the order they were put, so when the first copy did
// reach Redis it is written first and the second writes nothing.

// RejectKeeper is an optional RejectQueue capability: a refusal whose task
// may or may not be in Redis is put again, marked Once, until Redis takes it
// (Client.KeepRejected).
type RejectKeeper interface {
	KeepRejected(m model.Message, cause error) error
}
