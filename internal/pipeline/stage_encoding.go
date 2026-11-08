package pipeline

import (
	"context"
	"strconv"

	"github.com/yakugate/yakugate/lib/encoding"
)

// maxSegments caps concatenated SMS at YAKU_MAX_SEGMENTS parts (default 10) —
// a long-message flood is still one request but shouldn't fan out into an
// unbounded number of PDUs/vendor calls. [SCOPE P1-09]
var maxSegments = envInt("YAKU_MAX_SEGMENTS", 10)

// --- stage 9/18: encoding + gsm7 flag (recompute segments) --- [SCOPE P1-09]
type encodingStage struct{}

func (encodingStage) Name() string { return "encoding" }
func (encodingStage) Run(_ context.Context, e *Envelope) *Reject {
	enc, segs, err := encoding.Plan(e.Msg.Body, e.Msg.Gsm7)
	if err != nil {
		return reject(400, "ENCODING_MISMATCH")
	}
	if segs > maxSegments {
		return reject(400, "MESSAGE_TOO_LONG: exceeds "+strconv.Itoa(maxSegments)+" segments")
	}
	e.Msg.Encoding, e.Msg.Segments = enc, segs
	return nil
}
