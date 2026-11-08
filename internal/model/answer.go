package model

import "time"

// The results of a vendor's answer to one part of one try (sub_.result).
const (
	AnswerAccepted = "accepted" // the vendor took it
	AnswerRefused  = "refused"  // the vendor refused it for good
	AnswerNotNow   = "not_now"  // the vendor said try again (throttled, busy, a 5xx)
	AnswerTimeout  = "timeout"  // no answer came in time
)

// SubmitAnswer is one vendor answer: one part of one try of one message
// (issue 25.4, the submit-response log sub_<company>_<YYYYMMDD>). Every
// answer is a row — accepted, refused, not now, or no answer — so a message's
// parts and its tries can be read back, and each part's receipt matched by
// the part's own vendor id (issue 12).
type SubmitAnswer struct {
	MsgID     string `json:"m"`
	AccountID int64  `json:"a,omitempty"`
	CompanyID int64  `json:"co,omitempty"`
	// Part of Parts: the vendor's parts (PDUs); 1 of 1 for a message sent
	// as one call.
	Part    int `json:"p,omitempty"`
	Parts   int `json:"n,omitempty"`
	Attempt int `json:"t,omitempty"` // the message's try (its claims so far)

	VendorID       int64  `json:"v,omitempty"`
	VendorMsgID    string `json:"i,omitempty"`
	VendorProtocol string `json:"vp,omitempty"` // smpp | http | soap | blackhole
	// CommandStatus is an SMPP answer's command_status; HasStatus says it is
	// set (0 is ESME_ROK, an answer of its own).
	CommandStatus uint32 `json:"cs,omitempty"`
	HasStatus     bool   `json:"hs,omitempty"`
	HTTPStatus    int    `json:"hp,omitempty"`

	Result string `json:"r"`           // AnswerAccepted | AnswerRefused | AnswerNotNow | AnswerTimeout
	Code   string `json:"c,omitempty"` // SMPP_45, HTTP_503, TIMEOUT …
	Words  string `json:"w,omitempty"` // the vendor's own words, shortened

	SentAt     time.Time `json:"s,omitempty"`
	AnsweredAt time.Time `json:"at,omitempty"`
	Deployment string    `json:"d,omitempty"`
	Session    string    `json:"b,omitempty"` // the bind or worker that sent it

	// What this try cost (the vendor's rate row and the rates it was
	// converted at, fixed when it was priced) and the routing round it was
	// sent in (hot/0070): facts of a submission, kept with it since mt_ holds
	// nothing about the vendor (the three-table design, 27 Oct 2026). Filled
	// by the record store from the message when the writer leaves them out.
	BuyRateID  int64   `json:"br,omitempty"`
	BuyFX      float64 `json:"bx,omitempty"`
	BuyBase    float64 `json:"bb,omitempty"`
	RouteID    int64   `json:"ri,omitempty"`
	RouteLevel int     `json:"rl,omitempty"`
	SendRound  int     `json:"rr,omitempty"`

	// Call is an HTTP vendor's call as it went (hot/0077, Saleem 5 Nov 2026:
	// "check if we save vendor responses and requests in raw for mt, if not
	// then save them too"); nil for SMPP and for a write with no call.
	Call *VendorCall `json:"vc,omitempty"`
}

// VendorCall is one call to an HTTP vendor as it went: the request (method,
// URL, headers, body) and the answer (headers, body; its status is the
// answer's HTTPStatus), secrets masked, bodies cut at 16 KB / 8 KB (the cut
// says so). Sealed: the two bodies are encrypted under the message's company
// key (an all_encrypted company), or not kept when the role has no
// encryption. One bulk call is kept on each message it carried.
type VendorCall struct {
	Method      string `json:"m,omitempty"`
	URL         string `json:"u,omitempty"`
	ReqHeaders  string `json:"qh,omitempty"`
	ReqBody     string `json:"qb,omitempty"`
	ReqCut      bool   `json:"qc,omitempty"`
	RespHeaders string `json:"rh,omitempty"`
	RespBody    string `json:"rb,omitempty"`
	RespCut     bool   `json:"rc,omitempty"`
	Sealed      bool   `json:"se,omitempty"`
	TookMS      int64  `json:"t,omitempty"`
}

// LookupCall is one call to an HLR provider as it went (6 Nov 2026): the
// request and the answer as a VendorCall keeps them, with the answer's HTTP
// status (0 = no answer) and the error when the call failed. Never sealed
// nor masked: the call carries the number (on the row already), never the
// text.
type LookupCall struct {
	VendorCall
	Status int    `json:"st,omitempty"`
	Err    string `json:"er,omitempty"`
}

// TryOf is the try a vendor answers for the message as it is being sent:
// its tries before this one (Attempts, which a take no longer counts since
// the queues are taken, not leased; 27 Oct 2026) plus this one. Every
// answer of one try carries it, so a second try's answer is a row of its
// own and not a copy of the first's.
func TryOf(m Message) int { return m.Attempts + 1 }

// Ref is the record of the message the answer is about.
func (a SubmitAnswer) Ref() RecordRef { return RecordRef{MsgID: a.MsgID, CompanyID: a.CompanyID} }
