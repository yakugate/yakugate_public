package model

// ReceiptMatch is the message a vendor's receipt was matched to in the
// record store: what the receipt's path needs of it, and which vendor SENT it
// — with a carrier's TX and RX rows, not the vendor the receipt came in on
// (Saleem 2026-10-14).
type ReceiptMatch struct {
	MsgIDs      []string // usually one; the parts of one long message
	AccountID   int64
	NoReceipt   bool
	FailureOnly bool
	BindSlot    int
	Privacy     Privacy
	MCC, MNC    string
	VendorID    int64
	// CompanyID is the customer company of the messages: their records are
	// in mt_<company>_<day> (D-136).
	CompanyID int64
	// RecordPending: the receipt names a part of a long message that was
	// sent, but the message's record is not written yet (the DB worker
	// writes it a moment later). MsgIDs is empty: nothing can be done with
	// the receipt yet, and it is kept to be applied when the record is there.
	RecordPending bool
}

// RecordRef names one message's record (D-136, 2026-10-14): its id and its
// customer company. The record is in mt_<company>_<day> (mo_ for an
// incoming message), the day being its id's. Whoever asks about a message
// has its company already — the message itself, its waiting reference in
// Redis or open_msgs, the account a client signed in with — so no table is
// kept just to find a message.
type RecordRef struct {
	MsgID     string
	CompanyID int64
}

// Ref is the record reference of a message.
func (m Message) Ref() RecordRef { return RecordRef{MsgID: m.ID, CompanyID: m.CompanyID} }

// ReceiptClose is one final receipt to be filed on its message's record, and
// the message closed with it when it is the receipt that does: what
// CloseWithReceipt takes for one, for a batch of them (DAL plan, step 3,
// 30 Oct 2026 — a gateway without Redis applies a worker's receipts in one
// transaction a receipt table).
type ReceiptClose struct {
	Ref         RecordRef
	Status      Status
	VendorMsgID string
	ErrorCode   string
	// Entry is the receipt's row as the receipt path builds it (the JSON
	// CloseWithReceipt takes).
	Entry string
}

// ReceiptClosed is what became of one ReceiptClose. Found=false: the message
// has no record (yet), and nothing of the receipt was written. Mine: this
// receipt closed the message; else it was final already and the receipt is
// recorded only.
type ReceiptClosed struct {
	Found, Mine bool
}
