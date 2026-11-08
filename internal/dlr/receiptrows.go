package dlr

import (
	"context"
	"encoding/json"
	"log"
	"strings"

	"github.com/yakugate/yakugate/internal/model"
)

// The rows a receipt leaves: its raw entry on the message's record, an orphan
// when it matched nothing, and its text: part sealed or removed.

// entry is one raw_dlr array element on the record.
type entry struct {
	Direction   string `json:"direction"` // esme (from vendor) | smsc (to client)
	VendorMsgID string `json:"vendor_msg_id,omitempty"`
	Status      string `json:"status"`
	// Stat is the receipt's own SMPP word, one of the eight (2026-09-23).
	// Status stays the message status it implies, so every existing reader —
	// billing, the rollups, the portal's filters — is unchanged.
	Stat      string `json:"stat,omitempty"`
	ErrorCode string `json:"error_code,omitempty"`
	Raw       string `json:"raw,omitempty"`
	At        string `json:"at"`
	// DoneAt is when the vendor says the message reached the phone
	// (RFC 3339, UTC) — dlrlog.done_date. Empty when the vendor did not say.
	DoneAt string `json:"done_at,omitempty"`
	// Charge is what this receipt booked (billing.go), on its row.
	Charge string `json:"charge,omitempty"`
	// What it booked as rate version ids (issue 16): the receipt fee's
	// selling rate and the vendor's receipt cost.
	SellRate int64 `json:"sell_rate,omitempty"`
	BuyRate  int64 `json:"buy_rate,omitempty"`
}

// appendRaw files one receipt row: through the queue when it is set, else
// the store itself.
func (s *Service) appendRaw(ctx context.Context, ref model.RecordRef, entryJSON string) error {
	if s.writes != nil {
		return s.writes.AppendRawDLR(ctx, ref, entryJSON)
	}
	return s.store.AppendRawDLR(ctx, ref, entryJSON)
}

// orphans is where an orphan receipt is filed: the queue, else the store
// when it can.
func (s *Service) orphans() (OrphanRecorder, bool) {
	if s.writes != nil {
		return s.writes, true
	}
	rec, ok := s.store.(OrphanRecorder)
	return rec, ok
}

// recordReceipt writes one receipt row per message it covers. Best effort by
// design: a receipt that cannot be FILED must not be refused back to the
// vendor, which would have it sent again.
func (s *Service) recordReceipt(ctx context.Context, msgIDs []string, company int64, e entry) {
	b, _ := json.Marshal(e)
	for _, msgID := range msgIDs {
		if err := s.appendRaw(ctx, model.RecordRef{MsgID: msgID, CompanyID: company}, string(b)); err != nil {
			log.Printf("[dlr] receipt for %s (stat %s) not recorded: %v", msgID, e.Stat, err)
		}
	}
}

// storedReceipt is the vendor's raw text as it is kept: truncated, and with
// the `text:` part sealed for a company on encrypted bodies.
func (s *Service) storedReceipt(ctx context.Context, accountID int64, raw string, privacy model.Privacy) string {
	stored := trunc(raw, 512)
	if privacy == model.PrivacyEncrypted {
		stored = s.sealReceiptText(ctx, accountID, stored)
	}
	return stored
}

// withoutText is a receipt with what follows "text:" taken out.
func withoutText(receipt string) string {
	i := strings.Index(receipt, "text:")
	if i < 0 || i+len("text:") == len(receipt) {
		return receipt
	}
	return receipt[:i+len("text:")] + receiptTextRemoved
}

// sealReceiptText encrypts what follows "text:" in a receipt. If it cannot
// (no crypto here, or the key is unavailable) that part is REMOVED: a receipt
// is never refused over it, and never kept with the message text in plain.
func (s *Service) sealReceiptText(ctx context.Context, accountID int64, receipt string) string {
	i := strings.Index(receipt, "text:")
	if i < 0 {
		return receipt
	}
	head, tail := receipt[:i+len("text:")], receipt[i+len("text:"):]
	if tail == "" {
		return receipt
	}
	if s.sealer != nil {
		ct, err := s.sealer.EncryptAccount(ctx, accountID, tail)
		if err == nil {
			return head + ct
		}
		log.Printf("[dlr] receipt text for account %d not encrypted (%v) — removed instead", accountID, err)
	}
	return head + receiptTextRemoved
}

// receiptTextRemoved stands in for a receipt's text: part that could not be
// encrypted.
const receiptTextRemoved = "[removed]"
