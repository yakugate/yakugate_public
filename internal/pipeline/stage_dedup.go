package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log"
	"strconv"
	"time"
)

// Deduper is the duplicate check (package dedupe: Redis, MySQL or in
// memory). ONE call a message (Saleem, 29 Oct 2026): Check tries to create
// key holding msgID for ttl — firstID "" means it was created (a new
// message), else it is the id of the message this one duplicates. ttl is the
// account's window (PO 2026-07-27, resolveDedupeTTL below).
//
// Release removes the key of a message that was never queued (chunk-8
// ruling 8.1): the client's resend of a 503 must not be refused as a
// duplicate of a message nobody has. Called on the enqueue stage's failure
// paths only, and only while the key still holds that message's id.
type Deduper interface {
	Check(ctx context.Context, key, msgID string, ttl time.Duration) (firstID string, err error)
	Release(ctx context.Context, key, msgID string) error
}

// --- stage 17/18: duplicate check --- [SCOPE P1-16]
type dedupStage struct {
	d          Deduper
	defaultTTL time.Duration // YAKU_DEDUPE_TTL_SEC fallback when an account has no override
}

func (dedupStage) Name() string { return "dedup" }
func (s dedupStage) Run(ctx context.Context, e *Envelope) *Reject {
	if s.d == nil || !e.Account.DuplicateCheck {
		return nil
	}
	ttl := s.defaultTTL
	if e.Account.DedupeTTLSec > 0 {
		ttl = time.Duration(e.Account.DedupeTTLSec) * time.Second
	}
	// The same text, sender, receiver and client reference (HTTP messageId,
	// SMPP user_message_reference; "" when none) from the same account within
	// the window is a duplicate (Saleem, 29 Oct 2026). One key for both
	// edges: the reference is part of the hash on SMPP too (it was not since
	// 6 Sep), and a messageId reused for another text is simply a new message
	// (the separate "conflict" of 2026-07-18 is gone).
	key := DedupKey(e.Account.ID, e.Msg.Body, e.Msg.Sender, e.Msg.Destination, e.Msg.IdempotencyKey)
	firstID, err := s.d.Check(ctx, key, e.Msg.ID, ttl)
	if err != nil {
		// FAIL OPEN (chunk-8 ruling 8.2), one posture for every backend: a
		// blip risks accepting a duplicate, which is recoverable; refusing
		// live traffic is not.
		log.Printf("[pipeline] dedup check failed (%v) — proceeding without duplicate check for %s", err, e.Msg.ID)
		return nil
	}
	if firstID != "" {
		// An identical duplicate is REFUSED (Saleem, 28 Oct 2026). The edge
		// answers with the new id, logs the refusal, files a REJECTD receipt
		// row and sends it when the client wants receipts.
		r := reject(409, "DUPLICATE_MESSAGE: the same message was already accepted as "+firstID)
		r.Duplicate, r.DuplicateOf = true, firstID
		return r
	}
	// Created: the enqueue stage releases it if the message is never queued.
	e.DedupKey = key
	return nil
}

// DedupKey is the duplicate check's key: dedup:<account id>:<hash>, the hash
// being the first 16 bytes (hex) of SHA-256 over text, sender, receiver and
// the client's reference, each written with its length so no two different
// messages can run together into one hash.
func DedupKey(accountID int64, text, sender, receiver, ref string) string {
	h := sha256.New()
	for _, f := range []string{text, sender, receiver, ref} {
		h.Write([]byte(strconv.Itoa(len(f))))
		h.Write([]byte{':'})
		h.Write([]byte(f))
	}
	sum := h.Sum(nil)
	return "dedup:" + strconv.FormatInt(accountID, 10) + ":" + hex.EncodeToString(sum[:16])
}
