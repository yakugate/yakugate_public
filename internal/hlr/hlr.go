// Package hlr is the shared HLR / MNP core component. It resolves an MSISDN to
// its network identity (MCC/MNC), MNP-ported flag, and per-number sender block.
//
// HLR is a CORE/SHARED service used by multiple processes (Submit API, routing,
// abuse-monitor, importer). Callers use an hlr.Service.Lookup(msisdn) instead of
// touching the HLR tables directly. Data is sharded by the first 4 digits of the
// MSISDN (table `hlr_<first4>`; in Redis one key a number, `hlr:<msisdn>`).
//
// Backend policy (PO decision 2026-07-17, held again by Saleem 2026-10-14:
// one source, never both — except a number Redis has no copy of, which is
// read from the database before a paid lookup, Saleem 2026-11-06: "lookup is
// either new number or expiry"): Redis backends are read directly per message;
// SQL/Mongo backends go through the in-memory Cache — full load at
// startup (after a memory-vs-records precheck), then delta refresh every 5
// minutes on the indexed self-updating last_check column, or immediately via
// the POST /admin/hlr/refresh hook. Soft deletes (is_deleted) evict entries.
//
// [SCOPE P1-14]
package hlr

import (
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/model"
)

// Entry is one resolved HLR record.
type Entry struct {
	MSISDN string
	MCC    string // mobile country code (up to 3 digits)
	MNC    string // mobile network code (up to 3 digits)
	Ported bool   // MNP-ported (MCC/MNC reflect the current network)
	// OperatorName and CCC (6 Nov 2026): the operator's name and the country
	// calling code, as a live lookup, an import or the number range gave
	// them; "" = not known. Kept with the number (hlr_<first4>, the Redis
	// copy) and written on the lookup's log row.
	OperatorName string
	CCC          string
	BlockSenders string // "*" = blocked for ALL senders; else a specific sender mask ("" = no block)
	Deleted      bool   // soft-deleted (is_deleted=1): lookups miss, cache evicts
	// CheckedAt is when this answer was last confirmed (hlr_<first4>.last_check:
	// the import or live lookup that wrote it). Zero = unknown, never expires.
	// An account's HLR cache time limit is measured from it (2026-09-14).
	CheckedAt time.Time
	// Call (6 Nov 2026) is the call a live lookup made to its provider, for
	// the lookup's log row; set on a found answer and on a not-found one
	// (Live.Try, Dipper.Try). Never kept with the number: the dipper drops it
	// before the answer is saved.
	Call *model.LookupCall `json:"-"`
}

// Expired reports whether the answer is older than ttl. ttl <= 0, or an
// answer with no known time, never expires.
func (e Entry) Expired(ttl time.Duration, now time.Time) bool {
	return ttl > 0 && !e.CheckedAt.IsZero() && now.Sub(e.CheckedAt) > ttl
}

// Blocked reports whether a message from `sender` to this number is blocked.
// BlockSenders is a comma-separated list of sender masks: "" = no block,
// "*" = block ALL senders, otherwise the sender must match one list entry.
func (e Entry) Blocked(sender string) bool {
	if e.BlockSenders == "" {
		return false
	}
	for _, s := range strings.Split(e.BlockSenders, ",") {
		s = strings.TrimSpace(s)
		if s == "*" {
			return true
		}
		if s != "" && s == sender {
			return true
		}
	}
	return false
}

// Network returns the "MCC-MNC" identity used by routing/records.
func (e Entry) Network() string {
	if e.MCC == "" && e.MNC == "" {
		return ""
	}
	return e.MCC + "-" + e.MNC
}

// Shard is the storage shard for an MSISDN: the first 4 digits.
// DB table = "hlr_" + Shard(msisdn). (The Redis key is "hlr:" + msisdn, not sharded.)
func Shard(msisdn string) string {
	if len(msisdn) >= 4 {
		return msisdn[:4]
	}
	return msisdn
}

// Store is the pluggable HLR backend (mysql / mssql / redis shard lookup).
type Store interface {
	Lookup(msisdn string) (Entry, bool)
}

// Service is the entry point processes call. It wraps a Store (and can add an
// LRU/Redis hot cache later without changing callers).
type Service struct{ store Store }

// New builds an HLR service over a store.
func New(store Store) *Service { return &Service{store: store} }

// Lookup resolves an MSISDN. ok=false means "not found" (caller applies its fallback).
func (s *Service) Lookup(msisdn string) (Entry, bool) { return s.store.Lookup(msisdn) }
