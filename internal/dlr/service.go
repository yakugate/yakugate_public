package dlr

import (
	"context"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/mask"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/receiptpolicy"
	"github.com/yakugate/yakugate/internal/vendorstatus"
)

// Store is the hot-store surface the DLR service needs. *store/mysql.Store
// satisfies it. [SCOPE P5]
//
// ResolveVendorMsgID's noReceipt/failureOnly (PO 2026-09-03): the client's
// registered_delivery preference, carried by record_index because by the
// time a vendor receipt arrives the model.Message it came from is long gone
// — this locator row is the only place left to ask. bindSlot (PO 2026-09-05)
// is carried the same way and for the same reason: it names the bind the
// client submitted on, so the receipt can go back down that bind.
type Store interface {
	ResolveVendorMsgID(ctx context.Context, vendorID int64, vendorMsgID string) (msgID string, accountID int64, noReceipt, failureOnly bool, bindSlot int, privacy model.Privacy, mcc, mnc string, ok bool, err error)
	UpdateStatus(ctx context.Context, ref model.RecordRef, s model.Status, vendorMsgID, errorCode string) error
	AppendRawDLR(ctx context.Context, ref model.RecordRef, entryJSON string) error
}

// OrphanRecorder is an optional Store capability: keep a receipt that
// correlates to nothing of ours (PO 2026-08-31). Recording it HERE, in the one
// place every edge funnels through (D-023), covers the P5 webhook, P3's
// deliver_sm and P4's poll at once — two of which did not even log the drop.
// RECORD ONLY: never retried, never re-applied.
type OrphanRecorder interface {
	WriteOrphanDLR(ctx context.Context, vendorID int64, vendorMsgID, status, errorCode, raw, reason string) error
}

// GroupResolver is an optional Store capability: return EVERY msg_id sharing a
// vendor-assigned id. A concat group reassembled for an HTTP vendor (D-036) is one
// request to the vendor, so all its parts carry the single id the vendor returned
// and one receipt must finalize them all. Stores that don't implement it keep the
// single-message behaviour.
type GroupResolver interface {
	ResolveVendorMsgIDAll(ctx context.Context, vendorID int64, vendorMsgID string) (msgIDs []string, accountID int64, noReceipt, failureOnly bool, bindSlot int, privacy model.Privacy, mcc, mnc string, err error)
}

// PartTracker is an optional Store capability for multi-part sends: a message
// split across N PDUs gets N vendor ids and N receipts, so each receipt records
// its own part's status and the MESSAGE status is the aggregate — ready=false
// while parts are still outstanding, so the record is not finalized early.
// ok=false means this id is an ordinary single-part message. [D-037]
type PartTracker interface {
	MarkPartAndAggregate(ctx context.Context, vendorID int64, vendorMsgID string,
		st model.Status) (agg model.Status, ready, ok bool, err error)
}

// FirstPartTracker is PartTracker for a vendor whose first final receipt
// closes a long message (SetFirstReceiptCloses; Saleem 4 Nov 2026): ready
// for the group's first final part, with that part's status; every later
// part is recorded only.
type FirstPartTracker interface {
	MarkFirstPartFinal(ctx context.Context, vendorID int64, vendorMsgID string,
		st model.Status) (agg model.Status, ready, ok bool, err error)
}

// Finalizer is an optional Store capability: give a message its final status
// only if it has none yet, and say whether this call did it. Receipts are
// handled side by side, so two can close one message in the same moment — a
// late part whose group was just folded away, or the receipts of a message
// the vendor was sent twice — and without this each was pushed to the client
// (load test 2026-10-06). Stores without it keep the plain UpdateStatus.
type Finalizer interface {
	FinalizeOnce(ctx context.Context, ref model.RecordRef, st model.Status, vendorMsgID, errorCode string) (mine bool, err error)
}

// PartFolder is an optional Store capability: once a multi-part group reaches its
// verdict the per-PDU outcome is folded onto the record and the working rows
// dropped, so correlation state does not outlive its purpose. [D-040]
type PartFolder interface {
	FoldConcatParts(ctx context.Context, ref model.RecordRef) error
}

// ClientPusher delivers a correlated receipt back to the owning client (the smsc
// leg): deliver_sm on their SMPP bind if bound, else their HTTP dlr_url. The
// default implementation enqueues to client_dlr; the smpp-server / dlr_url poster
// drains it. nil = no push.
// bindSlot names the bind the message was submitted on, so the receipt can go
// back down the same one (PO 2026-09-05); 0 = no originating bind (an HTTP
// submit), which means "any live receive-capable bind".
type ClientPusher interface {
	PushToClient(ctx context.Context, accountID int64, r Receipt) error
}

// AddressLookup is an optional Store capability: the original sender and
// destination of one message, for the receipt the client is pushed (an SMPP
// client matches receipts on them). Stores without it push "0" addresses, as
// every store did before 2026-09-11.
type AddressLookup interface {
	MessageAddresses(ctx context.Context, ref model.RecordRef) (sender, destination string, err error)
}

// RecordWrites is where the receipt service files its rows when the record
// store is not written by the roles (Saleem, 28 Oct 2026: with the DB worker
// "every db work is always queue no matter from any role of the gateway"):
// recordq.Client. The store is then only READ here — to match a receipt and
// see whether its record is there. Every write goes through these four.
type RecordWrites interface {
	AppendRawDLR(ctx context.Context, ref model.RecordRef, entryJSON string) error
	CloseWithReceipt(ctx context.Context, ref model.RecordRef, st model.Status, vendorMsgID, errorCode, entryJSON string, mayClose bool) (mine bool, err error)
	WriteOrphanDLR(ctx context.Context, vendorID int64, vendorMsgID, status, errorCode, raw, reason string) error
	MarkOrphanMatched(ctx context.Context, table string, id int64, msgID string) error
}

// SetRecordWrites sends every row this service writes through w (the record
// queue). Set once, before use.
func (s *Service) SetRecordWrites(w RecordWrites) { s.writes = w }

// Service centralizes DLR application: correlate a vendor receipt to its record,
// append raw_dlr history, update status, and push the receipt to the client.
// Every vendor sender (P3/P4) and the P5 webhook receiver funnels through here so
// the logic lives in ONE place (PO decision D-023). [SCOPE P5]
type Service struct {
	store Store
	// firstCloses: vendor id -> its first part's final closes a long
	// message (dlr_config.first_receipt_closes), for receipts taken on the
	// record store's path. Set by the vendor role (SetFirstReceiptCloses).
	firstCloses sync.Map
	// writes, when set (SetRecordWrites), takes every row this service would
	// write to the record store: the DB worker's queue (28 Oct 2026).
	writes RecordWrites
	// marked: orphans applied whose mark waits in the record queue (rematch.go).
	markedMu sync.Mutex
	marked   map[string]time.Time
	pusher   ClientPusher
	// biller books what a receipt decides (billing.go); nil = nothing.
	biller Biller
	// overrides is the per-vendor receipt-code table. nil = none configured,
	// which is every deployment until a vendor opts in.
	overrides StatusOverrides
	// sealer encrypts the text: part of a receipt for a message that was
	// stored encrypted. nil = no crypto in this process, and that part is
	// then removed rather than kept in plain text.
	sealer Sealer
	// policies is the per-account receipt policy table (config/0119). nil =
	// none wired, which is how every process behaved before it existed.
	policies Policies
	// facts and writer let a receipt be finished without the record store
	// (fast.go). Both nil = every receipt takes the full path below.
	facts  FactStore
	writer RecordWriter
	// groups says which vendors are one carrier (groups.go); statusMaps each
	// vendor's own words for a status. Both nil = each vendor on its own.
	groups     VendorGroups
	statusMaps func(vendorID int64) map[string]string
	// held is receipt work the record store could not take, done again
	// until it can (hold.go).
	held holdList
	// later is receipts whose facts were not in Redis yet, looked for again
	// (later.go).
	later laterList
}

// Sealer encrypts text under an account's key — the same key the account's
// messages use (crypto.Service satisfies it).
type Sealer interface {
	EncryptAccount(ctx context.Context, accountID int64, plaintext string) (string, error)
}

// SetSealer installs the encryptor for receipts of encrypted messages. Set by
// every process that builds a Service (P3, P4, the dlr role), like
// SetStatusOverrides.
func (s *Service) SetSealer(c Sealer) { s.sealer = c }

// New builds the service. pusher may be nil (no client-facing push).
func New(store Store, pusher ClientPusher) *Service {
	return &Service{store: store, pusher: pusher}
}

// StatusOverrides is the per-vendor receipt-code table, consulted only for a
// vendor that opted in (vendors.dlr_code_overrides). Optional: a Service with
// none set behaves exactly as it did before overrides existed.
type StatusOverrides interface {
	DLR(vendorID int64, code string) (vendorstatus.Rule, bool)
}

// Policies is the per-account receipt policy table (internal/receiptpolicy,
// config/0119): what the CLIENT is told when that is not what the vendor
// said. Optional — a Service without one behaves exactly as it did before
// policies existed, which is also every gateway where nobody has set one.
type Policies interface {
	For(accountID int64) *receiptpolicy.Policy
	Any() bool
}

// SetPolicies installs the account receipt policies. Set by each process that
// builds a Service, like SetStatusOverrides, and safe on a nil Service.
func (s *Service) SetPolicies(p Policies) {
	if s == nil {
		return
	}
	s.policies = p
}

// SetStatusOverrides installs the table. Set by whichever process builds the
// Service — the receipt path is reached from three of them (P3's deliver_sm,
// P4's status poll, the dlr role's webhook), so this is how all three get the
// same answers from one cache.
//
// Safe on a nil Service: a vendor role whose hot store could not be built has
// no receipt service, and wiring the overrides into it used to panic at start
// (found 2026-09-14; since 4246fb2). The role now starts, says its stores are
// unavailable, and idles, like the other roles.
func (s *Service) SetStatusOverrides(o StatusOverrides) {
	if s == nil {
		return
	}
	s.overrides = o
}

func trunc(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// maskMinDigits is the shortest digit run hidden by pii_masked
// (YAKU_MASK_MIN_DIGITS, default 4).
// correlateGrace is YAKU_DLR_CORRELATE_GRACE_MS: how long Apply waits before
// deciding a receipt matches nothing (default 250ms, 0 disables the wait).
//
// It covers the gap between a vendor accepting a message and our own record of
// that acceptance reaching the database — the vendor roles batch those writes,
// flushing every 100ms. The cost is paid only by receipts that miss, which are
// otherwise unusable anyway; a receipt that correlates never waits.
func correlateGrace() time.Duration {
	if v := os.Getenv("YAKU_DLR_CORRELATE_GRACE_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return time.Duration(n) * time.Millisecond
		}
	}
	return 250 * time.Millisecond
}

func maskMinDigits() int {
	if v := os.Getenv("YAKU_MASK_MIN_DIGITS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return mask.DefaultMinDigits
}

// SetFirstReceiptCloses says whether vendorID sends one final receipt per
// long message (dlr_config.first_receipt_closes, off by default): its first
// part's final then closes the message on the record store's path. The Redis
// path reads the same setting from the message's facts.
func (s *Service) SetFirstReceiptCloses(vendorID int64, on bool) {
	if s == nil {
		return
	}
	if on {
		s.firstCloses.Store(vendorID, true)
	} else {
		s.firstCloses.Delete(vendorID)
	}
}

func (s *Service) firstReceiptCloses(vendorID int64) bool {
	_, on := s.firstCloses.Load(vendorID)
	return on
}
