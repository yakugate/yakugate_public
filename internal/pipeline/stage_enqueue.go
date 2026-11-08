package pipeline

import (
	"context"
	"errors"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/abuse"
	"github.com/yakugate/yakugate/internal/abusecount"
	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/mask"
	"github.com/yakugate/yakugate/internal/model"
)

// QueueWriter enqueues a durable pending message (satisfied by store backends).
type QueueWriter interface {
	Enqueue(ctx context.Context, m model.Message) error
}

// AcceptRecorder writes an accepted message's record (mt_) at acceptance
// (mysql.Store.WriteAccepted, recordq.Client.WriteAccepted).
type AcceptRecorder interface {
	WriteAccepted(ctx context.Context, msgs []model.Message) error
}

// OneStepAccepter is an AcceptRecorder that can queue the record in the same
// step as the vendor queue's write (recordq.Client.EnqueueAccepted): handled
// = false when this queue store cannot.
type OneStepAccepter interface {
	EnqueueAccepted(ctx context.Context, q any, m model.Message) (handled bool, err error)
}

// --- stage 18/18: enqueue (durable) --- [SCOPE P1-17]
type enqueueStage struct {
	q            QueueWriter
	crypto       Encryptor
	reserve      Reserver     // nil = reservation disabled, cheap credit-stage check only
	reservePrice float64      // per-segment estimate, used only when no pricer is wired
	pricer       Pricer       // exact sell charge at accept (PO 2026-09-17); nil = the estimate
	usage        UsageCounter // nil = quota usage not tracked (chunk-6 ruling 6.1)
	destCapUsage UsageCounter // nil = per-destination frequency cap not tracked [P1-13D]
	dedup        Deduper      // to RELEASE a dedupe claim when the durable write fails (chunk-8 ruling 8.1)
	campaigns    CampaignNoter
	abuse        AbuseNoter
	accepted     AcceptRecorder // nil = no record at acceptance
}

// enqueueAccepted puts the message on its vendor queue and writes its record
// (mt_): in one step when the record queue and the vendor queue can share a
// transaction, else the queue first and then the record. A record that could
// not be written after the message was queued is not a refusal — the message
// is the client's now — and the vendor role's first answer writes it.
func (s enqueueStage) enqueueAccepted(ctx context.Context, m model.Message) error {
	if one, ok := s.accepted.(OneStepAccepter); ok {
		handled, err := one.EnqueueAccepted(ctx, s.q, m)
		if handled {
			return err
		}
	}
	if err := s.q.Enqueue(ctx, model.ForQueue(m)); err != nil {
		return err
	}
	if s.accepted != nil {
		if err := s.accepted.WriteAccepted(ctx, []model.Message{m}); err != nil {
			acceptRecordFailed.say(m.Vendor, err)
		}
	}
	return nil
}

// CampaignNoter counts an accepted message against its text's signature in
// the live campaign window (campaignwin.Window). It must never block: the
// window buffers in memory and flushes on its own. nil = no live window.
type CampaignNoter interface {
	Note(hash string, accountID int64)
}

// AbuseNoter takes what the abuse monitor needs to know about one accepted
// message (abusecount.Counter). It must never block.
type AbuseNoter interface {
	Note(abusecount.Event)
}

// abuseEvent is an accepted message as the abuse counters take it. The text
// kept is the STORED text — ciphertext for a sealed message, digits hidden
// for a masked one, exactly what the record will hold — and the plain text is
// handed over for its shape only.
func abuseEvent(e *Envelope, quote billing.Quote, quoted bool) abusecount.Event {
	ev := abusecount.Event{
		AccountID:   e.Msg.AccountID,
		At:          billing.AcceptedAt(e.Msg.ID), // the id's own time = the record's accepted_at
		Encoding:    e.Msg.Encoding,
		HLRSource:   e.Msg.HLRSource,
		Destination: e.Msg.Destination,
		Sender:      e.Msg.Sender,
		Stored:      e.Msg.Body,
		Plain:       captureText(e),
	}
	if e.PlainBody == "" && e.Msg.MaskBody {
		ev.Stored = mask.Digits(e.Msg.Body, maskMinDigits())
	}
	if quoted {
		ev.Spend = quote.ChargeFor(maxInt(e.Msg.Segments, 1))
	}
	return ev
}

// maskMinDigits is the shortest digit run a masked text hides
// (YAKU_MASK_MIN_DIGITS, default 4) — the record store's own rule.
func maskMinDigits() int {
	if n, err := strconv.Atoi(strings.TrimSpace(os.Getenv("YAKU_MASK_MIN_DIGITS"))); err == nil && n > 0 {
		return n
	}
	return 4
}

// releaseDedup removes the dedup stage's key on any failure path here — the
// message never became durable, so a client retry must be a fresh submission,
// not a duplicate of an id that never reached the queue. Best-effort: a
// failed release is logged.
// queueUnavailableRetryAfterSec is the Retry-After a 503 QUEUE_UNAVAILABLE carries (PO 2026-09-17).
const queueUnavailableRetryAfterSec = 5

func (s enqueueStage) releaseDedup(ctx context.Context, e *Envelope) {
	if s.dedup == nil || e.DedupKey == "" {
		return
	}
	if err := s.dedup.Release(ctx, e.DedupKey, e.Msg.ID); err != nil {
		log.Printf("[pipeline] dedup release failed for %s (%s): %v — a resend is refused as a duplicate until the TTL passes", e.Msg.ID, e.DedupKey, err)
	}
}

// errNoEncryptor: the message must be encrypted and this process has no
// encryption configured.
var errNoEncryptor = errors.New("no encryption in this process")

func (enqueueStage) Name() string { return "enqueue" }
func (s enqueueStage) Run(ctx context.Context, e *Envelope) *Reject {

	// Body masking (PO 2026-09-08; the COMPANY's setting since 2026-09-13):
	// decided here, where the account is in scope, and carried on the
	// message — the vendor processes that write the record have no reference
	// cache of their own.
	if e.Account != nil && e.Account.BodyPrivacy == "pii_masked" {
		e.Msg.MaskBody = true
	}
	// An account whose clients get no receipt at all (dlr_levels = none,
	// config/0142): accepted as if the client had asked for none, so every
	// push site and the receipt charge — which all ask
	// model.WantsTerminalReceipt — already say no.
	if e.Account != nil && e.Account.NoReceipts {
		e.Msg.NoReceipt = true
		e.Msg.FailureReceiptOnly = false
		e.Msg.WantIntermediateReceipt = false
	}
	// No prices, no message (RaterPricer.Ready): the role has never read the
	// rate cards, so it cannot reserve, cannot stop a loss, and would hand
	// out messages nobody is charged for. A temporary refusal — the client
	// sends again — on both doors.
	if pr, ok := s.pricer.(interface{ Ready() bool }); ok && !pr.Ready() {
		return reject(503, "SERVICE_UNAVAILABLE")
	}

	// Real-time balance reservation (2026-07-23) — see Reserver's doc comment
	// for why this exists. Placed as late as possible (after dedup) so a
	// message that turns out to be a duplicate/conflict never reserves money
	// in the first place; a message reserved here that then fails encryption
	// or the durable write below is refunded before this stage returns.
	var reservedAmount float64
	var reserved bool
	// What the message costs, fixed here (billing.Quote, 2026-10-13): it
	// rides the queue row to the record and the billing role charges by it,
	// so a card set or changed after this moment never prices this message.
	var quote billing.Quote
	quoted := false
	if qp, ok := s.pricer.(Quoter); ok && e.Account != nil {
		quote, quoted = qp.Quote(ctx, e)
	}
	// The terms the message is charged by, fixed with its price: the
	// company's currency and the account's charge point. Every later moment
	// that books its money reads them here (billing.Quote).
	if quoted {
		quote.Cur, quote.Point = e.Account.CompanyCurrency, billing.NormalPoint(e.Account.ChargePoint)
	}
	// The billing queue (issues 3 and 18; Saleem, 26 Oct 2026): what is
	// decided now — the message's price when the account is charged at
	// acceptance, and the number lookup whatever the charge point — is booked
	// as events, moved (prepaid) and queued in ONE step. Refused whole when
	// a prepaid balance does not cover it.
	var booked []billing.Event
	eb, events := s.reserve.(EventBooker)
	if events && quoted && e.Account.CompanyID != 0 {
		evs := billing.AcceptEvents(billing.Accepted{MsgID: e.Msg.ID, Company: e.Account.CompanyID, Account: e.Account.ID,
			Vendor: e.Msg.VendorID, Model: billingModelOf(e), Parts: e.Msg.Segments, Quote: quote})
		if len(evs) > 0 {
			if e.Account.Prepaid() {
				ok, after, err := eb.TakeEvents(ctx, e.Account.CompanyID, evs)
				if err != nil {
					balanceRefused.say(e.Account.ID, err) // which store, and its error (30 Oct 2026)
					s.releaseDedup(ctx, e)
					return reject(503, "INTERNAL_ERROR: balance reservation failed")
				}
				if !ok {
					s.releaseDedup(ctx, e)
					return reject(402, "INSUFFICIENT_BALANCE")
				}
				e.BalanceAfter = after
			} else if err := eb.BookEvents(ctx, e.Account.CompanyID, evs); err != nil {
				usageRefused.say(e.Account.ID, err)
				s.releaseDedup(ctx, e)
				return reject(503, "INTERNAL_ERROR: usage could not be booked")
			}
			booked = evs
			for _, ev := range evs {
				if ev.Kind == billing.EventMT && ev.Sell != nil {
					// The message's own price was decided here: the reply
					// reports it (what the lookup added is in the balance).
					quote.Held = e.Account.Prepaid()
					e.Charged = ev.Sell.Amount()
				}
			}
		}
		quote.Booked = true
	} else if amount := billing.RoundLedger(s.reservation(ctx, e, quote, quoted)); amount > 0 {
		// No events here (an account with no company, a pricer with no
		// quote): the hold alone, as before. In whole ledger units
		// (billing.RoundLedger), the way the reservers take it: the reply's
		// cost and the balance are then the same figure to the last digit.
		ok, after, err := reserveFor(ctx, s.reserve, e.Account.ID, e.Account.CompanyID, amount, e.Msg.ID)
		if err != nil {
			balanceRefused.say(e.Account.ID, err)
			s.releaseDedup(ctx, e)
			return reject(503, "INTERNAL_ERROR: balance reservation failed")
		}
		if !ok {
			s.releaseDedup(ctx, e)
			return reject(402, "INSUFFICIENT_BALANCE")
		}
		reservedAmount, reserved = amount, true
		quote.Held = quoted
		// What the reply reports: the amount taken and the balance it left
		// (QA API-004 / BILL-005 — not the reference cache's old copy).
		e.Charged, e.BalanceAfter = amount, after
	}
	// giveBack undoes what was taken or booked here, for a message that
	// never reached the queue.
	giveBack := func() {
		if len(booked) > 0 {
			if err := eb.GiveBackEvents(context.WithoutCancel(ctx), e.Account.CompanyID, booked); err != nil {
				log.Printf("[pipeline] give-back FAILED acct=%d msg=%s: %v — needs manual reconciliation", e.Account.ID, e.Msg.ID, err)
			}
		}
		if reserved {
			refund(ctx, s.reserve, e.Account.ID, e.Account.CompanyID, reservedAmount, e.Msg.ID)
		}
	}

	e.Msg.Status = model.StatusQueued
	// How the company pays, as of THIS acceptance (queue/0036, QA BUG-024):
	// the same account view that decided the reservation above, carried to
	// the record so the billing role charges the message by the model it was
	// accepted under — not by whatever the company was switched to before the
	// next billing sweep. Unlinked accounts have no company to bill: "".
	e.Msg.BillingModel = billingModelOf(e)
	// Whose company it is (queue/0039, D-136): the record is filed in
	// mt_<company>_<day> by whichever role writes it.
	if e.Account != nil {
		e.Msg.CompanyID = e.Account.CompanyID
	}
	if quoted {
		e.Msg.Quote = quote.String()
	}
	// The gateway that accepted it (queue/0033). Stamped HERE, at the moment
	// the message becomes durable, because this is the last point the
	// accepting process still owns it — the record is written at the other
	// end of the queue, by whichever deployment sends it.
	e.Msg.DeploymentID = deployment.ID()
	// And whose queue it goes in: this deployment's own (D-121) — no other
	// gateway sends it. "" (no YAKU_DEPLOYMENT_ID) = the shared queue, as
	// before.
	e.Msg.QueueOwner = deployment.Owner()
	// Campaign (P10 register, ruled 2026-09-09): the client's campaignId is
	// kept, bounded to its column, and the text's signature is taken HERE —
	// after auto-replace, so it is the text that goes out, and before the
	// seal below, the last point it can be read.
	if len(e.Msg.CampaignID) > 100 {
		e.Msg.CampaignID = e.Msg.CampaignID[:100]
	}
	e.Msg.CampaignHash = abuse.CampaignHash(e.Msg.Body)
	// TTL (D-026): honour a request-set validity; else the account's own default
	// (accounts.default_ttl_sec, TC-P7-09); else the process-wide default so the
	// expiry sweep (P7) can always bound the message's lifetime.
	if e.Msg.ValidUntil.IsZero() {
		ttl := defaultTTL
		if e.Account.DefaultTTLSec > 0 {
			ttl = time.Duration(e.Account.DefaultTTLSec) * time.Second
		}
		e.Msg.ValidUntil = time.Now().Add(ttl)
	}
	// Priority (S-11, PO 2026-07-27): 0 means the client didn't set one. A
	// matched template's own default (stage_template.go) already filled it in
	// when that applies; anything still 0 here (no template match required, or
	// template check off) falls back to the generic default.
	if e.Msg.Priority == 0 {
		e.Msg.Priority = defaultPriority
	}
	// Encrypt-at-rest happens HERE, at the last moment before the durable write,
	// so every earlier stage still saw plaintext: dedupe fingerprints, encoding and
	// segment counts, spam and template matching must all describe the real text.
	// Vendor egress decrypts again on the way out. [SCOPE P1-CRYPTO]
	//
	// Whether: the company's privacy setting is all_encrypted (PO 2026-09-13),
	// or the gateway-wide switch is on. Which key: the company's own KMS key
	// if it has one, else the gateway key — Encrypt decides, and refuses when
	// there is neither. A company set to encrypted in a process with no
	// encryption at all is refused too: never stored plain.
	want := e.Account.BodyPrivacy == "all_encrypted" ||
		(s.crypto != nil && s.crypto.ShouldEncrypt(ctx, "body", e.Account.CompanyID))
	if want && e.Msg.Body != "" {
		var ct string
		err := errNoEncryptor
		if s.crypto != nil {
			ct, err = s.crypto.Encrypt(ctx, e.Msg.AccountID, e.Account.CompanyID, e.Msg.Body)
		}
		if err != nil {
			giveBack()
			s.releaseDedup(ctx, e)
			// Refuse rather than silently storing plaintext the operator believes
			// is encrypted.
			return reject(503, "ENCRYPTION_UNAVAILABLE: could not seal message body")
		}
		// What a route rewrite replaced is message text too, and is sealed
		// the same way — never kept plain beside a sealed body (queue/0029).
		var origCT string
		if e.Msg.OrigBody != "" {
			if origCT, err = s.crypto.Encrypt(ctx, e.Msg.AccountID, e.Account.CompanyID, e.Msg.OrigBody); err != nil {
				giveBack()
				s.releaseDedup(ctx, e)
				return reject(503, "ENCRYPTION_UNAVAILABLE: could not seal message body")
			}
		}
		// The capture keeps the plain text (captures are for debugging and
		// stay plain, PO 2026-09-13); only the stored copy is sealed.
		e.PlainBody = e.Msg.Body
		e.Msg.Body = ct
		if origCT != "" {
			e.Msg.OrigBody = origCT
		}
	}
	// Soft-flag rides the queue row to the record (config/0098): the record is
	// written later by a vendor process with no reference cache, so the accept
	// decision has to travel with the message.
	if len(e.SpamFlags) > 0 {
		e.Msg.SpamFlag = trunc190(strings.Join(e.SpamFlags, "; "))
	}
	if err := s.enqueueAccepted(ctx, *e.Msg); err != nil {
		queueRefused.say(e.Msg.Vendor, err) // the store's own reason, in the log (19 Oct 2026)
		giveBack()
		s.releaseDedup(ctx, e)
		// A short fixed hint (PO 2026-09-17, reversing the 2026-09-03 "no
		// hint" call): the outage has no knowable end, but a client that
		// retries at once only adds load, and 5 s is the pause we would ask
		// for by hand.
		return rejectAfter(503, "QUEUE_UNAVAILABLE: could not durably queue, retry later", queueUnavailableRetryAfterSec)
	}
	// Quota usage counts ONLY here, after the durable write succeeded (chunk-6
	// ruling 6.1): rejects never burned quota above, and the ReplayID early
	// return keeps idempotent duplicates from double-counting.
	if s.usage != nil {
		n := maxInt(e.Msg.Segments, 1)
		for _, k := range e.QuotaKeys {
			s.usage.Add(k, n)
		}
	}
	// The live campaign window, same accept-only rule: counted once the
	// message is durably queued.
	if s.campaigns != nil && e.Msg.CampaignHash != "" {
		s.campaigns.Note(e.Msg.CampaignHash, e.Msg.AccountID)
	}
	// The abuse monitor's counters, same rule: a message is counted once it
	// is durably queued. A replay and a refusal never get here.
	if s.abuse != nil {
		s.abuse.Note(abuseEvent(e, quote, quoted))
	}
	// Per-destination frequency cap, same accept-only rule — but ONE per
	// message, not per segment: a long message split into five parts is still
	// one message arriving on that subscriber's handset. [SCOPE P1-13D]
	if s.destCapUsage != nil {
		for _, k := range e.DestCapKeys {
			s.destCapUsage.Add(k, 1)
		}
	}
	return nil
}

// refund best-efforts a reservation reversal. A failure here is logged, not
// returned as a second reject: the caller is already on an error path (the
// message was NOT enqueued), and the account owner is not made worse off by
// a refund that fails to post immediately — a missed refund is a support/ops
// issue to chase (no ledger to reconcile against anymore, so this log line is
// the only trail — accepted per the same 2026-07-23 simplification that
// dropped Reserve's ledger row), not a reason to change the response the
// client already got.
func refund(ctx context.Context, r Reserver, accountID, companyID int64, amount float64, msgID string) {
	var err error
	if lr, ok := r.(LedgerReserver); ok {
		err = lr.RefundFor(ctx, accountID, companyID, amount, msgID)
	} else {
		err = r.Refund(ctx, accountID, companyID, amount)
	}
	if err != nil {
		log.Printf("[pipeline] refund FAILED acct=%d msg=%s amount=%.4f: %v — needs manual reconciliation", accountID, msgID, amount, err)
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// defaultPriority applies when neither the client nor a matched template set
// one (the retired `type` ladder's old "Default" rung). [SCOPE P1-TEMPLATE-PRIORITY]
const defaultPriority = 9

// defaultTTL bounds an accepted message's lifetime when the client didn't set a
// validity period (YAKU_DEFAULT_TTL_SEC, default 48h). [SCOPE P7 / D-026]
var defaultTTL = func() time.Duration {
	if v := os.Getenv("YAKU_DEFAULT_TTL_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return 48 * time.Hour
}()

// trunc190 bounds a value to the queue/record column width. A flag that is too
// long is worth keeping in truncated form — losing it entirely would make the
// message look unflagged, which is the one thing this column exists to say.
func trunc190(v string) string {
	if len(v) <= 190 {
		return v
	}
	return v[:190]
}

// reservation is what to hold for this message before it is queued: the
// exact sell charge when a pricer is wired, else the per-segment estimate;
// 0 = nothing. Only a prepaid account charged AT ACCEPTANCE reserves
// (charge_point, config/0114): an account charged on submit or delivery is
// gated by the credit stage alone and settled by the billing role later.
func (s enqueueStage) reservation(ctx context.Context, e *Envelope, quote billing.Quote, quoted bool) float64 {
	if s.reserve == nil || e.Account == nil || !e.Account.Prepaid() {
		return 0
	}
	if cp := e.Account.ChargePoint; cp != "" && cp != "acceptance" {
		return 0
	}
	if quoted {
		// The quote's own figure: what is held is what the record will say.
		return quote.ChargeFor(e.Msg.Segments)
	}
	if s.pricer != nil {
		return s.pricer.Price(ctx, e)
	}
	if s.reservePrice > 0 {
		return float64(maxInt(e.Msg.Segments, 1)) * s.reservePrice
	}
	return 0
}

// billingModelOf is the billing model stamped on an accepted message:
// "prepaid" or "postpaid" for a company-linked account, "" otherwise.
func billingModelOf(e *Envelope) string {
	if e.Account == nil || e.Account.CompanyID == 0 {
		return ""
	}
	if e.Account.Prepaid() {
		return model.BillingPrepaid
	}
	return model.BillingPostpaid
}

// reserveFor holds the amount, through the ledger when the reserver can.
// The balance left after the hold comes back when the reserver can say it
// (nil otherwise).
func reserveFor(ctx context.Context, r Reserver, accountID, companyID int64, amount float64, msgID string) (bool, *float64, error) {
	if br, ok := r.(BalanceReserver); ok {
		return br.ReserveForBalance(ctx, accountID, companyID, amount, msgID)
	}
	if lr, ok := r.(LedgerReserver); ok {
		ok, err := lr.ReserveFor(ctx, accountID, companyID, amount, msgID)
		return ok, nil, err
	}
	ok, err := r.Reserve(ctx, accountID, companyID, amount)
	return ok, nil, err
}
