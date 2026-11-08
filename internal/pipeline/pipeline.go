// Package pipeline is the canonical accept-time pipeline shared by every
// ingress edge (httpserver P1, smppserver P2). It owns the Stage framework and
// the 18 submit stages; NewSubmit builds them in the one true order, so the two
// ingress doors can never drift apart.
package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/hlr"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/refdata"
	"github.com/yakugate/yakugate/internal/seclog"
)

// RateAllower is the rate cap — consumer-side interface so the pipeline takes
// either the in-memory single-node limiter or the cluster-accurate
// ratelimit.Shared under the YAKU_REDIS_ADDR gate (2026-07-24), without
// importing either implementation. Units are SEGMENTS per second against a
// caller-chosen bucket key (PO 2026-07-26): "co:<id>" company-shared,
// "acct:<id>" unlinked account, "cc:<prefix>" destination country.
type RateAllower interface {
	Allow(key string, limit, n int) bool
}

// UsageCounter accumulates period-stamped quota usage (chunk-6 ruling 6.1,
// 2026-08-04) — consumer-side interface for ratelimit.Usage (in-memory) or
// ratelimit.SharedUsage (cluster-accurate under the YAKU_REDIS_ADDR gate).
// The quota stage READS (Get) to gate; the enqueue stage WRITES (Add) only
// once the message is durably accepted, so rejected traffic never burns
// quota. Units are segments.
type UsageCounter interface {
	Add(key string, n int)
	Get(key string) int
}

// Reject stops the pipeline with an HTTP status + reason code. RetryAfter
// (seconds, 0 = none) is surfaced as the HTTP Retry-After header — set by the
// auth ladders (423 lockout, 429 IP throttle), by rate-limit (always 1s, the
// bucket width) and by quota (seconds until the breached window rolls).
// [PO 2026-07-26; extended PO 2026-09-03]
//
// 503 QUEUE_UNAVAILABLE carries a fixed 5 s hint since 2026-09-17 (PO,
// reversing the 2026-09-03 "no hint" ruling): the outage has no knowable end,
// but an immediate retry only adds load.
type Reject struct {
	Code       int
	Reason     string
	RetryAfter int
	// Audit is extra detail for the OPERATOR only: it is appended to the
	// rejected-submit record and the server log, and is never rendered into
	// the client's response.
	//
	// It exists because Reason is client-visible on both halves — rejectCode
	// takes the part before the colon as the status, rejectDetail takes the
	// rest as the error message. So there was no way to record why we refused
	// without also telling the sender. The spam stage uses it to record WHICH
	// category matched (PO 2026-09-09: operator-only, so a spammer cannot
	// discover our rule taxonomy one send at a time).
	Audit string
	// Proven is the username of the account this refusal is about when its
	// credentials were PROVEN first — a switched-off, suspended, expired or
	// portal-only account refused after a correct token or password. "" for
	// every other refusal, above all a credential failure, where nothing says
	// whose request it was. Never rendered: it only lets the edge count and
	// record the refusal against the account, as a refusal further in is.
	Proven string
	// Duplicate marks a DUPLICATE_MESSAGE refusal (Saleem, 28 Oct 2026:
	// "reject the duplicate message, put it in rejected and send REJECTD dlr
	// if the client asks for a delivery report, and also log it in dlr_"). The
	// edge answers with the new message's id, sends that receipt and has its
	// row filed with the refusal's. DuplicateOf is the first message's id when
	// the check named one.
	Duplicate   bool
	DuplicateOf string
}

// NewReject builds a Reject (for callers outside this package, e.g. auth).
func NewReject(code int, reason string) *Reject { return &Reject{Code: code, Reason: reason} }

func reject(code int, reason string) *Reject { return &Reject{Code: code, Reason: reason} }

// rejectAfter is reject plus a Retry-After hint in seconds.
func rejectAfter(code int, reason string, secs int) *Reject {
	return &Reject{Code: code, Reason: reason, RetryAfter: secs}
}

// Envelope carries one recipient's message through the pipeline.
type Envelope struct {
	Msg      *model.Message
	Account  *refdata.Account
	ClientIP string
	// QuotaKeys are the period-stamped usage keys the quota stage resolved for
	// this message (company + country scopes, daily + monthly) — incremented
	// by the enqueue stage ONLY on a successful durable enqueue, so rejects
	// and idempotent replays never consume quota. [chunk-6 ruling 6.1]
	QuotaKeys []string
	// DestCapKeys are the period-stamped per-destination frequency-cap keys
	// the destcap stage resolved (hourly and/or daily). Same accept-only rule
	// as QuotaKeys, but counted in MESSAGES rather than segments: a 5-segment
	// message is still one message to that subscriber. [SCOPE P1-13D]
	DestCapKeys []string
	// TemplateMatched is the template stage's verdict: did the body match an
	// approved template? Set whether or not the account's own template gate is
	// on, so a spam rule with action='require_template' can ask. Meaningless
	// unless the template stage actually evaluated — see NeedsTemplateVerdict.
	TemplateMatched bool
	// SpamFlags collects rules with action='flag' that matched. The message is
	// still sent; these ride the queue row onto the record so an operator can
	// see what a rule WOULD have refused, before switching it to reject.
	SpamFlags []string
	// DedupKey is the duplicate-check key the dedup stage created for THIS
	// message ("" on a duplicate or with the check off): the enqueue stage
	// releases it when the message is never queued.
	DedupKey string
	// PlainBody is the message text as submitted, set by the enqueue stage
	// only when it replaces Msg.Body with ciphertext. Flow captures read it,
	// so they show the text and not ciphertext (PO 2026-09-13).
	PlainBody string
	// Charged is what the enqueue stage took from the balance at acceptance
	// (company currency; 0 = nothing taken), and BalanceAfter the company's
	// balance right after it, when the reserver can say (nil otherwise). The
	// send reply reports these rather than re-pricing against the reference
	// cache's old copy of the balance (QA API-004, BILL-005).
	Charged      float64
	BalanceAfter *float64
	// Notes are things a stage noticed and let pass — a check it could not
	// make, a vendor left out of a comparison. "Will this send?" shows them
	// on the stage that added them; real traffic logs them (rarely).
	Notes []string
}

// captureText is the message text a flow capture shows: the plain text, even
// once the enqueue stage has sealed the stored copy.
func captureText(e *Envelope) string {
	if e.PlainBody != "" {
		return e.PlainBody
	}
	return e.Msg.Body
}

// Stage is one composable step. Returning a *Reject stops the pipeline.
type Stage interface {
	Name() string
	Run(ctx context.Context, e *Envelope) *Reject
}

// Pipeline runs stages in order until one rejects.
type Pipeline struct{ stages []Stage }

// New builds a pipeline from an ordered list of stages.
func New(stages ...Stage) *Pipeline { return &Pipeline{stages: stages} }

// captureSnapshot is the part of a message a flow capture cares about.
//
// Taken before and after every stage so the record can say what that stage
// actually DID — "route: pass" is not an answer, "route chose rule 1268
// 'jazz' on vendor sample_jazz" is. Only fields a stage can plausibly change
// are here; the identity fields ride on every record separately.
func captureSnapshot(m *model.Message) map[string]any {
	return map[string]any{
		"from":       m.Sender,
		"to":         m.Destination,
		"text":       m.Body,
		"encoding":   m.Encoding,
		"segments":   m.Segments,
		"priority":   m.Priority,
		"templateId": m.TemplateID,
		"routeId":    m.RouteID,
		"vendor":     m.Vendor,
		"transport":  string(m.Transport),
		"mcc":        m.MCC,
		"mnc":        m.MNC,
		"hlrSource":  m.HLRSource,
		"status":     string(m.Status),
	}
}

// captureChanges reports what a stage altered, as "field: before -> after".
func captureChanges(before, after map[string]any) map[string]any {
	var out map[string]any
	for k, av := range after {
		bv := before[k]
		if fmt.Sprint(bv) == fmt.Sprint(av) {
			continue
		}
		if out == nil {
			out = map[string]any{}
		}
		out[k] = fmt.Sprintf("%v -> %v", bv, av)
	}
	return out
}

// Run executes each stage; the first *Reject wins.
//
// FLOW CAPTURE (Process 13 Type 3). When a capture is running for this
// message's account, the pipeline records what HAPPENED to the message — not
// a line per stage. Eighteen "pass" rows per message is noise: an operator
// reading a capture wants the message, the outcome, and — when it was
// refused — everything about the refusal.
//
// So one message produces at most three records here:
//
//	submit    what arrived, in full
//	route_chosen  which rule matched, by name (only when it routed)
//	accepted / rejected   the outcome, carrying the whole message state,
//	          every stage's timing, and every field the pipeline changed
//
// The per-stage detail is not lost — it is folded into the outcome record as
// `stages` and `changed`, where it can be read at a glance instead of
// scrolled through.
//
// Cost when nothing is being captured is a single atomic load
// (logging.Capturing's fast path), so this stays off the hot path's budget.
func (p *Pipeline) Run(ctx context.Context, e *Envelope) *Reject {
	var (
		lg  = logging.New("pipeline")
		sc  logging.Scope
		cap bool
		// Accumulated across the run and reported once, on the outcome.
		timings = map[string]any{}
		changed = map[string]any{}
	)
	if e.Account != nil {
		sc = logging.AccountScope(e.Account.Username)
		cap = logging.Capturing(sc)
	}
	if cap {
		logging.CaptureRecord(lg, sc, "submit", "message received", submitFields(e))
	}
	for _, s := range p.stages {
		var before map[string]any
		if cap {
			before = captureSnapshot(e.Msg)
		}
		start := time.Now()
		r := s.Run(ctx, e)
		if cap {
			timings[s.Name()] = time.Since(start).Microseconds()
			after := captureSnapshot(e.Msg)
			after["text"] = captureText(e) // plain, not the sealed copy
			for k, v := range captureChanges(before, after) {
				changed[k] = v
			}
		}
		if r != nil {
			if cap {
				// The refusal, with everything an operator needs to explain
				// it without opening anything else: which stage, which code,
				// which reason, the message as it stood at that moment, what
				// had already been decided, and where the time went.
				f := outcomeFields(e, timings, changed)
				f["stage"] = s.Name()
				f["code"] = r.Code
				f["reason"] = r.Reason
				f["outcome"] = "rejected"
				logging.CaptureRecord(lg, sc, "rejected",
					"refused at "+s.Name()+": "+r.Reason, f)
			}
			return r
		}
	}
	if cap {
		f := outcomeFields(e, timings, changed)
		f["outcome"] = "accepted"
		logging.CaptureRecord(lg, sc, "accepted", "accepted and queued", f)
	}
	return nil
}

// submitFields is everything known about a message at the door.
func submitFields(e *Envelope) logging.Fields {
	f := logging.Fields{
		"requestId":      e.Msg.RequestID,
		"messageId":      e.Msg.ID,
		"account":        e.Account.Username,
		"accountId":      e.Account.ID,
		"companyId":      e.Account.CompanyID,
		"from":           e.Msg.Sender,
		"to":             e.Msg.Destination,
		"text":           e.Msg.Body,
		"textLength":     len([]rune(e.Msg.Body)),
		"clientIp":       e.ClientIP,
		"ingress":        string(e.Msg.Ingress),
		"priority":       e.Msg.Priority,
		"encodingAsked":  e.Msg.Encoding,
		"idempotencyKey": e.Msg.IdempotencyKey,
	}
	for k, v := range map[string]string{
		"ap1": e.Msg.AP1, "ap2": e.Msg.AP2, "apg1": e.Msg.APG1, "apg2": e.Msg.APG2,
		"channel": e.Msg.Channel,
	} {
		if v != "" {
			f[k] = v
		}
	}
	return f
}

// outcomeFields is the message's whole state at the end of its run, plus how
// it got there. Shared by the accepted and rejected records so the two read
// the same way.
func outcomeFields(e *Envelope, timings, changed map[string]any) logging.Fields {
	f := logging.Fields{
		"messageId":  e.Msg.ID,
		"requestId":  e.Msg.RequestID,
		"account":    e.Account.Username,
		"accountId":  e.Account.ID,
		"from":       e.Msg.Sender,
		"to":         e.Msg.Destination,
		"text":       captureText(e),
		"encoding":   e.Msg.Encoding,
		"segments":   e.Msg.Segments,
		"priority":   e.Msg.Priority,
		"routeId":    e.Msg.RouteID,
		"vendor":     e.Msg.Vendor,
		"transport":  string(e.Msg.Transport),
		"templateId": e.Msg.TemplateID,
		"mcc":        e.Msg.MCC,
		"mnc":        e.Msg.MNC,
		"hlrSource":  e.Msg.HLRSource,
		"senderBy":   e.Msg.SenderBy, // "" = the client's own sender (2026-09-29)
		"clientIp":   e.ClientIP,
		// Every stage's own time, so a slow one is visible without a line
		// each. Microseconds.
		"stages": timings,
	}
	if len(changed) > 0 {
		f["changed"] = changed
	}
	if e.DedupKey != "" {
		f["dedupeKey"] = e.DedupKey
	}
	return f
}

// SubmitDeps are the services the canonical submit pipeline needs. Nil-able
// fields (HLR, Dedupe) degrade gracefully, matching each stage's contract.
type SubmitDeps struct {
	Ref *refdata.Mem
	HLR *hlr.Service
	// HLRDip is the optional HLR live-dip subsystem (P1 scope finalize
	// 2026-08-09). nil = cache-only (no live dip), the pre-2026-08-09 behaviour.
	HLRDip *hlr.Dipper
	// HLRLive runs live lookups with each account's own providers
	// (2026-09-14); when set it replaces HLRDip.
	HLRLive *hlr.Live
	// HLRRanges is the operators-table number-range index behind the
	// "range" and "country" steps of an account's HLR order. nil = those
	// steps find nothing.
	HLRRanges *hlr.Ranges
	Rate      RateAllower // per-account per-second cap (in-memory single-node, or ratelimit.Shared under the gate)
	Dedupe    Deduper
	Queue     QueueWriter
	// Accepted writes an accepted message's record (mt_) at acceptance — the
	// three-table design (Saleem and the PO, 27 Oct 2026): the record store,
	// or the DB worker's queue (with it, in ONE Redis step with Queue when
	// the queues are in Redis). nil = not recorded at acceptance (a test, a
	// role with no record store); the vendor role's first answer makes it.
	Accepted AcceptRecorder
	Crypto   Encryptor // optional: encrypt-at-rest on the enqueue write (nil = plaintext)
	// SecLog receives the pipeline's SECURITY refusals (Process 13 Type 2):
	// the IP allowlist violation — a caller with good credentials arriving
	// from an address the account never approved — and a TRAP-number hit,
	// where an account sent to a planted number nobody owns. nil = not
	// reported, which is what every test and any un-wired process gets.
	SecLog func(seclog.Event)
	// VendorHealth says whether a vendor has had no working connection for
	// its "down after" time (vendorhealth.View, PO 2026-09-14). nil = only
	// the operator's paused/disabled switch takes a vendor out of routing.
	VendorHealth VendorHealth
	// QualityMinSample is how many judged messages a vendor needs in the
	// quality window before route_type='quality' will hold its delivery ratio
	// against it. Below it the vendor is treated as good, so a new vendor
	// gets traffic and can earn a real score (PO ruling 2026-09-04).
	QualityMinSample int64
	Reserve          Reserver // optional: synchronous balance reservation (nil = disabled, cheap credit-stage check only)
	// Pricer gives the exact sell charge of a message at accept (PO
	// 2026-09-17: reserve the exact amount, not a per-segment estimate).
	// With it, a prepaid account charged at acceptance has that amount taken
	// and ledgered before the client is acked; nil falls back to
	// ReservePricePerSegment.
	Pricer Pricer
	// GlobalTPS is the GATEWAY-WIDE segments/sec ceiling (chunk-1 ruling 1.5,
	// PO 2026-07-31: "implement global limit of tps"), checked before any
	// per-company/account/country bucket. 0 = disabled. Under the Redis gate
	// the limiter is cluster-shared, so this is ONE ceiling across every
	// instance; in-memory mode it is per-instance. [SCOPE P1-03G]
	GlobalTPS int
	// ReservePricePerSegment prices a Reserve() call: segments * this. <= 0
	// disables reservation entirely (even with Reserve set), matching every
	// other optional-feature-off-by-default flag in this codebase (e.g.
	// YAKU_CRYPTO_AT_REST). [2026-07-23]
	ReservePricePerSegment float64
	// DedupeDefaultTTL is the fallback dedupe/idempotency window (YAKU_DEDUPE_TTL_SEC)
	// used when an account has no dedupe_ttl_sec override. [SCOPE P1-DEDUPE-TTL, PO 2026-07-27]
	DedupeDefaultTTL time.Duration
	// Usage is the quota-usage counter (chunk-6 ruling 6.1). nil = quotas fall
	// back to the DB-base-only check (the pre-6.1 behaviour, which could never
	// trip on live traffic).
	Usage UsageCounter
	// DestCapUsage counts the per-destination frequency cap (config/0096).
	// SEPARATE from Usage rather than sharing it: its keys are one per
	// (account, destination, period), so there are orders of magnitude more of
	// them and they must expire soon after their period rather than live the
	// 45 days a monthly quota key needs. nil = the cap cannot be enforced and
	// the stage skips. [SCOPE P1-13D]
	DestCapUsage UsageCounter
	// Campaigns is the live campaign window (campaignwin.Window, Redis).
	// nil = the signature is still taken and kept on the record, just not
	// counted live.
	Campaigns CampaignNoter
	// AbuseCounts is what the abuse monitor reads in place of the message
	// tables (abusecount.Counter, Redis; 2026-10-13). nil = nothing is
	// counted and the monitor reads the records, as before.
	AbuseCounts AbuseNoter
	// FX resolves live currency-pair rates for loss detection (S-03, PO
	// 2026-09-03) — the SAME resolver P9's async billing already trusts, not
	// a duplicate. nil (or a nil *billing.FXResolver) falls back to each rate
	// row's own pinned conversion_rate, its documented "always safe" contract.
	// The PO chose live-FX-in-the-hot-path explicitly over the safer
	// pinned-only default; be aware a cache-miss dip can take up to
	// FXResolver's own HTTP timeout before falling back.
	FX *billing.FXResolver
}

// Encryptor seals field values for storage. Declared as an interface here so the
// pipeline stays independent of the crypto implementation; internal/crypto's
// Service satisfies it. A nil Encryptor means "store plaintext". companyID
// selects a tenant-specific KEK when the account's company is onboarded onto
// tenant_kms_config (0 = unlinked account, always the gateway-wide default
// KEK). [SCOPE P1-CRYPTO, P1-KMS-MULTITENANT]
type Encryptor interface {
	// ShouldEncrypt reports whether a field is sealed for EVERYONE (the
	// gateway-wide switch). A single company is encrypted by its own privacy
	// setting (refdata.Account.BodyPrivacy = all_encrypted, PO 2026-09-13),
	// which the enqueue stage reads itself.
	ShouldEncrypt(ctx context.Context, field string, companyID int64) bool
	Encrypt(ctx context.Context, accountID, companyID int64, plaintext string) (string, error)
}

// Reserver atomically holds funds against a prepaid balance BEFORE a message
// is durably enqueued, closing the race a live load test exposed 2026-07-23:
// the cheap credit-stage check below reads a periodically-refreshed in-memory
// cache that nothing decrements in real time, so a concurrent burst can drive
// an account far past zero before async billing (which settles on its own
// interval, against records that must first reach a terminal delivery state)
// ever reacts. internal/billing's Reserve/Refund satisfy this; declared here
// (not imported directly) so the pipeline package — shared by every ingress
// edge — stays independent of the billing implementation, same reasoning as
// Encryptor above. A nil Reserver just means the feature is off; the existing
// cheap credit-stage check still runs regardless.
//
// Deliberately minimal (PO decision 2026-07-23): no ledger, no idempotency
// guard — just deduct-if-affordable, one round trip. companyID (0 = unlinked
// account) is passed by the caller, already known from the cached Account,
// so the implementation never needs its own lookup to decide which row to
// lock.
type Reserver interface {
	// Reserve returns ok=false (not an error) when the balance can't cover
	// amount — that's the normal "account is broke" outcome, not a fault.
	Reserve(ctx context.Context, accountID, companyID int64, amount float64) (ok bool, err error)
	// Refund reverses a Reserve() for a message that was held for but then
	// failed a LATER pipeline step and was never actually enqueued.
	Refund(ctx context.Context, accountID, companyID int64, amount float64) error
}

// NewSubmit builds the canonical 18-stage accept pipeline [SCOPE P1-PIPE
// P1-01..P1-17 + P1-05W whitelist]. Every ingress edge (HTTP, SMPP) MUST build
// its pipeline here — never assemble submit stages by hand inside a process.
func NewSubmit(d SubmitDeps) *Pipeline { return New(submitStages(d, nil)...) }

// submitStages is the one list of stages, in the one order. NewSubmit uses it
// as is; NewDryRun passes dry-run options that swap the stages with side
// effects for placeholders IN PLACE — so the check can never run the stages in
// a different order from real traffic. dry is nil for real traffic.
func submitStages(d SubmitDeps, dry *DryRunOptions) []Stage {
	if dry != nil {
		// No security events from a check. A dry run to a trap number must
		// not raise the trap alarm, and an operator trying an address against
		// an allow list is not an intruder. (d is a copy; the caller's deps
		// are untouched.)
		d.SecLog = nil
	}
	hs := newHLRStage(d, dry == nil || dry.LiveHLR)
	var (
		ipAllow   Stage = ipAllowStage{seclog: d.SecLog}
		rateLimit Stage = rateLimitStage{rl: d.Rate, ref: d.Ref, globalTPS: d.GlobalTPS, depID: deployment.ID()}
		dedup     Stage = dedupStage{d: d.Dedupe, defaultTTL: d.DedupeDefaultTTL}
		enqueue   Stage = enqueueStage{q: d.Queue, crypto: d.Crypto, reserve: d.Reserve, reservePrice: d.ReservePricePerSegment, pricer: d.Pricer,
			usage: d.Usage, destCapUsage: d.DestCapUsage, dedup: d.Dedupe, campaigns: d.Campaigns, abuse: d.AbuseCounts,
			accepted: d.Accepted}
		route = routeStage{ref: d.Ref, rr: newRR(), fx: d.FX, qualityMinSample: d.QualityMinSample, health: d.VendorHealth,
			lossAcceptNoRate: lossAcceptsNoRate(), baseCur: BaseCurrency()}
	)
	if dry != nil {
		// A check spends nothing and records nothing: no rate-limit tokens, no
		// dedupe claim, no queue write, no balance hold, no monitoring count.
		rateLimit = notChecked{name: "rate-limit", why: NotCheckedRateLimit}
		dedup = notChecked{name: "dedup", why: NotCheckedDedup}
		enqueue = notChecked{name: "enqueue", why: NotCheckedEnqueue}
		route.dryRun = true
		if dry.SkipIPAllow {
			ipAllow = notChecked{name: "ip-allow", why: NotCheckedIPAllow}
		}
	}
	// Rate-limit sits AFTER encoding (PO 2026-07-26): its unit is
	// segments/second, and the real segment count only exists once
	// auto-replace has rewritten the body and encoding has planned it.
	return []Stage{
		validateStage{},                          //  1 [P1-01] (+ picks the sender: client → account default)
		ipAllow,                                  //  2 [P1-02]
		deploymentStage{id: deployment.ID()},     // 2b [P1-02D] is this account served by THIS gateway (config/0122)
		quietHoursStage{},                        //  3 [P1-04]
		blockStage{ref: d.Ref, seclog: d.SecLog}, //  4 [P1-05] DND + trap numbers (config/0097)
		whitelistStage{},                         //  5 [P1-05W]
		countryStage{ref: d.Ref},                 //  6 [P1-06]
		// Auto-replace runs BEFORE the template gate (chunk-5 ruling 5.4,
		// 2026-08-03, swapping the original 7/8 order): the template check
		// certifies the text that will actually be SENT, so it must see the
		// post-replacement body — spam (stage 11) already did. The old order
		// validated a body that auto-replace then mutated.
		autoReplaceStage{ref: d.Ref}, //  7 [P1-08]
		templateStage{ref: d.Ref},    //  8 [P1-07]
		encodingStage{},              //  9 [P1-09]
		rateLimit,                    // 10 [P1-03] gateway-global + company-shared + per-country, in segments
		spamStage{ref: d.Ref},        // 11 [P1-10]
		senderIDStage{},              // 12 [P1-11]
		creditStage{fresh: newFreshBalance(d.Reserve)}, // 13 [P1-12] (a refusal looks at the balance as it stands)
		// Quota runs twice (PO 2026-09-21: quotas count the vendor's parts):
		// a one-part precheck here so a customer at the limit costs no HLR
		// dip, and the real check after the route, with the vendor's count.
		quotaStage{ref: d.Ref, usage: d.Usage, precheck: true}, // 14 [P1-13] company + country, base+live (chunk-6 rulings 6.1/6.2)
		// Before HLR on purpose: a message the per-destination cap refuses
		// should not first pay for a live HLR dip.
		destCapStage{usage: d.DestCapUsage},    // 15 [P1-13D] same number, same account, fixed window
		hs,                                     // 16 [P1-14] cache + optional live dip
		route,                                  // 17 [P1-15]
		quotaStage{ref: d.Ref, usage: d.Usage}, // 17b [P1-13] the real quota check, in the vendor's parts
		dedup,                                  // 18 [P1-16]
		enqueue,                                // 19 [P1-17]
	}
}

// Pricer prices one message the way the billing role will: the sell charge
// in the company's balance currency, 0 when the route is unpriced.
type Pricer interface {
	Price(ctx context.Context, e *Envelope) float64
}

// Quoter is a Pricer that can fix the whole price of a message at acceptance
// (billing.Quote, 2026-10-13): the enqueue stage puts it on the message, and
// the billing role charges by it. ok=false = it cannot say (nothing to price
// with): the message carries no quote and is priced by the billing role, by
// the rates of its accept time.
type Quoter interface {
	Quote(ctx context.Context, e *Envelope) (billing.Quote, bool)
}

// LedgerReserver is a Reserver that also writes the charge into the ledger
// under the message's id, so the reservation IS the charge (PO 2026-09-17):
// the billing role later finds the reference and takes nothing twice
// (S9-02 double deduction closed). A plain Reserver keeps the old balance
// hold without a ledger row.
type LedgerReserver interface {
	Reserver
	ReserveFor(ctx context.Context, accountID, companyID int64, amount float64, msgID string) (ok bool, err error)
	RefundFor(ctx context.Context, accountID, companyID int64, amount float64, msgID string) error
}

// EventBooker is the reserver of the billing queue (issues 3 and 18; Saleem,
// 26 Oct 2026): a message's acceptance money is booked as events, moved and
// queued in one step (billing.Booker). The accept path uses it whenever the
// reserver has it and the message is priced; the hold alone (Reserver) is
// left for an account with no company and a pricer with no quote.
type EventBooker interface {
	// TakeEvents holds what the events take from a prepaid balance and
	// queues them, refused (ok=false) when the balance does not cover it.
	TakeEvents(ctx context.Context, companyID int64, evs []billing.Event) (ok bool, after *float64, err error)
	// BookEvents queues events that move no balance (postpaid).
	BookEvents(ctx context.Context, companyID int64, evs []billing.Event) error
	// GiveBackEvents books their opposite: the money of a message that
	// never reached the queue.
	GiveBackEvents(ctx context.Context, companyID int64, evs []billing.Event) error
}

// BalanceReserver is a LedgerReserver that also says the company's balance
// right after the hold (nil = not known) — what the send reply shows as
// remainingBalance. Both billing reservers implement it.
type BalanceReserver interface {
	LedgerReserver
	ReserveForBalance(ctx context.Context, accountID, companyID int64, amount float64, msgID string) (ok bool, after *float64, err error)
}
