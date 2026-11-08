// Package refdata holds the in-memory reference data the Submit API reads on the
// hot path (accounts, routes, HLR, spam, templates, auto-replace). A Loader
// populates it from the backing store; Refresh reloads periodically.
// Aligned to database-structure.docx v2.0 (2026-07-13).
package refdata

import (
	"context"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/msisdn"
	"github.com/yakugate/yakugate/internal/platform/dbpool"
	"github.com/yakugate/yakugate/internal/vendorquality"
)

// Loader fills the cache from the backing store.
type Loader interface {
	LoadAccounts(ctx context.Context) ([]Account, error)
	LoadRoutes(ctx context.Context) ([]Route, error)
	LoadAutoReplace(ctx context.Context) (map[int64][]AutoReplaceRule, error)
	LoadSpam(ctx context.Context) (SpamLists, error)
	LoadTemplates(ctx context.Context) (map[int64][]TemplateRegex, error)
	LoadBlocklist(ctx context.Context) ([]Block, error)
	LoadCountryRules(ctx context.Context) (map[int64]map[string]CountryRule, error)
	LoadRevocations(ctx context.Context) (map[string]bool, error)
	LoadRates(ctx context.Context) (map[int64][]RateRow, error) // buying rates by rate_card_id (LCR)
}

// Mem is a concurrency-safe in-memory reference cache. [SCOPE P1-CACHE]
type Mem struct {
	mu              sync.RWMutex
	byUsername      map[string]*Account
	byID            map[int64]*Account
	bySMPPSysID     map[string]*Account
	byCustomPath    map[string]*Account
	byTokenHash     map[string]*Account // key: hex HMAC-SHA256 digest, see Account.TokenHash
	tokenCandidates []TokenCandidate
	subClaimNames   []string // distinct jwt sub_claim names in use; always starts with "sub"
	routes          []Route
	// Route index (chunk-7 ruling 7.2): the route stage used to scan EVERY
	// tenant's routes per message; now it walks the account's own list plus
	// the global list. Pointers into the routes slice above.
	routesByAcct map[int64][]*Route
	routesGlobal []*Route
	autoReplace  map[int64][]AutoReplaceRule
	spam         SpamLists
	// Preprocessed spam material (chunk-5 ruling 5.3): lowered/normalized
	// keywords and per-company allowed SETS, built once per refresh so the
	// spam stage never lowers/normalizes/scans linearly per message.
	spamUniPrep []SpamMatcher
	spamCoPrep  map[int64][]SpamMatcher
	spamCoAllow map[int64][]SpamMatcher
	templates   map[int64][]Template
	// blockGlobal/blockAcct map a block key to its EXPIRY (P1 scope finalize
	// 2026-08-09); the zero time = permanent. Presence is checked with the
	// two-value map form since a permanent entry stores the zero value, which
	// is indistinguishable from "absent" by value alone.
	blockGlobal map[string]time.Time
	// Precomputed at refresh so the template stage can ask "does anyone in
	// scope actually need me to form a verdict?" without walking rule lists
	// on the hot path. See NeedsTemplateVerdict.
	spamNeedsTplUni bool
	spamNeedsTplCo  map[int64]bool
	// blockTrap holds the TRAP numbers only (blocklist source='honeypot'),
	// keyed by cleaned msisdn with no scope suffix — a trap is global by
	// intent, and IsTrap only ever runs after Blocked already said yes, so it
	// need not repeat the three-tier scope walk.
	blockTrap     map[string]time.Time
	blockAcct     map[string]time.Time // key "msisdn|accountID"
	blockCo       map[string]time.Time // key "msisdn|c:companyID" (GW-18 company tier)
	countryRules  map[int64]map[string]CountryRule
	revoked       map[string]bool
	rates         map[int64][]RateRow   // buying rates by rate_card_id (LCR)
	queueDepths   map[string]QueueDepth // QueueDepthKey(vendor cid) -> live depth, for route_type='capacity' [SCOPE P1-15]
	queueDepthSrc QueueDepthSource      // where the queues really are (SetQueueDepthSource); nil = the Loader
	// quality is each vendor's recent record per network, for
	// route_type='quality' [SCOPE P1-15]; qualitySrc/qualityWindow say where
	// it comes from and how far back it looks.
	quality       map[vendorquality.Key]VendorQuality
	qualitySrc    QualitySource
	qualityWindow time.Duration
	loader        Loader
	ready         bool
	refreshedAt   time.Time // wall time of the last successful Refresh, for /metrics cache age
	// lastSig is the config store's fingerprint at the last reload (PO
	// 2026-09-17): a timer refresh that finds it unchanged reloads nothing.
	lastSig string
	// refreshMu lets one reload run at a time. The 30 s look (Refresh) and
	// the Admin API's nudge (RefreshForce) used to overlap: the slower one
	// could swap in data read BEFORE a change and then store a fingerprint
	// taken AFTER it, and the change was never loaded (QA SMPP-004).
	refreshMu sync.Mutex
	// onReload runs after every successful reload (onreload.go).
	onReload []func(context.Context)
}

// New builds an empty cache bound to a loader.
func New(l Loader) *Mem {
	return &Mem{
		byUsername:   map[string]*Account{},
		byID:         map[int64]*Account{},
		bySMPPSysID:  map[string]*Account{},
		byCustomPath: map[string]*Account{},
		byTokenHash:  map[string]*Account{},
		autoReplace:  map[int64][]AutoReplaceRule{},
		templates:    map[int64][]Template{},
		blockGlobal:  map[string]time.Time{},
		blockTrap:    map[string]time.Time{},
		blockAcct:    map[string]time.Time{},
		blockCo:      map[string]time.Time{},
		countryRules: map[int64]map[string]CountryRule{},
		revoked:      map[string]bool{},
		rates:        map[int64][]RateRow{},
		queueDepths:  map[string]QueueDepth{},
		quality:      map[vendorquality.Key]VendorQuality{},
		loader:       l,
	}
}

// ChangeSigner is optional on a Loader: a cheap fingerprint of the config
// store (counts and last-modified stamps), so the 30-second refresh reloads
// only when something changed (PO 2026-09-17).
type ChangeSigner interface {
	ChangeSignature(ctx context.Context) (string, error)
}

// Refresh reloads the reference data when the config store changed since
// the last reload, or always when the loader cannot say. An unchanged store
// still counts as a fresh cache for /metrics.
func (m *Mem) Refresh(ctx context.Context) error {
	if m.loader == nil {
		return nil
	}
	m.refreshMu.Lock()
	defer m.refreshMu.Unlock()
	if cs, ok := m.loader.(ChangeSigner); ok {
		sig, err := cs.ChangeSignature(ctx)
		if err != nil {
			return err
		}
		m.mu.RLock()
		same := sig != "" && sig == m.lastSig
		m.mu.RUnlock()
		if same {
			m.mu.Lock()
			m.refreshedAt = time.Now()
			m.mu.Unlock()
			return nil
		}
		if err := m.reload(ctx); err != nil {
			return err
		}
		m.mu.Lock()
		m.lastSig = sig
		m.mu.Unlock()
		return nil
	}
	return m.reload(ctx)
}

// RefreshForce reloads whatever the fingerprint says: the Admin API's nudge
// after a write, and a role's start.
func (m *Mem) RefreshForce(ctx context.Context) error {
	if m.loader == nil {
		return nil
	}
	m.refreshMu.Lock()
	defer m.refreshMu.Unlock()
	// Forced means "something was just written — load it now": with a read
	// instance beside the write instance, this load reads the write one
	// (dbpool.Fresh; the MySQL loader's cfgRead).
	ctx = dbpool.Fresh(ctx)
	// The fingerprint is taken BEFORE the reload, as Refresh does: taken
	// after, a row written while the reload ran (a vendor's block, say)
	// would be in the fingerprint but not in the cache, and no later tick
	// would see a change to load it for.
	sig := ""
	if cs, ok := m.loader.(ChangeSigner); ok {
		sig, _ = cs.ChangeSignature(ctx)
	}
	if err := m.reload(ctx); err != nil {
		return err
	}
	if sig != "" {
		m.mu.Lock()
		m.lastSig = sig
		m.mu.Unlock()
	}
	return nil
}

// reloadData reads everything from the loader and swaps it in (reload, in
// onreload.go, runs it and then the OnReload followers).
func (m *Mem) reloadData(ctx context.Context) error {
	accts, err := m.loader.LoadAccounts(ctx)
	if err != nil {
		return err
	}
	routes, err := m.loader.LoadRoutes(ctx)
	if err != nil {
		return err
	}
	ar, err := m.loader.LoadAutoReplace(ctx)
	if err != nil {
		return err
	}
	spam, err := m.loader.LoadSpam(ctx)
	if err != nil {
		return err
	}
	tplRaw, err := m.loader.LoadTemplates(ctx)
	if err != nil {
		return err
	}
	blocks, err := m.loader.LoadBlocklist(ctx)
	if err != nil {
		return err
	}
	countries, err := m.loader.LoadCountryRules(ctx)
	if err != nil {
		return err
	}
	revoked, err := m.loader.LoadRevocations(ctx)
	if err != nil {
		return err
	}
	rates, err := m.loader.LoadRates(ctx)
	if err != nil {
		return err
	}
	// Queue depth is OPTIONAL (route_type='capacity', 2026-07-28 routes
	// review) — a Loader that doesn't implement QueueDepthLoader just means
	// capacity routing has no depth data to pick by; never fails Refresh.
	queueDepths, _ := m.loadQueueDepths(ctx)
	// Delivery quality comes from its own source (vendorquality counts) on
	// its own timer — see RefreshQuality. Loaded here too so a fresh cache
	// starts with it.
	m.RefreshQuality(ctx)
	tpl := CompileTemplates(tplRaw)
	// Country-code prefixes are compared against msisdn.Clean'd destinations,
	// so a rule stored as "+92" or "92 " could never match — the same silent
	// no-match class as blocklist/whitelist rows (chunk-5 ruling 5.7,
	// 2026-08-04). Normalized here in Refresh so every Loader (MySQL, Redis
	// snapshot) gets the same treatment.
	//
	// Into a NEW map, never back into the one the loader returned: the Redis
	// loader hands every Refresh the same decoded snapshot, so writing into it
	// let two refreshes at once (a /admin/cache/refresh during the background
	// watch) write one map together — "concurrent map writes" killed
	// http-server on 2026-09-14.
	cleanedCountries := make(map[int64]map[string]CountryRule, len(countries))
	for acct, rules := range countries {
		cleaned := make(map[string]CountryRule, len(rules))
		for cc, r := range rules {
			// The "" key is the account's DEFAULT rule (NULL prefix, P1 scope
			// finalize 2026-08-09) — a catch-all that must stay "", never be
			// normalized (msisdn.Clean("") is "" anyway, but skip explicitly so
			// intent is clear and no spurious "normalized" log fires).
			if cc == "" {
				cleaned[""] = r
				continue
			}
			key := msisdn.Clean(cc)
			if key != cc {
				log.Printf("[refdata] country rule %q (account %d) normalized to %q", cc, acct, key)
			}
			if _, dup := cleaned[key]; dup {
				log.Printf("[refdata] duplicate country rule for prefix %q (account %d) — last row wins", key, acct)
			}
			cleaned[key] = r
		}
		cleanedCountries[acct] = cleaned
	}
	countries = cleanedCountries
	// Spam rule compilation (GW-17 regex remodel; chunk-5 5.3 still applies —
	// compile once per refresh, never per message).
	spamUniPrep := CompileSpamRules(spam.Universal)
	spamCoPrep := make(map[int64][]SpamMatcher, len(spam.CompanyBlocked))
	for id, list := range spam.CompanyBlocked {
		spamCoPrep[id] = CompileSpamRules(list)
	}
	spamCoAllow := make(map[int64][]SpamMatcher, len(spam.CompanyAllowed))
	for id, list := range spam.CompanyAllowed {
		spamCoAllow[id] = CompileSpamRules(list)
	}
	bg := map[string]time.Time{}
	ba := map[string]time.Time{}
	bc := map[string]time.Time{} // company tier (GW-18)
	bt := map[string]time.Time{} // trap numbers (config/0097)
	now := time.Now()
	for _, blk := range blocks {
		// Drop already-expired entries at load (P1 scope 2026-08-09): a lapsed
		// block need not sit in the map; Blocked() re-checks expiry at query
		// time too, so an entry that expires mid-refresh-cycle also passes.
		if !blk.ExpiresAt.IsZero() && !blk.ExpiresAt.After(now) {
			continue
		}
		// The pipeline compares against msisdn.Clean'd destinations, so a
		// blocklist row stored as "+9230..." or "0092 30..." could NEVER match
		// — a silent DND bypass (chunk-5 ruling 5.5, 2026-08-03). Normalize at
		// load; sender masks stay verbatim (they're alphanumeric masks).
		key := msisdn.Clean(blk.Msisdn)
		if key == "" {
			continue
		}
		// Recorded BEFORE the sender mask is appended and without any scope
		// suffix: "is this number a trap" is a question about the number
		// itself, not about who was sending to it.
		if blk.Source == "honeypot" {
			bt[key] = blk.ExpiresAt
		}
		if blk.Sender != "" {
			key += "|s:" + blk.Sender
		}
		switch {
		case blk.AccountID != 0:
			ba[key+"|"+itoa(blk.AccountID)] = blk.ExpiresAt
		case blk.CompanyID != 0: // GW-18: one row blocks the whole company
			bc[key+"|c:"+itoa(blk.CompanyID)] = blk.ExpiresAt
		default:
			bg[key] = blk.ExpiresAt
		}
	}
	byU := make(map[string]*Account, len(accts))
	byIDx := make(map[int64]*Account, len(accts))
	bySys := map[string]*Account{}
	byPath := map[string]*Account{}
	byTok := map[string]*Account{}
	seenCandidate := map[TokenCandidate]bool{}
	var tokCandidates []TokenCandidate
	// Distinct jwt sub_claim names in use across all accounts, "sub" first —
	// internal/auth resolves a JWT's account by trying each name, and needs to
	// know when the set is just {"sub"} so the common case stays a single
	// struct-field read (chunk-2 ruling 2.3, 2026-08-03).
	seenSubClaim := map[string]bool{"sub": true}
	subClaims := []string{"sub"}
	for i := range accts {
		a := &accts[i]
		byU[a.Username] = a
		byIDx[a.ID] = a
		// SMPPSystemID is the username (see parseSMPPConfig), which carries a
		// UNIQUE key — so unlike byCustomPath below, this map CANNOT collide
		// and needs no guard. The GW-20 two-pass build this replaced existed
		// only to reconcile a free-text override against the username; with the
		// override gone there is one identity and one pass.
		if a.SMPPSystemID != "" {
			bySys[a.SMPPSystemID] = a
		}
		if a.CustomPath != "" {
			// The generated column's UNIQUE key is the real guard against a
			// collision reaching this cache; this last-write-wins is only a
			// defensive fallback (e.g. a stale replica) rather than the
			// enforcement point.
			byPath[a.CustomPath] = a
		}
		if a.TokenHash != "" {
			byTok[a.TokenHash] = a
		}
		if sc := a.JWT.SubClaim; sc != "" && !seenSubClaim[sc] {
			seenSubClaim[sc] = true
			subClaims = append(subClaims, sc)
		}
		// TokenCandidates is deduped across every token-scheme account (not
		// per-account), so two accounts sharing the same header name (e.g.
		// both use "X-Custom-Auth") don't produce a redundant lookup attempt
		// — the account itself is resolved via byTokenHash once a candidate
		// value is extracted, not by which account listed the candidate.
		if a.Dialect != nil && a.Dialect.Auth != nil && a.Dialect.Auth.Scheme == "token" {
			for _, src := range tokenSourcesFor(a.Dialect.Auth.TokenSource) {
				c := TokenCandidate{Source: src, Key: a.Dialect.Auth.TokenKey}
				if !seenCandidate[c] {
					seenCandidate[c] = true
					tokCandidates = append(tokCandidates, c)
				}
			}
		}
	}
	m.mu.Lock()
	m.byUsername, m.bySMPPSysID, m.byCustomPath = byU, bySys, byPath
	m.byID = byIDx
	m.byTokenHash, m.tokenCandidates = byTok, tokCandidates
	m.subClaimNames = subClaims
	routesByAcct := map[int64][]*Route{}
	var routesGlobal []*Route
	for i := range routes {
		if r := &routes[i]; r.AccountID == 0 {
			routesGlobal = append(routesGlobal, r)
		} else {
			routesByAcct[r.AccountID] = append(routesByAcct[r.AccountID], r)
		}
	}
	m.routes, m.autoReplace, m.spam = routes, ar, spam
	m.routesByAcct, m.routesGlobal = routesByAcct, routesGlobal
	m.spamUniPrep, m.spamCoPrep, m.spamCoAllow = spamUniPrep, spamCoPrep, spamCoAllow
	m.spamNeedsTplUni = anyRequiresTemplate(spamUniPrep)
	needsTpl := make(map[int64]bool, len(spamCoPrep))
	for id, list := range spamCoPrep {
		if anyRequiresTemplate(list) {
			needsTpl[id] = true
		}
	}
	m.spamNeedsTplCo = needsTpl
	m.templates, m.blockGlobal, m.blockAcct, m.blockCo, m.countryRules = tpl, bg, ba, bc, countries
	m.blockTrap = bt
	m.revoked = revoked
	m.rates = rates
	if queueDepths != nil || m.queueDepths == nil { // a failed depth read keeps the last depths
		m.queueDepths = queueDepths
	}
	m.ready = true
	m.refreshedAt = time.Now()
	m.mu.Unlock()
	return nil
}

// Age reports how long ago the cache last refreshed successfully; ok=false
// before the first load. Feeds the /metrics yaku_cache_age_seconds gauge (P1
// scope finalize 2026-08-09 §2 — a stale cache means config changes are not
// landing).
func (m *Mem) Age() (time.Duration, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.refreshedAt.IsZero() {
		return 0, false
	}
	return time.Since(m.refreshedAt), true
}

// Ready reports whether the cache has ever loaded successfully.
func (m *Mem) Ready() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ready
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

// StartRefresh reloads on an interval until ctx is cancelled.
func (m *Mem) StartRefresh(ctx context.Context, every time.Duration, logf func(string, ...any)) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := m.Refresh(ctx); err != nil && logf != nil {
				logf("reference cache refresh FAILED: %v", err)
			}
		}
	}
}

// Counts reports how much reference data is loaded (diagnostic).
func (m *Mem) Counts() (accounts, revocations, routes int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.byUsername), len(m.revoked), len(m.routes)
}
