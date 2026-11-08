package billing

// FX resolution (S9 currency round, PO 2026-08-10: "test … curency difference,
// exchange rates, exchange rate providers"). The ruled base model (D-083.6) is
// row-pinned FX: every versioned rate_card_rates row carries conversion_rate,
// and money converts card-currency -> company-currency by that pinned factor.
// This file adds the provider layer on top: when a rate row names a provider
// (thirdparty_crp_id), the CURRENT rate for the pair is fetched from that
// provider and used instead; the pinned conversion_rate remains the fallback
// whenever the provider is missing, inactive, down, or has no rate for the
// pair. The versioned row itself is never mutated — pinned FX stays
// reproducible forever.
//
// GW-02 (2026-08-27): the provider catalog is http_endpoints rows with
// type='crp' (cr_providers was DROPPED by config/0052; reading it silently
// disabled live FX). provider_type comes from the row's config JSON.
//
// Central rates store (PO 2026-08-27): fetched rates are published to the
// shared fx_rates table with a TTL (the provider row's cache_ttl_sec). Every
// instance consults the store first and fetches only when the stored pair is
// missing or expired — one fetch serves the whole fleet, survives restarts,
// and the Admin API reads the same rows for its "current rates" view. The
// in-process cache below is only a seconds-long hot-loop shield.
//
// Provider types (config JSON):
//
//	fixed  {"provider_type":"fixed", "rates": {"USD:PKR": 278.5}}
//	http   {"provider_type":"http",
//	        "url": "https://fx.example/rate?from={from}&to={to}",
//	        "extract": "\"rate\":\\s*([0-9.]+)"}   — one capture group
//	       (the GW-15 request_endpoint_id reference model hydrates here later)
//
// Lookup is by name first, then by numeric id (thirdparty_crp_id is a VARCHAR
// and deployments may store either).
//
// A provider is never waited for on the accept path (2026-10-07, QA billing
// report #1). A provider that accepted the connection and never answered held
// every submit for its timeout (15 s) — and, through the rater's lock, every
// submit of every OTHER account on the process while a rate-card reload
// waited. So:
//
//   - a resolver built with NoWait (http-server, smpp-server) answers from
//     what is known — the shared fx_rates store, or the rate it last had,
//     for a short grace — and asks the provider in the background;
//   - every call to a provider is bounded by YAKU_FX_TIMEOUT_MS (3 s),
//     whatever the endpoint row's own timeout says;
//   - a provider that failed is not asked again for failFor, and its last
//     rate is not used again until it answers: the row-pinned rate applies
//     (it used to be asked once per message);
//   - a fixed provider's rate is its own configuration, read with the
//     catalog: nothing to fetch, and an edit applies at the next catalog
//     reload (30 s) instead of when a cached copy runs out.
//
// Pair is a second question: the gateway's best rate for two currencies,
// whichever provider has it — for COMPARING prices (the loss check, cheapest
// route), never for a charge.

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/vendors/httpadapter"
)

// FXRate is one resolved conversion, with its provenance for the audit JSON.
type FXRate struct {
	Rate     float64 `json:"rate"`
	Provider string  `json:"provider,omitempty"` // "" = row-pinned conversion_rate
	From     string  `json:"from"`
	To       string  `json:"to"`
}

type crProvider struct {
	id       int64
	name     string
	ptype    string
	config   string
	cacheTTL time.Duration
	// rates are a fixed provider's own pairs ("USD:PKR" -> 278.5), read once
	// with the catalog.
	rates map[string]float64
}

type cachedRate struct {
	rate float64
	at   time.Time
}

// FXResolver resolves currency-pair rates through the cr_providers catalog.
// Safe for concurrent use. A nil *FXResolver always falls back to pinned rates.
type FXResolver struct {
	db dal.FXRates
	cl *http.Client
	// noWait: never call a provider on the caller's path (see NoWait).
	noWait bool
	// timeout bounds one call to a provider.
	timeout time.Duration

	mu        sync.Mutex
	providers map[string]crProvider // keyed by name AND by stringified id
	ordered   []crProvider          // each provider once, by id
	loadedAt  time.Time
	loading   bool
	cache     map[string]cachedRate // "<provider>|<from>:<to>": the rate last known
	// failed is when a provider last failed for a pair. It stays until a
	// fetch succeeds: a rate from before the failure is not served again.
	failed map[string]time.Time
	// inflight are the pairs being fetched in the background.
	inflight map[string]bool

	// The rate book behind Pair: every pair any provider has, read at most
	// every bookEvery. stale marks a pair whose stored rate has run out.
	book     map[string]bookRate
	bookAt   time.Time
	bookBusy bool
	asked    map[string]time.Time // pair -> when providers were last asked for it
}

type bookRate struct {
	rate  float64
	stale bool
}

const (
	// fxTimeoutDefault bounds one call to a rate provider. A rate API answers
	// in milliseconds; a vendor call's 15 s is no measure for it.
	fxTimeoutDefault = 3 * time.Second
	// failFor is how long a provider that failed is left alone.
	failFor = 30 * time.Second
	// staleFor is how long a rate that has run out may still be used while
	// its replacement is being fetched (NoWait) — never after a failure.
	staleFor = 10 * time.Minute
	// bookEvery is how often the rate book is read again; bookKeep how old a
	// stored rate may be and still serve a comparison; pairAskEvery how often
	// providers are asked for a pair nobody has (or whose rate has run out).
	bookEvery    = 30 * time.Second
	bookKeep     = 24 * time.Hour
	pairAskEvery = 5 * time.Minute
)

// NewFXResolver builds a resolver over the config store.
func NewFXResolver(cfgDB dal.FXRates) *FXResolver {
	timeout := fxTimeoutDefault
	if ms, err := strconv.Atoi(strings.TrimSpace(os.Getenv("YAKU_FX_TIMEOUT_MS"))); err == nil && ms > 0 {
		timeout = time.Duration(ms) * time.Millisecond
	}
	return &FXResolver{
		db:        cfgDB,
		cl:        &http.Client{Timeout: 10 * time.Second},
		timeout:   timeout,
		providers: map[string]crProvider{},
		cache:     map[string]cachedRate{},
		failed:    map[string]time.Time{},
		inflight:  map[string]bool{},
		asked:     map[string]time.Time{},
	}
}

// FixedPairs is a resolver with no store and no provider to call: it knows
// the pairs it is given ("EUR:USD" -> 1.08), under the provider name "fixed".
// For tests and tools that need a rate without a config store.
func FixedPairs(pairs map[string]float64) *FXResolver {
	f := NewFXResolver(nil)
	p := crProvider{id: 1, name: "fixed", ptype: "fixed", rates: pairs, cacheTTL: 5 * time.Minute}
	f.providers = map[string]crProvider{"fixed": p, "1": p}
	f.ordered = []crProvider{p}
	return f
}

// NoWait makes the resolver answer from what is already known and ask a
// provider only in the background — for the roles that accept messages,
// where a rate provider must never hold a client's reply. It returns the
// resolver (nil stays nil).
func (f *FXResolver) NoWait() *FXResolver {
	if f != nil {
		f.noWait = true
	}
	return f
}

// reload refreshes the provider catalog at most every 30s. One caller reads;
// the others carry on with the catalog they have.
func (f *FXResolver) reload(ctx context.Context) {
	if f.db == nil {
		return // FixedPairs: the catalog is what it was built with
	}
	f.mu.Lock()
	stale := time.Since(f.loadedAt) > 30*time.Second && !f.loading
	if stale {
		f.loading = true
	}
	f.mu.Unlock()
	if !stale {
		return
	}
	defer func() {
		f.mu.Lock()
		f.loading = false
		f.mu.Unlock()
	}()
	rows, err := f.db.FXProviders(ctx)
	if err != nil {
		// Never silent (the cr_providers regression hid for weeks): pricing
		// falls back to pinned conversion rates, and the operator should know.
		log.Printf("[billing-fx] provider reload: %v (live FX disabled; pinned conversion rates apply)", err)
		f.mu.Lock()
		f.loadedAt = time.Now() // don't hammer a broken config store
		f.mu.Unlock()
		return
	}
	next := map[string]crProvider{}
	var ordered []crProvider
	for _, row := range rows {
		p := crProvider{id: row.ID, name: row.Name, ptype: row.Type, config: row.Config}
		ttl := row.CacheTTLSec
		if ttl <= 0 {
			ttl = 300 // a 0 TTL would re-query the provider per message
		}
		p.cacheTTL = time.Duration(ttl) * time.Second
		if p.ptype == "fixed" {
			var cfg struct {
				Rates map[string]float64 `json:"rates"`
			}
			if json.Unmarshal([]byte(p.config), &cfg) == nil {
				p.rates = cfg.Rates
			}
		}
		next[p.name] = p
		next[strconv.FormatInt(p.id, 10)] = p
		ordered = append(ordered, p)
	}
	sort.Slice(ordered, func(a, b int) bool { return ordered[a].id < ordered[b].id })
	f.mu.Lock()
	f.providers, f.ordered, f.loadedAt = next, ordered, time.Now()
	f.mu.Unlock()
}

// fixedRate is a fixed provider's rate for a pair: its own entry, or the
// inverse of the opposite one (PKR->USD from a USD:PKR entry).
func fixedRate(p crProvider, from, to string) (float64, bool) {
	if r, ok := p.rates[from+":"+to]; ok && r > 0 {
		return r, true
	}
	if r, ok := p.rates[to+":"+from]; ok && r > 0 {
		return 1 / r, true
	}
	return 0, false
}

// Resolve returns the provider's current rate for from->to. ok=false means the
// caller should use the row-pinned conversion_rate instead.
func (f *FXResolver) Resolve(ctx context.Context, providerRef, from, to string) (FXRate, bool) {
	if f == nil || providerRef == "" || from == "" || to == "" {
		return FXRate{}, false
	}
	if strings.EqualFold(from, to) {
		return FXRate{Rate: 1, From: from, To: to}, false // same currency: pinned/1.0 path
	}
	f.reload(ctx)
	f.mu.Lock()
	p, ok := f.providers[providerRef]
	f.mu.Unlock()
	if !ok {
		return FXRate{}, false
	}
	key := p.name + "|" + from + ":" + to
	hit := func(r float64) (FXRate, bool) {
		return FXRate{Rate: r, Provider: p.name, From: from, To: to}, true
	}

	// A fixed provider's rate is its configuration: nothing to fetch.
	if p.ptype == "fixed" {
		r, ok := fixedRate(p, from, to)
		if !ok {
			return FXRate{}, false
		}
		f.publishFixed(p, key, from, to, r)
		return hit(r)
	}

	f.mu.Lock()
	c, cached := f.cache[key]
	failedAt, hasFailed := f.failed[key]
	busy := f.inflight[key]
	f.mu.Unlock()
	if cached && time.Since(c.at) < memCacheTTL {
		return hit(c.rate)
	}
	usable := cached && !hasFailed && time.Since(c.at) < staleFor
	if hasFailed && time.Since(failedAt) < failFor {
		return FXRate{}, false // it failed a moment ago: the pinned rate applies
	}
	if busy { // being fetched now: what was last known, else pinned
		if usable {
			return hit(c.rate)
		}
		return FXRate{}, false
	}

	// Central store first (PO 2026-08-27): the TTL lives in fx_rates, shared
	// by every instance — a fetch happens only when the pair is missing or
	// expired there.
	r, fresh, found := f.storeLookup(ctx, p, from, to)
	if found && fresh {
		f.remember(key, r)
		f.cleared(key)
		return hit(r)
	}
	if f.noWait {
		f.fetchInBackground(p, key, from, to)
		switch {
		case hasFailed: // not served again until the provider answers
			return FXRate{}, false
		case found: // run out, but not long ago: used while the new one is fetched
			f.rememberOld(key, r)
			return hit(r)
		case usable:
			return hit(c.rate)
		}
		return FXRate{}, false
	}
	rate, ok := f.fetchBounded(ctx, p, from, to)
	if !ok {
		f.noteFailed(key)
		return FXRate{}, false // caller applies the row-pinned conversion_rate
	}
	f.storeSave(ctx, p, from, to, rate)
	f.remember(key, rate)
	f.cleared(key)
	return hit(rate)
}

// memCacheTTL bounds the in-process cache. It exists only so a rating batch
// does not SELECT fx_rates once per message; the real expiry is the central
// store's expires_at (the provider row's cache_ttl_sec).
const memCacheTTL = 5 * time.Second

func (f *FXResolver) remember(key string, rate float64) {
	f.mu.Lock()
	f.cache[key] = cachedRate{rate: rate, at: time.Now()}
	f.mu.Unlock()
}

// rememberOld keeps a rate that has already run out: known, but not fresh —
// the next look after memCacheTTL goes to the store again.
func (f *FXResolver) rememberOld(key string, rate float64) {
	f.mu.Lock()
	if c, ok := f.cache[key]; !ok || c.rate != rate || time.Since(c.at) >= staleFor {
		f.cache[key] = cachedRate{rate: rate, at: time.Now().Add(-memCacheTTL)}
	}
	f.mu.Unlock()
}

func (f *FXResolver) noteFailed(key string) {
	f.mu.Lock()
	f.failed[key] = time.Now()
	f.mu.Unlock()
}

func (f *FXResolver) cleared(key string) {
	f.mu.Lock()
	delete(f.failed, key)
	f.mu.Unlock()
}

// fetchBounded asks the provider, for at most the resolver's timeout.
func (f *FXResolver) fetchBounded(ctx context.Context, p crProvider, from, to string) (float64, bool) {
	ctx, cancel := context.WithTimeout(ctx, f.timeout)
	defer cancel()
	return f.fetch(ctx, p, from, to)
}

// fetchInBackground asks the provider off the caller's path, one fetch per
// pair at a time. A failure is remembered (and said once, not per message).
func (f *FXResolver) fetchInBackground(p crProvider, key, from, to string) {
	f.mu.Lock()
	if f.inflight[key] {
		f.mu.Unlock()
		return
	}
	f.inflight[key] = true
	f.mu.Unlock()
	go func() {
		defer func() {
			f.mu.Lock()
			delete(f.inflight, key)
			f.mu.Unlock()
		}()
		rate, ok := f.fetchBounded(context.Background(), p, from, to)
		if !ok {
			f.noteFailed(key)
			log.Printf("[billing-fx] %s gave no %s:%s rate within %s — pinned conversion rates apply; asking again in %s",
				p.name, from, to, f.timeout, failFor)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		f.storeSave(ctx, p, from, to, rate)
		f.remember(key, rate)
		f.cleared(key)
	}()
}

// publishFixed keeps a fixed provider's pair in the shared store current —
// the Admin API's rates view and its FX sync read it there — without a write
// per message: when the rate changed, or half its life has passed.
func (f *FXResolver) publishFixed(p crProvider, key, from, to string, rate float64) {
	f.mu.Lock()
	c, ok := f.cache[key]
	due := !ok || c.rate != rate || time.Since(c.at) > p.cacheTTL/2
	if due {
		f.cache[key] = cachedRate{rate: rate, at: time.Now()}
	}
	f.mu.Unlock()
	if !due {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		f.storeSave(ctx, p, from, to, rate)
	}()
}

// storeLookup consults the shared fx_rates store. Expiry is compared on the
// SQL session clock — the same clock storeSave writes with (never mixed with
// Go/UTC time; same rule as vendor_sessions, D-103's clock lesson inverted).
// fresh=false is a rate that ran out less than staleFor ago.
func (f *FXResolver) storeLookup(ctx context.Context, p crProvider, from, to string) (rate float64, fresh, found bool) {
	if f.db == nil {
		return 0, false, false
	}
	rate, isFresh, err := f.db.StoredRate(ctx, p.id, from, to, int64(staleFor/time.Second))
	if err != nil || rate <= 0 {
		return 0, false, false
	}
	return rate, isFresh, true
}

// storeSave publishes a fetched rate so other instances (and the Admin API's
// rates view) reuse it without their own fetch. Best-effort: a store failure
// only costs an extra fetch later, never a mispriced charge.
func (f *FXResolver) storeSave(ctx context.Context, p crProvider, from, to string, rate float64) {
	if f.db == nil {
		return
	}
	ttl := int64(p.cacheTTL / time.Second)
	if err := f.db.SaveRate(ctx, p.id, from, to, rate, ttl); err != nil {
		log.Printf("[billing-fx] store save %s %s:%s: %v", p.name, from, to, err)
	}
}

func (f *FXResolver) fetch(ctx context.Context, p crProvider, from, to string) (float64, bool) {
	switch p.ptype {
	case "fixed":
		return fixedRate(p, from, to)
	case "http":
		var cfg struct {
			URL               string `json:"url"`
			Extract           string `json:"extract"`
			RequestEndpointID int64  `json:"request_endpoint_id"`
		}
		if json.Unmarshal([]byte(p.config), &cfg) != nil {
			return 0, false
		}
		// GW-15 Phase B (PO 2026-08-21 reference model): an http provider row
		// points at its rate REQUEST as a separate crp endpoint row carrying
		// url/params/extraction (alias conversion_rate). Executed through the
		// same machinery P4 uses.
		if cfg.RequestEndpointID != 0 {
			return f.fetchViaEndpoint(ctx, cfg.RequestEndpointID, from, to)
		}
		if cfg.URL == "" || cfg.Extract == "" {
			return 0, false
		}
		u := strings.NewReplacer("{from}", from, "{to}", to).Replace(cfg.URL)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return 0, false
		}
		resp, err := f.cl.Do(req)
		if err != nil {
			return 0, false
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		re, err := regexp.Compile(cfg.Extract)
		if err != nil {
			return 0, false
		}
		m := re.FindStringSubmatch(string(body))
		if len(m) < 2 {
			return 0, false
		}
		r, err := strconv.ParseFloat(strings.TrimSpace(m[1]), 64)
		if err != nil || r <= 0 {
			return 0, false
		}
		return r, true
	}
	return 0, false
}

// fetchViaEndpoint executes the provider's referenced rate-request row (GW-15
// Phase B): load the crp request endpoint, build the call with the fixed crp
// source vocabulary (from_currency/to_currency), extract alias conversion_rate.
func (f *FXResolver) fetchViaEndpoint(ctx context.Context, endpointID int64, from, to string) (float64, bool) {
	var row httpadapter.EndpointRow
	var headers, auth, cfg string
	got, err := f.db.RateRequest(ctx, endpointID)
	if err == nil {
		row.ID, row.Name, row.URL, row.Method = got.ID, got.Name, got.URL, got.Method
		headers, auth, cfg = got.Headers, got.Auth, got.Config
		row.TimeoutMS, row.CacheTTLSec, row.BodyTemplate = got.TimeoutMS, got.CacheTTLSec, got.BodyTemplate
	}
	if err != nil {
		log.Printf("[billing-fx] rate request endpoint %d: %v (pinned conversion applies)", endpointID, err)
		return 0, false
	}
	_ = json.Unmarshal([]byte(headers), &row.Headers)
	_ = json.Unmarshal([]byte(auth), &row.Auth)
	if json.Unmarshal([]byte(cfg), &row.Config) != nil {
		return 0, false
	}
	call, err := httpadapter.CallFromEndpoint(row, "crp")
	if err != nil {
		log.Printf("[billing-fx] rate request endpoint %d: %v", endpointID, err)
		return 0, false
	}
	vals, err := httpadapter.FetchValues(ctx, f.cl, call, map[string]string{
		"from_currency": from, "to_currency": to})
	if err != nil {
		log.Printf("[billing-fx] rate fetch %s:%s via endpoint %d: %v", from, to, endpointID, err)
		return 0, false
	}
	r, err := strconv.ParseFloat(vals["conversion_rate"], 64)
	if err != nil || r <= 0 {
		return 0, false
	}
	return r, true
}

// ---- Pair: the gateway's rate for two currencies, whoever has it ----

// Pair is the best rate the gateway knows for from->to: a fixed provider's
// own pair, a rate any provider published to fx_rates in the last day, the
// inverse of either, or two of them through a third currency. ok=false = no
// provider has anything for the pair.
//
// It exists to COMPARE prices held in different currencies — a vendor's cost
// against a customer's price in the loss check, vendors against each other in
// the cheapest-route choice. A buy card's own conversion_rate cannot do that:
// it is pinned for the card OWNER's currency (the vendor's company), so a EUR
// card of a EUR vendor carries 1, and read against a USD customer its 0.0115
// EUR became 0.0115 USD — a loss on every message, accepted (QA billing
// report #2, 2026-10-07). Never used for a charge: a charge converts by the
// selling row's own pinned rate or provider, as it always did.
//
// Nothing here waits for a provider. An unknown pair (or one whose stored
// rate has run out) is asked for in the background, at most every
// pairAskEvery.
func (f *FXResolver) Pair(ctx context.Context, from, to string) (float64, bool) {
	if f == nil || from == "" || to == "" {
		return 0, false
	}
	from, to = strings.ToUpper(from), strings.ToUpper(to)
	if from == to {
		return 1, true
	}
	f.reload(ctx)
	book := f.rateBook(ctx)
	r, ok := pairFrom(book, from, to)
	if !ok || r.stale {
		f.askProviders(from, to)
	}
	return r.rate, ok
}

// pairFrom reads one pair out of a rate book: directly, or through one middle
// currency (USD first, then EUR, then the rest in order — one answer for the
// same book every time).
func pairFrom(book map[string]bookRate, from, to string) (bookRate, bool) {
	if r, ok := book[from+":"+to]; ok {
		return r, true
	}
	seen := map[string]bool{}
	var middles []string
	for k := range book {
		if a, _, ok := strings.Cut(k, ":"); ok && !seen[a] {
			seen[a] = true
			middles = append(middles, a)
		}
	}
	rank := func(c string) int {
		switch c {
		case "USD":
			return 0
		case "EUR":
			return 1
		}
		return 2
	}
	sort.Slice(middles, func(a, b int) bool {
		if ra, rb := rank(middles[a]), rank(middles[b]); ra != rb {
			return ra < rb
		}
		return middles[a] < middles[b]
	})
	for _, m := range middles {
		if m == from || m == to {
			continue
		}
		a, okA := book[from+":"+m]
		b, okB := book[m+":"+to]
		if okA && okB {
			return bookRate{rate: a.rate * b.rate, stale: a.stale || b.stale}, true
		}
	}
	return bookRate{}, false
}

// rateBook is every pair any provider has, read at most every bookEvery. The
// first read is the caller's; after that a stale book is served while one
// caller reads the next in the background.
func (f *FXResolver) rateBook(ctx context.Context) map[string]bookRate {
	f.mu.Lock()
	book, at, busy := f.book, f.bookAt, f.bookBusy
	due := time.Since(at) > bookEvery && !busy
	if due {
		f.bookBusy = true
	}
	f.mu.Unlock()
	if !due {
		return book
	}
	read := func(ctx context.Context) map[string]bookRate {
		next := f.readBook(ctx)
		f.mu.Lock()
		if next != nil {
			f.book = next
		}
		f.bookAt, f.bookBusy = time.Now(), false
		got := f.book
		f.mu.Unlock()
		return got
	}
	if book == nil { // nothing to serve yet
		return read(ctx)
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		read(ctx)
	}()
	return book
}

// readBook builds the book: fixed providers' own pairs first (their word is
// current by definition), then what any provider published in the last
// bookKeep, newest first; the inverse of each pair where nobody has it
// directly. nil = the store could not be read (the old book stays).
func (f *FXResolver) readBook(ctx context.Context) map[string]bookRate {
	book := map[string]bookRate{}
	add := func(from, to string, r float64, stale bool) {
		from, to = strings.ToUpper(from), strings.ToUpper(to)
		if r <= 0 || from == "" || to == "" || from == to {
			return
		}
		if _, ok := book[from+":"+to]; !ok {
			book[from+":"+to] = bookRate{rate: r, stale: stale}
		}
	}
	f.mu.Lock()
	providers := f.ordered
	f.mu.Unlock()
	for _, p := range providers {
		if p.ptype != "fixed" {
			continue
		}
		pairs := make([]string, 0, len(p.rates))
		for k := range p.rates {
			pairs = append(pairs, k)
		}
		sort.Strings(pairs)
		for _, k := range pairs {
			if from, to, ok := strings.Cut(k, ":"); ok {
				add(from, to, p.rates[k], false)
			}
		}
	}
	if f.db != nil {
		rows, err := f.db.RecentRates(ctx, int64(bookKeep/time.Second))
		if err != nil {
			log.Printf("[billing-fx] rate book: %v (prices in different currencies cannot be compared until it reads)", err)
			if len(book) == 0 {
				return nil
			}
		} else {
			for _, r := range rows {
				add(r.From, r.To, r.Rate, r.Stale)
			}
		}
	}
	// The inverse of every pair nobody gives directly.
	direct := make([]string, 0, len(book))
	for k := range book {
		direct = append(direct, k)
	}
	sort.Strings(direct)
	for _, k := range direct {
		from, to, _ := strings.Cut(k, ":")
		r := book[k]
		add(to, from, 1/r.rate, r.stale)
	}
	return book
}

// askProviders asks the HTTP providers for a pair nobody has (or whose rate
// has run out), in the background, lowest id first; the first answer is
// published to fx_rates under that provider and the book is read again.
func (f *FXResolver) askProviders(from, to string) {
	pair := from + ":" + to
	f.mu.Lock()
	if last, ok := f.asked[pair]; ok && time.Since(last) < pairAskEvery {
		f.mu.Unlock()
		return
	}
	f.asked[pair] = time.Now()
	var ask []crProvider
	for _, p := range f.ordered {
		if p.ptype == "http" {
			ask = append(ask, p)
		}
	}
	f.mu.Unlock()
	if len(ask) == 0 {
		return
	}
	go func() {
		for _, p := range ask {
			rate, ok := f.fetchBounded(context.Background(), p, from, to)
			if !ok {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			f.storeSave(ctx, p, from, to, rate)
			cancel()
			f.mu.Lock()
			f.bookAt = time.Time{} // read again at the next look
			f.mu.Unlock()
			return
		}
	}()
}
