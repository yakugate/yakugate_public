package hlr

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/yakugate/yakugate/internal/dal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/vendors/httpadapter"
)

// Live runs an account's live lookups against the account's own list of HLR
// providers (sms_service_config.hlr_provider_ids, tried in order), 2026-09-14.
// Before this there was one provider for the whole gateway: the first active
// hlr endpoint, read once at start-up.
//
// Each provider is an active http_endpoints row of type 'hlr' with a URL. It
// gets its own Dipper, so one slow provider cannot use up another's ceiling,
// and the set is reloaded every minute so a provider added in the portal is
// used without a restart.
//
// An empty list means no live lookups — with one exception kept for older
// deployments: when the gateway has NO provider rows at all, the provider in
// the settings file (YAKU_HLR_DIP_URL) serves every account, as it did before.
type Live struct {
	mu      sync.RWMutex
	dippers map[int64]*Dipper
	sigs    map[int64]string
	names   map[int64]string
	legacy  *Dipper

	writer      Writer
	timeout     time.Duration
	maxInflight int
	logf        func(string, ...any)
	differs     func(msisdn, mcc, mnc string) bool // ported by range (6 Nov 2026); nil = off
}

// SetPortedByRange gives every provider's answers the operators-table check:
// a number whose range is another network's is marked ported (Ranges.Differs).
// Called before Start.
func (l *Live) SetPortedByRange(f func(msisdn, mcc, mnc string) bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.differs = f
	if l.legacy != nil {
		l.legacy.differs = f
	}
	for _, d := range l.dippers {
		d.differs = f
	}
}

// NewLive builds an empty provider set. writer receives every answer (cache,
// Redis copy, hlr_<first4> table); timeout and maxInflight are the defaults
// for a provider row that sets no timeout of its own.
func NewLive(writer Writer, timeout time.Duration, maxInflight int, logf func(string, ...any)) *Live {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Live{
		dippers: map[int64]*Dipper{}, sigs: map[int64]string{}, names: map[int64]string{},
		writer: writer, timeout: timeout, maxInflight: maxInflight, logf: logf,
	}
}

// SetLegacy installs the settings-file provider (nil = none).
func (l *Live) SetLegacy(p Provider) {
	if l == nil {
		return
	}
	var d *Dipper
	if p != nil {
		d = NewDipper(p, l.writer, l.timeout, l.maxInflight, l.logf)
		d.differs = l.differs
	}
	l.mu.Lock()
	l.legacy = d
	l.mu.Unlock()
}

// Providers returns the ids and names of the providers in use, for logs.
func (l *Live) Providers() map[int64]string {
	out := map[int64]string{}
	if l == nil {
		return out
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	for id, n := range l.names {
		out[id] = n
	}
	return out
}

// Try looks the number up with each provider on the list in turn and stops at
// the first answer. attempted reports whether any provider was actually asked.
// The entry carries the call that answered — or, when none found the number,
// the last call made (Entry.Call; nil when none was made or kept).
func (l *Live) Try(ctx context.Context, providerIDs []int64, msisdn string) (e Entry, found, attempted bool) {
	if l == nil {
		return Entry{}, false, false
	}
	l.mu.RLock()
	var ds []*Dipper
	for _, id := range providerIDs {
		if d := l.dippers[id]; d != nil {
			ds = append(ds, d)
		}
	}
	if len(providerIDs) == 0 && len(l.dippers) == 0 && l.legacy != nil {
		ds = append(ds, l.legacy)
	}
	l.mu.RUnlock()
	// A lookup that found nothing still hands back the last call it made
	// (6 Nov 2026), for the log row of the lookup.
	var last *model.LookupCall
	for _, d := range ds {
		e, ok, tried := d.Try(ctx, msisdn)
		attempted = attempted || tried
		if ok {
			return e, true, attempted
		}
		if e.Call != nil {
			last = e.Call
		}
	}
	return Entry{Call: last}, false, attempted
}

// Load reads the active hlr provider rows (the config store's
// http_endpoints, through the DAL) and rebuilds the ones that changed.
func (l *Live) Load(ctx context.Context, db dal.HLRLookupConfig) error {
	if db == nil {
		return errNoConfigStore
	}
	rows, err := db.HLRProviders(ctx)
	if err != nil {
		return err
	}
	type loaded struct {
		row httpadapter.EndpointRow
		sig string
	}
	var got []loaded
	for _, r := range rows {
		var row httpadapter.EndpointRow
		row.ID, row.Name, row.URL, row.Method = r.ID, r.Name, r.URL, r.Method
		row.TimeoutMS, row.CacheTTLSec, row.BodyTemplate = r.TimeoutMS, r.CacheTTLSec, r.BodyTemplate
		headers, auth, cfgJSON := r.Headers, r.Auth, r.Config
		_ = json.Unmarshal([]byte(headers), &row.Headers)
		_ = json.Unmarshal([]byte(auth), &row.Auth)
		if json.Unmarshal([]byte(cfgJSON), &row.Config) != nil {
			l.logf("[hlr-live] provider %d %q: config is not valid JSON, skipped", row.ID, row.Name)
			continue
		}
		sig := strings.Join([]string{row.Name, row.URL, row.Method, headers, auth,
			strconv.Itoa(row.TimeoutMS), cfgJSON, row.BodyTemplate}, "\x00")
		got = append(got, loaded{row, sig})
	}

	l.mu.RLock()
	oldSigs := l.sigs
	l.mu.RUnlock()
	dippers, sigs, names := map[int64]*Dipper{}, map[int64]string{}, map[int64]string{}
	l.mu.RLock()
	for _, g := range got {
		id := g.row.ID
		if old := l.dippers[id]; old != nil && oldSigs[id] == g.sig {
			dippers[id], sigs[id], names[id] = old, g.sig, g.row.Name
		}
	}
	l.mu.RUnlock()
	var added []string
	for _, g := range got {
		id := g.row.ID
		if dippers[id] != nil {
			continue
		}
		call, err := httpadapter.CallFromEndpoint(g.row, "hlr")
		if err != nil {
			l.logf("[hlr-live] provider %d %q cannot be used: %v", id, g.row.Name, err)
			continue
		}
		timeout := lookupLimit(l.timeout, g.row.TimeoutMS)
		dippers[id] = NewDipper(NewEndpointProvider(call, timeout), l.writer, timeout, l.maxInflight, l.logf)
		dippers[id].differs = l.differs
		sigs[id], names[id] = g.sig, g.row.Name
		added = append(added, fmt.Sprintf("%d %q", id, g.row.Name))
	}
	l.mu.Lock()
	removed := len(l.dippers) - countKept(l.dippers, dippers)
	l.dippers, l.sigs, l.names = dippers, sigs, names
	l.mu.Unlock()
	if len(added) > 0 || removed > 0 {
		sort.Strings(added)
		l.logf("[hlr-live] %d provider(s) in use; added or changed: %s; removed: %d",
			len(dippers), strings.Join(added, ", "), removed)
	}
	return nil
}

func countKept(old, cur map[int64]*Dipper) int {
	n := 0
	for id, d := range old {
		if cur[id] == d {
			n++
		}
	}
	return n
}

// Start loads now and then every interval until ctx ends. A failed load keeps
// the providers already in use.
func (l *Live) Start(ctx context.Context, db dal.HLRLookupConfig, every time.Duration) {
	if every <= 0 {
		every = time.Minute
	}
	load := func() {
		lctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		if err := l.Load(lctx, db); err != nil {
			l.logf("[hlr-live] loading providers failed (keeping the current ones): %v", err)
		}
	}
	load()
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				load()
			}
		}
	}()
}

// lookupLimit is how long one live lookup may take: the gateway's own limit
// (YAKU_HLR_DIP_TIMEOUT_MS, 300 ms unless changed), or the provider row's
// Timeout when that is SHORTER.
//
// Until 2026-10-12 the row's Timeout simply replaced the setting. Every
// http_endpoints row has one — 15,000 ms unless somebody changed it — so the
// setting was never the limit: a provider that hung held each send for 15
// seconds on the accept path (go-live test P1 E5: "Hung provider: 15.0 s an
// answer"). A lookup is made while the client waits for its answer, so the
// setting is the ceiling and a row can only tighten it. To allow a slower
// provider, raise YAKU_HLR_DIP_TIMEOUT_MS.
func lookupLimit(setting time.Duration, rowTimeoutMS int) time.Duration {
	if setting <= 0 {
		setting = 300 * time.Millisecond
	}
	if row := time.Duration(rowTimeoutMS) * time.Millisecond; row > 0 && row < setting {
		return row
	}
	return setting
}
