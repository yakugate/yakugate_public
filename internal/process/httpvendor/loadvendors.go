package httpvendor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/yakugate/yakugate/internal/attemptcap"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
	dlrsvc "github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/queues"
	"github.com/yakugate/yakugate/internal/vendors/httpadapter"
)

// Loading the HTTP vendors: an adapter per vendor built from its endpoint
// rows, and the quick look that loads them again when one was edited.

// changeCheckEvery is how often the role looks whether a vendor or one of its
// calls was edited. One small read of two short tables.
const changeCheckEvery = 3 * time.Second

// settingsChanged reports that an HTTP vendor or a vendor call is not what it
// was at the last look, so the vendors are loaded again now.
//
// Until 2026-10-12 a save was picked up only by the reload timer
// (YAKU_VENDOR_RELOAD_SEC, 30 s): the Admin API nudges the http-server's
// cache after a write, and nothing reaches this role. Saleem corrected a send
// call's success rule and sent a test message 16 seconds later — it was
// judged by the rule from before the save, he took the fix for wrong, and
// changed a right setting into a wrong one. A person who has just pressed
// Save tests at once; 3 seconds is inside that.
//
// The reload timer stays: it is what picks a change up when this read fails,
// and what re-reads everything loadVendors takes from elsewhere.
func (p *Process) settingsChanged(ctx context.Context) bool {
	if time.Since(p.settingsSigAt) < changeCheckEvery {
		return false
	}
	p.settingsSigAt = time.Now()
	sig, err := p.traffic.VendorRows().HTTPSettingsSignature(ctx)
	if err != nil {
		return false
	}
	changed := p.settingsSig != "" && sig != p.settingsSig
	p.settingsSig = sig
	if changed {
		p.log.Printf("a vendor or one of its calls was changed: loading the vendors again now")
		// Whatever was wrong with a login may be what was just changed: the
		// waits earned by its failures do not hold the new settings back.
		p.forgetSessionWaits()
	}
	return changed
}

// loadVendors builds an adapter per active HTTP (or SOAP, same config-driven
// adapter — 2026-07-28) vendor from its adapter_config. Session-auth vendors
// get the SHARED MySQL-backed session store (PO 2026-08-10) so the token
// survives this 30s rebuild and is reused by every instance; a vendor that
// LEFT the active set (deactivated/removed) has its session revoked.
func (p *Process) loadVendors(ctx context.Context) {
	// The most times a message may be sent, for the claim (attemptcap): read
	// with the vendors, so a changed max_attempts applies at the next reload.
	attemptcap.Apply(ctx, p.engine().VendorSends, p.queue, p.log.Printf)
	// The vendors' companies, for matching receipts (vendorgroup).
	if err := p.groups.Load(ctx, p.traffic.VendorCompanies()); err != nil {
		p.log.Printf("vendor companies for receipts: %v", err)
	}
	// Cleared each pass so a vendor whose config was fixed since the last
	// reload stops being reported as broken.
	p.stats.clearSkipped()
	// Only the vendors assigned to THIS deployment (config/0122): this
	// deployment's roles open their queues and nobody else's, which is what
	// a carrier that allowlists one source IP requires. A vendor assigned
	// nowhere is claimed everywhere, as before. The read is the DAL's
	// (dal.VendorRows).
	var sess httpadapter.SessionStore
	if p.sessions != nil {
		sess = p.sessions
	}
	var out []vendorAdapter
	err := p.traffic.VendorRows().HTTPVendors(ctx, deployment.ID(), func(r dal.HTTPVendorRow, err error) {
		if err != nil {
			return
		}
		va := vendorAdapter{id: r.ID, cid: r.CID}
		cfgJSON, deployJSON, dlrJSON := r.AdapterConfig, r.Deployments, r.DLRConfig
		downSec, recheckSec, ownIntermediate := r.DownAfterSec, r.RecheckAfterSec, r.OwnIntermediate
		va.sendsNoReceipts, va.receiptWaitHours = dlrsvc.ReceiptSettings(dlrJSON)
		// This gateway's share of the vendor's rate (config/0124). The
		// account-style entry is the only source now — submit_throughput is
		// retired — and a vendor with no rate here is uncapped on this box.
		deps, _ := deployment.Parse(deployJSON)
		va.submitTPS = deps.Rate(deployment.ID(), deployment.MT)
		// This vendor sends its own ACCEPTD, so the gateway does not add one
		// of its own at submit (PO 2026-09-23: the vendor's wins).
		va.ownIntermediateDLR = ownIntermediate != 0
		va.downAfter = time.Duration(downSec) * time.Second
		va.recheckAfter = time.Duration(recheckSec) * time.Second
		var prof httpadapter.Profile
		// GW-15 (PO 2026-08-21, "no legacy config for http"): the portal saves
		// a vendor as endpoint REFERENCES — {"endpoints":{"send":id,"poll":id}}
		// — and the Profile is hydrated from those http_endpoints rows. The
		// login row is discovered through the send row's session_endpoint_id
		// (one definition, D-103 shared-session semantics). Hand-written
		// Profile blobs keep working unchanged.
		var refs struct {
			Endpoints map[string]int64 `json:"endpoints"`
		}
		if err := json.Unmarshal([]byte(cfgJSON), &refs); err == nil && refs.Endpoints["send"] != 0 {
			hp, err := p.profileFromEndpointRefs(ctx, refs.Endpoints)
			if err != nil {
				// The vendor named in the text too: on the console only the
				// text is printed (P4-3).
				p.log.ForVendor(va.cid).Errorf("config", "vendor %s: its calls cannot be loaded (%v); skipping", va.cid, err)
				p.stats.noteSkipped(va.cid, "its calls cannot be loaded: "+err.Error())
				return
			}
			prof = hp
			va.batch, va.concurrency = p.sendKnobs(ctx, refs.Endpoints["send"])
			va.breaker = p.sendBreaker(ctx, refs.Endpoints["send"])
		} else if err := json.Unmarshal([]byte(cfgJSON), &prof); err != nil || prof.Send.URL == "" {
			p.log.ForVendor(va.cid).Errorf("config", "vendor %s: no/invalid adapter_config; skipping", va.cid)
			// Counted, not just logged. A skipped vendor is configured,
			// expected to carry traffic, and carrying none — and a log line
			// scrolls away while a monitoring row does not. Without this the
			// dashboard shows "1 vendor" and an operator has no way to know
			// that two more were dropped for bad config.
			p.stats.noteSkipped(va.cid, "no/invalid adapter_config")
			return
		}
		// Regex/mode validation (TC-P4-40, 2026-08-19): an uncompilable
		// extract used to load silently and extract nothing forever.
		if err := prof.Validate(); err != nil {
			p.log.ForVendor(va.cid).Errorf("config", "vendor %s: adapter_config INVALID (%v); skipping — fix the profile and it reloads within %v", va.cid, err, p.reload)
			// Listed as skipped like the two cases above. It was the one
			// skip the dashboard could not see: the vendor simply vanished.
			p.stats.noteSkipped(va.cid, "invalid profile: "+err.Error())
			return
		}
		va.ad = httpadapter.NewShared(prof, va.id, va.cid, sess)
		cid := va.cid
		va.ad.ObserveResponses(func(code int) { p.stats.noteResponse(cid, code) })
		if va.submitTPS > 0 {
			// Said out loud: shared, the deployment's processes together call
			// this; per process, two http-vendor processes call twice this.
			// Better read here than inferred from a carrier complaint.
			p.log.ForVendor(va.cid).Infof("rate_limit",
				"call rate capped at %d/s for this deployment (%s), %s",
				va.submitTPS, deployment.ID(), rateUnit(p.rateShared, "http-vendor"))
		}
		out = append(out, va)
	})
	if err != nil {
		p.log.Printf("load vendors: %v", err)
		return
	}
	// Revoke sessions of vendors no longer served: end the vendor-side session
	// (profile auth.revoke, when configured) and drop the shared row so every
	// instance stops using it. Only on a SUCCESSFUL reload — a query error
	// above must never mass-revoke live sessions.
	if !p.loadedAt.IsZero() {
		still := make(map[int64]bool, len(out))
		for _, va := range out {
			still[va.id] = true
		}
		for _, old := range p.vendors {
			if !still[old.id] {
				if err := old.ad.Revoke(ctx); err != nil {
					p.log.ForVendor(old.cid).Warnf("session", "revoke session: %v", err)
				} else {
					p.log.ForVendor(old.cid).Infof("session", "left the active set — session revoked")
				}
			}
		}
	}
	// How each vendor's rate is shared between its queues (config/0161,
	// issue 22).
	modes := queues.SplitModes(ctx, p.engine().VendorSends)
	for i := range out {
		out[i].split = modes[out[i].cid]
	}
	p.vmu.Lock()
	replaced := p.vendors
	p.vendors = out
	p.vmu.Unlock()
	// The replaced adapters' own TLS connection pools (QA EP-018) are closed
	// once idle; a call still in flight on one finishes normally.
	for _, old := range replaced {
		if old.ad != nil {
			old.ad.CloseIdleConnections()
		}
	}
	served := make(map[int64]bool, len(out))
	for _, va := range out {
		served[va.id] = true
	}
	p.health.Keep(served)
	p.breakers.keep(served)
	for _, old := range replaced {
		if !served[old.id] && p.polls != nil {
			p.polls.forget(old.id)
		}
	}
	active := make(map[string]*httpadapter.Adapter, len(out))
	for _, va := range out {
		active[va.cid] = va.ad
	}
	p.stats.setActive(active)
	p.loadedAt = time.Now()
	// A vendor that sends under a session gets it now, not at its first
	// message (sessions.go).
	p.keepSessions(ctx, out)
}

// sendKnobs reads the send endpoint's claim batch and concurrency (D-088:
// rate_limit and submit_concurrency). Read here rather than carried on the
// adapter's EndpointRow because they steer this worker, not the vendor call.
func (p *Process) sendKnobs(ctx context.Context, sendID int64) (batch, concurrency int) {
	batch, concurrency = 50, 1
	b, c, err := p.traffic.VendorCalls().SendKnobs(ctx, sendID)
	if err != nil {
		return
	}
	if b > 0 {
		batch = b
	}
	if c > 0 {
		concurrency = c
	}
	return
}

// loadEndpointRow fetches one active http_endpoints row for hydration.
func (p *Process) loadEndpointRow(ctx context.Context, id int64) (httpadapter.EndpointRow, error) {
	var row httpadapter.EndpointRow
	var headers, auth, cfg, tlsJSON, retryJSON string
	// body_template (2026-09-14): the SOAP wizard's envelope, or any raw
	// body, was saved on the row and never read — the call went out without it.
	// tls and retry (2026-10-01, QA EP-018): the same, for the Advanced tab.
	calls := p.traffic.VendorCalls()
	r, err := calls.Call(ctx, id)
	row.ID, row.Name, row.URL, row.Method, headers, auth = r.ID, r.Name, r.URL, r.Method, r.Headers, r.Auth
	row.TimeoutMS, row.CacheTTLSec, cfg, row.BodyTemplate, tlsJSON, retryJSON = r.TimeoutMS, r.CacheTTLSec, r.Config, r.BodyTemplate, r.TLS, r.Retry
	if errors.Is(err, dal.ErrNoRows) {
		// Said as what it is (go-live test of 18 Oct 2026, P4 A2, old bug
		// P4-2): a call switched off read "sql: no rows in result set" in the
		// log, the role's report and Monitoring, as if the database had failed.
		switch _, e := calls.CallActive(ctx, id); {
		case e == nil:
			return row, fmt.Errorf("HTTP endpoint %d is switched off", id)
		case errors.Is(e, dal.ErrNoRows):
			return row, fmt.Errorf("HTTP endpoint %d does not exist (deleted?)", id)
		}
	}
	if err != nil {
		return row, fmt.Errorf("endpoint %d: %w", id, err)
	}
	_ = json.Unmarshal([]byte(headers), &row.Headers)
	_ = json.Unmarshal([]byte(auth), &row.Auth)
	if err := json.Unmarshal([]byte(cfg), &row.Config); err != nil {
		return row, fmt.Errorf("endpoint %d: bad config JSON: %w", id, err)
	}
	if err := rowExtras(ctx, &row, tlsJSON, retryJSON); err != nil {
		return row, fmt.Errorf("endpoint %d: %w", id, err)
	}
	return row, nil
}

// profileFromEndpointRefs hydrates a vendor Profile from its referenced rows
// (GW-15). send is required; poll optional; the login row comes from the send
// row's config.session_endpoint_id.
func (p *Process) profileFromEndpointRefs(ctx context.Context, refs map[string]int64) (httpadapter.Profile, error) {
	send, err := p.loadEndpointRow(ctx, refs["send"])
	if err != nil {
		return httpadapter.Profile{}, err
	}
	var login *httpadapter.EndpointRow
	if sid, ok := send.Config["session_endpoint_id"].(float64); ok && sid != 0 {
		l, err := p.loadEndpointRow(ctx, int64(sid))
		if err != nil {
			return httpadapter.Profile{}, fmt.Errorf("login row: %w", err)
		}
		login = &l
	}
	var poll *httpadapter.EndpointRow
	if pid := refs["poll"]; pid != 0 {
		pl, err := p.loadEndpointRow(ctx, pid)
		if err != nil {
			return httpadapter.Profile{}, fmt.Errorf("poll row: %w", err)
		}
		poll = &pl
	}
	// Poll raw-status mapping stays passthrough-lowercase until a per-vendor
	// status vocabulary table is wired here (vendor_status_codes models SMPP
	// command_status, not HTTP poll strings) — recorded in the GW-15 entry.
	return httpadapter.ProfileFromEndpoints(send, login, poll, nil)
}
