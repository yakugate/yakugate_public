package smppserver

// Binds: making one, and ending the ones the account no longer allows.

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/refdata"
	"github.com/yakugate/yakugate/internal/seclog"
	"github.com/yakugate/yakugate/lib/smpp/pdu"
)

// handleBind authenticates a bind and replies. [SCOPE P2-BIND]
//
// Lockout ladders (2026-07-26, parity with P1's spec — identity 5 fails/min,
// source-IP 20 fails/min, rungs 1/5/30/60min): the IP throttle answers
// PRE-auth (ESME_RTHROTTLED, cheapest possible path); the identity ladder
// locks the system_id (ESME_RBINDFAIL). The allowlist skip-table applies: a
// failure for an allowlisted account from a NON-allowlisted IP counts toward
// the IP ladder only, so an attacker hammering a protected account's
// system_id cannot lock the real customer out (DoS rule, PO 2026-07-26).
func (p *Process) handleBind(s *session, pd *pdu.PDU, log *logging.Logger) {
	respID := map[uint32]uint32{
		pdu.BindTransmitter: pdu.BindTxResp,
		pdu.BindReceiver:    pdu.BindReceiverResp,
		pdu.BindTransceiver: pdu.BindTrxResp,
	}[pd.Header.CommandID]
	mode := map[uint32]bindMode{
		pdu.BindTransmitter: modeTX,
		pdu.BindReceiver:    modeRX,
		pdu.BindTransceiver: modeTRX,
	}[pd.Header.CommandID]

	// pre-auth source-IP throttle (20 fails/min → refuse before any work)
	if p.ipLock != nil {
		if locked, retry := p.ipLock.Locked("ip:" + s.ip); locked {
			p.countAuthFail()
			// [SCOPE P13-SECLOG] The bind is refused before the PDU is even
			// decoded, so there is no system_id to name yet — the IP is the
			// whole fact, and it belongs to the system stream, not a tenant.
			p.security(seclog.Event{Type: seclog.EventIPThrottled, IP: s.ip,
				Reason: "pre-auth source-IP throttle", RetryAfter: retry})
			s.send(pdu.BindResp(respID, pdu.ESME_RTHROTTLED, pd.Header.SequenceNumber, ""))
			return
		}
	}
	// Credential-spray guard (PO 2026-07-27): an IP cycling through many
	// distinct system_ids is blocked immediately — see lockout.SprayGuard.
	if p.spray != nil {
		if locked, retry := p.spray.Locked(s.ip); locked {
			p.countAuthFail()
			p.security(seclog.Event{Type: seclog.EventSprayBlocked, IP: s.ip,
				Reason: "source IP blocked for trying many distinct system_ids", RetryAfter: retry})
			s.send(pdu.BindResp(respID, pdu.ESME_RBINDFAIL, pd.Header.SequenceNumber, ""))
			return
		}
	}

	b, err := pdu.DecodeBind(pd.Body)
	if err == nil {
		s.ifaceVersion = b.InterfaceVersion
	}
	if err != nil {
		s.send(pdu.GenericNackPDU(pdu.ESME_RINVCMDLEN, pd.Header.SequenceNumber))
		return
	}

	// identity ladder gate (5 fails/min → 1/5/30/60min locks)
	if p.lock != nil {
		if locked, retry := p.lock.Locked("smpp:" + b.SystemID); locked {
			p.countAuthFail()
			p.security(seclog.Event{Type: seclog.EventAuthLocked, Account: p.accountFor(b.SystemID),
				Identity: "u:" + b.SystemID, IP: s.ip, Reason: "bind-auth ladder locked", RetryAfter: retry})
			s.send(pdu.BindResp(respID, pdu.ESME_RBINDFAIL, pd.Header.SequenceNumber, b.SystemID))
			return
		}
	}

	fail := func(acct *refdata.Account, status uint32, reason string) {
		p.countAuthFail()
		// skip-table: allowlisted account + non-allowlisted IP → IP ladder only
		countIdentity := true
		if acct != nil && len(acct.AllowedIPs) > 0 && !acct.IPAllowed(s.ip) {
			countIdentity = false
		}
		acctName := ""
		if acct != nil {
			acctName = acct.Username
		}
		// [SCOPE P13-SECLOG] The refusal itself, at warn: an everyday wrong
		// password. Filed under the account when we know which one, so a
		// customer can be shown their own failed logins, and always to the
		// system stream an operator reads.
		p.security(seclog.Event{Type: seclog.EventAuthFail, Account: acctName,
			Identity: "u:" + b.SystemID, IP: s.ip, Reason: reason})
		// Rung-3 alerts (chunk-2 ruling 2.6, 2026-08-03): the third rung means
		// an attack is in progress, not a typo — seclog logs those at error, so
		// they survive an account or system set to level "error". These were
		// ad-hoc [audit] printf lines until 2026-09-05.
		if countIdentity && p.lock != nil {
			if _, retry, level := p.lock.Fail("smpp:" + b.SystemID); level >= 3 {
				p.security(seclog.Event{Type: seclog.EventLadderRung, Account: acctName,
					Identity: "u:" + b.SystemID, IP: s.ip, Reason: "bind-auth ladder",
					Rung: level, RetryAfter: retry})
			}
		}
		if p.ipLock != nil {
			if _, retry, level := p.ipLock.Fail("ip:" + s.ip); level >= 3 {
				p.security(seclog.Event{Type: seclog.EventLadderRung, IP: s.ip,
					Reason: "source-IP throttle", Rung: level, RetryAfter: retry})
			}
		}
		// Credential-spray guard (PO 2026-07-27): observe this system_id against
		// this IP regardless of the skip-table above — a spraying attacker's
		// system_id is by definition unknown/not the allowlisted account's own.
		if p.spray != nil {
			if _, retry, level := p.spray.Observe(s.ip, "smpp:"+b.SystemID); level >= 3 {
				p.security(seclog.Event{Type: seclog.EventSprayBlocked, IP: s.ip,
					Identity: "u:" + b.SystemID, Reason: "many distinct system_ids from one IP",
					Rung: level, RetryAfter: retry})
			}
		}
		// [SCOPE P13-CAPTURE] and into a running flow capture, when the
		// system_id names an account we actually have. A customer whose binds
		// are being refused sees nothing in the pipeline's capture — no
		// message was ever created — so this is the only place their own
		// failure can come from.
		p.captureBind(log, acctName, modeName(mode), s.ip, status, 0, reason)
		s.send(pdu.BindResp(respID, status, pd.Header.SequenceNumber, b.SystemID))
	}

	acct, ok := p.ref.AccountBySMPPSystemID(b.SystemID)
	if !ok {
		fail(nil, pdu.ESME_RINVSYSID, "unknown system_id")
		return
	}
	// account-state + channel checks
	if !acct.SMPP || acct.Suspended() || expired(acct) || acct.AccessScope == "portal" {
		fail(acct, pdu.ESME_RBINDFAIL, "account not bindable (smpp off, suspended, expired or portal-only)")
		return
	}
	if !modeAllowed(acct, mode) {
		fail(acct, pdu.ESME_RBINDFAIL, "bind mode not allowed for this account")
		return
	}
	// The account's IP allow-list (2026-10-08). It was applied to every
	// submit and never to the bind itself — so anyone holding the system_id
	// and password could bind from anywhere, and a receiver bind was then
	// handed the account's receipts and incoming messages. Before the
	// password: an address that is not allowed gets no answer about it.
	if !ipPermitted(acct, s.ip) {
		p.security(seclog.Event{Type: seclog.EventIPNotAllowed, Account: acct.Username,
			Identity: "u:" + b.SystemID, IP: s.ip, Reason: "bind from an address that is not on this account's allow list"})
		fail(acct, pdu.ESME_RBINDFAIL, "source address not allowed")
		return
	}
	// password: bcrypt verify
	if bcrypt.CompareHashAndPassword([]byte(acct.SMPPPasswordHash), []byte(b.Password)) != nil {
		fail(acct, pdu.ESME_RINVPASWD, "wrong password")
		return
	}
	// max_binds [SCOPE P2-MAXBINDS] (DEC-2/D-065, 2026-07-26): cap live binds
	// across ALL modes; 0 = explicitly unlimited, absent = 10 (refdata parse).
	// An account that is not served by this gateway is not bound here
	// either: every submit would be refused RESTRICTED_DEPLOYMENT, and its
	// receipts wait on the gateway it IS served by. The credentials were
	// right, so this is no security event and counts on no ladder.
	if id := deployment.ID(); id != "" && !acct.Deployments.Allows(id) {
		p.logBindWarn(log, "bind_refused", "bind refused: this account is not served by this gateway", acct.Username,
			logging.Fields{"systemId": b.SystemID, "ip": s.ip, "deployment": id})
		s.send(pdu.BindResp(respID, pdu.ESME_RBINDFAIL, pd.Header.SequenceNumber, b.SystemID))
		return
	}
	s.mode = mode
	s.acct = acct
	s.boundAt = time.Now()
	if p.stats != nil {
		username := acct.Username
		s.onSubmitResp = func(status uint32) { p.stats.NoteAccountSMPP(username, pdu.StatusName(status)) }
	}
	// [SCOPE P2-WINDOW] Size the ingress window now that the account is
	// known: its own window_size if set, else the process default.
	s.openWindow(windowFor(acct, p.window))
	// Counted and added in one step (addIfRoom): two binds arriving together
	// must not both take the last place. ALL modes are registered (max_binds
	// counts TX too), and only now — a bind on the list can be handed a
	// receipt, so it goes on the list just before it is told it is bound.
	if !p.binds.addIfRoom(acct.ID, s, acct.SMPPMaxBinds) {
		s.mode, s.acct, s.sem, s.onSubmitResp = unbound, nil, nil, nil
		p.countAuthFail()
		// Deliberately NOT a security event: the credentials were right, the
		// client simply has too many connections open. That is a capacity
		// message for its own account's log, not an attack signal.
		p.logBindWarn(log, "bind_refused", "bind refused: max_binds reached", acct.Username,
			logging.Fields{"systemId": b.SystemID, "ip": s.ip, "maxBinds": acct.SMPPMaxBinds})
		s.send(pdu.BindResp(respID, pdu.ESME_RALYBND, pd.Header.SequenceNumber, b.SystemID))
		return
	}
	if p.lock != nil {
		p.lock.Reset("smpp:" + b.SystemID) // successful auth clears the fail counter (keeps level)
	}
	s.send(pdu.BindResp(respID, pdu.ESME_ROK, pd.Header.SequenceNumber, b.SystemID))
	// [SCOPE P13-LOG] A bind is a login, so it is worth a real record rather
	// than the console line it used to be (PO 2026-09-05). Scoped to the
	// account, so it lands in that customer's own daily file as well as the
	// system one: who connected, from where, in which mode, on which slot —
	// enough to answer "when did this client last connect" after the console
	// has scrolled away, and enough for the customer to see it themselves.
	p.logBind(log, "bind_ok", "client bound", acct.Username,
		logging.Fields{"systemId": b.SystemID, "ip": s.ip, "mode": modeName(mode), "bindSlot": s.slot})
	p.captureBind(log, acct.Username, modeName(mode), s.ip, pdu.ESME_ROK, s.slot, "")
	// Server-initiated keepalive [SCOPE P2-KEEPALIVE, PO 2026-07-27]: P2 used
	// to only RESPOND to the client's own enquire_link; it now also proactively
	// pings the client at its configured interval (accounts.smpp_config.
	// enquire_link_sec, default 30s), mirroring P3's existing client-side
	// pattern. Stops when the session closes (s.done).
	go p.enquireLinkLoop(s)
}

// ── binds that may no longer be held ────────────────────────────────────────
//
// A bind is checked when it is made. Until 2026-10-08 it was never looked at
// again: an account switched off, suspended, or with SMPP turned off kept
// every bind it had (its submits were refused, the bind stayed up — and a
// receiver bind went on being handed receipts and incoming messages); a bind
// limit lowered from 4 to 1 left four; a password changed, a bind mode taken
// away or an address taken off the allow list changed nothing for whoever was
// already connected. Only a restart of this role ended them. watchBinds looks
// at every bind every few seconds with the account as it is NOW and ends the
// ones that could not be made today.

// bindCheckEvery is how often live binds are checked against their accounts.
var bindCheckEvery = 5 * time.Second

// unbindGrace is how long a client gets to answer our unbind before the
// connection is closed anyway.
var unbindGrace = 2 * time.Second

// ipPermitted is the account's IP allow-list as the submit path reads it
// (pipeline ip-allow): no list, or a list that is for the portal only, lets
// every address in.
func ipPermitted(a *refdata.Account, ip string) bool {
	if a == nil || len(a.AllowedIPs) == 0 || a.AccessScope == "portal" {
		return true
	}
	return a.IPAllowed(ip)
}

// notBindable says why a bind of this mode from this address could not be
// made on the account as it is now, "" when it could. was is the account as
// it was when the bind was made.
func notBindable(was, now *refdata.Account, mode bindMode, ip string) string {
	switch {
	case now == nil:
		return "the account is gone"
	case now.Suspended():
		return "the account is switched off or suspended"
	case !now.SMPP:
		return "SMPP is switched off for the account"
	case expired(now):
		return "the account has expired"
	case now.AccessScope == "portal":
		return "the account is for the portal only"
	case !modeAllowed(now, mode):
		return "this bind mode is no longer allowed for the account"
	case !ipPermitted(now, ip):
		return "this address is no longer on the account's allow list"
	case was != nil && was.SMPPPasswordHash != now.SMPPPasswordHash:
		return "the account's password was changed"
	}
	if id := deployment.ID(); id != "" && !now.Deployments.Allows(id) {
		return "the account is no longer served by this gateway"
	}
	return ""
}

// watchBinds ends the binds that could not be made today. See above.
func (p *Process) watchBinds(ctx context.Context) {
	t := time.NewTicker(bindCheckEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.checkBinds()
		}
	}
}

// checkBinds is one pass of watchBinds.
func (p *Process) checkBinds() {
	if p.ref == nil {
		return
	}
	for id, ss := range p.binds.bound() {
		now, ok := p.ref.AccountByID(id)
		if !ok || now == nil {
			// Not in the reference data. A deleted account — or a reload that
			// went wrong: wait for three checks in a row before believing it.
			for _, s := range ss {
				if s.unknown++; s.unknown >= 3 {
					p.endBind(s, "the account is gone")
				}
			}
			continue
		}
		kept := ss[:0]
		for _, s := range ss {
			s.unknown = 0
			if why := notBindable(s.acct, now, s.mode, s.ip); why != "" {
				p.endBind(s, why)
				continue
			}
			kept = append(kept, s)
		}
		// Over the account's bind limit: the newest go, the oldest stay.
		if max := now.SMPPMaxBinds; max > 0 && len(kept) > max {
			sort.SliceStable(kept, func(i, j int) bool { return kept[i].boundAt.Before(kept[j].boundAt) })
			for _, s := range kept[max:] {
				p.endBind(s, fmt.Sprintf("the account may hold %d bind(s) and held %d", max, len(kept)))
			}
		}
	}
}

// endBind ends one bind from our side: it is told to unbind, and the
// connection is closed when it answers or after unbindGrace. Safe to call
// more than once.
func (p *Process) endBind(s *session, why string) {
	if s == nil || !s.ending.CompareAndSwap(false, true) {
		return
	}
	if s.acct != nil {
		p.logBindWarn(logging.New(p.Name()), "bind_dropped", "bind ended by the gateway: "+why, s.acct.Username,
			logging.Fields{"ip": s.ip, "mode": modeName(s.mode), "bindSlot": s.slot, "reason": why})
	}
	p.unbindMu.Lock()
	seq := wrapSeq(&p.unbindSeq, seqUnbind)
	p.unbindMu.Unlock()
	s.send(pdu.UnbindPDU(seq))
	// Read here, not in the goroutine: a test sets unbindGrace for its own
	// gateway while a goroutine of the test before is still waiting (the race
	// detector's only finding in the go-live run of 14 Oct 2026).
	grace := unbindGrace
	go func() {
		select {
		case <-s.done: // it answered (or went on its own): serve() has cleaned up
		case <-time.After(grace):
		}
		_ = s.conn.Close()
	}()
}

// liveAccount is the session's account as the reference cache has it NOW.
//
// A bind used to keep the copy it authenticated with for the whole life of the
// connection, and every reload of the reference data builds new Account
// values — so a changed deployment rate, a lifted or new abuse penalty, a
// company cap or a suspension reached a connected client only after it bound
// again (production, 2026-10-06: 2000/s set on the deployment, the client
// still held to the 10/s it had at bind). The HTTP edge resolves the account
// on every request; this does the same for SMPP, by id, at the cost of one
// map read. An account gone from the cache (deleted) keeps its bind-time copy.
func (p *Process) liveAccount(s *session) *refdata.Account {
	if s == nil || s.acct == nil {
		return nil
	}
	if p.ref != nil {
		if a, ok := p.ref.AccountByID(s.acct.ID); ok && a != nil {
			return a
		}
	}
	return s.acct
}

func expired(a *refdata.Account) bool {
	return a.AccountExpiry != nil && a.AccountExpiry.Before(time.Now())
}

func modeAllowed(a *refdata.Account, m bindMode) bool {
	if len(a.SMPPAllowedModes) == 0 {
		return true // default: all modes
	}
	// Both spellings of each mode are accepted (2026-09-16): the portal's
	// account form writes the full words ("transceiver", "transmitter",
	// "receiver") while this check was written against the abbreviations —
	// so every account whose operator had ticked a mode could never bind, and
	// the refusal said only "bind mode not allowed".
	want := map[bindMode][]string{
		modeTX:  {"TX", "transmitter"},
		modeRX:  {"RX", "receiver"},
		modeTRX: {"TRX", "transceiver"},
	}[m]
	for _, am := range a.SMPPAllowedModes {
		for _, w := range want {
			if strings.EqualFold(strings.TrimSpace(am), w) {
				return true
			}
		}
	}
	return false
}
