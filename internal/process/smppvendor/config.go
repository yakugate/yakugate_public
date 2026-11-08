// A vendor's settings: read from the vendors table, and which of them need
// a new bind and which reach a running session.

package smppvendor

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/attemptcap"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
	dlrsvc "github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/queues"
	"github.com/yakugate/yakugate/lib/encoding"
	"github.com/yakugate/yakugate/lib/smpp/pdu"
)

// vendorConn is one SMPP vendor's connection config, parsed from
// queue_config + protocol_config + the vendors table's own tuning columns.
type vendorConn struct {
	// submitTPS is THIS DEPLOYMENT's share of the vendor's rate
	// (vendors.deployments[].mt_throughput, config/0124): the most PDUs a second we may
	// hand this vendor. 0 = no limit, which is every vendor's current value
	// and the behaviour before this column was read at all.
	//
	// PER PROCESS, by ruling (PO 2026-09-09), not a shared bucket: N running
	// smpp-vendor processes therefore send N times this rate. That is a real
	// consequence, so a vendor with a limit says so at start-up rather than
	// leaving anyone to discover it from the carrier.
	submitTPS int
	id        int64
	cid       string
	// statusMap is this vendor's own words for the eight delivery states
	// (dlr_config.status_map, config/0120); nil = it uses the standard ones.
	statusMap map[string]string
	// ownIntermediateDLR: this vendor sends its own ACCEPTD/ENROUTE, so the
	// gateway does not send a submit notice of its own for it.
	ownIntermediateDLR bool
	// The vendor's receipt settings (dlr_config, D-136): sendsNoReceipts =
	// it sends none, so nothing waits for one; receiptWaitHours = how long a
	// sent message waits for its receipt (0 = the default).
	sendsNoReceipts  bool
	receiptWaitHours int
	// firstReceiptCloses: the vendor sends one final receipt per long
	// message, so the first part's final closes it (4 Nov 2026).
	firstReceiptCloses bool

	hosts    []string // one or more "host:port" endpoints; sessions round-robin across them
	sessions int      // concurrent binds to open (>=1)

	systemID string
	password string
	mode     uint32
	window   int

	bindOpt   pdu.BindOptions
	submitOpt pdu.SubmitOptions

	dialTimeout      time.Duration
	respTimeout      time.Duration // bounds Expired() staleness for outstanding sends
	enquireLinkEvery time.Duration
	retryInterval    time.Duration // base reconnect backoff (doubles up to 30s cap, as before)
	// downAfter / recheckAfter (config/0103, PO 2026-09-14): how long with no
	// bound session (or with the error breaker open) before routing counts
	// this vendor as down, and while down, how often to try binding again.
	downAfter, recheckAfter time.Duration
	maxReconnect            int // 0 = unlimited (unchanged default behavior)
	throttlePause           time.Duration
	batchSize               int

	multipartMode string // "udh" (default) | "tlv"
	gsm7Packing   string // "unpacked" (default: 8-bit, one septet an octet; "gsm" is read as this) | "packed" (7-bit): MT sent and MO read so
	// wire says, per encoding, what this vendor is sent it as and with which
	// data_coding (protocol_config.encodings, PO 2026-09-21). Defaults send
	// every encoding as itself — Latin-1 as Latin-1 (3), which it never was.
	wire         encoding.WireRules
	validityMode string // "relative" (default) | "absolute" | "off" — submit_sm validity_period
	// split is how the vendor's rate is shared between its queues (issue
	// 22): fair (default) or priority. It took the place of fair_share_by.
	split          queues.Mode
	msgidTransform string // "" | "none" | "hex2dec" | "dec2hex" | "trim:<n>" — DLR vendor-msg-id normalization

	cb circuitBreakerConfig

	tlsConfig *tls.Config // nil = plaintext, unchanged default

	fingerprint string // raw-config signature; any change here restarts every session
}

// bindMode maps a config bind_mode string to the SMPP command id.
func bindMode(s string) uint32 {
	switch s {
	case "TX", "tx", "transmitter":
		return pdu.BindTransmitter
	case "RX", "rx", "receiver":
		return pdu.BindReceiver
	default:
		return pdu.BindTransceiver // TRX default (needed to receive DLRs)
	}
}

// queueCfgJSON is the connection-endpoint shape of vendors.queue_config.
// host may be a single string (unchanged/legacy) or an array of strings
// (multi-host, item 1) — sessions round-robin across whichever it is.
type queueCfgJSON struct {
	Host     json.RawMessage `json:"host"`
	Port     int             `json:"port"`
	SystemID string          `json:"system_id"`
	Password string          `json:"password"`
	BindMode string          `json:"bind_mode"`
	Window   int             `json:"window"`
}

// protocolCfgJSON is the extended per-vendor SMPP config shape read from
// vendors.protocol_config — the unified JSON slot migration 0050 introduced
// (previously smpp_config/http_config, dead columns nothing read). This is
// the Hadal-derived scope: bind identity, addressing, multipart mode, GSM-7
// packing, fair share, circuit breaker, and TLS. [SCOPE P3-HADAL]
type protocolCfgJSON struct {
	SystemType           string `json:"system_type"`
	InterfaceVersion     *int   `json:"interface_version"`
	AddressRange         string `json:"address_range"`
	SourceTON            *int   `json:"source_ton"`
	SourceNPI            *int   `json:"source_npi"`
	DestTON              *int   `json:"dest_ton"`
	DestNPI              *int   `json:"dest_npi"`
	MaxReconnectAttempts *int   `json:"max_reconnect_attempts"`
	MultipartMode        string `json:"multipart_mode"`
	GSM7Packing          string `json:"gsm7_packing"`
	// Encodings: per encoding, what to send it as and its data_coding
	// (encoding.ParseWireRules). Absent = every encoding as itself.
	Encodings    json.RawMessage `json:"encodings"`
	ValidityMode string          `json:"validity_mode"`
	// QueueSplit is how the rate is shared between the vendor's queues:
	// "fair" (default) or "priority" (issue 22). fair_share_by (per account)
	// is no longer read: the take splits by short code.
	QueueSplit     string `json:"queue_split"`
	CircuitBreaker *struct {
		ThresholdPct int `json:"threshold_pct"`
		WindowSec    int `json:"window_sec"`
		CooldownSec  int `json:"cooldown_sec"`
		MinSamples   int `json:"min_samples"`
	} `json:"circuit_breaker"`
	TLS *struct {
		Enabled            bool   `json:"enabled"`
		MinVersion         string `json:"min_version"`
		MaxVersion         string `json:"max_version"`
		ServerName         string `json:"server_name"`
		RejectUnauthorized *bool  `json:"reject_unauthorized"`
		MutualTLS          bool   `json:"mutual_tls"`
		CARef              string `json:"ca_ref"`
		CertRef            string `json:"cert_ref"`
		KeyRef             string `json:"key_ref"`
	} `json:"tls"`
}

// loadVendors reads active SMPP vendors and parses their connection config —
// queue_config (endpoint/credentials) + protocol_config (the Hadal-derived
// extended settings) + the vendors table's own tuning columns (item 2: these
// were schema-only before this pass; every one is now actually read).
//
// ok is false when the vendors could not be read at all (the query, or the
// rows, failed): the caller must keep what it has, not take it as "no
// vendors". A vendor whose row was read but whose secrets or TLS material
// could not be resolved this time keeps the settings it was last loaded with,
// if it had any, so its running sessions are not stopped for it.
func (p *Process) loadVendors(ctx context.Context) (vendors []vendorConn, ok bool) {
	// The most times a message may be sent, for the claim (attemptcap): read
	// with the vendors, so a changed max_attempts applies at the next reload.
	attemptcap.Apply(ctx, p.engine().VendorSends, p.queue, p.log.Printf)
	// The vendors' companies, for matching receipts (vendorgroup).
	if err := p.groups.Load(ctx, p.traffic.VendorCompanies()); err != nil {
		p.log.Printf("vendor companies for receipts: %v", err)
	}
	// Only the vendors assigned to THIS deployment (config/0122): this
	// deployment's roles hold their binds and open their queues, which is
	// what a carrier that allowlists one source IP requires. A vendor
	// assigned nowhere is bound everywhere, as before. The read is the
	// DAL's (dal.VendorRows).
	var out []vendorConn
	// last is what a vendor was loaded with before, for one whose secrets
	// cannot be resolved this time.
	last := func(id int64) (vendorConn, bool) {
		p.mu.Lock()
		defer p.mu.Unlock()
		v, ok := p.current[id]
		return v, ok
	}
	err := p.traffic.VendorRows().SMPPVendors(ctx, deployment.ID(), func(r dal.SMPPVendorRow, err error) {
		if err != nil {
			p.log.Printf("scan vendor row: %v", err)
			return
		}
		v := vendorConn{id: r.ID}
		cid, queueJSON, protocolJSON, credRef, msgidTransform := r.CID, r.QueueConfig, r.ProtocolConfig, r.CredentialRef, r.MsgIDTransform
		deployJSON := r.Deployments
		sessions, windowSize, retrySec, enquireSec, respSec := r.Sessions, r.WindowSize, r.RetrySec, r.EnquireSec, r.RespSec
		throttleSec, batchSize := r.ThrottleSec, r.BatchSize
		downSec, recheckSec := r.DownAfterSec, r.RecheckAfterSec
		acceptedJSON, dlrJSON := r.Encodings, r.DLRConfig
		ownIntermediate := r.OwnIntermediate
		v.cid = cid
		// This vendor's own words for the eight delivery states, if it uses
		// odd ones (config/0120): {"DLVD":"DELIVRD","2":"ENROUTE"}. Until now
		// only an HTTP vendor could have a map, so an SMSC saying anything
		// unusual had its receipts dropped.
		var dc struct {
			StatusMap map[string]string `json:"status_map"`
		}
		_ = json.Unmarshal([]byte(dlrJSON), &dc)
		v.statusMap = dc.StatusMap
		v.sendsNoReceipts, v.receiptWaitHours = dlrsvc.ReceiptSettings(dlrJSON)
		// One final receipt per long message, on any part: the first closes
		// it (dlr_config.first_receipt_closes, Saleem 4 Nov 2026).
		v.firstReceiptCloses = dlrsvc.FirstReceiptCloses(dlrJSON)
		// The record store's path reads it by vendor (no Redis).
		p.svc.SetFirstReceiptCloses(v.id, v.firstReceiptCloses)
		// The vendor sends its own ACCEPTD/ENROUTE, so the gateway does not
		// add one of its own at submit (PO 2026-09-23: the vendor's wins).
		v.ownIntermediateDLR = ownIntermediate != 0
		v.msgidTransform = msgidTransform
		// This gateway's share of the vendor's rate (config/0124). The
		// account-style entry is the only source now — submit_throughput is
		// retired — and a vendor with no rate here is uncapped on this box.
		deps, _ := deployment.Parse(deployJSON)
		v.submitTPS = deps.Rate(deployment.ID(), deployment.MT)
		v.downAfter = time.Duration(downSec) * time.Second
		v.recheckAfter = time.Duration(recheckSec) * time.Second
		var qc queueCfgJSON
		_ = json.Unmarshal([]byte(queueJSON), &qc)
		hosts := parseHosts(qc.Host, qc.Port)
		if len(hosts) == 0 {
			p.log.ForVendor(cid).Errorf("config", "no host/port in queue_config; skipping")
			return
		}
		v.hosts = hosts
		v.sessions = sessions
		if v.sessions < 1 {
			v.sessions = 1
		}
		v.mode = bindMode(qc.BindMode)
		v.window = windowSize
		if qc.Window > 0 {
			v.window = qc.Window // explicit per-vendor override wins over the column
		}
		if v.window <= 0 {
			v.window = 10
		}
		v.dialTimeout = 10 * time.Second
		v.respTimeout = time.Duration(respSec) * time.Second
		v.enquireLinkEvery = time.Duration(enquireSec) * time.Second
		v.retryInterval = time.Duration(retrySec) * time.Second
		// throttle_pause_sec: column first; a zero/absent column falls back to
		// YAKU_SMPP_THROTTLE_PAUSE_SEC, then 1s. Closes the P3 backlog docs
		// drift (2026-08-19): the env knob was documented in the spec/backlog
		// but never read — the docs and the register now both hold.
		if throttleSec <= 0 {
			if es, err := strconv.Atoi(os.Getenv("YAKU_SMPP_THROTTLE_PAUSE_SEC")); err == nil && es > 0 {
				throttleSec = es
			} else {
				throttleSec = 1
			}
		}
		v.throttlePause = time.Duration(throttleSec) * time.Second
		v.batchSize = batchSize

		var pc protocolCfgJSON
		_ = json.Unmarshal([]byte(protocolJSON), &pc)
		v.bindOpt = pdu.BindOptions{SystemType: pc.SystemType, AddressRange: pc.AddressRange}
		if pc.InterfaceVersion != nil {
			v.bindOpt.InterfaceVersion = byte(*pc.InterfaceVersion)
		}
		v.submitOpt = pdu.DefaultSubmitOptions
		if pc.SourceTON != nil {
			v.submitOpt.SourceTON = byte(*pc.SourceTON)
		}
		if pc.SourceNPI != nil {
			v.submitOpt.SourceNPI = byte(*pc.SourceNPI)
		}
		if pc.DestTON != nil {
			v.submitOpt.DestTON = byte(*pc.DestTON)
		}
		if pc.DestNPI != nil {
			v.submitOpt.DestNPI = byte(*pc.DestNPI)
		}
		if pc.MaxReconnectAttempts != nil {
			v.maxReconnect = *pc.MaxReconnectAttempts
		}
		// How a long message goes to this vendor (PO 2026-09-21, #20):
		//   udh (default)       — parts, each with a concatenation header;
		//   tlv / sar           — parts, each with sar_* TLVs;
		//   payload / message_payload — ONE submit_sm, the whole text in
		//                         message_payload; the SMSC splits it.
		// Until 2026-09-21 all four TLV words sent SAR parts, though the
		// 2026-09-17 ruling read them all as message_payload. The TLVs only
		// exist from SMPP 3.4: a vendor bound as 3.3 gets UDH parts whatever
		// the setting says.
		v.multipartMode = encoding.VendorMultipartMode(pc.MultipartMode)
		if v.multipartMode != "udh" && v.bindOpt.InterfaceVersion != 0 && v.bindOpt.InterfaceVersion < 0x34 {
			p.log.ForVendor(v.cid).Warnf("config", "multipart_mode %s needs SMPP 3.4, bind declares 0x%02x — using UDH parts", v.multipartMode, v.bindOpt.InterfaceVersion)
			v.multipartMode = "udh"
		}
		// validity_mode (2026-09-14): what submit_sm says about how long the
		// SMSC may keep trying. relative (default) = the time left until the
		// message expires here; absolute = that instant in UTC; off = empty,
		// the SMSC's own default (for a vendor that rejects or mishandles it).
		switch strings.ToLower(strings.TrimSpace(pc.ValidityMode)) {
		case "absolute":
			v.validityMode = "absolute"
		case "off", "none":
			v.validityMode = "off"
		default:
			v.validityMode = "relative"
		}
		v.gsm7Packing = gsm7PackingOf(pc.GSM7Packing)
		var probs []string
		// Both settings: the portal's tick-list (vendors.encodings) and the
		// detailed rules (protocol_config.encodings), which win where set.
		v.wire, probs = encoding.ParseVendorEncodings(pc.Encodings, json.RawMessage(acceptedJSON))
		for _, pr := range probs {
			p.log.ForVendor(v.cid).Warnf("config", "%s — using the default for it", pr)
		}
		if pc.CircuitBreaker != nil {
			v.cb = circuitBreakerConfig{
				ThresholdPct: pc.CircuitBreaker.ThresholdPct,
				WindowSec:    pc.CircuitBreaker.WindowSec,
				CooldownSec:  pc.CircuitBreaker.CooldownSec,
				MinSamples:   pc.CircuitBreaker.MinSamples,
			}
		}

		systemID, password, ok := p.resolveCredentials(ctx, credRef, qc.SystemID, qc.Password, cid)
		if !ok {
			// Secrets resolution failed: skip this vendor until fixed — but a
			// vendor already bound keeps its bind on what it had.
			if prev, had := last(v.id); had {
				p.log.ForVendor(cid).Warnf("config", "login not resolved; its sessions keep the settings they were loaded with until it is fixed")
				out = append(out, prev)
			}
			return
		}
		v.systemID, v.password = systemID, password

		tlsCfg, err := resolveTLS(ctx, pc.TLS)
		if err != nil {
			p.log.ForVendor(cid).Errorf("config", "TLS config: %v; skipping until fixed", err)
			if prev, had := last(v.id); had {
				out = append(out, prev)
			}
			return
		}
		v.tlsConfig = tlsCfg

		// Only what a live connection cannot take without binding again
		// (2026-09-29, Saleem: "only necessary settings make a rebind; things
		// like tps should be enforced when data refreshes without rebind").
		// Everything else — rate, throttle pause, batch size, message-id
		// transform, validity, GSM-7 packing, encodings, multipart mode,
		// TON/NPI, fair share, circuit breaker, status words — is read by the
		// running session through withLive at its next tick.
		tlsJSON, _ := json.Marshal(pc.TLS)
		v.fingerprint = bindFingerprint(v, tlsJSON)
		out = append(out, v)
	})
	// A query that failed, or a connection lost half way through the rows (a
	// list cut short, not a smaller set of vendors): the sessions are kept.
	if err != nil {
		p.log.Printf("load vendors: %v", err)
		return nil, false
	}
	// How each vendor's rate is shared between its queues (config/0161,
	// issue 22): live, like the rate.
	modes := queues.SplitModes(ctx, p.engine().VendorSends)
	for i := range out {
		out[i].split = modes[out[i].cid]
	}
	return out, true
}

// bindFingerprint is what a connection was bound with: hosts, bind type,
// login, system type, address range, SMPP version, TLS, window size, the
// keepalive and the response timeout. A change to any of these restarts the
// bind; nothing else does.
func bindFingerprint(v vendorConn, tlsJSON []byte) string {
	return fmt.Sprintf("%v|%d|%s|%s|%s|%s|%d|%s|%d|%s|%s",
		v.hosts, v.mode, v.systemID, v.password, v.bindOpt.SystemType, v.bindOpt.AddressRange,
		v.bindOpt.InterfaceVersion, tlsJSON, v.window, v.enquireLinkEvery, v.respTimeout)
}

// withLive returns the vendor's latest loaded settings for a running session,
// as long as its connection settings are the ones this session bound with. If
// they changed, the session is about to be restarted by reconcile, and it
// keeps what it bound with until then.
func (p *Process) withLive(v vendorConn) vendorConn {
	p.mu.Lock()
	cur, ok := p.current[v.id]
	p.mu.Unlock()
	if !ok || cur.fingerprint != v.fingerprint {
		return v
	}
	return cur
}
