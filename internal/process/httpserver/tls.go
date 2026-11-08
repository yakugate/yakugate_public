package httpserver

// TLS on the HTTP edge (PO 2026-09-08).
//
// Until now P1 served plain HTTP only, while the SMPP edge had a TLS listener.
// That mattered more after the same session added an admin credential and
// required client credentials on the status API: every one of those secrets,
// and every message body, crossed the wire in the clear.
//
// Shape, as ruled: TLS is opt-in by giving a certificate and key PATH; with no
// paths the engine serves plaintext exactly as before, so no existing
// deployment changes. `YAKU_API_TLS=off` force-disables it even when paths are
// set, for a deployment that wants plaintext deliberately. Certificates are
// read once at start-up — renewal means a restart (ruled), so the expiry date
// is logged at start-up to make that plannable.

import (
	"crypto/tls"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/tlsconf"
)

// tlsSettings is one edge's TLS configuration, read from the environment.
type tlsSettings struct {
	addr string // "" = no TLS listener
	cert string
	key  string
	off  bool // explicit kill switch, independent of the paths
	// clientCA is a PEM bundle of the authorities allowed to issue CLIENT
	// certificates (mutual TLS, PO 2026-09-08). Unset = clients present no
	// certificate, exactly as before. Set = the TLS port requires one signed
	// by this bundle; the plaintext port is unaffected, having no TLS to
	// carry a certificate at all.
	//
	// Ruled deliberately: ANY certificate from the bundle is accepted. It
	// proves the connection came from someone we issued to; the system_id and
	// password still prove WHICH customer. Pinning a certificate to an
	// account was considered and rejected as unnecessary setup for now.
	clientCA string
}

// enabled reports whether a TLS listener should be opened at all.
func (t tlsSettings) enabled() bool {
	return !t.off && t.addr != "" && t.cert != "" && t.key != ""
}

// readTLSSettings reads one edge's TLS environment. prefix is "API" or "SMPP",
// giving YAKU_<prefix>_TLS_ADDR / _CERT / _KEY and the YAKU_<prefix>_TLS
// on/off switch.
func readTLSSettings(prefix string) tlsSettings {
	off := strings.EqualFold(strings.TrimSpace(env("YAKU_"+prefix+"_TLS", "")), "off")
	return tlsSettings{
		addr:     env("YAKU_"+prefix+"_TLS_ADDR", ""),
		cert:     env("YAKU_"+prefix+"_TLS_CERT", ""),
		key:      env("YAKU_"+prefix+"_TLS_KEY", ""),
		off:      off,
		clientCA: env("YAKU_"+prefix+"_TLS_CLIENT_CA", ""),
	}
}

// tlsConfigFor loads the key pair and builds the server config.
//
// MinVersion is set EXPLICITLY rather than left to the Go default. The default
// happens to be TLS 1.2 today, but "the language decides our transport
// security policy" is not an answer to a compliance question, and it can move
// under us on a Go upgrade.
func tlsConfigFor(t tlsSettings) (*tls.Config, time.Time, error) {
	cert, err := tls.LoadX509KeyPair(t.cert, t.key)
	if err != nil {
		return nil, time.Time{}, err
	}
	var notAfter time.Time
	if len(cert.Certificate) > 0 && cert.Leaf != nil {
		notAfter = cert.Leaf.NotAfter
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	// Mutual TLS, opt-in by CA bundle path. A bundle that cannot be read is an
	// ERROR rather than a quiet downgrade: silently accepting anonymous
	// clients because a file was missing is the one outcome nobody would spot.
	if t.clientCA != "" {
		pool, err := tlsconf.ClientCAPool(t.clientCA)
		if err != nil {
			return nil, time.Time{}, err
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, notAfter, nil
}
