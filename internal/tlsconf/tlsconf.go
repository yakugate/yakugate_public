// Package tlsconf holds the transport-security pieces both client-facing
// edges need, so the HTTP and SMPP doors cannot drift apart on what they
// accept (PO 2026-09-08, mutual TLS).
//
// Small on purpose: the listeners themselves stay in their own processes,
// because their lifecycles differ — one is an http.Server, the other a raw
// net.Listener with a bind protocol on top. What must NOT differ is what
// counts as a trusted client, and that lives here.
package tlsconf

import (
	"crypto/x509"
	"fmt"
	"os"
)

// ClientCAPool reads a PEM bundle of the authorities allowed to issue CLIENT
// certificates for mutual TLS.
//
// An unreadable or empty bundle is an ERROR, never an empty pool. Returning
// something usable-looking would let a listener come up accepting anonymous
// clients because a file was missing or mistyped — the one failure here that
// nobody would notice, since everything would appear to work.
//
// Rotation is by editing the bundle and restarting (ruled, matching the
// server-certificate posture). A revoked client certificate therefore keeps
// working until that restart, so cutting off the customer's account access is
// the immediate action and the bundle rotation is the follow-up.
func ClientCAPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("client CA bundle: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("client CA bundle %s contains no usable certificate", path)
	}
	return pool, nil
}
