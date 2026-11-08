// A vendor's login, hosts and TLS settings, resolved before a bind.

package smppvendor

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/yakugate/yakugate/internal/platform/config"
)

// resolveCredentials resolves the vendor's system id and password, each of
// which may be a secret reference (env://, file://) or a plain value.
//
// vendors.credential_ref is RETIRED (PO 2026-09-21): it pointed at a secret
// holding both values as JSON, overriding these two fields — a second way to
// do what a reference in the password field already does. A vendor that
// still has one is warned about on every reload and bound with its own
// fields; the Admin API refuses new ones.
func (p *Process) resolveCredentials(ctx context.Context, credRef, systemID, password, cid string) (string, string, bool) {
	if credRef != "" {
		p.log.ForVendor(cid).Warnf("config", "credential_ref %s is retired and ignored — put the login in the system id and password fields (each may be an env:// or file:// reference)", credRef)
	}
	sid, err := config.Secrets.Maybe(ctx, systemID)
	if err != nil {
		p.log.ForVendor(cid).Errorf("config", "system_id ref: %v", err)
		return "", "", false
	}
	pw, err := config.Secrets.Maybe(ctx, password)
	if err != nil {
		p.log.ForVendor(cid).Errorf("config", "password ref: %v", err)
		return "", "", false
	}
	return sid, pw, true
}

// parseHosts normalizes queue_config.host (a single string, or an array of
// strings for multi-host — item 1) into "host:port" endpoints.
func parseHosts(raw json.RawMessage, port int) []string {
	if len(raw) == 0 {
		return nil
	}
	var arr []string
	if json.Unmarshal(raw, &arr) == nil && len(arr) > 0 {
		out := make([]string, 0, len(arr))
		for _, h := range arr {
			if h = strings.TrimSpace(h); h != "" {
				out = append(out, fmt.Sprintf("%s:%d", h, port))
			}
		}
		return out
	}
	var s string
	if json.Unmarshal(raw, &s) == nil && strings.TrimSpace(s) != "" {
		return []string{fmt.Sprintf("%s:%d", strings.TrimSpace(s), port)}
	}
	return nil
}

// resolveTLS builds a *tls.Config from protocol_config.tls, resolving
// cert/key/ca material through the same secrets registry as vendor
// credentials (item 4.4). Returns (nil, nil) when TLS is absent/disabled —
// the plaintext default, unchanged for any vendor that doesn't opt in.
func resolveTLS(ctx context.Context, t *struct {
	Enabled            bool   `json:"enabled"`
	MinVersion         string `json:"min_version"`
	MaxVersion         string `json:"max_version"`
	ServerName         string `json:"server_name"`
	RejectUnauthorized *bool  `json:"reject_unauthorized"`
	MutualTLS          bool   `json:"mutual_tls"`
	CARef              string `json:"ca_ref"`
	CertRef            string `json:"cert_ref"`
	KeyRef             string `json:"key_ref"`
}) (*tls.Config, error) {
	if t == nil || !t.Enabled {
		return nil, nil
	}
	cfg := &tls.Config{ServerName: t.ServerName}
	if t.RejectUnauthorized != nil {
		cfg.InsecureSkipVerify = !*t.RejectUnauthorized
	}
	if v, ok := tlsVersion(t.MinVersion); ok {
		cfg.MinVersion = v
	}
	if v, ok := tlsVersion(t.MaxVersion); ok {
		cfg.MaxVersion = v
	}
	if t.MutualTLS {
		if t.CertRef == "" || t.KeyRef == "" {
			return nil, fmt.Errorf("mutual_tls enabled but cert_ref/key_ref missing")
		}
		certPEM, err := config.Secrets.Maybe(ctx, t.CertRef)
		if err != nil {
			return nil, fmt.Errorf("resolve cert_ref: %w", err)
		}
		keyPEM, err := config.Secrets.Maybe(ctx, t.KeyRef)
		if err != nil {
			return nil, fmt.Errorf("resolve key_ref: %w", err)
		}
		cert, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
		if err != nil {
			return nil, fmt.Errorf("parse client cert/key: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	if t.CARef != "" {
		caPEM, err := config.Secrets.Maybe(ctx, t.CARef)
		if err != nil {
			return nil, fmt.Errorf("resolve ca_ref: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(caPEM)) {
			return nil, fmt.Errorf("ca_ref did not contain a valid PEM certificate")
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}

func tlsVersion(s string) (uint16, bool) {
	switch strings.TrimSpace(s) {
	case "1.0":
		return tls.VersionTLS10, true
	case "1.1":
		return tls.VersionTLS11, true
	case "1.2":
		return tls.VersionTLS12, true
	case "1.3":
		return tls.VersionTLS13, true
	default:
		return 0, false
	}
}
