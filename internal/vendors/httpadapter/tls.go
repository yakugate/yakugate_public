package httpadapter

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"strings"
)

// Resolver turns a secret reference (env://, file://, …) into its value —
// config.Secrets satisfies it.
type Resolver interface {
	ResolveString(ctx context.Context, ref string) (string, error)
}

// TLSConfig builds an endpoint's TLS settings from its tls column
// (2026-09-14): {"insecure_skip_verify": false, "server_name": "...",
// "min_version": "1.2", "ca_ref": REF, "cert_ref": REF, "key_ref": REF}.
// The *_ref values are secret references to PEM text. nil, nil = the
// column says nothing, so the system defaults apply.
func TLSConfig(ctx context.Context, m map[string]any, res Resolver) (*tls.Config, error) {
	if len(m) == 0 {
		return nil, nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	set := false
	if v, _ := m["insecure_skip_verify"].(bool); v {
		cfg.InsecureSkipVerify, set = true, true
	}
	if v, _ := m["server_name"].(string); strings.TrimSpace(v) != "" {
		cfg.ServerName, set = strings.TrimSpace(v), true
	}
	if v, _ := m["min_version"].(string); v != "" {
		switch strings.TrimSpace(v) {
		case "1.2":
			cfg.MinVersion = tls.VersionTLS12
		case "1.3":
			cfg.MinVersion = tls.VersionTLS13
		default:
			return nil, fmt.Errorf("tls.min_version must be 1.2 or 1.3")
		}
		set = true
	}
	pem := func(key string) (string, error) {
		ref, _ := m[key].(string)
		if strings.TrimSpace(ref) == "" {
			return "", nil
		}
		if res == nil {
			return "", fmt.Errorf("tls.%s needs a secrets store", key)
		}
		v, err := res.ResolveString(ctx, strings.TrimSpace(ref))
		if err != nil {
			return "", fmt.Errorf("tls.%s: %v", key, err)
		}
		return v, nil
	}
	ca, err := pem("ca_ref")
	if err != nil {
		return nil, err
	}
	if ca != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(ca)) {
			return nil, errors.New("tls.ca_ref holds no PEM certificate")
		}
		cfg.RootCAs, set = pool, true
	}
	cert, err := pem("cert_ref")
	if err != nil {
		return nil, err
	}
	key, err := pem("key_ref")
	if err != nil {
		return nil, err
	}
	if (cert == "") != (key == "") {
		return nil, errors.New("tls.cert_ref and tls.key_ref go together")
	}
	if cert != "" {
		pair, err := tls.X509KeyPair([]byte(cert), []byte(key))
		if err != nil {
			return nil, fmt.Errorf("tls client certificate: %v", err)
		}
		cfg.Certificates, set = []tls.Certificate{pair}, true
	}
	if !set {
		return nil, nil
	}
	return cfg, nil
}
