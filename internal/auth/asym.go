package auth

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"math/big"
)

// publicKey parses (and caches) an account's PEM public key for RS256/ES256
// verification (chunk-2 ruling 2.3, 2026-08-03). The cache is keyed on the
// PEM text itself, so a key rotation in accounts.jwt simply misses the cache
// and parses the new PEM — no invalidation hook needed.
func (a *Authenticator) publicKey(pemText string) (any, error) {
	if v, ok := a.pubKeys.Load(pemText); ok {
		return v, nil
	}
	block, _ := pem.Decode([]byte(pemText))
	if block == nil {
		return nil, errors.New("not PEM")
	}
	var key any
	var err error
	switch block.Type {
	case "RSA PUBLIC KEY": // PKCS#1
		key, err = x509.ParsePKCS1PublicKey(block.Bytes)
	default: // "PUBLIC KEY" (PKIX) — the form openssl and every JWT library emit
		key, err = x509.ParsePKIXPublicKey(block.Bytes)
	}
	if err != nil {
		return nil, err
	}
	a.pubKeys.Store(pemText, key)
	return key, nil
}

// verifyAsym checks a JWS signature for RS256/ES256. ES256 signatures arrive
// in JOSE format (r||s, exactly 64 raw bytes), NOT ASN.1/DER — passing a DER
// signature is a client bug and correctly fails here.
func verifyAsym(alg string, pub any, signing string, sig []byte) bool {
	digest := sha256.Sum256([]byte(signing))
	switch alg {
	case "RS256":
		k, ok := pub.(*rsa.PublicKey)
		if !ok {
			return false
		}
		return rsa.VerifyPKCS1v15(k, crypto.SHA256, digest[:], sig) == nil
	case "ES256":
		k, ok := pub.(*ecdsa.PublicKey)
		if !ok || len(sig) != 64 {
			return false
		}
		r := new(big.Int).SetBytes(sig[:32])
		s := new(big.Int).SetBytes(sig[32:])
		return ecdsa.Verify(k, digest[:], r, s)
	}
	return false
}
