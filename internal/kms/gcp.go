package kms

import (
	"context"
	"fmt"
	"strings"

	kmsapi "cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"google.golang.org/api/option"
)

// GCPScheme is the ref scheme this backend registers under, matching
// kms-provider-connections.md: gcp-kms://projects/P/locations/L/keyRings/R/cryptoKeys/K
const GCPScheme = "gcp-kms"

// GCPKeyManager wraps a Cloud KMS client. One instance is shared across every
// tenant configured on GCP — the key ref itself (not the client) carries the
// project/location/ring/key, so one client can serve many tenants' KEKs as
// long as the credential behind it has access to each. A tenant needing a
// DIFFERENT credential (true per-tenant service account, not just a
// per-tenant key within one shared project) gets its own GCPKeyManager
// instance instead — see NewGCPKeyManager's credential parameter.
type GCPKeyManager struct {
	client *kmsapi.KeyManagementClient
}

// NewGCPKeyManager builds a client. credentialJSON is the service account key
// (already resolved via internal/secrets — e.g. from a credential_ref on
// tenant_kms_config or kms_providers.config); nil/empty uses Application
// Default Credentials (Workload Identity on GKE, the preferred production
// path per kms-provider-connections.md §2.3 — no credential ever touches
// Yakugate's own config in that case).
func NewGCPKeyManager(ctx context.Context, credentialJSON []byte) (*GCPKeyManager, error) {
	var opts []option.ClientOption
	if len(credentialJSON) > 0 {
		opts = append(opts, option.WithCredentialsJSON(credentialJSON))
	}
	client, err := kmsapi.NewKeyManagementClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("%w: gcp kms client: %v", ErrUnavailable, err)
	}
	return &GCPKeyManager{client: client}, nil
}

// Close releases the underlying gRPC connection.
func (g *GCPKeyManager) Close() error { return g.client.Close() }

// resourceName strips the gcp-kms:// scheme, leaving the bare Cloud KMS
// resource name the API expects (projects/P/locations/L/keyRings/R/cryptoKeys/K).
func resourceName(keyRef string) (string, error) {
	_, rest := schemeOf(keyRef)
	if rest == "" || !strings.HasPrefix(rest, "projects/") {
		return "", fmt.Errorf("kms: malformed gcp-kms ref %q (want gcp-kms://projects/.../cryptoKeys/...)", keyRef)
	}
	return rest, nil
}

// Encrypt calls Cloud KMS Encrypt — the KEK plaintext never leaves Google's
// side; only ciphertext crosses the wire back to this process.
func (g *GCPKeyManager) Encrypt(ctx context.Context, keyRef string, plaintext []byte) ([]byte, error) {
	name, err := resourceName(keyRef)
	if err != nil {
		return nil, err
	}
	resp, err := g.client.Encrypt(ctx, &kmspb.EncryptRequest{Name: name, Plaintext: plaintext})
	if err != nil {
		return nil, fmt.Errorf("%w: gcp kms encrypt: %v", ErrUnavailable, err)
	}
	return resp.Ciphertext, nil
}

// Decrypt calls Cloud KMS Decrypt.
func (g *GCPKeyManager) Decrypt(ctx context.Context, keyRef string, ciphertext []byte) ([]byte, error) {
	name, err := resourceName(keyRef)
	if err != nil {
		return nil, err
	}
	resp, err := g.client.Decrypt(ctx, &kmspb.DecryptRequest{Name: name, Ciphertext: ciphertext})
	if err != nil {
		return nil, fmt.Errorf("%w: gcp kms decrypt: %v", ErrUnavailable, err)
	}
	return resp.Plaintext, nil
}

// GenerateDataKey: Cloud KMS has no native "generate and wrap in one call"
// the way AWS KMS does (kms-provider-connections.md §2.3) — the caller
// (internal/crypto) already generates the DEK locally via crypto/rand and
// calls Encrypt to wrap it; that path needs no change for this backend.
func (g *GCPKeyManager) GenerateDataKey(ctx context.Context, keyRef string) ([]byte, []byte, error) {
	return nil, nil, ErrNotSupported
}
