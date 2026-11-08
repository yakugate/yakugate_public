package crypto

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/yakugate/yakugate/internal/kms"
	"github.com/yakugate/yakugate/internal/secrets"
)

// Config is the crypto env surface. [SCOPE P1-CRYPTO]
type Config struct {
	KEKRef string   // YAKU_CRYPTO_KEK_REF
	AtRest bool     // YAKU_CRYPTO_AT_REST=on
	Fields []string // YAKU_CRYPTO_FIELDS (default body)
}

// LoadConfig reads the crypto settings from the environment.
func LoadConfig() Config {
	fields := strings.Split(env("YAKU_CRYPTO_FIELDS", "body"), ",")
	return Config{
		KEKRef: env("YAKU_CRYPTO_KEK_REF", "file://configs/dev-kek"),
		AtRest: strings.EqualFold(env("YAKU_CRYPTO_AT_REST", "off"), "on"),
		Fields: fields,
	}
}

// kmsConfig is the multi-tenant KMS backend env surface. [SCOPE P1-KMS-MULTITENANT]
type kmsConfig struct {
	gcpEnabled  bool
	gcpCredsRef string // secrets ref to a service-account JSON; "" = Application Default Credentials
}

func loadKMSConfig() kmsConfig {
	return kmsConfig{
		gcpEnabled:  strings.EqualFold(env("YAKU_KMS_GCP_ENABLED", "off"), "on"),
		gcpCredsRef: env("YAKU_KMS_GCP_CREDENTIALS_REF", ""),
	}
}

// buildKeyManagers registers the KMS backends into a fresh Registry: HTTP
// key services whenever the store can read them, plus Google Cloud KMS when
// enabled by env (registerGCP). [SCOPE P1-KMS-MULTITENANT]
func buildKeyManagers(ctx context.Context, reg *secrets.Registry, ring Keyring) (*kms.Registry, *kms.HTTPKeyManager) {
	registry := kms.NewRegistry()
	// HTTP key services (PO 2026-09-13): always on when the store can read
	// providers. It does nothing until a company is assigned to one.
	var httpMgr *kms.HTTPKeyManager
	if src, ok := ring.(kms.ProviderSource); ok {
		var sr kms.SecretResolver
		if reg != nil {
			sr = reg
		}
		httpMgr = kms.NewHTTPKeyManager(src, sr)
		registry.WithManager(kms.HTTPScheme, httpMgr)
	}
	return registry, httpMgr
}

// registerGCP adds Google Cloud KMS when YAKU_KMS_GCP_ENABLED=on. A backend
// that is enabled but fails to start is logged and left out; a company
// assigned to it is then REFUSED (ErrNoKEK), never sealed under another key.
func registerGCP(ctx context.Context, reg *secrets.Registry, registry *kms.Registry) {
	cfg := loadKMSConfig()
	if !cfg.gcpEnabled {
		return
	}
	var credJSON []byte
	if cfg.gcpCredsRef != "" {
		if reg == nil {
			log.Printf("[crypto] YAKU_KMS_GCP_CREDENTIALS_REF set but no secrets registry available — GCP KeyManager NOT registered")
			return
		}
		b, err := reg.Resolve(ctx, cfg.gcpCredsRef)
		if err != nil {
			log.Printf("[crypto] gcp kms credentials ref %s failed: %v — GCP KeyManager NOT registered", cfg.gcpCredsRef, err)
			return
		}
		credJSON = b
	}
	// credJSON stays nil when gcpCredsRef is empty — NewGCPKeyManager then uses
	// Application Default Credentials (Workload Identity on GKE), the
	// preferred production path per kms-provider-connections.md §2.3.
	mgr, err := kms.NewGCPKeyManager(ctx, credJSON)
	if err != nil {
		log.Printf("[crypto] gcp kms client init failed: %v — GCP KeyManager NOT registered", err)
		return
	}
	registry.WithManager(kms.GCPScheme, mgr)
	log.Printf("[crypto] GCP Cloud KMS registered (%s://)", kms.GCPScheme)
}

// FromEnv builds a Service from the environment, resolving the gateway KEK
// through the secrets registry. With no gateway KEK and at-rest encryption
// OFF it still builds a Service, without a default key: companies assigned
// to a KMS provider are encrypted through it, everything else stays plain.
// With encryption ON and no usable key it returns the error — the caller must
// then refuse traffic (Broken), never fall back to plain text.
//
// In DEV, a missing file:// KEK is generated once and a loud warning is logged:
// a dev KEK sitting next to the data protects nothing, and must never reach
// production. Any other provider (env://, vault://) is never auto-created.
// Only on a first install, though (2026-10-18): once a data key is sealed
// under a gateway key, a missing key file is reported and NOT made new — see
// ensureDevKEK.
//
// Multi-tenant KMS [SCOPE P1-KMS-MULTITENANT]: when ring ALSO implements
// TenantKMSLookup (internal/store/mysql.Store does, reading tenant_kms_config)
// this wires WithTenantKMS automatically — no separate call site needed in any
// of the five process definition.go files that already call FromEnv. Backends
// enabled via env (today: GCP Cloud KMS, YAKU_KMS_GCP_ENABLED) are registered
// into the same Registry; a tenant whose kekRef has no registered backend
// (never configured, or the backend failed to initialize) simply falls back
// to the default KEK below — additive in every direction.
func FromEnv(ctx context.Context, ring Keyring, reg *secrets.Registry) (*Service, error) {
	cfg := LoadConfig()
	if ring == nil || reg == nil {
		return nil, nil
	}
	if err := ensureDevKEK(ctx, cfg.KEKRef, ring); err != nil {
		return nil, err
	}
	var kek []byte
	raw, err := reg.Resolve(ctx, cfg.KEKRef)
	if err == nil {
		if kek, err = decodeKEK(raw); err != nil {
			return nil, err
		}
	} else if cfg.AtRest {
		// Encryption for everyone was asked for, and there is no key to do it
		// with. The caller must refuse traffic (see Broken), not store plain.
		return nil, err
	} else {
		// No gateway key and no gateway-wide encryption. Companies assigned to
		// a KMS provider still get encrypted — they never needed this key —
		// and everything else stays plain, as before.
		log.Printf("[crypto] no gateway KEK (%s): %v — only companies with a KMS provider are encrypted", cfg.KEKRef, err)
	}
	opts := []Option{WithAtRest(cfg.AtRest, cfg.Fields)}
	var httpMgr *kms.HTTPKeyManager
	if tk, ok := ring.(TenantKMSLookup); ok {
		registry, hm := buildKeyManagers(ctx, reg, ring)
		registerGCP(ctx, reg, registry)
		httpMgr = hm
		opts = append(opts, WithTenantKMS(tk, registry))
	}
	svc, err := New(ring, kek, cfg.KEKRef, opts...)
	if svc != nil {
		svc.httpKMS = httpMgr
	}
	return svc, err
}

// Broken stands in for a Service that could not be built when encryption was
// required: every field is "to be encrypted" and every encryption fails, so
// each message is refused 503 ENCRYPTION_UNAVAILABLE. Until 2026-09-13 a
// failed start left the process with no encryptor at all — and it stored
// every message in plain text, silently.
type Broken struct{ Err error }

// ShouldEncrypt is always true: nothing may be stored unencrypted.
func (Broken) ShouldEncrypt(context.Context, string, int64) bool { return true }

// Encrypt always fails with the start-up error.
func (b Broken) Encrypt(context.Context, int64, int64, string) (string, error) {
	return "", fmt.Errorf("%w: encryption failed to start: %v", ErrNoKEK, b.Err)
}

// decodeKEK accepts a 32-byte raw key or a 64-char hex encoding of one, so a key
// file can be written either way.
func decodeKEK(raw []byte) ([]byte, error) {
	if len(raw) == dekBytes {
		return raw, nil
	}
	s := strings.TrimSpace(string(raw))
	if len(s) == dekBytes*2 {
		if b, err := hex.DecodeString(s); err == nil {
			return b, nil
		}
	}
	return nil, errKEKSize(len(raw))
}

func errKEKSize(n int) error {
	return &kekSizeError{n}
}

type kekSizeError struct{ n int }

func (e *kekSizeError) Error() string {
	return "crypto: KEK must be 32 raw bytes or 64 hex chars, got " +
		itoa(e.n) + " bytes"
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// DataKeyCounter is a Keyring that can say how many data keys are sealed
// under each key ref (data_keys.kek_ref). The MySQL config store is one.
type DataKeyCounter interface {
	DataKeysByKEK(ctx context.Context) (map[string]int64, error)
}

// ensureDevKEK generates a file:// KEK if absent — DEV convenience only, and
// only on a FIRST install (2026-10-18, go-live P8 A4).
//
// It used to make a new key whenever the file was missing: on the second
// host of a gateway, or after a path was mistyped, a new key appeared with
// one log line, and that host then sealed new data keys under a key the
// other host cannot open — its backups left those tables out, and the other
// host's could not read them. Now, when the store already holds a data key
// sealed under a gateway key (any kek_ref that is not a company's KMS
// provider), the missing file is reported at full volume and nothing is made:
// the gateway then has no gateway key, which fails closed — with at-rest
// encryption on, traffic is refused until the key file is put back. A store
// that cannot be asked counts as "has keys" (a new key could be the wrong
// one). A store that cannot count at all (not MySQL) keeps the old rule.
func ensureDevKEK(ctx context.Context, ref string, ring Keyring) error {
	if !strings.HasPrefix(ref, "file://") {
		return nil
	}
	path := strings.TrimPrefix(ref, "file://")
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	if c, ok := ring.(DataKeyCounter); ok {
		byKEK, err := c.DataKeysByKEK(ctx)
		if err != nil {
			log.Printf("[crypto] ERROR: the gateway key file %s is missing, and the data keys could not be read (%v) — NO new key is made; "+
				"put the key file back (the same file as the other deployments)", path, err)
			return nil
		}
		var sealed int64
		var under []string
		for kek, n := range byKEK {
			if isRemote(kek) {
				continue // a company's own KMS key: the gateway key never touched these
			}
			sealed += n
			under = append(under, kek)
		}
		if sealed > 0 {
			log.Printf("[crypto] ERROR: the gateway key file %s is missing, and %d data key(s) are already sealed under a gateway key (%s) — "+
				"NO new key is made: a new one would seal data the other deployments cannot open. Copy the key file from another "+
				"deployment of this gateway, or set YAKU_CRYPTO_KEK_REF to where it is", path, sealed, strings.Join(under, ", "))
			return nil
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	key := make([]byte, dekBytes)
	if _, err := rand.Read(key); err != nil {
		return err
	}
	if err := os.WriteFile(path, []byte(hex.EncodeToString(key)), 0o600); err != nil {
		return err
	}
	log.Printf("[crypto] WARNING: generated a DEV KEK at %s — this key sits next to "+
		"the data it protects and must NEVER be used in production. Set "+
		"YAKU_CRYPTO_KEK_REF to a real secret store before going live.", path)
	return nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
