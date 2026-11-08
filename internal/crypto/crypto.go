// Package crypto provides envelope encryption for data at rest. [SCOPE P1-CRYPTO]
//
// Envelope model: each account-month (legacy/unconfigured tenants) or each
// TENANT-window (companies onboarded onto tenant_kms_config — see [SCOPE
// P1-KMS-MULTITENANT] below) gets its own DATA key (DEK) which encrypts
// message content; the DEK itself is stored only in wrapped form, sealed by a
// KEY-ENCRYPTION key (KEK). Compromising the database therefore yields no
// plaintext, and revoking one key (crypto-erasure) makes exactly that data
// unreadable without touching anything else.
//
// Wire format (self-describing, so ciphertext carries what is needed to read it):
//
//	enc:v1:<key_ref>:<nonce_b64>:<ciphertext_b64>
//
// The KEY RULE is passthrough on read: Decrypt returns any value that does not
// begin with "enc:" unchanged. That makes the at-rest flag WRITE-only — turning
// it on encrypts new writes while every existing plaintext row keeps working,
// and turning it off does not strand data already encrypted. Mixed data is
// always readable, in both directions, with no migration step.
//
// [SCOPE P1-KMS-MULTITENANT] (eocean-multi-tenant-kms-architecture.md,
// 2026-07-28): a company (tenant) may be onboarded onto its own KEK via the
// tenant_kms_config table, backed by a real KMS (GCP Cloud KMS first) through
// internal/kms.Registry — that KEK's plaintext NEVER enters this process; the
// registry performs Encrypt/Decrypt remotely. Two things decide which path a
// given DEK uses:
//   - CREATION time (Encrypt, first use of a key ref): resolveKeyRef looks up
//     the calling message's company_id in tenant_kms_config; found+active ->
//     a per-tenant, time-windowed key ref (dek:co:<company>:<window>) wrapped
//     under that tenant's kekRef; not found (company_id=0, unlinked account,
//     or no row) -> the unchanged legacy path (dek:<account>:<YYYYMM>, wrapped
//     by the single gateway-wide file/env KEK, exactly as Phase 1 shipped).
//   - UNWRAP time (every subsequent read): the kek_ref already stored
//     alongside the wrapped DEK (data_keys.kek_ref) says which KEK sealed it;
//     wrap/unwrap check whether THAT ref's scheme has a registered
//     kms.KeyManager (remote) or not (local AES-GCM with the gateway KEK).
//     This needs no "is this tenant configured" branch at read time at all —
//     the stored kek_ref is self-describing, same principle as the ciphertext
//     wire format itself.
//
// Known simplification, not addressed here: the local wrap path binds a
// wrapped DEK to its key_ref via GCM's additional-authenticated-data (AAD),
// defending against a wrapped-DEK swapped across key refs. The remote GCP
// path (this round) does not carry that same AAD binding — Cloud KMS's own
// EncryptRequest supports additional_authenticated_data and a future pass
// should thread key_ref through as AAD there too for parity; flagged rather
// than silently assumed equivalent.
package crypto

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/kms"
)

const (
	prefix     = "enc:" // any value starting with this is ciphertext
	version    = "v1"   // bump only for a format change, never for key rotation
	dekBytes   = 32     // AES-256
	nonceBytes = 12     // GCM standard nonce
)

var (
	// ErrKeyRevoked means the DEK was revoked — crypto-erasure. The ciphertext is
	// intentionally unreadable; this is not a failure to be retried.
	ErrKeyRevoked = errors.New("crypto: data key revoked")
	// ErrMalformed means the value looked like ciphertext but could not be parsed.
	ErrMalformed = errors.New("crypto: malformed ciphertext")
	// ErrKeyMissing means the named DEK is not in the keyring.
	ErrKeyMissing = errors.New("crypto: data key not found")
	// ErrBadKEK means the DEK could not be unwrapped — usually the wrong KEK.
	ErrBadKEK = errors.New("crypto: cannot unwrap data key (wrong KEK?)")
	// ErrNoKEK means a key could not be locked or unlocked because the master
	// key it needs is not available: its KMS provider is not registered, or the
	// gateway runs without a default key. Never a reason to fall back to
	// another key — that is how data ended up under the wrong one.
	ErrNoKEK = errors.New("crypto: master key not available")
	// ErrTenantLookup means a company's KMS assignment could not be read. The
	// message is refused rather than stored under a guessed key, or in plain.
	ErrTenantLookup = errors.New("crypto: cannot read the company's KMS assignment")
)

// remoteSchemes are key-ref schemes that name a key held by a KMS provider.
// A DEK sealed under one of these must be unwrapped by that provider or not
// at all — never by the local gateway key.
var remoteSchemes = map[string]bool{kms.GCPScheme: true, kms.HTTPScheme: true}

func isRemote(ref string) bool {
	i := strings.Index(ref, "://")
	return i > 0 && remoteSchemes[strings.ToLower(ref[:i])]
}

// Keyring stores wrapped DEKs. The config store implements it.
type Keyring interface {
	// GetWrappedDEK returns the wrapped key and whether it has been revoked.
	// found=false (with nil error) means no such key yet.
	GetWrappedDEK(ctx context.Context, keyRef string) (wrapped []byte, kekRef string, revoked, found bool, err error)
	// PutWrappedDEK stores a wrapped key, keeping any row that already exists —
	// two processes creating the same account-month key must converge on ONE key,
	// or data written by one becomes unreadable by the other.
	PutWrappedDEK(ctx context.Context, keyRef string, wrapped []byte, kekRef string) error
}

// TenantConfig is one company's active KMS assignment (tenant_kms_config
// joined with its kms_providers catalog entry). [SCOPE P1-KMS-MULTITENANT]
type TenantConfig struct {
	KeyRef         string // the tenant's master key: a key NAME at its provider, or a full ref (gcp-kms://…)
	DEKWindowHours int    // bucket width for this tenant's DEK (default 24 = daily)
	// ProviderID is tenant_kms_config.kms_provider_id — an http_endpoints row
	// of type 'kms'. ProviderKind is its config's kind ("http" or "").
	ProviderID   int64
	ProviderKind string
}

// KEKRef is the full ref of the tenant's master key: KeyRef as it stands when
// it already names a scheme (gcp-kms://…), else the key at the tenant's HTTP
// provider (http-kms://<provider id>/<key>). "" when neither can be built.
func (c TenantConfig) KEKRef() string {
	if strings.Contains(c.KeyRef, "://") {
		return c.KeyRef
	}
	if c.ProviderID > 0 && strings.TrimSpace(c.KeyRef) != "" && (c.ProviderKind == "" || c.ProviderKind == "http") {
		return kms.HTTPKeyRef(c.ProviderID, strings.TrimSpace(c.KeyRef))
	}
	return ""
}

// TenantKMSLookup resolves a company's active KMS config, if any. The config
// store implements it (internal/store/mysql.Store.TenantKMS). found=false
// (nil error) means the tenant has no row — callers must fall back to the
// gateway-wide default KEK, never fail the message. [SCOPE P1-KMS-MULTITENANT]
type TenantKMSLookup interface {
	TenantKMS(ctx context.Context, companyID int64) (cfg TenantConfig, found bool, err error)
}

// Service encrypts and decrypts values using the envelope scheme.
type Service struct {
	ring   Keyring
	kek    []byte // the gateway-wide DEFAULT KEK (file/env, dev or unconfigured-tenant path)
	kekRef string

	tenantKMS TenantKMSLookup     // nil = multi-tenant KMS feature off entirely (today's shipped behavior)
	keyMgrs   *kms.Registry       // nil-safe: HasManager/Encrypt/Decrypt on a nil *Registry all report "no manager"
	httpKMS   *kms.HTTPKeyManager // the HTTP provider manager inside keyMgrs, kept for ForgetProvider

	// Company -> KMS assignment, cached: the lookup was one or two config
	// reads per message. A config-store outage serves the last good answer.
	tmu       sync.Mutex
	tenants   map[int64]tenantEntry
	companies map[int64]companyEntry // account -> company, for EncryptAccount

	mu   sync.RWMutex
	deks map[string][]byte // unwrapped DEKs — memory only, never persisted

	writeOn bool            // encrypt new writes? (reads always auto-detect)
	fields  map[string]bool // which fields to encrypt when writing
	now     func() time.Time
}

// Option configures a Service.
type Option func(*Service)

// WithAtRest turns encryption ON for new writes of the named fields. Reads are
// unaffected — they always auto-detect, so this flag is safe to toggle.
func WithAtRest(on bool, fields []string) Option {
	return func(s *Service) {
		s.writeOn = on
		s.fields = map[string]bool{}
		for _, f := range fields {
			if f = strings.TrimSpace(strings.ToLower(f)); f != "" {
				s.fields[f] = true
			}
		}
	}
}

// WithTenantKMS arms the per-tenant KMS path: lookup finds a company's active
// tenant_kms_config row, registry resolves its kekRef's scheme to a real
// KeyManager (e.g. gcp-kms://). Omitting this option (nil lookup/registry,
// the zero value) keeps every account on the single gateway-wide KEK exactly
// as Phase 1 shipped — this is additive, never a behavior change for an
// account whose company was never onboarded. [SCOPE P1-KMS-MULTITENANT]
func WithTenantKMS(lookup TenantKMSLookup, registry *kms.Registry) Option {
	return func(s *Service) {
		s.tenantKMS = lookup
		s.keyMgrs = registry
	}
}

type tenantEntry struct {
	cfg   TenantConfig
	found bool
	at    time.Time
}

// tenantTTL: an edited assignment is picked up within this long.
const tenantTTL = 30 * time.Second

// tenant returns a company's KMS assignment through the cache.
func (s *Service) tenant(ctx context.Context, companyID int64) (TenantConfig, bool, error) {
	now := s.now()
	s.tmu.Lock()
	e, ok := s.tenants[companyID]
	s.tmu.Unlock()
	if ok && now.Sub(e.at) < tenantTTL {
		return e.cfg, e.found, nil
	}
	cfg, found, err := s.tenantKMS.TenantKMS(ctx, companyID)
	if err != nil {
		// Through a config-store outage the last known answer stands, however
		// old: refusing would stop companies that never use KMS, and the
		// answer rarely changes. Only a company never looked up is refused.
		if ok {
			return e.cfg, e.found, nil
		}
		return TenantConfig{}, false, fmt.Errorf("%w %d: %v", ErrTenantLookup, companyID, err)
	}
	s.tmu.Lock()
	if s.tenants == nil {
		s.tenants = map[int64]tenantEntry{}
	}
	s.tenants[companyID] = tenantEntry{cfg: cfg, found: found, at: now}
	s.tmu.Unlock()
	return cfg, found, nil
}

// withClock is for tests that need a fixed account-month.
func withClock(f func() time.Time) Option { return func(s *Service) { s.now = f } }

// New builds a Service. kek must be 32 bytes (AES-256); kekRef names it so a
// wrapped DEK records which KEK sealed it. This is always the gateway-wide
// DEFAULT KEK — WithTenantKMS layers the per-tenant path on top, it does not
// replace this one (an unconfigured/legacy tenant still needs somewhere to
// land).
//
// kek may be empty: the gateway then has no default key, and only companies
// assigned to a KMS provider can be encrypted (everything else stays plain,
// and asking for the default key is ErrNoKEK). That is how a deployment whose
// customers each bring their own KMS runs without a gateway key at all.
func New(ring Keyring, kek []byte, kekRef string, opts ...Option) (*Service, error) {
	if len(kek) != 0 && len(kek) != dekBytes {
		return nil, fmt.Errorf("crypto: KEK must be %d bytes, got %d", dekBytes, len(kek))
	}
	if len(kek) == 0 {
		kekRef = ""
	}
	s := &Service{
		ring: ring, kek: append([]byte(nil), kek...), kekRef: kekRef,
		deks: map[string][]byte{}, fields: map[string]bool{}, now: time.Now,
	}
	for _, o := range opts {
		o(s)
	}
	return s, nil
}

// ShouldEncrypt reports whether a named field is encrypted on write for
// EVERYONE: the gateway-wide flag (YAKU_CRYPTO_AT_REST) with the field in its
// allow-list. Whether one company is encrypted is that company's privacy
// setting (companies.body_privacy = all_encrypted, PO 2026-09-13), which the
// caller reads from the reference cache and does not ask here.
//
// Until 2026-09-13 a company with an active KMS assignment was encrypted for
// that reason alone. The assignment now only says WHICH key an encrypted
// company uses; companyID is kept in the signature for the callers that
// already pass it.
func (s *Service) ShouldEncrypt(_ context.Context, field string, _ int64) bool {
	if s == nil || !s.fields[strings.ToLower(field)] {
		return false
	}
	return s.writeOn
}

// AccountCompanyLookup finds the company an account belongs to (0 = none).
// The MySQL config store satisfies it. EncryptAccount needs it.
type AccountCompanyLookup interface {
	AccountCompany(ctx context.Context, accountID int64) (int64, error)
}

type companyEntry struct {
	id int64
	at time.Time
}

// companyTTL: an account moved to another company is picked up within this
// long. Moves are rare and a stale answer only picks the older company's key.
const companyTTL = 5 * time.Minute

// companyOf returns an account's company through a cache. A config-store
// outage serves the last known answer; an account never looked up is an
// error, so the caller does not encrypt under a guessed key.
func (s *Service) companyOf(ctx context.Context, accountID int64) (int64, error) {
	lk, ok := s.ring.(AccountCompanyLookup)
	if !ok {
		return 0, fmt.Errorf("%w: cannot find account %d's company", ErrTenantLookup, accountID)
	}
	now := s.now()
	s.tmu.Lock()
	e, cached := s.companies[accountID]
	s.tmu.Unlock()
	if cached && now.Sub(e.at) < companyTTL {
		return e.id, nil
	}
	id, err := lk.AccountCompany(ctx, accountID)
	if err != nil {
		if cached {
			return e.id, nil
		}
		return 0, fmt.Errorf("%w: account %d: %v", ErrTenantLookup, accountID, err)
	}
	s.tmu.Lock()
	if s.companies == nil {
		s.companies = map[int64]companyEntry{}
	}
	s.companies[accountID] = companyEntry{id: id, at: now}
	s.tmu.Unlock()
	return id, nil
}

// EncryptAccount is Encrypt for a caller that knows only the account: the
// receipt path, which learns a message's owner from record_index. It uses the
// same key the account's messages use (the company's own, else the gateway
// key).
func (s *Service) EncryptAccount(ctx context.Context, accountID int64, plaintext string) (string, error) {
	if s == nil {
		return "", ErrNoKEK
	}
	companyID, err := s.companyOf(ctx, accountID)
	if err != nil {
		return "", err
	}
	return s.Encrypt(ctx, accountID, companyID, plaintext)
}

// KeyRefFor is the DEK name for an account in the current month — the legacy/
// default path, used whenever a message's company is unlinked (companyID=0)
// or has no active tenant_kms_config row.
func (s *Service) KeyRefFor(accountID int64) string {
	return fmt.Sprintf("dek:%d:%s", accountID, s.now().UTC().Format("200601"))
}

// tenantKeyRefFor is the DEK name for a company within its configured time
// window — a bucket number (unix time / window seconds) rather than a
// calendar format, so any window width (daily, hourly, or anything a
// tenant's compliance team asks for) works without a format-string special
// case per width. [SCOPE P1-KMS-MULTITENANT]
func tenantKeyRefFor(companyID int64, now time.Time, windowHours int) string {
	if windowHours <= 0 {
		windowHours = 24
	}
	bucket := now.UTC().Unix() / int64(windowHours*3600)
	return fmt.Sprintf("dek:co:%d:%d", companyID, bucket)
}

// resolveKeyRef picks which DEK (and which KEK it's wrapped under) a NEW
// encryption should use: the company's own master key when it has an active
// assignment, else the per-account-month path under the gateway-wide default
// KEK. A company whose assignment cannot be read, or names a provider that
// cannot be used, is REFUSED (the caller answers 503) — until 2026-09-13 a
// lookup failure fell back to the default key, putting that company's data
// under a key it never chose. [SCOPE P1-KMS-MULTITENANT]
func (s *Service) resolveKeyRef(ctx context.Context, accountID, companyID int64) (keyRef, kekRefForCreate string, err error) {
	if s.tenantKMS != nil && companyID != 0 {
		cfg, found, err := s.tenant(ctx, companyID)
		if err != nil {
			log.Printf("[crypto] %v — message refused", err)
			return "", "", err
		}
		if found {
			ref := cfg.KEKRef()
			if ref == "" {
				return "", "", fmt.Errorf("%w: company %d's KMS assignment names no usable provider or key", ErrNoKEK, companyID)
			}
			return tenantKeyRefFor(companyID, s.now(), cfg.DEKWindowHours), ref, nil
		}
	}
	if len(s.kek) == 0 {
		return "", "", fmt.Errorf("%w: no gateway key (YAKU_CRYPTO_KEK_REF) and company %d has no KMS assignment", ErrNoKEK, companyID)
	}
	return s.KeyRefFor(accountID), s.kekRef, nil
}

// Encrypt seals plaintext under the message's DEK — the company's own
// tenant-configured key when onboarded (resolveKeyRef), else the unchanged
// per-account-month default. An empty input stays empty (an empty body is
// not a secret, and keeps NULL/"" semantics).
func (s *Service) Encrypt(ctx context.Context, accountID, companyID int64, plaintext string) (string, error) {
	if plaintext == "" {
		return "", nil
	}
	keyRef, kekRefForCreate, err := s.resolveKeyRef(ctx, accountID, companyID)
	if err != nil {
		return "", err
	}
	dek, err := s.dek(ctx, keyRef, kekRefForCreate)
	if err != nil {
		return "", err
	}
	gcm, err := gcmFor(dek)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, nonceBytes)
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	// key_ref as AAD binds the ciphertext to its key: a blob cannot be replayed
	// under a different key ref without failing authentication.
	ct := gcm.Seal(nil, nonce, []byte(plaintext), []byte(keyRef))
	return prefix + version + ":" + keyRef + ":" +
		base64.RawStdEncoding.EncodeToString(nonce) + ":" +
		base64.RawStdEncoding.EncodeToString(ct), nil
}

// Decrypt opens a value produced by Encrypt. Anything not starting with "enc:"
// is returned UNCHANGED — see the package doc: this is what lets encrypted and
// plaintext rows coexist with no migration.
func (s *Service) Decrypt(ctx context.Context, v string) (string, error) {
	if !strings.HasPrefix(v, prefix) {
		return v, nil // plaintext passthrough
	}
	keyRef, nonce, ct, err := parse(v)
	if err != nil {
		return "", err
	}
	// "" for kekRefForCreate: a Decrypt is always reading an EXISTING key, so
	// dek()'s create-on-first-use branch (the only place that parameter is
	// consulted) should never fire here — a genuinely missing key is an error
	// (ErrKeyMissing), not something to silently create under some guessed KEK.
	dek, err := s.dek(ctx, keyRef, "")
	if err != nil {
		return "", err
	}
	gcm, err := gcmFor(dek)
	if err != nil {
		return "", err
	}
	pt, err := gcm.Open(nil, nonce, ct, []byte(keyRef))
	if err != nil {
		return "", fmt.Errorf("%w: authentication failed", ErrMalformed)
	}
	return string(pt), nil
}

// parse splits the wire format. key_ref itself contains colons
// (dek:<account>:<YYYYMM>), so the two base64 fields are taken from the RIGHT
// and everything between the version and them is the key ref.
func parse(v string) (keyRef string, nonce, ct []byte, err error) {
	parts := strings.Split(v, ":")
	if len(parts) < 5 || parts[0] != "enc" || parts[1] != version {
		return "", nil, nil, fmt.Errorf("%w: bad envelope header", ErrMalformed)
	}
	keyRef = strings.Join(parts[2:len(parts)-2], ":")
	if keyRef == "" {
		return "", nil, nil, fmt.Errorf("%w: empty key ref", ErrMalformed)
	}
	// Strict() rejects non-canonical trailing bits. Without it the decoder silently
	// ignores unused bits in the final character, so a tampered last byte can
	// decode to identical bytes and slip through unnoticed.
	b64 := base64.RawStdEncoding.Strict()
	if nonce, err = b64.DecodeString(parts[len(parts)-2]); err != nil {
		return "", nil, nil, fmt.Errorf("%w: bad nonce", ErrMalformed)
	}
	if len(nonce) != nonceBytes {
		return "", nil, nil, fmt.Errorf("%w: bad nonce length", ErrMalformed)
	}
	if ct, err = b64.DecodeString(parts[len(parts)-1]); err != nil {
		return "", nil, nil, fmt.Errorf("%w: bad ciphertext", ErrMalformed)
	}
	return keyRef, nonce, ct, nil
}

// dek returns the unwrapped DEK for a key ref, creating it on first use.
// kekRefForCreate is only consulted when the key does not exist yet — an
// EXISTING key's own stored kek_ref (not this parameter) decides how it gets
// unwrapped, so a rotation that changes which KEK NEW keys use never affects
// reading keys wrapped before the change. [SCOPE P1-KMS-MULTITENANT]
func (s *Service) dek(ctx context.Context, keyRef, kekRefForCreate string) ([]byte, error) {
	s.mu.RLock()
	k, ok := s.deks[keyRef]
	s.mu.RUnlock()
	if ok {
		return k, nil
	}
	wrapped, kekRef, revoked, found, err := s.ring.GetWrappedDEK(ctx, keyRef)
	if err != nil {
		return nil, err
	}
	if found && revoked {
		return nil, fmt.Errorf("%w: %s", ErrKeyRevoked, keyRef)
	}
	if !found && kekRefForCreate == "" {
		// A read (Decrypt) of a key that does not exist. Until 2026-09-13 this
		// fell through and CREATED a key row with no KEK — a decrypt call could
		// write to the keyring. A missing key is an error, nothing more.
		return nil, fmt.Errorf("%w: %s", ErrKeyMissing, keyRef)
	}
	if !found {
		// Create-once. Two processes can race here; both INSERT, the store keeps
		// the first, and both then RE-READ so they converge on the same key.
		fresh := make([]byte, dekBytes)
		if _, err := rand.Read(fresh); err != nil {
			return nil, err
		}
		sealed, err := s.wrap(ctx, kekRefForCreate, keyRef, fresh)
		if err != nil {
			return nil, err
		}
		if err := s.ring.PutWrappedDEK(ctx, keyRef, sealed, kekRefForCreate); err != nil {
			return nil, err
		}
		if wrapped, kekRef, revoked, found, err = s.ring.GetWrappedDEK(ctx, keyRef); err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("%w: %s (vanished after create)", ErrKeyMissing, keyRef)
		}
		if revoked {
			return nil, fmt.Errorf("%w: %s", ErrKeyRevoked, keyRef)
		}
	}
	dek, err := s.unwrap(ctx, kekRef, keyRef, wrapped)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.deks[keyRef] = dek
	s.mu.Unlock()
	return dek, nil
}

// wrap seals a DEK under kekRef — remotely via a registered kms.KeyManager
// when kekRef's scheme has one (the KEK plaintext never enters this process
// in that case), else locally with the gateway-wide default KEK (nonce||
// ciphertext, bound to its key ref), exactly as Phase 1 shipped.
// [SCOPE P1-KMS-MULTITENANT]
func (s *Service) wrap(ctx context.Context, kekRef, keyRef string, dek []byte) ([]byte, error) {
	if s.keyMgrs != nil && s.keyMgrs.HasManager(kekRef) {
		return s.keyMgrs.Encrypt(ctx, kekRef, dek)
	}
	// A provider's key with no provider to call must NOT be sealed locally:
	// the DEK would be stored as sealed by the provider, and fail to open the
	// moment the provider is registered (seen before 2026-09-13 for GCP).
	if isRemote(kekRef) {
		return nil, fmt.Errorf("%w: no KMS provider registered for %s", ErrNoKEK, schemeName(kekRef))
	}
	if len(s.kek) == 0 {
		return nil, fmt.Errorf("%w: no gateway key", ErrNoKEK)
	}
	gcm, err := gcmFor(s.kek)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, nonceBytes)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return append(nonce, gcm.Seal(nil, nonce, dek, []byte(keyRef))...), nil
}

func (s *Service) unwrap(ctx context.Context, kekRef, keyRef string, sealed []byte) ([]byte, error) {
	if s.keyMgrs != nil && s.keyMgrs.HasManager(kekRef) {
		dek, err := s.keyMgrs.Decrypt(ctx, kekRef, sealed)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrBadKEK, err)
		}
		return dek, nil
	}
	if isRemote(kekRef) {
		return nil, fmt.Errorf("%w: no KMS provider registered for %s", ErrNoKEK, schemeName(kekRef))
	}
	if len(s.kek) == 0 {
		return nil, fmt.Errorf("%w: no gateway key", ErrNoKEK)
	}
	if len(sealed) <= nonceBytes {
		return nil, fmt.Errorf("%w: wrapped key too short", ErrBadKEK)
	}
	gcm, err := gcmFor(s.kek)
	if err != nil {
		return nil, err
	}
	dek, err := gcm.Open(nil, sealed[:nonceBytes], sealed[nonceBytes:], []byte(keyRef))
	if err != nil {
		return nil, ErrBadKEK
	}
	return dek, nil
}

// Zero clears cached DEKs from memory (call on shutdown).
func (s *Service) Zero() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ref, k := range s.deks {
		for i := range k {
			k[i] = 0
		}
		delete(s.deks, ref)
	}
}

func gcmFor(key []byte) (cipher.AEAD, error) {
	blk, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(blk)
}

// HasDefaultKey reports whether this process has the gateway-wide key, which
// seals everything that is not under a company's own KMS provider.
func (s *Service) HasDefaultKey() bool { return s != nil && len(s.kek) > 0 }

func schemeName(ref string) string {
	if i := strings.Index(ref, "://"); i > 0 {
		return ref[:i] + "://"
	}
	return ref
}

// TestKEK proves a master key works end to end: it locks a random throwaway
// key through the key's provider and unlocks it again. Nothing is stored. The
// "Test connection" button on the KMS Provider screen calls this through the
// engine's admin API. [SCOPE P1-KMS-MULTITENANT]
func (s *Service) TestKEK(ctx context.Context, kekRef string) (wrap, unwrap time.Duration, err error) {
	if s == nil {
		return 0, 0, fmt.Errorf("%w: encryption is not set up in this process", ErrNoKEK)
	}
	if s.keyMgrs == nil || !s.keyMgrs.HasManager(kekRef) {
		return 0, 0, fmt.Errorf("%w: no KMS provider registered for %s", ErrNoKEK, schemeName(kekRef))
	}
	probe := make([]byte, dekBytes)
	if _, err := rand.Read(probe); err != nil {
		return 0, 0, err
	}
	t0 := time.Now()
	sealed, err := s.keyMgrs.Encrypt(ctx, kekRef, probe)
	wrap = time.Since(t0)
	if err != nil {
		return wrap, 0, err
	}
	t1 := time.Now()
	back, err := s.keyMgrs.Decrypt(ctx, kekRef, sealed)
	unwrap = time.Since(t1)
	if err != nil {
		return wrap, unwrap, err
	}
	if string(back) != string(probe) {
		return wrap, unwrap, errors.New("crypto: the provider's decrypt did not return what encrypt was given")
	}
	return wrap, unwrap, nil
}

// ForgetProvider drops a cached HTTP provider so the next call reads its
// config afresh — the connection test runs right after an operator edits one.
func (s *Service) ForgetProvider(id int64) {
	if s != nil && s.httpKMS != nil {
		s.httpKMS.Forget(id)
	}
}

// IsCiphertext reports whether a stored value is enveloped.
func IsCiphertext(v string) bool { return strings.HasPrefix(v, prefix) }
