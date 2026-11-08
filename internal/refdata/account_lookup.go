// Finding an account in the cache: by id, username, SMPP system id or token,
// and the send paths and dialects that name one.

package refdata

import (
	"github.com/yakugate/yakugate/internal/dialect"
)

// TokenCandidate is one distinct (source, key) pair drawn from every
// token-scheme account's dialect.AuthConfig — the small, data-driven set
// internal/auth tries an incoming request against BEFORE it knows which
// account (if any) the request belongs to. Bounded by the number of distinct
// legacy integrations actually configured, not the number of accounts or
// request fields. [2026-07-23]
type TokenCandidate struct {
	Source string // dialect.TokenSourceHeader | TokenSourceQuery | TokenSourceBody
	Key    string
}

// AccountBySMPPSystemID resolves an account by its SMPP bind system_id. [SCOPE P2]
func (m *Mem) AccountBySMPPSystemID(sysID string) (*Account, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	a, ok := m.bySMPPSysID[sysID]
	return a, ok
}

// AccountByTokenHash resolves an account by the hex HMAC-SHA256 digest of a
// presented opaque token (internal/auth computes the digest; this is a plain
// O(1) map lookup, never a per-row compare — that's the whole point of
// storing a deterministic digest instead of a bcrypt hash here). [2026-07-23]
func (m *Mem) AccountByTokenHash(hash string) (*Account, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	a, ok := m.byTokenHash[hash]
	return a, ok
}

// TokenCandidates returns the small, deduped set of (source, key) pairs drawn
// from every token-scheme account's dialect.AuthConfig — what internal/auth
// tries a request against before it knows which account (if any) applies.
// The returned slice is a snapshot; callers must not mutate it.
func (m *Mem) TokenCandidates() []TokenCandidate {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tokenCandidates
}

// SubClaimNames returns the distinct jwt sub_claim names configured across
// all accounts, "sub" always first. Length 1 means no account uses a custom
// subject claim, letting internal/auth skip the generic-claims decode. The
// returned slice is a snapshot; callers must not mutate it.
func (m *Mem) SubClaimNames() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.subClaimNames
}

// tokenSourcesFor expands an AuthConfig's TokenSource into the concrete
// locations to try — a specific one, or all three when unset (empty means
// "try all", same fallback semantics credential() already uses for
// username/password across header/query/body).
func tokenSourcesFor(source string) []string {
	switch source {
	case dialect.TokenSourceHeader, dialect.TokenSourceQuery, dialect.TokenSourceBody:
		return []string{source}
	default:
		return []string{dialect.TokenSourceHeader, dialect.TokenSourceQuery, dialect.TokenSourceBody}
	}
}

// AccountByID resolves an account by its numeric id — the identity every
// QUEUE row carries (client_dlr, mo_queue), as opposed to the username the
// submit edges resolve by.
func (m *Mem) AccountByID(id int64) (*Account, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	a, ok := m.byID[id]
	return a, ok
}

func (m *Mem) AccountByUsername(u string) (*Account, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	a, ok := m.byUsername[u]
	return a, ok
}

// KnownSendPath reports whether p is a valid submit path for ANY account —
// the native path, or someone's registered custom path. Used as a cheap
// pre-filter so the HTTP edge can 404 garbage paths before running auth; it is
// NOT an ownership check (see Account.ExpectedSendPath for that).
func (m *Mem) KnownSendPath(p string) bool {
	if p == nativeSendPath {
		return true
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.byCustomPath[p]
	return ok
}

// DialectForPath returns the dialect a response on path p should be rendered
// in, independent of whether authentication on this specific request
// succeeds. A custom_path is unique to one account (DB UNIQUE constraint), so
// the path alone — not the credentials presented — determines the format:
// every caller hitting an account's registered URL sees that account's shape,
// including on auth failure. This is deliberately NOT a function of which
// username/password was tried, so it cannot be used to distinguish a valid
// username from an invalid one; only the URL matters, and the URL is already
// known to anyone calling it. The native path (or any path with no owner)
// returns nil, meaning "render natively" — unchanged from today.
func (m *Mem) DialectForPath(p string) *dialect.Config {
	if p == nativeSendPath {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	a, ok := m.byCustomPath[p]
	if !ok {
		return nil
	}
	return a.Dialect
}

// Revoked reports whether a JWT jti is in the revocation blocklist.
func (m *Mem) Revoked(jti string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.revoked[jti]
}
