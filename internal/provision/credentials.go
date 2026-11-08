package provision

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"

	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/yakugate/yakugate/internal/dal"
)

// An account's credentials: its API token and its password. [SCOPE P14-CREDS]
//
// The same rules as the portal's credentials endpoints
// (p13-backend/src/routes/accountRoutes.js mintToken / setPassword, PO
// 2026-08-25), because both write the same columns:
//
//   - ONE live token per account. Generating one makes a FRESH HS256 signing
//     secret and signs with it, so every earlier token fails the engine's
//     signature check on its next refresh (~30 s). That is the whole
//     revocation story — there is no separate revocation list;
//   - the token carries what the engine reads (internal/auth): sub = the
//     username (and the account's own sub_claim, if it has one), iss/aud when
//     the account's policy names them, iat, and a jti so the audit trail can
//     say which token is live. No exp: API tokens last until the next one;
//   - ONE password for both doors: one bcrypt hash in sms_service_config.password
//     (HTTP username+password) and smpp_config.password_hash (SMPP bind, where
//     system_id is always the username). 10–64 characters, or generated;
//   - both are kept, readable again, in account_secrets — the portal's
//     "stored, not shown once" rule — and every change is audited.
//
// Two things the portal does not have to face:
//
//   - account_secrets is the ADMIN API's table (admin/0013). On a gateway
//     installed without the Admin API it does not exist, and then the value is
//     shown once and never kept: the engine holds only the hash (password) or
//     the signing secret (token), neither of which gives the value back;
//   - an account whose policy REQUIRES tokens to expire (require_exp /
//     max_lifetime_sec) would refuse a lifetime token, so here the token gets
//     an exp at the account's maximum lifetime, or is refused with the reason.
//     (The portal mints a lifetime token there, which the engine then
//     refuses — noted for the portal, not copied.)

// Credentials is what an account can sign in with, as the console shows it.
type Credentials struct {
	AccountID   int64
	Username    string
	HasSecret   bool   // an HS256 signing secret exists
	SecretRef   string // the secret lives in a reference instead (env://…)
	PublicKey   bool   // RS256/ES256: the client signs its own tokens
	TokenStored bool
	PasswordSet bool
	StoredAt    string // "kept in account_secrets" or why not
	// Kept: this install keeps tokens and passwords (account_secrets, the
	// Admin API's table). Without it, "no token stored" cannot tell "none
	// made" from "made and shown once" (2026-10-02).
	Kept         bool
	Token        string // only when asked to show them
	Password     string
	SMPPEnabled  bool
	HTTPEnabled  bool
	MaxLifetime  int
	RequireExp   bool
	secretsTable bool
}

// jwtPolicy is the part of sms_service_config.jwt this file reads and writes.
// Unknown keys are kept: the JSON is read into a map and written back whole.
type jwtPolicy struct {
	Secret         string `json:"secret"`
	SecretRef      string `json:"secret_ref"`
	PublicKey      string `json:"public_key"`
	Alg            string `json:"alg"`
	SubClaim       string `json:"sub_claim"`
	Issuer         string `json:"issuer"`
	Audience       string `json:"audience"`
	RequireExp     bool   `json:"require_exp"`
	MaxLifetimeSec int    `json:"max_lifetime_sec"`
}

// hasSecretsTable says whether this install has the Admin API's secrets table.
func (s Store) hasSecretsTable(ctx context.Context) bool {
	n, err := s.DB.AccountSecretsTables(ctx)
	return err == nil && n > 0
}

type credRow struct {
	id, company     int64
	username        string
	jwtRaw, smppRaw string
	password        dal.NullString
	http, smpp      bool
}

func (s Store) credRow(ctx context.Context, q dal.ConfigQueries, id int64, lock bool) (credRow, error) {
	var r credRow
	c, err := q.AccountCredentials(ctx, id, lock)
	r.id, r.company, r.username, r.jwtRaw, r.smppRaw, r.password = c.ID, c.Company, c.Username, c.JWT, c.SMPPConfig, c.Password
	httpOn, smppOn := c.HTTP, c.SMPP
	if err == dal.ErrNoRows {
		return r, refusef("there is no account #%d", id)
	}
	if err != nil {
		return r, fmt.Errorf("read account %d: %w", id, err)
	}
	r.http, r.smpp = httpOn != 0, smppOn != 0
	return r, nil
}

// GetCredentials says what an account can sign in with. reveal=true also
// returns the stored token and password, when this install keeps them.
func (s Store) GetCredentials(ctx context.Context, accountID int64, reveal bool) (Credentials, error) {
	r, err := s.credRow(ctx, s.DB, accountID, false)
	if err != nil {
		return Credentials{}, err
	}
	var j jwtPolicy
	_ = json.Unmarshal([]byte(r.jwtRaw), &j)
	var sc map[string]any
	_ = json.Unmarshal([]byte(r.smppRaw), &sc)
	c := Credentials{
		AccountID: r.id, Username: r.username,
		HasSecret: j.Secret != "", SecretRef: j.SecretRef, PublicKey: j.PublicKey != "",
		PasswordSet: r.password.Valid && r.password.String != "",
		HTTPEnabled: r.http, SMPPEnabled: r.smpp,
		MaxLifetime: j.MaxLifetimeSec, RequireExp: j.RequireExp,
	}
	c.secretsTable = s.hasSecretsTable(ctx)
	c.Kept = c.secretsTable
	if !c.secretsTable {
		c.StoredAt = "not kept: this gateway has no account_secrets table (it comes with the Admin API), so a token or password is shown once, when it is made"
		return c, nil
	}
	c.StoredAt = "kept in account_secrets, where the portal reads them too"
	if tok, pw, err := s.DB.AccountKeptSecrets(ctx, accountID); err == nil {
		c.TokenStored = tok.Valid && tok.String != ""
		if reveal {
			c.Token, c.Password = tok.String, pw.String
		}
	}
	return c, nil
}

// IssuedToken is a freshly generated token.
type IssuedToken struct {
	Token     string
	JTI       string
	ExpiresAt time.Time // zero = lasts until the next one
	Kept      bool      // stored in account_secrets
	RefNote   string    // set when a key reference was removed with the old secret
}

// GenerateToken makes the account's ONE live API token, retiring every
// earlier one within ~30 seconds.
func (s Store) GenerateToken(ctx context.Context, accountID int64) (IssuedToken, error) {
	var out IssuedToken
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	r, err := s.credRow(ctx, tx, accountID, true)
	if err != nil {
		return out, err
	}
	raw := map[string]any{}
	_ = json.Unmarshal([]byte(r.jwtRaw), &raw)
	var j jwtPolicy
	_ = json.Unmarshal([]byte(r.jwtRaw), &j)

	// The engine accepts only the account's configured algorithm, so an
	// RS256/ES256 account refuses an HS256 token even if a secret is also set.
	asym := strings.EqualFold(j.Alg, "RS256") || strings.EqualFold(j.Alg, "ES256")
	if asym || (j.PublicKey != "" && j.Secret == "") {
		alg := j.Alg
		if alg == "" {
			alg = "RS256"
		}
		return out, refusef("this account checks %s tokens against a public key: they are signed with the CUSTOMER's private key, which we do not hold. The customer makes their own tokens", alg)
	}
	var exp int64
	if j.RequireExp || j.MaxLifetimeSec > 0 {
		if j.MaxLifetimeSec <= 0 {
			return out, refusef("this account requires every token to expire but sets no maximum lifetime, so there is no expiry to give it. Set its maximum token lifetime, or let the customer sign their own")
		}
		exp = time.Now().Add(time.Duration(j.MaxLifetimeSec) * time.Second).Unix()
		out.ExpiresAt = time.Unix(exp, 0).UTC()
	}

	// A fresh secret IS the revocation: every token signed with the old one
	// now fails the engine's signature check.
	secret, err := randomHex(24)
	if err != nil {
		return out, err
	}
	raw["secret"] = secret
	// ...and a key held by reference goes with it: the gateway tries
	// secret_ref, then secret_ref_prev, then the inline secret, so a
	// reference left in place kept every token signed with that key valid —
	// the new token revoked nothing (QA billing report #13, 2026-10-07; the
	// Admin API's mintToken removes them the same way).
	var removedRefs []string
	for _, k := range []string{"secret_ref", "secret_ref_prev"} {
		if v, _ := raw[k].(string); v != "" {
			removedRefs = append(removedRefs, v)
		}
		delete(raw, k)
	}
	if _, ok := raw["alg"]; !ok || raw["alg"] == "" {
		raw["alg"] = "HS256"
	}
	jwtJSON, err := json.Marshal(raw)
	if err != nil {
		return out, err
	}
	if err := tx.SetAccountJWT(ctx, r.id, string(jwtJSON)); err != nil {
		return out, fmt.Errorf("store the new signing secret: %w", err)
	}

	jti, err := newUUID()
	if err != nil {
		return out, err
	}
	claims := map[string]any{"sub": r.username, "jti": jti, "iat": time.Now().Unix()}
	if j.SubClaim != "" && j.SubClaim != "sub" {
		claims[j.SubClaim] = r.username
	}
	if j.Issuer != "" {
		claims["iss"] = j.Issuer
	}
	if j.Audience != "" {
		claims["aud"] = j.Audience
	}
	if exp != 0 {
		claims["exp"] = exp
	}
	token, err := signHS256(claims, secret)
	if err != nil {
		return out, err
	}
	out.Token, out.JTI = token, jti

	if s.hasSecretsTable(ctx) {
		if err := tx.KeepAccountToken(ctx, r.id, token); err != nil {
			return out, fmt.Errorf("keep the token: %w", err)
		}
		out.Kept = true
	}
	detail := map[string]any{"jti": jti}
	if len(removedRefs) > 0 {
		detail["secret_refs_removed"] = removedRefs // pointers, never the key
	}
	if err := auditAccount(ctx, tx, r, "accounts.credentials.token.generate",
		"API token generated from the console", detail); err != nil {
		return out, err
	}
	if err := tx.Commit(); err != nil {
		return out, err
	}
	if len(removedRefs) > 0 {
		out.RefNote = "this account's key reference (" + strings.Join(removedRefs, ", ") + ") was removed: tokens signed with that key no longer work"
	}
	return out, nil
}

// SetPassword sets the account's one password (HTTP and SMPP). An empty
// plaintext generates one. It returns the password, which is the only time it
// can be read if this install does not keep it.
func (s Store) SetPassword(ctx context.Context, accountID int64, plaintext string) (string, bool, error) {
	if plaintext != "" {
		// 5 to 16 characters (Saleem 2026-11-04), as the Admin API's setPassword.
		if n := len(plaintext); n < 5 || n > 16 {
			return "", false, refusef("a password is 5 to 16 characters; that one is %d. Leave it empty and one is made for you", n)
		}
		if strings.TrimSpace(plaintext) != plaintext {
			return "", false, refusef("a password may not start or end with a space: a customer copying it would almost certainly lose it")
		}
	} else {
		b := make([]byte, 12)
		if _, err := rand.Read(b); err != nil {
			return "", false, fmt.Errorf("make a password: %w", err)
		}
		plaintext = base64.RawURLEncoding.EncodeToString(b) // 16 characters, safe in a URL and a shell
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(plaintext), 10)
	if err != nil {
		return "", false, fmt.Errorf("hash the password: %w", err)
	}

	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback()
	r, err := s.credRow(ctx, tx, accountID, true)
	if err != nil {
		return "", false, err
	}
	sc := map[string]any{}
	_ = json.Unmarshal([]byte(r.smppRaw), &sc)
	sc["password_hash"] = string(hash)
	sc["system_id"] = r.username // always the username (PO 2026-08-25)
	scJSON, _ := json.Marshal(sc)
	if err := tx.SetAccountPassword(ctx, r.id, string(hash), string(scJSON)); err != nil {
		return "", false, fmt.Errorf("store the password: %w", err)
	}
	kept := false
	if s.hasSecretsTable(ctx) {
		if err := tx.KeepAccountPassword(ctx, r.id, plaintext); err != nil {
			return "", false, fmt.Errorf("keep the password: %w", err)
		}
		kept = true
	}
	if err := auditAccount(ctx, tx, r, "accounts.credentials.password.set",
		"Password set from the console", nil); err != nil {
		return "", false, err
	}
	if err := tx.Commit(); err != nil {
		return "", false, err
	}
	return plaintext, kept, nil
}

// ClearPassword removes the password from both doors. The API token, if any,
// keeps working.
func (s Store) ClearPassword(ctx context.Context, accountID int64) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	r, err := s.credRow(ctx, tx, accountID, true)
	if err != nil {
		return err
	}
	sc := map[string]any{}
	_ = json.Unmarshal([]byte(r.smppRaw), &sc)
	delete(sc, "password_hash")
	var scVal dal.NullString
	if len(sc) > 0 {
		b, _ := json.Marshal(sc)
		scVal = dal.NullString{String: string(b), Valid: true}
	}
	if err := tx.ClearAccountPassword(ctx, r.id, scVal); err != nil {
		return fmt.Errorf("clear the password: %w", err)
	}
	if s.hasSecretsTable(ctx) {
		if err := tx.ClearKeptPassword(ctx, r.id); err != nil {
			return fmt.Errorf("clear the kept password: %w", err)
		}
	}
	if err := auditAccount(ctx, tx, r, "accounts.credentials.password.clear",
		"Password cleared from the console", nil); err != nil {
		return err
	}
	return tx.Commit()
}

// auditAccount writes one audit row naming the account, never the secret.
func auditAccount(ctx context.Context, tx dal.ConfigTx, r credRow, action, summary string, detail map[string]any) error {
	var d dal.NullString
	if detail != nil {
		b, _ := json.Marshal(detail)
		d = dal.NullString{String: string(b), Valid: true}
	}
	err := tx.WriteCredentialAudit(ctx, dal.CredentialAuditRow{
		AccountID: r.id, CompanyID: r.company, Username: r.username,
		Action: action, Summary: summary, Detail: d,
	})
	if err != nil {
		return fmt.Errorf("write the audit row: %w", err)
	}
	return nil
}

// signHS256 makes a compact JWT exactly as internal/auth verifies it:
// base64url without padding, header {"alg":"HS256","typ":"JWT"}.
func signHS256(claims map[string]any, secret string) (string, error) {
	enc := base64.RawURLEncoding
	h, _ := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT"})
	c, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	signing := enc.EncodeToString(h) + "." + enc.EncodeToString(c)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signing))
	return signing + "." + enc.EncodeToString(mac.Sum(nil)), nil
}

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("make a signing secret: %w", err)
	}
	return hex.EncodeToString(b), nil
}
