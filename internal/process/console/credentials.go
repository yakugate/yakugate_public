package console

import (
	"bufio"
	"context"

	"github.com/yakugate/yakugate/internal/provision"
)

// An account's credentials. [SCOPE P14-CREDS]
//
// The token and password are SHOWN on this screen (PO 2026-09-19: "I need to
// see tokens and passwords in the CLI"), the same as the portal shows them to
// a role holding View credentials. The console is operator-only by
// construction: whoever runs it already holds the config store's database
// login. Only a kept value can be shown — where the Admin API's
// account_secrets table does not exist, a value is shown once, when it is
// made. The rules (one live token, one password for both doors) are in
// provision/credentials.go.

func (p *Process) credentialsMenu(ctx context.Context, sc *bufio.Scanner, accountID int64) {
	for {
		c, err := p.store.GetCredentials(ctx, accountID, true)
		if err != nil {
			p.refusal(err)
			return
		}
		p.printf("\n  Credentials of %q\n\n", c.Username)
		p.printf("   Username / SMPP system id   %s\n", c.Username)
		p.printf("   API token                   %s\n", tokenLine(c))
		p.printf("   Password                    %s\n", passwordLine(c))
		if !c.HTTPEnabled && !c.SMPPEnabled {
			p.warnf("\n  Note: this account may use neither HTTP nor SMPP, so no credential will\n")
			p.warnf("  let it send until one of them is switched on.\n")
		}
		p.printf("\n")

		opts := []opt{}
		opts = append(opts, item("1", "Generate a new API token — every earlier token stops working"))
		opts = append(opts, item("2", "Set or generate the password (HTTP and SMPP)"))
		if c.PasswordSet {
			opts = append(opts, item("3", "Remove the password"))
		}
		opts = append(opts, item("0", "Back"))
		choice, ok := p.choose(sc, "Choose", "", opts)
		if !ok {
			return
		}
		switch choice {
		case "1":
			p.generateToken(ctx, sc, c)
		case "2":
			p.setPassword(ctx, sc, c)
		case "3":
			if !c.PasswordSet {
				p.errf("  There is no password to remove.\n")
				continue
			}
			p.warnf("\n  Without a password this account can no longer bind over SMPP or sign in\n")
			p.warnf("  with username and password over HTTP. Its API token, if any, still works.\n")
			if !p.confirm(sc, c.Username) {
				p.printf("  Left alone.\n")
				continue
			}
			if err := p.store.ClearPassword(ctx, c.AccountID); err != nil {
				p.refusal(err)
				continue
			}
			p.okf("  Password removed. A running gateway stops accepting it within 30 seconds.\n")
		case "0", "":
			return
		default:
			p.errf("  %q is not one of the choices.\n", choice)
		}
	}
}

func (p *Process) generateToken(ctx context.Context, sc *bufio.Scanner, c provision.Credentials) {
	if c.TokenStored || c.HasSecret {
		if c.TokenStored {
			p.warnf("\n  A new token REPLACES the current one: within 30 seconds every request\n")
			p.warnf("  still using the old token is refused. Make sure the customer is ready.\n")
		} else {
			p.warnf("\n  No token was made here, but the account already has a signing secret, and\n")
			p.warnf("  a customer may be signing their own tokens with it. A new token changes the\n")
			p.warnf("  secret, so within 30 seconds every token signed with the old one is refused.\n")
		}
		if !p.askYes(sc, "Generate a new token", false) {
			p.printf("  Left alone.\n")
			return
		}
	}
	t, err := p.store.GenerateToken(ctx, c.AccountID)
	if err != nil {
		p.refusal(err)
		return
	}
	p.printf("\n  API token for %q:\n\n", c.Username)
	p.printf("  %s\n\n", t.Token)
	p.printf("  Send it in the yaku-sms-key header (or Authorization: Bearer) with\n")
	p.printf("  POST /sms/v4/send. It is now the ONLY valid token for this account.\n")
	if !t.ExpiresAt.IsZero() {
		p.printf("  This account's policy makes tokens expire: this one ends %s UTC.\n",
			t.ExpiresAt.Format("2006-01-02 15:04"))
	}
	if t.Kept {
		p.printf("  It is kept, so it can be shown again here or copied in the portal.\n")
	} else {
		p.warnf("  COPY IT NOW: this gateway does not keep it, and it cannot be shown again.\n")
	}
	if t.RefNote != "" {
		p.warnf("  Note: %s.\n", t.RefNote)
	}
}

func (p *Process) setPassword(ctx context.Context, sc *bufio.Scanner, c provision.Credentials) {
	p.printf("\n  One password for both doors: HTTP username+password and SMPP bind\n")
	p.printf("  (system id %q). 5 to 16 characters. Enter on its own makes a strong one.\n", c.Username)
	if c.PasswordSet {
		p.warnf("  The current password stops working within 30 seconds.\n")
	}
	typed, ok := p.ask(sc, "Password")
	if !ok || typed == "0" {
		return
	}
	pw, kept, err := p.store.SetPassword(ctx, c.AccountID, typed)
	if err != nil {
		p.refusal(err)
		return
	}
	if typed == "" {
		p.printf("\n  Password for %q:\n\n  %s\n\n", c.Username, pw)
	} else {
		p.okf("\n  Password set.\n")
	}
	if kept {
		p.printf("  It is kept, so it can be shown again here or copied in the portal.\n")
	} else if typed == "" {
		p.warnf("  COPY IT NOW: this gateway keeps only its hash, and it cannot be shown again.\n")
	}
	if !c.SMPPEnabled {
		p.printf("  (SMPP is off for this account, so for now it works over HTTP only.)\n")
	}
}

// tokenLine is the API token as the screen shows it: the value when it is
// kept, otherwise what state the account is in.
func tokenLine(c provision.Credentials) string {
	switch {
	case c.PublicKey && !c.HasSecret:
		return "none — the customer signs their own with their private key"
	case c.Token != "":
		return c.Token
	case c.TokenStored:
		return "made, but its value was not kept — generate a new one to see it"
	case c.HasSecret && !c.Kept:
		// No account_secrets table (no Admin API): a token is shown once,
		// when it is made, and nothing here can say whether one was.
		return "not kept on this install — a token made here is shown once, when it is made"
	case c.HasSecret:
		return "none made yet (the account has a signing secret, but no token was generated here or in the portal)"
	}
	return "none yet"
}

func passwordLine(c provision.Credentials) string {
	switch {
	case c.Password != "":
		return c.Password + "   (HTTP and SMPP)"
	case c.PasswordSet:
		return "set, but its value was not kept — set a new one to see it"
	}
	return "none"
}
