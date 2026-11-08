package console

import (
	"bufio"
	"context"
	"fmt"
	"strconv"

	"github.com/yakugate/yakugate/internal/provision"
)

// The Accounts menu: adding, listing, showing and changing an account.

func (p *Process) accountsMenu(ctx context.Context, sc *bufio.Scanner) {
	for {
		p.printf("\n  Accounts\n")
		opts := []opt{}
		opts = append(opts, item("1", "Add an account"))
		opts = append(opts, item("2", "List accounts"))
		opts = append(opts, item("0", "Back"))
		choice, ok := p.choose(sc, "Choose", "", opts)
		if !ok {
			return
		}
		switch choice {
		case "1":
			p.addAccount(ctx, sc)
		case "2":
			p.listAccounts(ctx, sc)
		case "0", "":
			return
		default:
			p.errf("  %q is not one of the choices.\n", choice)
		}
	}
}

// addAccount asks the questions one at a time and checks each answer as it
// arrives, so a mistake is caught at the field that caused it rather than
// after eight more questions. [SCOPE P14-ACCT]
func (p *Process) addAccount(ctx context.Context, sc *bufio.Scanner) {
	p.printf("\n  Add an account. Enter on its own leaves a setting at its default; 0 to abandon.\n\n")

	companies, err := p.store.ListCompanies(ctx, 200)
	if err != nil {
		p.errf("  %v\n", err)
		return
	}
	if len(companies) == 0 {
		p.printf("  There are no companies yet, and every account must belong to one:\n")
		p.printf("  balance, suspension and quotas all settle at company level.\n")
		return
	}
	p.printf("  Companies\n")
	var a provision.Account
	cid, ok := p.chooseID(sc, "Company id", companyRows(companies))
	if !ok || cid == 0 {
		return
	}
	a.CompanyID = cid

	name, ok := p.ask(sc, fmt.Sprintf("Username (max %d, it is also the SMPP system_id)", 15))
	if !ok || name == "0" || name == "" {
		return
	}
	a.Username = name

	a.HTTP = p.askYes(sc, "May it submit over HTTP", true)
	a.SMPP = p.askYes(sc, "May it bind over SMPP", false)

	a.ChargePoint, ok = p.askChoice(sc, "Charge point", provision.ChargePoints(), "acceptance",
		"when the account is charged — not whether")
	if !ok {
		return
	}
	if a.SMPP {
		a.MultipartMode, ok = p.askChoice(sc, "Long inbound messages", []string{"udh", "tlv"}, "udh",
			"udh = concatenated parts; tlv = the whole message in one message_payload, SMPP 3.4+ binds only")
		if !ok {
			return
		}
	}
	// Which gateways serve it, and its rate on each (config/0122 +
	// config/0123). MANDATORY, exactly as in the portal — throughput lives
	// here now, so an account with no deployment has no rate and no gateway
	// that admits it.
	deps, ok := p.askDeployments(sc)
	if !ok {
		return
	}
	a.Deployments = deps

	// Check before writing, so the refusal names the setting the operator
	// chose rather than a database constraint.
	if err := provision.ValidateAccount(a); err != nil {
		p.refusal(err)
		return
	}
	id, err := p.store.CreateAccount(ctx, a)
	if err != nil {
		p.refusal(err)
		return
	}
	p.okf("\n  Created account #%d %q.\n", id, a.Username)
	p.printf("  Nothing needs restarting: a running gateway picks this up within 30 seconds.\n")
	p.warnf("\n  It cannot sign in yet: it has no API token and no password.\n")
	if p.askYes(sc, "Make its credentials now", true) {
		p.credentialsMenu(ctx, sc, id)
	}
}

func (p *Process) listAccounts(ctx context.Context, sc *bufio.Scanner) {
	rows, err := p.store.ListAccounts(ctx, 200)
	if err != nil {
		p.errf("  %v\n", err)
		return
	}
	if len(rows) == 0 {
		p.printf("\n  No accounts yet.\n")
		return
	}
	p.printf("\n   %-6s %-16s %-24s %-9s %-11s %s\n", "id", "username", "company", "enabled", "throughput", "charge point")
	list := make([]opt, 0, len(rows))
	for _, r := range rows {
		enabled := "yes"
		if r.Disabled {
			enabled = "NO"
		}
		tp := strconv.Itoa(r.Throughput)
		if r.Throughput == 0 {
			tp = "unlimited"
		}
		list = append(list, row(r.ID, fmt.Sprintf("   %-6d %-16s %-24s %-9s %-11s %s",
			r.ID, trim(r.Username, 16), trim(r.Company, 24), enabled, tp, r.ChargePoint), ""))
	}
	id, ok := p.chooseID(sc, "Open an account by id (0 to go back)", list)
	if !ok || id == 0 {
		return
	}
	for _, r := range rows {
		if r.ID == id {
			p.accountMenu(ctx, sc, r)
			return
		}
	}
	p.errf("  There is no account #%d in this list.\n", id)
}

func (p *Process) accountMenu(ctx context.Context, sc *bufio.Scanner, r provision.AccountRow) {
	for {
		d, err := p.store.GetAccount(ctx, r.ID)
		if err != nil {
			p.refusal(err)
			return
		}
		state := "enabled"
		if d.Disabled {
			state = "DISABLED"
		}
		p.printf("\n  Account #%d %q — %s, %s\n", d.ID, d.Username, d.Company, state)
		opts := []opt{}
		opts = append(opts, item("1", "View everything"))
		opts = append(opts, item("2", "Change a setting"))
		if d.Disabled {
			opts = append(opts, item("3", "Enable"))
		} else {
			opts = append(opts, item("3", "Disable — stops it sending, and can be undone"))
		}
		opts = append(opts, item("4", "Delete — permanent"))
		opts = append(opts, item("5", "Credentials — API token and password"))
		opts = append(opts, item("6", "Webhooks — the DLR and MO webhook it uses"))
		opts = append(opts, item("0", "Back"))
		choice, ok := p.choose(sc, "Choose", "", opts)
		if !ok {
			return
		}
		switch choice {
		case "5":
			p.credentialsMenu(ctx, sc, d.ID)
		case "6":
			p.accountWebhooks(ctx, sc, d)
		case "1":
			p.viewAccount(d)
			if c, err := p.store.GetCredentials(ctx, d.ID, true); err == nil {
				p.printf("   %-24s %s\n", "API token", tokenLine(c))
				p.printf("   %-24s %s\n", "Password", passwordLine(c))
			}
		case "2":
			p.editOneShowing(ctx, sc, provision.AccountFields(), nil, func(key string) string {
				switch key {
				case "allowed_senders":
					return p.sendersNow(ctx, d)
				case "default_sender":
					return orDefault(d.DefaultSender, "none")
				case "dlr_channel", "dlr_bind_attempts":
					return provision.PushWords("dlr", d.DLRChannel, d.DLRBindAttempts)
				case "mo_channel", "mo_bind_attempts":
					return provision.PushWords("mo", d.MOChannel, d.MOBindAttempts)
				case "accept_window":
					return provision.SMPPWindowWords(d.AcceptWindow, "YAKU_SMPP_WINDOW")
				case "deliver_window":
					return provision.SMPPWindowWords(d.DeliverWindow, "YAKU_SMPP_DELIVER_WINDOW")
				}
				return ""
			}, func(key, val string) error {
				if key == "allowed_senders" && !p.confirmAnySender(sc, d, val) {
					return errDraft("left as it was: %s", provision.SenderListWords(d.AllowedSenders))
				}
				return p.store.UpdateAccount(ctx, d.ID, key, val)
			})
		case "3":
			if err := p.store.SetAccountDisabled(ctx, d.ID, !d.Disabled); err != nil {
				p.refusal(err)
				return
			}
			p.okf("  Done. A running gateway acts on it within 30 seconds.\n")
		case "4":
			p.warnf("\n  Deleting account %q also removes its dialect row and its stored\n", d.Username)
			p.warnf("  credentials. Its message history stays, but nothing can sign in as it\n")
			p.warnf("  again. Disabling is reversible; this is not.\n")
			if !p.confirm(sc, d.Username) {
				p.printf("  Left alone.\n")
				continue
			}
			if err := p.store.DeleteAccount(ctx, d.ID); err != nil {
				p.refusal(err)
				return
			}
			p.okf("  Deleted account #%d %q.\n", d.ID, d.Username)
			return
		case "0", "":
			return
		default:
			p.errf("  %q is not one of the choices.\n", choice)
		}
	}
}

func (p *Process) viewAccount(d provision.AccountDetail) {
	p.printf("\n  Account #%d\n", d.ID)
	for _, kv := range [][2]string{
		{"Username", d.Username},
		{"Company", fmt.Sprintf("%s (#%d)", d.Company, d.CompanyID)},
		{"Enabled", yesNo(!d.Disabled)},
		{"May submit over HTTP", yesNo(d.HTTP)},
		{"May bind over SMPP", yesNo(d.SMPP)},
		{"SMPP system id", orDash(d.SystemID)},
		{"Long inbound messages", orDefault(d.MultipartMode, "udh (default)")},
		{"Throughput", throughput(d.Throughput)},
		{"Charge point", d.ChargePoint},
		{"Duplicate window", d.DedupeTTL},
		{"Spam check", spamCheckLine(d)},
		{"Allowed senders", provision.SenderListWords(d.AllowedSenders)},
		{"Default sender", orDefault(d.DefaultSender, "none — a refused sender is refused")},
		{"Receipts go to", provision.PushWords("dlr", d.DLRChannel, d.DLRBindAttempts)},
		{"Incoming go to", provision.PushWords("mo", d.MOChannel, d.MOBindAttempts)},
		{"Accept window", provision.SMPPWindowWords(d.AcceptWindow, "YAKU_SMPP_WINDOW")},
		{"Delivery window", provision.SMPPWindowWords(d.DeliverWindow, "YAKU_SMPP_DELIVER_WINDOW")},
		{"Created", orDash(d.Created)},
	} {
		p.printf("   %-24s %s\n", kv[0], kv[1])
	}
}

// accountRows is the "which account?" list every picker draws.
func accountRows(accounts []provision.AccountRow) []opt {
	out := make([]opt, 0, len(accounts))
	for _, a := range accounts {
		out = append(out, row(a.ID, fmt.Sprintf("   %4d  %-16s %s", a.ID, trim(a.Username, 16), trim(a.Company, 24)), ""))
	}
	return out
}
