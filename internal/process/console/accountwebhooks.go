package console

import (
	"bufio"
	"context"
	"fmt"
	"strconv"

	"github.com/yakugate/yakugate/internal/provision"
)

// accountWebhooks picks the DLR or MO webhook an account uses from the
// webhooks there are (config/0174, Saleem 6 Nov 2026: one webhook may serve
// many accounts). The rule is provision.SetAccountWebhook, the Admin API's
// twin; a new webhook is made under HTTP calls.
func (p *Process) accountWebhooks(ctx context.Context, sc *bufio.Scanner, d provision.AccountDetail) {
	now, err := p.store.GetAccountWebhooks(ctx, d.ID)
	if err != nil {
		p.refusal(err)
		return
	}
	p.printf("\n   %-24s %s\n", "DLR webhook", webhookWords(now.DLR))
	p.printf("   %-24s %s\n", "MO webhook", webhookWords(now.MO))
	kind, ok := p.choose(sc, "Which one to change", "0", []opt{
		item("1", "DLR webhook — where its delivery receipts are pushed"),
		item("2", "MO webhook — where its incoming messages are pushed"),
		item("0", "Back"),
	})
	if !ok || (kind != "1" && kind != "2") {
		return
	}
	k := map[string]string{"1": "dlr", "2": "mo"}[kind]
	rows, err := p.store.ListEndpoints(ctx, k, 200)
	if err != nil {
		p.errf("  %v\n", err)
		return
	}
	p.printf("\n  Which %s webhook? Any number of accounts may use the same one.\n", map[string]string{"dlr": "DLR", "mo": "MO"}[k])
	list := []opt{}
	for _, r := range rows {
		act := ""
		if !r.Active {
			act = "  (switched off)"
		}
		list = append(list, row(r.ID, fmt.Sprintf("   %4d  %-36s %s%s", r.ID, trim(r.URL, 36), trim("used by "+r.Owner, 32), act), ""))
	}
	list = append(list, opt{Key: "0", Text: "      0  none"})
	id, back, ok := p.chooseWithZero(sc, "Webhook id", list)
	if !ok || back {
		return
	}
	if err := p.store.SetAccountWebhook(ctx, d.ID, k, id); err != nil {
		p.refusal(err)
		return
	}
	p.okf("  Done. A running gateway uses it within 30 seconds.\n")
}

// webhookWords is an account's webhook as its screen shows it.
func webhookWords(id int64) string {
	if id == 0 {
		return "none"
	}
	return "#" + strconv.FormatInt(id, 10)
}
