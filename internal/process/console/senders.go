package console

import (
	"bufio"
	"context"
	"strings"

	"github.com/yakugate/yakugate/internal/provision"
)

// An account's allowed senders. [SCOPE P14-ACCT]
//
// Typed as one line: senders separated by commas, "*" for any sender, or
// nothing — which ALSO means any sender, as the engine reads an empty list.
// The rules are provision's (ParseSenderList, and the Admin API's
// default-sender rule); this file only shows the list and asks before a
// change opens the account to every sender.

// sendersNow is the current list in words, with the approved sender IDs
// that count beside it.
func (p *Process) sendersNow(ctx context.Context, d provision.AccountDetail) string {
	now := provision.SenderListWords(d.AllowedSenders)
	if provision.AllowsAnySender(d.AllowedSenders) {
		return now
	}
	if approved, err := p.store.ApprovedSenders(ctx, d.ID); err == nil && len(approved) > 0 {
		now += "; and its approved sender IDs: " + strings.Join(approved, ", ")
	}
	return now
}

// confirmAnySender asks before a list that restricts the account is replaced
// by one that does not ("*", or empty). false = leave it.
func (p *Process) confirmAnySender(sc *bufio.Scanner, d provision.AccountDetail, typed string) bool {
	list, err := provision.ParseSenderList(typed)
	if err != nil || !provision.AllowsAnySender(list) || provision.AllowsAnySender(d.AllowedSenders) {
		return true // nothing is opened up (or the rules will refuse it and say why)
	}
	p.warnf("  That lets %s send with ANY sender, not only %s.\n", d.Username, strings.Join(d.AllowedSenders, ", "))
	yes, ok := p.askYesOK(sc, "Allow every sender", false)
	return ok && yes
}
