package provision

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
)

// An account names its DLR webhook and its MO webhook (config/0174, Saleem
// 6 Nov 2026: "one dlr or mo endpoint can be assigned to many accounts at
// same time"). Any number of accounts may name the same http_endpoints row;
// each still gets its own push lane at the webhook's rate_limit and
// submit_concurrency. http_endpoints.account_id is bookkeeping no rule reads.
//
// The Admin API keeps the same rules (services/accountWebhooks.js — change
// together):
//
//	the id is NULL or a row of that type;   anything else: "there is no DLR webhook #N"
//	url or both needs an active webhook;    so clearing it (or naming a switched-off
//	                                        one) is refused while the channel needs it
//	a named webhook is not deleted;         EndpointUsers names the accounts

// AccountWebhooks is the webhook of each kind an account names (0 = none).
type AccountWebhooks struct {
	DLR, MO int64
}

// GetAccountWebhooks reads the webhook of each kind an account names.
func (s Store) GetAccountWebhooks(ctx context.Context, accountID int64) (AccountWebhooks, error) {
	dlr, mo, err := s.DB.AccountWebhookIDs(ctx, accountID)
	if errors.Is(err, dal.ErrNoRows) {
		return AccountWebhooks{}, refusef("there is no account #%d", accountID)
	}
	if err != nil {
		return AccountWebhooks{}, fmt.Errorf("read account %d's webhooks: %w", accountID, err)
	}
	return AccountWebhooks{DLR: dlr, MO: mo}, nil
}

// webhookField is the account setting of a kind, as both doors name it.
func webhookField(kind string) string { return kind + "_webhook_id" }

// noWebhook is the one refusal for an id that is not a webhook of the kind:
// no such row, or a row of another type (the Admin API adds "out of reach").
func noWebhook(kind string, id int64) error {
	return refusef("%s: there is no %s webhook #%d", webhookField(kind), pushKindWords[kind], id)
}

// SetAccountWebhook points an account's webhook of kind ("dlr" | "mo") at
// endpointID; 0 = none. The id must be a webhook of that kind; a switched-off
// one may be named (it delivers nothing). While the account's channel of that
// kind is url or both, the webhook it names must be switched on and, under
// both, outlast the bind attempts — the channel's own rule (pushchannel.go),
// judged on the webhook as it would be. Writes the accounts.update audit row
// naming the field, as the other account settings do.
func (s Store) SetAccountWebhook(ctx context.Context, accountID int64, kind string, endpointID int64) error {
	if kind != "dlr" && kind != "mo" {
		return refusef("%q: an account's webhook is a dlr or an mo webhook", kind)
	}
	cur, err := s.GetAccount(ctx, accountID)
	if err != nil {
		return err
	}
	if endpointID != 0 {
		typ, _, _, err := s.DB.EndpointKind(ctx, endpointID)
		if errors.Is(err, dal.ErrNoRows) || err == nil && typ != kind {
			return noWebhook(kind, endpointID)
		}
		if err != nil {
			return fmt.Errorf("read call %d: %w", endpointID, err)
		}
	}
	now, err := s.GetAccountWebhooks(ctx, accountID)
	if err != nil {
		return err
	}
	before := now.DLR
	channel, attempts := cur.DLRChannel, cur.DLRBindAttempts
	if kind == "mo" {
		before = now.MO
		channel, attempts = cur.MOChannel, cur.MOBindAttempts
	}
	if before == endpointID {
		return nil
	}
	if channel != PushBind {
		retry, err := "", error(dal.ErrNoRows)
		if endpointID != 0 {
			retry, err = s.DB.ActiveWebhookRetry(ctx, endpointID, kind)
		}
		if err := judgePushChannel(kind, channel, attempts, retry, err); err != nil {
			return err
		}
	}
	return s.pointAccountAt(ctx, cur, kind, before, endpointID)
}

// pointAccountAt writes the account's webhook of kind and its audit row.
func (s Store) pointAccountAt(ctx context.Context, cur AccountDetail, kind string, before, after int64) error {
	if err := s.DB.SetAccountWebhookID(ctx, cur.ID, kind, after); err != nil {
		return fmt.Errorf("update account %d: %w", cur.ID, err)
	}
	field := webhookField(kind)
	return s.audit(ctx, auditEntry{
		Action: "accounts.update", Entity: "account", Feature: "accounts", EntityID: cur.ID,
		AccountID: cur.ID, CompanyID: cur.CompanyID, CompanyName: cur.Company,
		Label:   cur.Username,
		Summary: "Updated account " + cur.Username + " from the console",
		Detail:  map[string]any{"fields": []string{field}},
		Before:  map[string]any{field: idOrNil(before)},
		After:   map[string]any{field: idOrNil(after)},
	})
}

// idOrNil is a webhook id as the audit row shows it: null for none.
func idOrNil(id int64) any {
	if id == 0 {
		return nil
	}
	return id
}

// webhookUserWords is one account that names a webhook, as EndpointUsers
// lists it: "account acme (its DLR webhook)".
func webhookUserWords(u dal.WebhookUser) string {
	return fmt.Sprintf("account %s (its %s webhook)", u.Username, pushKindWords[u.Kind])
}

// webhookOwners is the Owner of a dlr/mo row in the endpoint list: the
// accounts that name it, or "nobody yet".
func webhookOwners(users []dal.WebhookUser) string {
	if len(users) == 0 {
		return "nobody yet"
	}
	names := make([]string, 0, len(users))
	for _, u := range users {
		if len(names) == 0 || names[len(names)-1] != u.Username {
			names = append(names, u.Username)
		}
	}
	return strings.Join(names, ", ")
}
