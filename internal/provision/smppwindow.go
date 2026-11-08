package provision

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
)

// An account's two SMPP windows (Saleem, 28 Oct 2026: "introduce that setting
// in account page, accept window there, one setting for both mo and dlr";
// "no fixed limits for windows, etc, can be any value"):
//
//	accept_window  -> smpp_config.window_size     submits one bind may have
//	                  unanswered (else YAKU_SMPP_WINDOW)
//	deliver_window -> smpp_config.deliver_window  receipts AND incoming
//	                  messages one bind may have unanswered (else
//	                  YAKU_SMPP_DELIVER_WINDOW)
//
// Any whole number above 0, no upper limit; empty or 0 = the gateway's own
// (the key is taken out). The Admin API's rule is
// p13-backend/src/services/smppWindows.js — change together; both read
// sqa/e2e/shared/smpp-window-vectors.json.

// smppWindowKeys maps the console's setting to its smpp_config key.
var smppWindowKeys = map[string]string{"accept_window": "window_size", "deliver_window": "deliver_window"}

var smppWindowLabels = map[string]string{"accept_window": "Accept window", "deliver_window": "Delivery window"}

// CleanSMPPWindow reads a window as typed: a whole number above 0, or 0 for
// the gateway's own (empty and 0 alike).
func CleanSMPPWindow(label, v string) (int, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		if strings.Trim(v, "-0123456789") == "" && len(strings.TrimLeft(v, "-")) > 0 {
			return 0, refusef("%s is too large to be a number", label)
		}
		return 0, refusef("%s must be a whole number (blank for the gateway's own)", label)
	}
	if n < 0 {
		return 0, refusef("%s cannot be below 0 (0 or blank = the gateway's own)", label)
	}
	if n > 1<<53-1 { // the Admin API's Number.isSafeInteger
		return 0, refusef("%s is too large to be a number", label)
	}
	return int(n), nil
}

// SMPPWindowWords says a window as the console shows it.
func SMPPWindowWords(n int, gatewayOwn string) string {
	if n <= 0 {
		return "the gateway's own (" + gatewayOwn + ")"
	}
	return strconv.Itoa(n)
}

// setSMPPWindow writes one of the two windows and its accounts.update audit
// row, so a portal form opened before it refuses to save over it.
func (s Store) setSMPPWindow(ctx context.Context, cur AccountDetail, key, value string) error {
	label := smppWindowLabels[key]
	// "none" is how the console clears a setting (0 there is Back).
	if strings.EqualFold(strings.TrimSpace(value), "none") {
		value = ""
	}
	n, err := CleanSMPPWindow(label, value)
	if err != nil {
		return err
	}
	jsonKey := smppWindowKeys[key]
	path := "$." + jsonKey
	old := cur.AcceptWindow
	if key == "deliver_window" {
		old = cur.DeliverWindow
	}
	var was any
	if old > 0 {
		was = old
	}
	var now any
	if n > 0 {
		now = n
		err = s.DB.SetAccountSMPPWindow(ctx, cur.ID, path, n)
	} else {
		err = s.DB.RemoveAccountSMPPWindow(ctx, cur.ID, path)
	}
	if err != nil {
		return fmt.Errorf("update account %d: %w", cur.ID, err)
	}
	company, e := s.DB.CompanyName(ctx, cur.CompanyID)
	if e != nil && !errors.Is(e, dal.ErrNoRows) {
		return fmt.Errorf("read company %d: %w", cur.CompanyID, e)
	}
	return s.audit(ctx, auditEntry{
		Action: "accounts.update", Entity: "account", Feature: "accounts", EntityID: cur.ID,
		AccountID: cur.ID, CompanyID: cur.CompanyID, CompanyName: company,
		Label:   cur.Username,
		Summary: "Updated account " + cur.Username + " from the console",
		Detail:  map[string]any{"fields": []string{"smpp_config"}},
		Before:  map[string]any{"smpp_config": map[string]any{jsonKey: was}},
		After:   map[string]any{"smpp_config": map[string]any{jsonKey: now}},
	})
}

// smppWindowsOf reads the two windows out of a stored smpp_config.
func smppWindowsOf(smppCfg string) (accept, deliver int) {
	var w struct {
		Accept  json.Number `json:"window_size"`
		Deliver json.Number `json:"deliver_window"`
	}
	_ = json.Unmarshal([]byte(smppCfg), &w)
	a, _ := w.Accept.Int64()
	d, _ := w.Deliver.Int64()
	return int(a), int(d)
}
