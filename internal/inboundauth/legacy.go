package inboundauth

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/auth"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/webhookauth"
)

// MoveLegacy moves every vendor's pre-0121 shared inbound settings
// (vendors.inbound_config — one URL token, one header secret, one IP list for
// BOTH callback kinds) onto its two doors, and clears the old column (PO
// 2026-09-24: "delete older settings").
//
// Run by the dlr and mo roles as they start, so a gateway upgraded in place
// never has a vendor that was guarded by the old settings standing
// unguarded: each door with no credential of its own takes the old secret as
// its token, and a door with no address list takes the old one. A door that
// already has a credential keeps it — the operator's newer choice wins.
//
// The old secrets were stored in plain text; the doors keep only a digest
// under the server pepper, so after the move the database holds no copy of
// them. The one thing a door cannot hold is TWO secrets at once: the old
// design could demand a URL token AND a header secret; a door's token is one.
// There the header secret is kept (it never appears in access logs) and the
// URL token stops being checked — said in the log, per vendor.
//
// Idempotent and serialised (one lock, held by the DAL on its own
// connection: GET_LOCK on MySQL), so two roles starting together move each
// vendor once.
func MoveLegacy(ctx context.Context, db dal.InboundDoors, pepper string, logf func(string, ...any)) error {
	if db == nil {
		return nil
	}
	_, err := db.MoveLegacy(ctx, func(mv dal.LegacyDoorMove) error {
		vendors, err := mv.Vendors(ctx)
		if err != nil {
			return err
		}
		type pending struct {
			id  int64
			cid string
			old webhookauth.Config
		}
		var todo []pending
		for _, v := range vendors {
			todo = append(todo, pending{id: v.ID, cid: v.CID, old: webhookauth.Parse(v.Raw)})
		}
		for _, p := range todo {
			if err := moveOne(ctx, mv, pepper, p.id, p.cid, p.old, logf); err != nil {
				logf("inbound: could not move vendor %s's old shared settings onto its doors: %v — they keep answering until this succeeds", p.cid, err)
			}
		}
		return nil
	})
	return err // not got: another role is doing it
}

func moveOne(ctx context.Context, mv dal.LegacyDoorMove, pepper string, vendorID int64, cid string, old webhookauth.Config, logf func(string, ...any)) error {
	var token, source, header string
	switch {
	case old.Header.Name != "" && old.Header.Value != "":
		token, source, header = old.Header.Value, "header", old.Header.Name
		if old.Token != "" {
			logf("inbound: vendor %s had BOTH a URL token and a header secret; its doors keep the header secret (%s) and no longer check the URL token", cid, old.Header.Name)
		}
	case old.Token != "":
		// The old URL token was read from /<kind>/<cid>/<token> only.
		token, source = old.Token, "path"
	}
	// One transaction: the doors, then the vendor's old settings cleared
	// (by the DAL) and committed.
	moved := []string{}
	err := mv.MoveVendor(ctx, vendorID, func(tx dal.LegacyDoorTx) error {
		for _, kind := range Kinds() {
			id, hasCred, ips, found, err := tx.Door(ctx, vendorID, kind)
			if err != nil {
				return err
			}
			if !found {
				if id, err = tx.AddDoor(ctx, vendorID, kind, time.Now().UTC()); err != nil {
					return err
				}
				hasCred, ips = false, "[]"
			}
			if token != "" && !hasCred {
				if err := tx.SetDoorToken(ctx, id, auth.HashToken(pepper, token), source, header); err != nil {
					return err
				}
				moved = append(moved, kind+" token")
			}
			var have []string
			_ = json.Unmarshal([]byte(ips), &have)
			if len(old.IPAllow) > 0 && len(have) == 0 {
				b, _ := json.Marshal(old.IPAllow)
				if err := tx.SetDoorIPs(ctx, id, string(b)); err != nil {
					return err
				}
				moved = append(moved, kind+" addresses")
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(moved) == 0 {
		moved = append(moved, "nothing needed — both doors had their own")
	}
	logf("inbound: vendor %s's old shared settings moved onto its doors (%s) and cleared", cid, strings.Join(moved, ", "))
	return nil
}
