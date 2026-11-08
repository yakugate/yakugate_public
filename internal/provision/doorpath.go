package provision

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/inboundauth"
)

// A vendor's inbound door may answer on a path of its own beside /dlr/<cid>
// and /mo/<cid> (issue 20, 2026-10-27; inbound_endpoints.custom_path,
// config/0152). The rule is the Admin API's — routes/inboundEndpointRoutes.js
// with services/inboundPlaces.js cleanCustomPath and assertPathFree — CHANGE
// TOGETHER (the pair is in the root CLAUDE.md):
//
//   - well formed and not under a path the gateway keeps for itself
//     (inboundauth.CleanCustomPath; the shared cases are
//     sqa/e2e/shared/door-path-vectors.json);
//   - no other door's path, of either kind, and never one under another's
//     (inboundauth.PathsNest): a vendor that sends its token in the path
//     calls <path>/<token>;
//   - not an account's own submit path (an MT dialect's url), for the roles
//     that share an address with the http-server.
//
// Since 27 Oct a DLR or MO dialect may have a URL of its own too
// (SetVendorDialectURL, "similar to MT one"), under the same form rule, and
// every path the gateway answers is judged by one rule (assertPathFree,
// inboundauth.PathsClash): no two the same, and none under another, except
// that two accounts' submit paths clash only when they are the same, as
// before.
//
// Letter case is ignored when two paths are compared, as the store's unique
// key compares them.

// DoorPath is one door's own path as the console shows it.
type DoorPath struct {
	Kind string // dlr | mo
	Path string // "" = none
}

// DoorPaths lists a vendor's two doors' own paths, a door with no row as none.
func (s Store) DoorPaths(ctx context.Context, vendorID int64) ([]DoorPath, error) {
	out := []DoorPath{{Kind: inboundauth.KindDLR}, {Kind: inboundauth.KindMO}}
	doors, err := s.DB.DoorKindPaths(ctx, vendorID)
	if err != nil {
		return nil, wordedErr(err, "read the vendor's doors")
	}
	for _, d := range doors {
		for i := range out {
			if out[i].Kind == d.Kind {
				out[i].Path = d.Path
			}
		}
	}
	return out, nil
}

// SetDoorPath gives a vendor's door of one kind its own path, or takes it away
// (raw empty). The door is made when the vendor has none of that kind yet, as
// the Admin API makes one on first use. It answers the path as stored.
func (s Store) SetDoorPath(ctx context.Context, vendorID int64, kind, raw string) (string, error) {
	if kind != inboundauth.KindDLR && kind != inboundauth.KindMO {
		return "", refusef("a door is dlr (receipts) or mo (incoming messages), not %q", kind)
	}
	cid, company, err := s.DB.DoorVendor(ctx, vendorID)
	if err != nil {
		return "", refusef("there is no vendor #%d", vendorID)
	}
	path, err := inboundauth.CleanCustomPath(raw)
	if err != nil {
		return "", refusef("custom path: %s", err.Error())
	}
	if path != "" {
		if err := s.assertDoorPathFree(ctx, vendorID, kind, path); err != nil {
			return "", err
		}
	}
	doorID, err := s.DB.DoorID(ctx, vendorID, kind)
	if errors.Is(err, dal.ErrNoRows) {
		id, ierr := s.DB.InsertDoor(ctx, vendorID, kind)
		if ierr != nil {
			return "", fmt.Errorf("make the vendor's %s door: %w", kind, ierr)
		}
		doorID = id
	} else if err != nil {
		return "", fmt.Errorf("read the vendor's %s door: %w", kind, err)
	}
	if err := s.DB.SetDoorCustomPath(ctx, doorID, path); err != nil {
		if isDuplicate(err) {
			// Taken between the check and the write.
			return "", refusef("custom path: %q is already the path of another door", path)
		}
		return "", fmt.Errorf("save the door's path: %w", err)
	}
	co := int64(0)
	if company.Valid {
		co = company.Int64
	}
	// What the Admin API writes for a door change (vendors.inbound.update),
	// naming the field and never a secret.
	return path, s.audit(ctx, auditEntry{
		Action: "vendors.inbound.update", Entity: "inbound_endpoint", Feature: "vendors",
		Summary: "Updated inbound_endpoint " + cid + " " + kind, Label: cid + " " + kind,
		EntityID: doorID, CompanyID: co,
		Detail: map[string]any{"vendor": cid, "kind": kind, "fields": []string{"custom_path"}},
	})
}

// usedPath is one path the gateway answers beside its own, for the clash
// check: a door's own path, or a dialect's url of any kind.
type usedPath struct {
	what      string // inboundauth.PathOfDoor | PathOfMT | PathOfDLR | PathOfMO
	path      string
	vendorID  int64  // a door's vendor
	cid       string // a door's vendor
	doorKind  string // a door's kind
	dialectID int64  // a dialect's id
	name      string // a dialect's name
}

// words names the path's owner in a refusal.
func (u usedPath) words() string {
	switch u.what {
	case inboundauth.PathOfDoor:
		return fmt.Sprintf("the path of vendor %s's %s door", u.cid, doorWord(u.doorKind))
	case inboundauth.PathOfMT:
		return fmt.Sprintf("the submit path of the client dialect %q", u.name)
	}
	return fmt.Sprintf("the URL of the %s dialect %q", doorWord(u.what), u.name)
}

// pathOwner is what a path is being saved for: its own row is no clash.
type pathOwner struct {
	what      string // inboundauth.PathOfDoor | PathOfMT | PathOfDLR | PathOfMO
	vendorID  int64  // a door's
	doorKind  string // a door's
	dialectID int64  // a dialect's; 0 = one being made
}

func (o pathOwner) owns(u usedPath) bool {
	if u.what == inboundauth.PathOfDoor {
		return o.what == inboundauth.PathOfDoor && o.vendorID == u.vendorID && o.doorKind == u.doorKind
	}
	return o.what != inboundauth.PathOfDoor && o.dialectID != 0 && o.dialectID == u.dialectID
}

// pathsInUse reads every path the gateway answers beside its own: each
// door's own path and each dialect's url of any kind, the shared default left
// out. A store before config/0152 has no door paths.
func (s Store) pathsInUse(ctx context.Context) ([]usedPath, error) {
	var out []usedPath
	doors, err := s.DB.DoorCustomPaths(ctx)
	var re dal.RowsError
	switch {
	case errors.As(err, &re):
		return nil, re.Err
	case err != nil && strings.Contains(err.Error(), "Unknown column"):
	case err != nil:
		return nil, fmt.Errorf("read the doors' paths: %w", err)
	default:
		for _, d := range doors {
			out = append(out, usedPath{what: inboundauth.PathOfDoor, vendorID: d.VendorID, cid: d.CID, doorKind: d.Kind, path: d.Path})
		}
	}
	urls, err := s.DB.DialectURLs(ctx, inboundauth.NativeSubmitPath)
	if err != nil {
		return nil, wordedErr(err, "read the dialects' paths")
	}
	for _, d := range urls {
		out = append(out, usedPath{dialectID: d.ID, name: d.Name, what: d.Kind, path: d.URL})
	}
	return out, nil
}

// assertPathFree refuses a path that cannot be kept beside one the gateway
// already answers (inboundauth.PathsClash): every door's own path and every
// dialect's url of any kind, its own row left out. label starts the refusal
// ("custom path: ", "url: ", "the path "). The Admin API's twin is
// services/inboundPlaces.js assertPathFree — the same rule and the same words.
func (s Store) assertPathFree(ctx context.Context, label, path string, own pathOwner) error {
	used, err := s.pathsInUse(ctx)
	if err != nil {
		return err
	}
	for _, u := range used {
		if own.owns(u) || !inboundauth.PathsClash(own.what, path, u.what, u.path) {
			continue
		}
		other := strings.TrimRight(u.path, "/")
		if inboundauth.PathKey(strings.TrimRight(path, "/")) == inboundauth.PathKey(other) {
			return refusef("%s%q is already %s — choose another", label, path, u.words())
		}
		return refusef("%s%q and %q (%s) sit one under the other — a vendor that sends its token in the path could not be told apart. Choose a path beside it",
			label, path, other, u.words())
	}
	return nil
}

// assertDoorPathFree refuses a door's own path that another path has, or
// sits under or over: another door's (either kind), a dialect's URL (DLR or
// MO), an account's submit path.
func (s Store) assertDoorPathFree(ctx context.Context, vendorID int64, kind, path string) error {
	return s.assertPathFree(ctx, "custom path: ", path,
		pathOwner{what: inboundauth.PathOfDoor, vendorID: vendorID, doorKind: kind})
}

// assertDialectPathFree refuses a dialect's url that another path has: for a
// client (MT) dialect another client dialect's same path, and any door's path
// or DLR / MO dialect's URL it sits under or over; for a DLR or MO dialect
// any of them, the same or one under the other. A store with no door paths
// yet (before config/0152) has only dialects to compare.
func (s Store) assertDialectPathFree(ctx context.Context, kind, url string, dialectID int64) error {
	url = strings.TrimSpace(url)
	if url == "" || inboundauth.PathKey(strings.TrimRight(url, "/")) == inboundauth.PathKey(inboundauth.NativeSubmitPath) {
		return nil
	}
	label := "url: "
	if kind == inboundauth.PathOfMT {
		label = "the path "
	}
	return s.assertPathFree(ctx, label, url, pathOwner{what: kind, dialectID: dialectID})
}

// VendorDialect is one DLR or MO dialect as the console lists it, with its
// own URL and how many doors read with it.
type VendorDialect struct {
	ID    int64
	Name  string
	Kind  string // dlr | mo
	URL   string // "" = none
	Doors int
}

// ListVendorDialects lists the DLR and MO dialects (a vendor's callbacks).
func (s Store) ListVendorDialects(ctx context.Context) ([]VendorDialect, error) {
	list, err := s.DB.VendorDialectList(ctx)
	if err != nil {
		return nil, wordedErr(err, "list the vendor dialects")
	}
	var out []VendorDialect
	for _, r := range list {
		d := VendorDialect(r)
		if own, err := inboundauth.DialectURL(d.URL); err == nil {
			d.URL = own
		}
		out = append(out, d)
	}
	return out, nil
}

// SetVendorDialectURL gives a DLR or MO dialect its own URL, or takes it away
// (raw empty) — 27 Oct, Saleem: "dialects for mo and dlr will also have custom
// urls, similar to MT one". The vendors whose doors read with the dialect may
// post their callbacks to it; their credentials say which door it is. The
// rule is the Admin API's (services/dialectKinds.js cleanVendorDialect with
// inboundPlaces.js cleanCustomPath and assertPathFree): CHANGE TOGETHER. It
// answers the path as stored ("" = none).
func (s Store) SetVendorDialectURL(ctx context.Context, dialectID int64, raw string) (string, error) {
	name, kind, was, err := s.DB.DialectForURL(ctx, dialectID)
	if err != nil {
		return "", refusef("there is no dialect #%d", dialectID)
	}
	if kind != inboundauth.KindDLR && kind != inboundauth.KindMO {
		return "", refusef("dialect %q is a client's API (MT): its submit path is given when it is made", name)
	}
	path, err := inboundauth.CleanCustomPath(raw)
	if err != nil {
		return "", refusef("url: %s", err.Error())
	}
	if path != "" {
		if err := s.assertDialectPathFree(ctx, kind, path, dialectID); err != nil {
			return "", err
		}
	}
	stored := path
	if stored == "" {
		stored = inboundauth.NativeSubmitPath
	}
	if err := s.DB.SetDialectURL(ctx, dialectID, stored); err != nil {
		return "", fmt.Errorf("save the dialect's URL: %w", err)
	}
	// What the Admin API writes for a dialect's save (dialects.update),
	// naming the field.
	return path, s.audit(ctx, auditEntry{
		Action: "dialects.update", Entity: "dialect", Feature: "dialects",
		Summary: "Updated dialect " + name, Label: name, EntityID: dialectID,
		Detail: map[string]any{"fields": []string{"url"}},
		Before: map[string]any{"url": was}, After: map[string]any{"url": stored},
	})
}

func doorWord(kind string) string {
	if kind == inboundauth.KindMO {
		return "MO"
	}
	return "DLR"
}

// isDuplicate reports MySQL's duplicate-key refusal (1062).
func isDuplicate(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Error 1062")
}
