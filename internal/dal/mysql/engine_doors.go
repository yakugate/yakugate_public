package mysql

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// The vendors' inbound doors (internal/inboundauth: cache.go's loader and
// legacy.go's one-time move), moved here unchanged on 29 Oct 2026.

func init() {
	// Every shape doorQuery builds; the loader asks the newest first.
	for shape := dal.DoorShapeNone; shape <= dal.DoorShapePath; shape++ {
		EngineStmts.Add("inbound_doors.read_shape_"+string(rune('0'+shape)), doorQuery(shape), false)
	}
}

var (
	stLegacyLock    = EngineStmts.Add("inbound_doors.legacy_lock", "SELECT GET_LOCK('yaku_inbound_legacy_move', 10)", false)
	stLegacyRelease = EngineStmts.Add("inbound_doors.legacy_release", "DO RELEASE_LOCK('yaku_inbound_legacy_move')", false)
	stLegacyVendors = EngineStmts.Add("inbound_doors.legacy_vendors", `
		SELECT id, cid, CAST(inbound_config AS CHAR)
		  FROM vendors
		 WHERE inbound_config IS NOT NULL AND JSON_TYPE(inbound_config) = 'OBJECT'
		   AND JSON_LENGTH(inbound_config) > 0`, false)
	stLegacyDoor = EngineStmts.Add("inbound_doors.legacy_door", `
			SELECT id,
			       (token_hash IS NOT NULL AND token_hash <> '')
			       OR (username IS NOT NULL AND username <> '' AND password IS NOT NULL AND password <> '')
			       OR (jwt IS NOT NULL AND JSON_LENGTH(jwt) > 0),
			       COALESCE(CAST(allowed_ip_list AS CHAR),'[]')
			  FROM inbound_endpoints WHERE vendor_id = ? AND kind = ? FOR UPDATE`, false)
	stLegacyAddDoor = EngineStmts.Add("inbound_doors.legacy_add_door",
		"INSERT INTO inbound_endpoints (vendor_id, kind, created_at, updated_at) VALUES (?, ?, ?, ?)", false)
	stLegacySetToken = EngineStmts.Add("inbound_doors.legacy_set_token",
		`UPDATE inbound_endpoints SET token_hash = ?, token_source = ?, token_header = ? WHERE id = ?`, false)
	stLegacySetIPs = EngineStmts.Add("inbound_doors.legacy_set_ips",
		`UPDATE inbound_endpoints SET allowed_ip_list = ? WHERE id = ?`, false)
	stLegacyClear = EngineStmts.Add("inbound_doors.legacy_clear",
		`UPDATE vendors SET inbound_config = NULL WHERE id = ?`, false)
)

// InboundDoors is dal.InboundDoors on the config store.
type InboundDoors struct{ db *sql.DB }

// NewInboundDoors is the doors on db.
func NewInboundDoors(db *sql.DB) *InboundDoors { return &InboundDoors{db: db} }

// Doors reads every door. The mapping columns (config/0125) are read when
// they exist: a role that starts before the migration has run must still
// load every door — losing the doors would refuse every vendor's callbacks —
// so a store without them is read the old way, with no mappings. Since
// config/0128 a door's mapping, reply and credential places come from its
// DIALECT; before that, from the door's own columns (0127, then 0125).
// Newest shape first, then each older one.
func (d *InboundDoors) Doors(ctx context.Context) ([]dal.DoorRow, int, error) {
	shape := dal.DoorShapePath
	rows, err := d.db.QueryContext(ctx, doorQuery(shape))
	for err != nil && strings.Contains(err.Error(), "Unknown column") && shape > dal.DoorShapeNone {
		shape--
		rows, err = d.db.QueryContext(ctx, doorQuery(shape))
	}
	if err != nil {
		return nil, shape, err
	}
	defer rows.Close()
	withMaps, withPlaces, withDialect := shape >= dal.DoorShapeMaps, shape >= dal.DoorShapePlaces, shape >= dal.DoorShapeDialect
	var out []dal.DoorRow
	for rows.Next() {
		var r dal.DoorRow
		dest := []any{&r.VendorID, &r.CID, &r.LegacyRaw, &r.ID, &r.Kind, &r.Disabled,
			&r.JWTRaw, &r.TokenHash, &r.TokenSrc, &r.TokenHdr, &r.Username, &r.Password,
			&r.IPRaw, &r.Throughput}
		if withMaps {
			dest = append(dest, &r.ReqRaw, &r.ReplyRaw)
		}
		if withPlaces {
			dest = append(dest, &r.PlacesRaw)
		}
		if withDialect {
			dest = append(dest, &r.DialectKind, &r.DialectName, &r.DialectFormat, &r.DialectWrapper, &r.DialectKeep, &r.DialectURL)
		}
		if shape >= dal.DoorShapePath {
			dest = append(dest, &r.CustomPath)
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, shape, dal.RowsError{Err: err}
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, shape, dal.RowsError{Err: err}
	}
	return out, shape, nil
}

// doorQuery reads every vendor's doors in one of the shapes above.
// hmac_config is not read: the signature scheme was retired (2026-09-28).
func doorQuery(shape int) string {
	maps, join := "", ""
	switch {
	case shape >= dal.DoorShapeDialect:
		// The dialect's kind and name ride along so a mismatched assignment
		// is caught here rather than reading the wrong fields.
		maps = ", COALESCE(d.request_map,''), COALESCE(d.response_map,''), COALESCE(d.auth_config,'')" +
			", COALESCE(d.kind,''), COALESCE(d.name,'')" +
			// The dialect's other request and reply settings, as a client's
			// dialect has them (columns older than config/0128).
			", COALESCE(d.request_format,''), COALESCE(d.request_wrapper,''), COALESCE(d.keep_unmapped,0)" +
			// Its own URL, if any (27 Oct; the column is older than
			// config/0128, so no new shape).
			", COALESCE(d.url,'')"
		join = " LEFT JOIN sms_service_dialects d ON d.id = e.dialect_id"
		if shape >= dal.DoorShapePath {
			maps += ", COALESCE(e.custom_path,'')"
		}
	case shape == dal.DoorShapePlaces:
		maps = ", COALESCE(e.request_map,''), COALESCE(e.response_map,''), COALESCE(e.credential_places,'')"
	case shape == dal.DoorShapeMaps:
		maps = ", COALESCE(e.request_map,''), COALESCE(e.response_map,'')"
	}
	return `
		SELECT v.id, v.cid, COALESCE(v.inbound_config,''),
		       COALESCE(e.id,0), COALESCE(e.kind,''), COALESCE(e.is_disabled,0),
		       COALESCE(e.jwt,'{}'), COALESCE(e.token_hash,''), COALESCE(e.token_source,'any'),
		       COALESCE(e.token_header,''), COALESCE(e.username,''), COALESCE(e.password,''),
		       COALESCE(e.allowed_ip_list,'[]'), COALESCE(e.max_throughput,0)` + maps + `
		  FROM vendors v
		  LEFT JOIN inbound_endpoints e ON e.vendor_id = v.id` + join + `
		 WHERE v.service_status <> 'disabled'`
}

// MoveLegacy takes the move's lock on a connection of its own (waiting up to
// 10 s), so two roles starting together move each vendor once.
func (d *InboundDoors) MoveLegacy(ctx context.Context, fn func(dal.LegacyDoorMove) error) (bool, error) {
	conn, err := d.db.Conn(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, stLegacyLock.SQL).Scan(&got); err != nil {
		return false, err
	}
	if !got.Valid || got.Int64 != 1 {
		return false, nil // another role is doing it
	}
	defer conn.ExecContext(context.Background(), stLegacyRelease.SQL)
	return true, fn(legacyMove{conn: conn})
}

// legacyMove is the move on the connection that holds its lock.
type legacyMove struct{ conn *sql.Conn }

// Vendors lists the vendors whose old settings are still there.
func (m legacyMove) Vendors(ctx context.Context) ([]dal.LegacyVendor, error) {
	rows, err := m.conn.QueryContext(ctx, stLegacyVendors.SQL)
	if err != nil {
		if strings.Contains(err.Error(), "doesn't exist") || strings.Contains(err.Error(), "Unknown column") {
			return nil, nil
		}
		return nil, err
	}
	var todo []dal.LegacyVendor
	for rows.Next() {
		var p dal.LegacyVendor
		if err := rows.Scan(&p.ID, &p.CID, &p.Raw); err != nil {
			rows.Close()
			return nil, err
		}
		todo = append(todo, p)
	}
	rows.Close()
	return todo, nil
}

// MoveVendor runs fn in one transaction and clears the vendor's old
// settings in it.
func (m legacyMove) MoveVendor(ctx context.Context, vendorID int64, fn func(dal.LegacyDoorTx) error) error {
	tx, err := m.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(legacyTx{tx: tx}); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, stLegacyClear.SQL, vendorID); err != nil {
		return err
	}
	return tx.Commit()
}

// legacyTx is one vendor's move.
type legacyTx struct{ tx *sql.Tx }

func (t legacyTx) Door(ctx context.Context, vendorID int64, kind string) (int64, bool, string, bool, error) {
	var id int64
	var hasCred int
	var ips string
	err := t.tx.QueryRowContext(ctx, stLegacyDoor.SQL, vendorID, kind).Scan(&id, &hasCred, &ips)
	if err == sql.ErrNoRows {
		return 0, false, "", false, nil
	}
	if err != nil {
		return 0, false, "", false, err
	}
	return id, hasCred != 0, ips, true, nil
}

func (t legacyTx) AddDoor(ctx context.Context, vendorID int64, kind string, at time.Time) (int64, error) {
	res, err := t.tx.ExecContext(ctx, stLegacyAddDoor.SQL, vendorID, kind, at, at)
	if err != nil {
		return 0, err
	}
	id, _ := res.LastInsertId()
	return id, nil
}

func (t legacyTx) SetDoorToken(ctx context.Context, doorID int64, hash, source, header string) error {
	_, err := t.tx.ExecContext(ctx, stLegacySetToken.SQL, hash, source, nsStr(header), doorID)
	return err
}

func (t legacyTx) SetDoorIPs(ctx context.Context, doorID int64, ips string) error {
	_, err := t.tx.ExecContext(ctx, stLegacySetIPs.SQL, ips, doorID)
	return err
}

var _ dal.InboundDoors = (*InboundDoors)(nil)
