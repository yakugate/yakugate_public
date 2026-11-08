package mssql

import (
	"context"
	"database/sql"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// The vendors' inbound doors (internal/inboundauth: the loader and the
// one-time legacy move) on SQL Server, made like
// internal/dal/mysql/engine_doors.go. MySQL's GET_LOCK is an application
// lock here, owned by the session of a pinned connection.

// The lock's wait, as GET_LOCK(…, 10).
const legacyLockWait = 10 * time.Second

var (
	// Every shape doorQuery builds; the loader asks the newest first.
	_ = func() bool {
		for shape := dal.DoorShapeNone; shape <= dal.DoorShapePath; shape++ {
			addEngine("inbound_doors.read_shape_"+string(rune('0'+shape)), doorQuery(shape))
		}
		return true
	}()

	stLegacyLock = addEngine("inbound_doors.legacy_lock", `SET NOCOUNT ON;
DECLARE @yk_got INT;
EXEC @yk_got = sp_getapplock @Resource = 'yaku_inbound_legacy_move', @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = @p1, @DbPrincipal = 'public';
SELECT @yk_got`)
	stLegacyRelease = addEngine("inbound_doors.legacy_release",
		"EXEC sp_releaseapplock @Resource = 'yaku_inbound_legacy_move', @LockOwner = 'Session', @DbPrincipal = 'public'")
	stLegacyVendors = addEngine("inbound_doors.legacy_vendors", `
		SELECT id, cid, inbound_config
		  FROM vendors
		 WHERE inbound_config IS NOT NULL AND ISJSON(inbound_config, OBJECT) = 1
		   AND `+jsonLength("inbound_config")+` > 0`)
	// FOR UPDATE: the door's row, and the gap where it would be, held.
	stLegacyDoor = addEngine("inbound_doors.legacy_door", `
			SELECT id,
			       CASE WHEN (token_hash IS NOT NULL AND token_hash <> '')
			              OR (username IS NOT NULL AND username <> '' AND password IS NOT NULL AND password <> '')
			              OR (jwt IS NOT NULL AND `+jsonLength("jwt")+` > 0) THEN 1 ELSE 0 END,
			       COALESCE(allowed_ip_list,'[]')
			  FROM inbound_endpoints WITH (UPDLOCK, HOLDLOCK) WHERE vendor_id = ? AND kind = ?`)
	stLegacyAddDoor = addEngine("inbound_doors.legacy_add_door",
		"INSERT INTO inbound_endpoints (vendor_id, kind, created_at, updated_at) OUTPUT INSERTED.id VALUES (?, ?, ?, ?)")
	stLegacySetToken = addEngine("inbound_doors.legacy_set_token",
		`UPDATE inbound_endpoints SET token_hash = ?, token_source = ?, token_header = ?, updated_at = SYSUTCDATETIME() WHERE id = ?`)
	stLegacySetIPs = addEngine("inbound_doors.legacy_set_ips",
		`UPDATE inbound_endpoints SET allowed_ip_list = ?, updated_at = SYSUTCDATETIME() WHERE id = ?`)
	stLegacyClear = addEngine("inbound_doors.legacy_clear",
		`UPDATE vendors SET inbound_config = NULL, updated_at = SYSUTCDATETIME() WHERE id = ?`)
)

// InboundDoors is dal.InboundDoors on the config store.
type InboundDoors struct{ db *sql.DB }

// NewInboundDoors is the doors on db.
func NewInboundDoors(db *sql.DB) *InboundDoors { return &InboundDoors{db: db} }

// invalidColumn is SQL Server's "Invalid column name" (207): MySQL's
// "Unknown column".
func invalidColumn(err error) bool {
	n, ok := ErrorNumber(err)
	return ok && n == 207
}

// Doors reads every door, in the newest shape the store has, then each
// older one (mysql/engine_doors.go says why).
func (d *InboundDoors) Doors(ctx context.Context) ([]dal.DoorRow, int, error) {
	shape := dal.DoorShapePath
	rows, err := d.db.QueryContext(ctx, doorQuery(shape))
	for err != nil && invalidColumn(err) && shape > dal.DoorShapeNone {
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

// doorQuery reads every vendor's doors in one of the shapes.
func doorQuery(shape int) string {
	maps, join := "", ""
	switch {
	case shape >= dal.DoorShapeDialect:
		maps = ", COALESCE(d.request_map,''), COALESCE(d.response_map,''), COALESCE(d.auth_config,'')" +
			", COALESCE(d.kind,''), COALESCE(d.name,'')" +
			", COALESCE(d.request_format,''), COALESCE(d.request_wrapper,''), COALESCE(d.keep_unmapped,0)" +
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
// 10 s), so two roles starting together move each vendor once. The lock
// belongs to this database, so two gateways with their own config databases
// on one server never wait on each other.
func (d *InboundDoors) MoveLegacy(ctx context.Context, fn func(dal.LegacyDoorMove) error) (bool, error) {
	conn, err := d.db.Conn(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, stLegacyLock.SQL, int(legacyLockWait/time.Millisecond)).Scan(&got); err != nil {
		return false, err
	}
	if !got.Valid || got.Int64 < 0 {
		return false, nil // another role is doing it
	}
	defer conn.ExecContext(context.Background(), stLegacyRelease.SQL)
	return true, fn(legacyMove{conn: conn})
}

// legacyMove is the move on the connection that holds its lock.
type legacyMove struct{ conn *sql.Conn }

// Vendors lists the vendors whose old settings are still there. A store
// without the table or the column has none.
func (m legacyMove) Vendors(ctx context.Context) ([]dal.LegacyVendor, error) {
	rows, err := m.conn.QueryContext(ctx, stLegacyVendors.SQL)
	if err != nil {
		if MissingTable(err) || invalidColumn(err) {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()
	var todo []dal.LegacyVendor
	for rows.Next() {
		var p dal.LegacyVendor
		if err := rows.Scan(&p.ID, &p.CID, &p.Raw); err != nil {
			return nil, err
		}
		todo = append(todo, p)
	}
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
	err := t.tx.QueryRowContext(ctx, stLegacyDoor.SQL, vendorID, Varchar(kind)).Scan(&id, &hasCred, &ips)
	if err == sql.ErrNoRows {
		return 0, false, "", false, nil
	}
	if err != nil {
		return 0, false, "", false, err
	}
	return id, hasCred != 0, ips, true, nil
}

func (t legacyTx) AddDoor(ctx context.Context, vendorID int64, kind string, at time.Time) (int64, error) {
	var id int64
	err := t.tx.QueryRowContext(ctx, stLegacyAddDoor.SQL, vendorID, Varchar(kind), Time(at), Time(at)).Scan(&id)
	return id, err
}

func (t legacyTx) SetDoorToken(ctx context.Context, doorID int64, hash, source, header string) error {
	_, err := t.tx.ExecContext(ctx, stLegacySetToken.SQL, hash, source, cfgStr(header), doorID)
	return err
}

func (t legacyTx) SetDoorIPs(ctx context.Context, doorID int64, ips string) error {
	_, err := t.tx.ExecContext(ctx, stLegacySetIPs.SQL, ips, doorID)
	return err
}

var _ dal.InboundDoors = (*InboundDoors)(nil)
