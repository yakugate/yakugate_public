package mssql

import (
	"context"
	"database/sql"

	"github.com/yakugate/yakugate/internal/dal"
)

// What is set for the whole gateway (dal.GatewaySettings) on SQL Server:
// the deployments' client addresses, gateway_settings, a vendor's inbound
// doors and the dialects. Made like internal/dal/mysql/config_settings.go.

// deploymentClientCols is what a deployment's client addresses are read with.
const deploymentClientCols = `id, deployment_id, COALESCE(name,''), COALESCE(is_disabled,0),
	COALESCE(client_api_url,''), COALESCE(client_smpp_host,''),
	COALESCE(client_smpp_port,0), COALESCE(client_smpp_tls_port,0)`

const stampSetting = ", updated_at = SYSUTCDATETIME()"

var (
	// ---- deployments ----
	stDeploymentRegistered = addConfig("config.settings.deployment_registered",
		"SELECT COUNT(*) FROM deployments WHERE deployment_id = ?")
	stDeploymentsActive = addConfig("config.settings.deployments_active",
		"SELECT COUNT(*) FROM deployments WHERE is_disabled = 0")
	stDeploymentClientList = addConfig("config.settings.deployment_client_list",
		"SELECT "+wholeTable+" "+deploymentClientCols+" FROM deployments ORDER BY deployment_id")
	stDeploymentClient = addConfig("config.settings.deployment_client",
		"SELECT "+deploymentClientCols+" FROM deployments WHERE id = ?")
	// The column is one of the four client addresses, which provision has
	// picked.
	stDeploymentClientSet = addConfig("config.settings.deployment_client_set",
		"UPDATE deployments SET [client_api_url] = ?"+stampSetting+" WHERE id = ?")

	// ---- gateway_settings ----
	stSettingText   = addConfig("config.settings.setting_text", `SELECT value FROM gateway_settings WHERE name = ?`)
	stSettingDelete = addConfig("config.settings.setting_delete", `DELETE FROM gateway_settings WHERE name = ?`)
	stSettingSave   = AddUpsert(ConfigStmts, "config.settings.setting_save", UpsertSpec{
		Table: "gateway_settings", Keys: []string{"name"},
		Set:    []string{"value = $value", "updated_at = SYSUTCDATETIME()"},
		Insert: []string{"name", "value"},
	}, false)

	// ---- a vendor's inbound doors ----
	stDoorKindPaths = addConfig("config.settings.door_kind_paths",
		"SELECT kind, COALESCE(custom_path,'') FROM inbound_endpoints WHERE vendor_id = ?")
	stDoorVendor  = addConfig("config.settings.door_vendor", "SELECT cid, company_id FROM vendors WHERE id = ?")
	stDoorID      = addConfig("config.settings.door_id", "SELECT id FROM inbound_endpoints WHERE vendor_id = ? AND kind = ?")
	stDoorInsert  = addConfig("config.settings.door_insert", "INSERT INTO inbound_endpoints (vendor_id, kind) OUTPUT INSERTED.id VALUES (?, ?)")
	stDoorSetPath = addConfig("config.settings.door_set_path", "UPDATE inbound_endpoints SET custom_path = ?"+stampSetting+" WHERE id = ?")
	stDoorPaths   = addConfig("config.settings.door_paths", `
		SELECT v.id, v.cid, e.kind, e.custom_path
		  FROM inbound_endpoints e JOIN vendors v ON v.id = e.vendor_id
		 WHERE e.custom_path IS NOT NULL AND e.custom_path <> ''`)

	// ---- dialects ----
	stDialectURLs = addConfig("config.settings.dialect_urls", `
		SELECT id, COALESCE(name,''), COALESCE(kind,'mt'), url FROM sms_service_dialects
		 WHERE url <> ? AND url <> ''`)
	stVendorDialects = addConfig("config.settings.vendor_dialects", `
		SELECT d.id, COALESCE(d.name,''), d.kind, COALESCE(d.url,''),
		       (SELECT COUNT(*) FROM inbound_endpoints e WHERE e.dialect_id = d.id)
		  FROM sms_service_dialects d
		 WHERE d.kind IN ('dlr','mo')
		 ORDER BY d.kind, d.id`)
	stDialectForURL = addConfig("config.settings.dialect_for_url",
		"SELECT COALESCE(name,''), COALESCE(kind,'mt'), COALESCE(url,'') FROM sms_service_dialects WHERE id = ?")
	stDialectSetURL = addConfig("config.settings.dialect_set_url", "UPDATE sms_service_dialects SET url = ?"+stampSetting+" WHERE id = ?")
	stDialectInsert = addConfig("config.settings.dialect_insert", `
		INSERT INTO sms_service_dialects (name, url, request_format, request_map, response_map, auth_config, keep_unmapped)
		OUTPUT INSERTED.id
		VALUES (?,?,?,?,?,'{}',?)`)
	// Client API shapes only: a dlr/mo dialect is a vendor callback
	// (config/0128) and is managed with its inbound doors.
	stDialectList = addConfig("config.settings.dialect_list", `
		SELECT TOP (@p1) d.id, COALESCE(d.name,''), COALESCE(d.url,''), COALESCE(d.request_format,''),
		       COALESCE(`+jsonLength("d.request_map")+`,0),
		       (SELECT COUNT(*) FROM sms_service_config a WHERE a.dialect_id = d.id)
		  FROM sms_service_dialects d
		 WHERE d.kind = 'mt'
		 ORDER BY d.id`)
	stDialectExists = addConfig("config.settings.dialect_exists", "SELECT COUNT(*) FROM sms_service_dialects WHERE id = ?")
	stDialectIsMT   = addConfig("config.settings.dialect_is_mt", "SELECT COUNT(*) FROM sms_service_dialects WHERE id = ? AND kind = 'mt'")
	// Rows CHANGED, as MySQL counts them.
	stAccountSetDialect = addConfig("config.settings.account_set_dialect",
		"UPDATE sms_service_config SET dialect_id = ?"+stampSetting+" WHERE id = ? AND dialect_id IS DISTINCT FROM @p1")
	stDialectAccountUsers = addConfig("config.settings.dialect_account_users", "SELECT COUNT(*) FROM sms_service_config WHERE dialect_id = ?")
	stDialectDoorUsers    = addConfig("config.settings.dialect_door_users", "SELECT COUNT(*) FROM inbound_endpoints WHERE dialect_id = ?")
	stDialectDelete       = addConfig("config.settings.dialect_delete", "DELETE FROM sms_service_dialects WHERE id = ?")
)

// ---- deployments ----

func (c configQ) DeploymentRegistered(ctx context.Context, deploymentID string) (int, error) {
	return c.count(ctx, stDeploymentRegistered, deploymentID)
}

func (c configQ) ActiveDeploymentCount(ctx context.Context) (int, error) {
	return c.count(ctx, stDeploymentsActive)
}

func scanDeploymentClient(sc interface{ Scan(...any) error }) (dal.DeploymentClientRow, error) {
	var d dal.DeploymentClientRow
	err := sc.Scan(&d.ID, &d.DeploymentID, &d.Name, &d.Disabled, &d.APIURL, &d.SMPPHost, &d.SMPPPort, &d.SMPPTLSPort)
	return d, err
}

func (c configQ) DeploymentClientList(ctx context.Context) ([]dal.DeploymentClientRow, error) {
	rows, err := c.q.QueryContext(ctx, stDeploymentClientList.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.DeploymentClientRow
	for rows.Next() {
		d, err := scanDeploymentClient(rows)
		if err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, d)
	}
	return out, rowsErr(rows)
}

func (c configQ) DeploymentClient(ctx context.Context, id int64) (dal.DeploymentClientRow, error) {
	return scanDeploymentClient(c.q.QueryRowContext(ctx, stDeploymentClient.SQL, id))
}

func (c configQ) SetDeploymentClientColumn(ctx context.Context, id int64, column string, value any) error {
	_ = stDeploymentClientSet
	_, err := c.q.ExecContext(ctx, "UPDATE deployments SET "+quoteIdent(column)+" = @p1"+stampSetting+" WHERE id = @p2", value, id)
	return err
}

// ---- gateway_settings ----

func (c configQ) GatewaySettingText(ctx context.Context, name string) (dal.NullString, error) {
	var raw dal.NullString
	err := c.q.QueryRowContext(ctx, stSettingText.SQL, name).Scan(&raw)
	return raw, err
}

func (c configQ) DeleteGatewaySetting(ctx context.Context, name string) error {
	_, err := c.q.ExecContext(ctx, stSettingDelete.SQL, name)
	return err
}

func (c configQ) SaveGatewaySetting(ctx context.Context, name, valueJSON string) error {
	_, err := stSettingSave.Exec(ctx, c.q, sql.Named("name", name), sql.Named("value", valueJSON))
	return err
}

// ---- a vendor's inbound doors ----

func (c configQ) DoorKindPaths(ctx context.Context, vendorID int64) ([]dal.DoorKindPath, error) {
	rows, err := c.q.QueryContext(ctx, stDoorKindPaths.SQL, vendorID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.DoorKindPath
	for rows.Next() {
		var d dal.DoorKindPath
		if err := rows.Scan(&d.Kind, &d.Path); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, d)
	}
	return out, rowsErr(rows)
}

func (c configQ) DoorVendor(ctx context.Context, vendorID int64) (cid string, company dal.NullInt64, err error) {
	err = c.q.QueryRowContext(ctx, stDoorVendor.SQL, vendorID).Scan(&cid, &company)
	return cid, company, err
}

func (c configQ) DoorID(ctx context.Context, vendorID int64, kind string) (int64, error) {
	var id int64
	err := c.q.QueryRowContext(ctx, stDoorID.SQL, vendorID, Varchar(kind)).Scan(&id)
	return id, err
}

func (c configQ) InsertDoor(ctx context.Context, vendorID int64, kind string) (int64, error) {
	return c.insertID(ctx, stDoorInsert.SQL, vendorID, Varchar(kind))
}

func (c configQ) SetDoorCustomPath(ctx context.Context, doorID int64, path string) error {
	_, err := c.q.ExecContext(ctx, stDoorSetPath.SQL, cfgStr(path), doorID)
	return err
}

func (c configQ) DoorCustomPaths(ctx context.Context) ([]dal.DoorCustomPath, error) {
	rows, err := c.q.QueryContext(ctx, stDoorPaths.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.DoorCustomPath
	for rows.Next() {
		var d dal.DoorCustomPath
		if err := rows.Scan(&d.VendorID, &d.CID, &d.Kind, &d.Path); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, d)
	}
	return out, rowsErr(rows)
}

// ---- dialects ----

func (c configQ) DialectURLs(ctx context.Context, native string) ([]dal.DialectURLRow, error) {
	rows, err := c.q.QueryContext(ctx, stDialectURLs.SQL, native)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.DialectURLRow
	for rows.Next() {
		var d dal.DialectURLRow
		if err := rows.Scan(&d.ID, &d.Name, &d.Kind, &d.URL); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, d)
	}
	return out, rowsErr(rows)
}

func (c configQ) VendorDialectList(ctx context.Context) ([]dal.VendorDialectRow, error) {
	rows, err := c.q.QueryContext(ctx, stVendorDialects.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.VendorDialectRow
	for rows.Next() {
		var d dal.VendorDialectRow
		if err := rows.Scan(&d.ID, &d.Name, &d.Kind, &d.URL, &d.Doors); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, d)
	}
	return out, rowsErr(rows)
}

func (c configQ) DialectForURL(ctx context.Context, id int64) (name, kind, url string, err error) {
	err = c.q.QueryRowContext(ctx, stDialectForURL.SQL, id).Scan(&name, &kind, &url)
	return name, kind, url, err
}

func (c configQ) SetDialectURL(ctx context.Context, id int64, url string) error {
	_, err := c.q.ExecContext(ctx, stDialectSetURL.SQL, url, id)
	return err
}

func (c configQ) InsertDialect(ctx context.Context, d dal.DialectInsert) (int64, error) {
	return c.insertID(ctx, stDialectInsert.SQL,
		d.Name, d.URL, Varchar(d.Format), d.RequestMap, d.ResponseMap, d.KeepUnmapped)
}

func (c configQ) DialectList(ctx context.Context, limit int) ([]dal.DialectListRow, error) {
	rows, err := c.q.QueryContext(ctx, stDialectList.SQL, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.DialectListRow
	for rows.Next() {
		var r dal.DialectListRow
		if err := rows.Scan(&r.ID, &r.Name, &r.URL, &r.Format, &r.Mappings, &r.Accounts); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, r)
	}
	return out, rowsErr(rows)
}

func (c configQ) DialectExists(ctx context.Context, id int64) (bool, error) {
	return c.exists(ctx, stDialectExists, id)
}

func (c configQ) DialectIsMT(ctx context.Context, id int64) (bool, error) {
	return c.exists(ctx, stDialectIsMT, id)
}

func (c configQ) SetAccountDialect(ctx context.Context, accountID int64, dialectID dal.NullInt64) (int64, error) {
	var id any
	if dialectID.Valid {
		id = dialectID.Int64
	}
	return c.execRows(ctx, stAccountSetDialect.SQL, id, accountID)
}

func (c configQ) DialectAccountUsers(ctx context.Context, id int64) (int, error) {
	return c.count(ctx, stDialectAccountUsers, id)
}

func (c configQ) DialectDoorUsers(ctx context.Context, id int64) (int, error) {
	return c.count(ctx, stDialectDoorUsers, id)
}

func (c configQ) DeleteDialectRow(ctx context.Context, id int64) (int64, error) {
	return c.execRows(ctx, stDialectDelete.SQL, id)
}
