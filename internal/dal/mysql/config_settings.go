package mysql

import (
	"context"

	"github.com/yakugate/yakugate/internal/dal"
)

// What is set for the whole gateway (dal.GatewaySettings): the deployments'
// client addresses, gateway_settings, a vendor's inbound doors and the
// dialects. Moved from internal/provision (account.go, deployment_client.go,
// receiptdefault.go, doorpath.go, dialect.go) on 29 Oct 2026, word for word.

// deploymentClientCols is what a deployment's client addresses are read with.
const deploymentClientCols = `id, deployment_id, COALESCE(name,''), COALESCE(is_disabled,0),
	COALESCE(client_api_url,''), COALESCE(client_smpp_host,''),
	COALESCE(client_smpp_port,0), COALESCE(client_smpp_tls_port,0)`

var (
	// ---- deployments ----
	stDeploymentRegistered = ConfigStmts.Add("config.settings.deployment_registered",
		"SELECT COUNT(*) FROM deployments WHERE deployment_id = ?", false)
	stDeploymentsActive = ConfigStmts.Add("config.settings.deployments_active",
		"SELECT COUNT(*) FROM deployments WHERE is_disabled = 0", false)
	stDeploymentClientList = ConfigStmts.Add("config.settings.deployment_client_list",
		"SELECT "+deploymentClientCols+" FROM deployments ORDER BY deployment_id", false)
	stDeploymentClient = ConfigStmts.Add("config.settings.deployment_client",
		"SELECT "+deploymentClientCols+" FROM deployments WHERE id = ?", false)
	// The column is one of the four client addresses, which provision has
	// picked.
	stDeploymentClientSet = ConfigStmts.Add("config.settings.deployment_client_set",
		"UPDATE deployments SET `client_api_url` = ? WHERE id = ?", false)

	// ---- gateway_settings ----
	stSettingText   = ConfigStmts.Add("config.settings.setting_text", `SELECT CAST(value AS CHAR) FROM gateway_settings WHERE name = ?`, false)
	stSettingDelete = ConfigStmts.Add("config.settings.setting_delete", `DELETE FROM gateway_settings WHERE name = ?`, false)
	stSettingSave   = ConfigStmts.Add("config.settings.setting_save", `INSERT INTO gateway_settings (name, value) VALUES (?, CAST(? AS JSON))
			ON DUPLICATE KEY UPDATE value = VALUES(value)`, false)

	// ---- a vendor's inbound doors ----
	stDoorKindPaths = ConfigStmts.Add("config.settings.door_kind_paths",
		"SELECT kind, COALESCE(custom_path,'') FROM inbound_endpoints WHERE vendor_id = ?", false)
	stDoorVendor  = ConfigStmts.Add("config.settings.door_vendor", "SELECT cid, company_id FROM vendors WHERE id = ?", false)
	stDoorID      = ConfigStmts.Add("config.settings.door_id", "SELECT id FROM inbound_endpoints WHERE vendor_id = ? AND kind = ?", false)
	stDoorInsert  = ConfigStmts.Add("config.settings.door_insert", "INSERT INTO inbound_endpoints (vendor_id, kind) VALUES (?, ?)", false)
	stDoorSetPath = ConfigStmts.Add("config.settings.door_set_path", "UPDATE inbound_endpoints SET custom_path = ? WHERE id = ?", false)
	stDoorPaths   = ConfigStmts.Add("config.settings.door_paths", `
		SELECT v.id, v.cid, e.kind, e.custom_path
		  FROM inbound_endpoints e JOIN vendors v ON v.id = e.vendor_id
		 WHERE e.custom_path IS NOT NULL AND e.custom_path <> ''`, false)

	// ---- dialects ----
	stDialectURLs = ConfigStmts.Add("config.settings.dialect_urls", `
		SELECT id, COALESCE(name,''), COALESCE(kind,'mt'), url FROM sms_service_dialects
		 WHERE url <> ? AND url <> ''`, false)
	stVendorDialects = ConfigStmts.Add("config.settings.vendor_dialects", `
		SELECT d.id, COALESCE(d.name,''), d.kind, COALESCE(d.url,''),
		       (SELECT COUNT(*) FROM inbound_endpoints e WHERE e.dialect_id = d.id)
		  FROM sms_service_dialects d
		 WHERE d.kind IN ('dlr','mo')
		 ORDER BY d.kind, d.id`, false)
	stDialectForURL = ConfigStmts.Add("config.settings.dialect_for_url",
		"SELECT COALESCE(name,''), COALESCE(kind,'mt'), COALESCE(url,'') FROM sms_service_dialects WHERE id = ?", false)
	stDialectSetURL = ConfigStmts.Add("config.settings.dialect_set_url", "UPDATE sms_service_dialects SET url = ? WHERE id = ?", false)
	stDialectInsert = ConfigStmts.Add("config.settings.dialect_insert", `
		INSERT INTO sms_service_dialects (name, url, request_format, request_map, response_map, auth_config, keep_unmapped)
		VALUES (?,?,?,CAST(? AS JSON),CAST(? AS JSON),'{}',?)`, false)
	stDialectList = ConfigStmts.Add("config.settings.dialect_list", `
		SELECT d.id, COALESCE(d.name,''), COALESCE(d.url,''), COALESCE(d.request_format,''),
		       COALESCE(JSON_LENGTH(d.request_map),0),
		       (SELECT COUNT(*) FROM sms_service_config a WHERE a.dialect_id = d.id)
		  FROM sms_service_dialects d
		 -- Client API shapes only: a dlr/mo dialect is a VENDOR's callback
		 -- (config/0128) and is managed with its inbound doors.
		 WHERE d.kind = 'mt'
		 ORDER BY d.id LIMIT ?`, false)
	stDialectExists       = ConfigStmts.Add("config.settings.dialect_exists", "SELECT COUNT(*) FROM sms_service_dialects WHERE id = ?", false)
	stDialectIsMT         = ConfigStmts.Add("config.settings.dialect_is_mt", "SELECT COUNT(*) FROM sms_service_dialects WHERE id = ? AND kind = 'mt'", false)
	stAccountSetDialect   = ConfigStmts.Add("config.settings.account_set_dialect", "UPDATE sms_service_config SET dialect_id = ? WHERE id = ?", false)
	stDialectAccountUsers = ConfigStmts.Add("config.settings.dialect_account_users", "SELECT COUNT(*) FROM sms_service_config WHERE dialect_id = ?", false)
	stDialectDoorUsers    = ConfigStmts.Add("config.settings.dialect_door_users", "SELECT COUNT(*) FROM inbound_endpoints WHERE dialect_id = ?", false)
	stDialectDelete       = ConfigStmts.Add("config.settings.dialect_delete", "DELETE FROM sms_service_dialects WHERE id = ?", false)
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
	return out, accountRowsErr(rows)
}

func (c configQ) DeploymentClient(ctx context.Context, id int64) (dal.DeploymentClientRow, error) {
	return scanDeploymentClient(c.q.QueryRowContext(ctx, stDeploymentClient.SQL, id))
}

func (c configQ) SetDeploymentClientColumn(ctx context.Context, id int64, column string, value any) error {
	_ = stDeploymentClientSet
	_, err := c.q.ExecContext(ctx, "UPDATE deployments SET `"+column+"` = ? WHERE id = ?", value, id)
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
	_, err := c.q.ExecContext(ctx, stSettingSave.SQL, name, valueJSON)
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
	return out, accountRowsErr(rows)
}

func (c configQ) DoorVendor(ctx context.Context, vendorID int64) (cid string, company dal.NullInt64, err error) {
	err = c.q.QueryRowContext(ctx, stDoorVendor.SQL, vendorID).Scan(&cid, &company)
	return cid, company, err
}

func (c configQ) DoorID(ctx context.Context, vendorID int64, kind string) (int64, error) {
	var id int64
	err := c.q.QueryRowContext(ctx, stDoorID.SQL, vendorID, kind).Scan(&id)
	return id, err
}

func (c configQ) InsertDoor(ctx context.Context, vendorID int64, kind string) (int64, error) {
	res, err := c.q.ExecContext(ctx, stDoorInsert.SQL, vendorID, kind)
	if err != nil {
		return 0, err
	}
	id, _ := res.LastInsertId()
	return id, nil
}

func (c configQ) SetDoorCustomPath(ctx context.Context, doorID int64, path string) error {
	_, err := c.q.ExecContext(ctx, stDoorSetPath.SQL, nullStr(path), doorID)
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
	return out, accountRowsErr(rows)
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
	return out, accountRowsErr(rows)
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
	return out, accountRowsErr(rows)
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
	res, err := c.q.ExecContext(ctx, stDialectInsert.SQL,
		d.Name, d.URL, d.Format, d.RequestMap, d.ResponseMap, d.KeepUnmapped)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
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
	return out, accountRowsErr(rows)
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
	res, err := c.q.ExecContext(ctx, stAccountSetDialect.SQL, id, accountID)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (c configQ) DialectAccountUsers(ctx context.Context, id int64) (int, error) {
	return c.count(ctx, stDialectAccountUsers, id)
}

func (c configQ) DialectDoorUsers(ctx context.Context, id int64) (int, error) {
	return c.count(ctx, stDialectDoorUsers, id)
}

func (c configQ) DeleteDialectRow(ctx context.Context, id int64) (int64, error) {
	res, err := c.q.ExecContext(ctx, stDialectDelete.SQL, id)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}
