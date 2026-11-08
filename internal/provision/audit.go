package provision

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/yakugate/yakugate/internal/dal"
)

// auditEntry is one audit_log row written by the console, in the shape the
// Admin API's crud layer writes for the same change (services/audit.js via
// http/crud.js): action "<feature>.<create|update|delete>", entity = the
// resource's singular label, feature = its RBAC feature. The portal's Audit
// Logs screen then shows a console change beside a portal one, told apart
// only by the actor.
type auditEntry struct {
	Action, Entity, Feature, Summary, Label string
	EntityID                                int64
	AccountID, CompanyID                    int64 // 0 = none
	// CompanyName is audit_log.company_name, which the Admin API fills so a
	// row still reads right after the company is renamed. "" = NULL.
	CompanyName string
	// EntityKey is entity_id when the thing has a name, not a number (a
	// gateway setting: "receipt_policy_default"). "" = EntityID.
	EntityKey string
	Detail    map[string]any
	// Before and After are audit_log.before_data / after_data: the fields
	// that changed, as the Admin API writes them. nil = NULL. They are filled
	// where a reader of the trail looks at those columns — the Admin API
	// decides "a person switched this account off" from after_data naming
	// is_disabled (accountRoutes.js switchedByPersonAt).
	Before, After map[string]any
}

// audit writes the row. A failure is returned, not swallowed: a change that
// cannot be accounted for is reported to the operator, who then knows the
// trail has a gap (the change itself has already been made).
func (s Store) audit(ctx context.Context, e auditEntry) error {
	d, err := json.Marshal(e.Detail)
	if err != nil || e.Detail == nil {
		d = []byte("{}")
	}
	entityID := strconv.FormatInt(e.EntityID, 10)
	if e.EntityKey != "" {
		entityID = e.EntityKey
	}
	err = s.DB.WriteAudit(ctx, dal.AuditRow{
		AccountID: e.AccountID, CompanyID: e.CompanyID, CompanyName: e.CompanyName,
		Action: e.Action, Summary: e.Summary, Entity: e.Entity, EntityID: entityID,
		Label: e.Label, Feature: e.Feature, Detail: string(d),
		Before: jsonText(e.Before), After: jsonText(e.After),
	})
	if err != nil {
		return fmt.Errorf("saved, but the audit row could not be written: %w", err)
	}
	return nil
}

// jsonText is "" (written NULL) for nothing, else the map as JSON text.
func jsonText(m map[string]any) string {
	if v, ok := nullJSON(m).(string); ok {
		return v
	}
	return ""
}

// nullJSON is NULL for nothing, else the map as JSON text.
func nullJSON(m map[string]any) any {
	if m == nil {
		return nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil
	}
	return string(b)
}

// nullID is NULL for 0, the "none" of every optional id column.
func nullID(id int64) any {
	if id == 0 {
		return nil
	}
	return id
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}
