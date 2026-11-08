package dal

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/yakugate/yakugate/internal/platform/config"
)

// An install on a SQL database whose DAL does not serve every area yet is
// refused at start, with the areas named, never half run (DAL plan, step 4:
// the SQL Server DAL gets its areas one by one). The yardstick is the MySQL
// DAL: every area it opens, the install's SQL DAL must open too.

// Unserved names the areas the MySQL DAL serves and the DAL of the install's
// SQL database does not; nil on MySQL, and when the two serve the same.
func Unserved(cfg config.Config) []string {
	on := SQL(cfg)
	if on == config.BackendMySQL {
		return nil
	}
	ref, ok := Lookup(config.BackendMySQL)
	if !ok {
		return nil // nothing to compare with (a test binary without MySQL)
	}
	d, ok := Lookup(on)
	if !ok {
		return []string{"every area (no " + string(on) + " DAL in this binary)"}
	}
	return unservedBy(ref, d)
}

// unservedBy names the openers ref has and d has not.
func unservedBy(ref, d Driver) []string {
	rv, dv := reflect.ValueOf(ref), reflect.ValueOf(d)
	var out []string
	for i := 0; i < rv.NumField(); i++ {
		f := rv.Type().Field(i)
		if f.Type.Kind() != reflect.Func {
			continue
		}
		if !rv.Field(i).IsNil() && dv.Field(i).IsNil() {
			out = append(out, f.Name)
		}
	}
	return out
}

// CheckServed refuses an install whose SQL DAL does not serve every area
// yet: ErrNotServed, with the database and the areas named.
func CheckServed(cfg config.Config) error {
	missing := Unserved(cfg)
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("the gateway's databases are on %s, whose DAL does not serve %s yet: %w (run it on MySQL: YAKU_CONFIG_BACKEND=mysql and MySQL DSNs)",
		SQL(cfg), strings.Join(missing, ", "), ErrNotServed)
}
