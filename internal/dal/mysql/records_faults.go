package mysql

import (
	"database/sql"
	"errors"

	mysqldrv "github.com/go-sql-driver/mysql"
)

// What a failed record write says about itself, for the DB worker
// (internal/recordq, isAway and isDup; moved here unchanged on 29 Oct 2026,
// the DAL plan: only the DAL knows MySQL's errors).

// Away says a record write failed for a reason that is not its row's: MySQL
// could not be reached, was busy, is read-only, lacks a table or a grant —
// anything but an error that names the ROW (recordRowFault) or a message with no
// record yet (sql.ErrNoRows: the task's own matter, parked and tried again).
// Anything unknown counts as away.
func Away(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	var me *mysqldrv.MySQLError
	if errors.As(err, &me) {
		return !recordRowFault[me.Number]
	}
	return true
}

// recordRowFault are MySQL's errors about the values of a row.
var recordRowFault = map[uint16]bool{
	1048: true, // column cannot be null
	1062: true, // duplicate entry
	1264: true, // out of range value
	1265: true, // data truncated
	1292: true, // truncated incorrect value
	1300: true, // invalid character string
	1364: true, // field has no default value
	1366: true, // incorrect value for column
	1367: true, // illegal value found during parsing
	1406: true, // data too long
	1690: true, // value is out of range
	3140: true, // invalid JSON text
	3141: true, // invalid JSON text in argument
	3156: true, // invalid JSON value for cast
	3157: true, // JSON document too large
}

// Duplicate reports MySQL's duplicate-key error (1062).
func Duplicate(err error) bool {
	var me *mysqldrv.MySQLError
	return errors.As(err, &me) && me.Number == 1062
}

// recordFault is Away and Duplicate as the DAL asks them (dal.RecordAway,
// dal.RecordDuplicate; Driver.RecordFault): mine = a MySQL error, row = one
// of recordRowFault, dup = 1062.
func recordFault(err error) (mine, row, dup bool) {
	var me *mysqldrv.MySQLError
	if !errors.As(err, &me) {
		return false, false, false
	}
	return true, recordRowFault[me.Number], me.Number == 1062
}
