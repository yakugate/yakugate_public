package mssql

// What a failed record write says about itself on SQL Server, for the DB
// worker (dal.RecordAway, dal.RecordDuplicate; mirrors
// internal/dal/mysql/records_faults.go). A SQL Server error that names the
// ROW's values is the row's own matter; any other SQL Server error (a table
// not there, 208; a deadlock, 1205; a lost or read-only database; a login or
// a grant) and anything that is not a SQL Server error at all (a connection
// lost) reads as the database away.

// recRowFault are SQL Server's errors about the values of a row, by the
// MySQL errors they stand for.
var recRowFault = map[int32]bool{
	515:   true, // cannot insert NULL (MySQL 1048, 1364)
	2627:  true, // duplicate key, a constraint (1062)
	2601:  true, // duplicate key, a unique index (1062)
	2628:  true, // string or binary data would be truncated, with the column (1406)
	8152:  true, // string or binary data would be truncated (1406, 1265)
	220:   true, // arithmetic overflow for a type (1264)
	8115:  true, // arithmetic overflow converting (1264, 1690)
	517:   true, // a date out of range (1292)
	241:   true, // conversion failed, a date from text (1292)
	242:   true, // a date out of range from text (1292)
	245:   true, // conversion failed, a number from text (1366)
	8114:  true, // error converting a type (1366, 1367)
	295:   true, // conversion failed, a smalldatetime (1292)
	547:   true, // a CHECK constraint: an enum's values, a JSON column (1265, 3140)
	13609: true, // JSON text is not properly formatted (3140, 3141)
}

// recordFault is the driver's RecordFault: mine = a SQL Server error, row =
// one of recRowFault, dup = 2627/2601.
func recordFault(err error) (mine, row, dup bool) {
	n, ok := ErrorNumber(err)
	if !ok {
		return false, false, false
	}
	return true, recRowFault[n], n == 2627 || n == 2601
}
