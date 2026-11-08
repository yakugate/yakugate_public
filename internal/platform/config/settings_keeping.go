// The settings of keeping and backing up data.
package config

var _ = settings("Keeping and backing up data", []setting{
	{"YAKU_HOT_DAYS", "Days of records kept before archiving, unless a company sets its own"},
	{"YAKU_HOT_MONTHS", "Retired: read as days (x 30) while YAKU_HOT_DAYS is unset"},
	{"YAKU_RETENTION_INTERVAL_SEC", "Seconds between archiving passes (default 3600)"},
	{"YAKU_RETENTION_SKIP_CHECKSUM", "true = archive on row count alone when checksum fails"},
	{"YAKU_BACKUP_ROOT", "Folder for backups (default ./backups)"},
	{"YAKU_BACKUP_INTERVAL_SEC", "Seconds between backups (default 86400, once a day)"},
	{"YAKU_BACKUP_KEEP", "Complete backups kept (default 7)"},
})
