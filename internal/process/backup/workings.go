package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/crypto"
	"github.com/yakugate/yakugate/internal/dal"
)

// Step runs one input -> logic -> output cycle for the "backup" process.
//
//	INPUT:  the config, cold, and hot databases
//	LOGIC:  once per interval, and only when every database answers, snapshot
//	        every table of each DB to <root>/<timestamp>/<db>/<table>.jsonl.gz
//	        (Go-native logical dump; cold, hot, then config) plus <table>.sql
//	        (its CREATE TABLE) and <db>/_triggers.json; a table that fails
//	        because its database stopped answering is read again once it
//	        answers; write <root>/<timestamp>/MANIFEST.json LAST (SHA-256, size
//	        and row count of every file — a run without it is incomplete),
//	        verify the run against it, then rotate — keep the newest `keep`
//	        WHOLE runs. A run that left something out is tried again sooner.
//	OUTPUT: rotated, verified, gzipped JSONL backup artifacts on the filesystem
func (p *Process) Step(ctx context.Context) error {
	if len(p.targets) == 0 {
		return nil
	}
	// A role that is stopping begins nothing (2026-10-18, go-live P8 E1): the
	// process that had just cut a run short and let go of its lease was
	// stepped once more on its way out and began another, empty run folder.
	if ctx.Err() != nil {
		return nil
	}
	// Gateway-wide role: one deployment runs it, the rest stand by
	// (internal/singleton). Two copies of backup against one shared store
	// would write two runs into one directory and rotate each other away.
	// Under the supervisor, a lease another deployment holds ends this
	// process instead (singleton.Elsewhere): no idle copy is kept.
	if held, err := p.lease.Held(ctx); !held {
		return err
	}
	now := time.Now().UTC()
	// A process that has just started asks the disk when the last run was,
	// instead of taking "never" for the answer (2026-10-12). Until then EVERY
	// start of the role began a full backup: the gateway was restarted twice
	// in one afternoon on the dev box and ran two 40-minute, 13 GB backups
	// back to back, each of them on top of a load test — and in production
	// every deploy would do the same in working hours.
	if !p.looked {
		p.looked = true
		p.readDisk(now)
	}
	if !p.nextAt.IsZero() && now.Before(p.nextAt) {
		return nil // not due
	}
	p.job.SetInterval(p.interval)

	// Every database must answer before a run begins (2026-10-18, go-live P8
	// E3). A run that fell due while MySQL was down or refusing connections
	// was made all the same: every database "failed", the run had 0 files,
	// it got a manifest, was logged `verify ok: 0 files` and was "the last
	// complete backup" — so the next real one came a whole interval later.
	if err := p.reachable(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		if !p.waiting {
			p.waiting = true
			p.log.Printf("a backup is due, but %v — no run is begun; asking again every %s", err, waitRetry)
		}
		p.job.NoteError(fmt.Errorf("backup waits: %w", err))
		p.setNext(now.Add(waitRetry))
		return nil
	}
	if p.waiting {
		p.waiting = false
		p.log.Printf("every database answers again: the backup begins")
	}
	p.lastRun = now
	p.warnRootMode()

	stamp := now.Format("20060102T150405Z")
	runDir := filepath.Join(p.root, stamp)
	var tables, rows, failedDBs int
	man := Manifest{Run: stamp, StartedAt: now}
	fileRows := map[string]int64{} // data file (relative path) -> rows written
	for _, t := range p.targets {
		n, r, err := p.dumpDB(ctx, t, runDir, fileRows, &man)
		if err != nil {
			p.log.Printf("backup %s: %v", t.name, err)
			p.job.NoteError(err)
			failedDBs++
			man.DatabasesFailed = append(man.DatabasesFailed, t.name)
			continue
		}
		tables += n
		rows += r
		p.job.NoteKey(t.name, int64(r))
	}
	p.job.Done.Add(int64(tables))

	// A run the role was stopped in the middle of is NOT a run (2026-10-12).
	// It used to get its manifest all the same: every table not yet reached
	// was "EXCLUDED from this snapshot", the run was verified against that
	// short manifest and called good, it counted towards `keep` — pushing an
	// older WHOLE run out — and it is what a restore of "the newest run"
	// would have used. One stopped after 380 of 431 tables did exactly that
	// (381 MB where the whole run before it was 1.9 GB). Without a manifest
	// it is incomplete: it counts for nothing, the next start makes the
	// backup again, and rotation removes it once a whole run exists.
	if ctx.Err() != nil {
		p.lastRun = time.Time{}
		p.setNext(time.Time{})
		p.log.Printf("backup %s was cut short after %d table(s) (the role is stopping): it gets NO manifest, so it does not count as a run and the backup is made again",
			stamp, tables)
		p.job.NoteError(fmt.Errorf("backup %s cut short: %w", stamp, ctx.Err()))
		return nil
	}

	// The manifest goes last, and the run is checked against it straight away
	// (S8-05): a file that did not reach the disk whole is found now, not on
	// the day of the restore.
	verify := "no manifest"
	var runErr error
	if _, err := WriteManifest(runDir, man, fileRows); err != nil {
		runErr = fmt.Errorf("manifest %s: %w", stamp, err)
	} else if _, rep, err := Verify(runDir); err != nil {
		verify, runErr = "could not verify", fmt.Errorf("verify %s: %w", stamp, err)
	} else {
		verify = rep.String()
		if !rep.OK() {
			runErr = fmt.Errorf("verify %s: %s", stamp, verify)
		}
	}
	if runErr != nil {
		p.log.Printf("backup %s: %v", stamp, runErr)
		p.job.NoteError(runErr)
	}
	// A run that left a table or a database out is not a whole backup
	// (2026-10-18, go-live P8 B1/E2/E3): its files are kept and verified, but
	// it does not count as the last complete backup, does not push a whole
	// run out, and the next try comes sooner. It used to be all of those.
	whole := man.Whole()
	good := runErr == nil && whole
	if good {
		p.partials = 0
		p.setNext(now.Add(p.interval))
	} else {
		p.partials++
		p.setNext(now.Add(p.retryWait()))
	}
	if !whole {
		why := fmt.Errorf("backup %s is NOT whole: %d table(s) left out%s — it does not count as a backup; the next try is in %s",
			stamp, len(man.ExcludedTables), failedText(man.DatabasesFailed), p.retryWait())
		p.log.Printf("%v", why)
		p.job.NoteError(why)
	} else if runErr != nil {
		p.log.Printf("backup %s could not be finished whole: the next try is in %s", stamp, p.retryWait())
	}
	p.job.Finish(now, map[string]any{"tables": tables, "rows": rows,
		"databases_failed": failedDBs, "excluded_tables": len(man.ExcludedTables), "whole": whole,
		"run": stamp, "verified": good, "verify": verify}, nil)
	p.log.Printf("backup %s: %d tables, %d rows -> %s; verify %s", stamp, tables, rows, runDir, verify)
	p.rotate()
	return nil
}

// The backup's schedule when something stands in its way (2026-10-18).
const (
	// waitRetry: a run is due and a database does not answer — ask again.
	waitRetry = time.Minute
	// partialRetry: the first wait after a run that was not whole; it doubles
	// with each such run in a row, up to the interval (retryWait), so a fault
	// that passes is mended within minutes and one that stays does not keep
	// the box backing up all day.
	partialRetry = 15 * time.Minute
	// pingFor: how long one database is given to answer the look before a run.
	pingFor = 5 * time.Second
	// tableTries: how often a table is read when its database stops answering
	// in the middle of it; dbWait: how long the run waits for it to answer
	// again, asking every pingEvery.
	tableTries = 3
	dbWait     = 2 * time.Minute
	pingEvery  = 2 * time.Second
)

// setNext records when the next run is due (zero: now).
func (p *Process) setNext(t time.Time) {
	p.nextAt = t
	if t.IsZero() {
		p.next.Store(0)
		return
	}
	p.next.Store(t.UnixNano())
}

// retryWait is the wait before the next try after p.partials runs in a row
// that were not whole: partialRetry, doubled for each one after the first, at
// most the interval.
func (p *Process) retryWait() time.Duration {
	w := partialRetry
	for i := 1; i < p.partials && w < p.interval; i++ {
		w *= 2
	}
	if p.interval > 0 && w > p.interval {
		w = p.interval
	}
	return w
}

// readDisk sets the schedule from the newest finished run on disk: a whole
// one makes the next run due an interval after it began, one that left
// something out makes it due sooner. No finished run = due now.
func (p *Process) readDisk(now time.Time) {
	last, whole := lastFinishedRun(p.root)
	if last.IsZero() {
		return
	}
	p.lastRun = last
	if whole {
		p.setNext(last.Add(p.interval))
		if wait := p.nextAt.Sub(now); wait > 0 {
			p.log.Printf("the last complete backup was started %s ago (%s); the next one is due in %s",
				now.Sub(last).Truncate(time.Second), last.Format("20060102T150405Z"), wait.Truncate(time.Second))
		}
		return
	}
	p.partials = 1
	p.setNext(last.Add(p.retryWait()))
	p.log.Printf("the newest backup on disk (%s) left something out and is not a whole backup; the next try is due in %s",
		last.Format("20060102T150405Z"), maxDur(p.nextAt.Sub(now), 0).Truncate(time.Second))
}

// reachable asks every database for an answer before a run begins.
func (p *Process) reachable(ctx context.Context) error {
	var down []string
	for _, t := range p.targets {
		c, cancel := context.WithTimeout(ctx, pingFor)
		err := t.dump.Ping(c)
		cancel()
		if err != nil {
			down = append(down, fmt.Sprintf("%s does not answer (%v)", t.name, err))
		}
	}
	if len(down) > 0 {
		return errors.New(strings.Join(down, "; "))
	}
	return nil
}

// waitFor says whether a failed read is worth doing again: the error is the
// database's connection going (not a table of its own, a seal, a full disk),
// and the database answers again within dbWait. It waits for that.
func (p *Process) waitFor(ctx context.Context, t target, what string, err error) bool {
	if err == nil || ctx.Err() != nil || !isConnErr(t.dump, err) {
		return false
	}
	p.log.Printf("backup %s %s: %v — the database does not answer; waiting up to %s for it", t.name, what, err, dbWait)
	deadline := time.Now().Add(dbWait)
	for {
		c, cancel := context.WithTimeout(ctx, pingFor)
		perr := t.dump.Ping(c)
		cancel()
		if perr == nil {
			p.log.Printf("backup %s answers again: reading %s again", t.name, what)
			return true
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(pingEvery):
		}
	}
}

// isConnErr says an error is the database connection going away (MySQL
// restarted, refusing connections, a network cut) rather than something
// about the table being read. Which errors those are is the database's DAL
// to say (dal.TableDumps.ConnLost).
func isConnErr(d dal.TableDumps, err error) bool {
	if err == nil {
		return false
	}
	var se *sealError
	if errors.As(err, &se) {
		return false // a body that could not be sealed: never the database's
	}
	return d.ConnLost(err)
}

// sealError is a body that could not be sealed: the table is left out of the
// run rather than written in plain text, and is not read again.
type sealError struct {
	row int
	err error
}

func (e *sealError) Error() string { return fmt.Sprintf("seal body (row %d): %v", e.row, e.err) }
func (e *sealError) Unwrap() error { return e.err }

// warnRootMode says once when the backup root itself is open to others on the
// host. Each run folder is made 0700, so its files cannot be read; the root
// is the operator's folder and is not changed.
func (p *Process) warnRootMode() {
	if p.saidRoot {
		return
	}
	if st, err := os.Stat(p.root); err == nil && st.Mode().Perm()&0o077 != 0 {
		p.saidRoot = true
		p.log.Printf("WARNING: the backup root %s is %#o — others on this host can list its runs; each run is made 0700, set the root to 0700 too",
			p.root, st.Mode().Perm())
	}
}

// failedText names the databases a run could not dump, for the log.
func failedText(dbs []string) string {
	if len(dbs) == 0 {
		return ""
	}
	return ", database(s) not dumped: " + strings.Join(dbs, ", ")
}

func maxDur(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

// dumpDB writes one gzipped JSONL file per table under dir. Returns table+row
// counts. A table that fails (including a body that CANNOT be sealed — see
// dumpTable) is logged and its partial file removed, but the remaining tables
// still dump: one bad table must not cost the whole database's snapshot, and a
// table that can't meet the encryption ruling is dropped from the artifact
// rather than written in plaintext (fail-closed per table, D-104).
func (p *Process) dumpDB(ctx context.Context, t target, runDir string, fileRows map[string]int64, man *Manifest) (int, int, error) {
	dir := filepath.Join(runDir, t.name)
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return 0, 0, err
	}
	// MkdirAll gives the mode only to folders it makes; the run's own folder
	// is made here too, so set both.
	_ = os.Chmod(runDir, dirMode)
	// One pool for the whole of this database's dump: the list of tables and
	// every table come from the same instance.
	db := t.dump.Reads()
	names, err := db.Tables(ctx)
	if err != nil && p.waitFor(ctx, t, "list of tables", err) {
		db = t.dump.Reads()
		names, err = db.Tables(ctx)
	}
	if err != nil {
		return 0, 0, err
	}
	var nTables, nRows int
	for _, tbl := range names {
		path := filepath.Join(dir, tbl+".jsonl.gz")
		r, err := p.dumpTable(ctx, db, tbl, path)
		// The database went away in the middle (2026-10-18, go-live P8 E3: a
		// 15 s MySQL restart left out all 42 hot tables within 2 s, no wait,
		// no retry): wait for it to answer and read the table again.
		for try := 1; err != nil && try < tableTries && p.waitFor(ctx, t, "table "+tbl, err); try++ {
			db = t.dump.Reads()
			r, err = p.dumpTable(ctx, db, tbl, path)
		}
		if err != nil {
			p.log.Printf("backup %s table %s: %v — table EXCLUDED from this snapshot", t.name, tbl, err)
			p.job.NoteError(fmt.Errorf("%s.%s excluded: %w", t.name, tbl, err))
			os.Remove(path)
			man.ExcludedTables = append(man.ExcludedTables, t.name+"."+tbl)
			continue
		}
		fileRows[t.name+"/"+tbl+".jsonl.gz"] = int64(r)
		// The table's own CREATE TABLE rides along, so a restore can rebuild a
		// table the target does not have (the per-day log tables are made at
		// run time, no migration creates them). A miss costs a log line only.
		if err := db.DumpSchema(ctx, tbl, filepath.Join(dir, tbl+".sql"), fileMode); err != nil {
			p.log.Printf("backup %s table %s: schema not saved: %v", t.name, tbl, err)
		}
		nTables++
		nRows += r
	}
	if err := db.DumpTriggers(ctx, filepath.Join(dir, triggersFile), fileMode); err != nil {
		p.log.Printf("backup %s: triggers not saved: %v", t.name, err)
	}
	return nTables, nRows, nil
}

// dumpTable writes every row of tbl into a gzip file of one JSON object per
// row. The database's DAL reads the rows and writes the file (values as
// strings or null, binary data as {"b64":"<base64>"}, 2026-09-17). Message
// BODY columns are sealed with the gateway crypto envelope before writing
// (D-104, body-only artifact encryption): a value already encrypted at rest
// passes through untouched (no double-seal), everything else is exactly as
// protected as the filesystem — deliberately, per the PO's "body only"
// ruling.
func (p *Process) dumpTable(ctx context.Context, db dal.TableReads, tbl, path string) (int, error) {
	// Sealing needs the gateway key. Without one (a deployment whose companies
	// each bring their own KMS), bodies already encrypted under a company key
	// stay encrypted in the dump and plain ones stay plain, with a warning —
	// the same as having no crypto at all, rather than failing every backup.
	seal := dal.BodySeal{On: p.cry.HasDefaultKey()}
	seal.Plain = func() {
		if !p.warned {
			p.warned = true
			p.log.Printf("WARNING: no gateway encryption key — plain body columns are dumped UNENCRYPTED")
		}
	}
	seal.Seal = func(ctx context.Context, row int, acct int64, v string) (string, error) {
		if crypto.IsCiphertext(v) {
			return v, nil
		}
		ct, err := p.cry.Encrypt(ctx, acct, 0, v)
		if err != nil && acct != 0 {
			// The account's stored DEK may be unwrappable under THIS
			// process's KEK (dev-KEK drift, rotated key): the artifact
			// still must not carry plaintext, so seal under the
			// gateway-default key instead. Said once per account
			// (go-live P8 A4: it happened with no log line, and the
			// other deployment then could not open the run).
			p.saidFallback(acct, err)
			ct, err = p.cry.Encrypt(ctx, 0, 0, v)
		}
		if err != nil {
			return "", &sealError{row: row, err: err}
		}
		return ct, nil
	}
	return db.DumpTable(ctx, tbl, path, fileMode, seal)
}

// saidFallback logs, once per account in this process, that the account's own
// data key could not be used and its bodies are sealed under the gateway key.
func (p *Process) saidFallback(acct int64, err error) {
	if p.fellBack == nil {
		p.fellBack = map[int64]bool{}
	}
	if p.fellBack[acct] {
		return
	}
	p.fellBack[acct] = true
	p.log.Printf("WARNING: account %d's data key cannot be used here (%v) — its bodies in the backup are sealed under the gateway key instead. Check that YAKU_CRYPTO_KEK_REF names the SAME key on every deployment",
		acct, err)
	p.job.NoteError(fmt.Errorf("account %d: own data key unusable, sealed under the gateway key: %w", acct, err))
}

// triggersFile holds one database's triggers: [{name, table, create}].
const triggersFile = "_triggers.json"

// rotate keeps the newest `keep` WHOLE runs under root. The manifest is part
// of the run: a directory without one is an incomplete run, so it never
// counts towards `keep` (seven broken runs must not push out the last good
// one) and it is removed once a newer whole run exists. An incomplete run
// newer than every whole one is left alone — it may be another instance's
// run still being written. A run that left something out (a manifest, not
// whole) does not count towards `keep` either (2026-10-18, go-live P8 E2: a
// run that lost its three biggest tables to a full disk rotated FIVE whole
// runs out with keep 3); it goes once a newer whole run exists, and until
// then the newest `keep` of them are kept — the best copies there are.
func (p *Process) rotate() {
	if p.keep <= 0 {
		return
	}
	ents, err := os.ReadDir(p.root)
	if err != nil {
		return
	}
	var dirs []string
	for _, e := range ents {
		if e.IsDir() && strings.HasSuffix(e.Name(), "Z") {
			dirs = append(dirs, e.Name())
		}
	}
	sort.Strings(dirs) // lexical == chronological for the fixed stamp format
	for _, old := range rotateOut(dirs, p.keep, func(d string) runState { return stateOf(filepath.Join(p.root, d)) }) {
		if err := os.RemoveAll(filepath.Join(p.root, old)); err != nil {
			p.log.Printf("rotate remove %s: %v", old, err)
		} else {
			p.log.Printf("rotated out old backup %s", old)
		}
	}
}

// lastCompleteRun is when the newest WHOLE run under root was started — a
// run's directory is named by that moment — or the zero time when there is
// none. An incomplete run (no manifest: stopped half way, or still being
// written) does not count, so a backup that was cut short is made again; nor
// does a run that left a table or a database out (2026-10-18).
func lastCompleteRun(root string) time.Time {
	var last time.Time
	eachRun(root, func(at time.Time, st runState) {
		if st == runWhole && at.After(last) {
			last = at
		}
	})
	return last
}

// lastFinishedRun is when the newest run WITH a manifest under root was
// started, and whether it is whole — the run the schedule follows.
func lastFinishedRun(root string) (time.Time, bool) {
	var last time.Time
	whole := false
	eachRun(root, func(at time.Time, st runState) {
		if st != runIncomplete && at.After(last) {
			last, whole = at, st == runWhole
		}
	})
	return last, whole
}

// eachRun calls fn for every run directory under root (named by its start).
func eachRun(root string, fn func(at time.Time, st runState)) {
	ents, err := os.ReadDir(root)
	if err != nil {
		return
	}
	for _, e := range ents {
		if !e.IsDir() {
			continue
		}
		at, err := time.Parse("20060102T150405Z", e.Name())
		if err != nil {
			continue
		}
		fn(at, stateOf(filepath.Join(root, e.Name())))
	}
}

// rotateOut picks the runs to remove from dirs (sorted oldest first).
func rotateOut(dirs []string, keep int, state func(string) runState) []string {
	var out []string
	whole, partial := 0, 0
	for i := len(dirs) - 1; i >= 0; i-- { // newest first
		switch state(dirs[i]) {
		case runWhole:
			if whole < keep {
				whole++
			} else {
				out = append(out, dirs[i])
			}
		case runPartial:
			if whole > 0 || partial >= keep { // a newer whole run exists, or enough of these
				out = append(out, dirs[i])
			} else {
				partial++
			}
		default: // incomplete
			if whole > 0 { // a newer whole run exists
				out = append(out, dirs[i])
			}
		}
	}
	return out
}
