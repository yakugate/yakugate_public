package mysql

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// AbuseCounts is the abuse monitor's counters in the config store
// (dal.AbuseCounts) for a gateway with no Redis (config/0166), keyed and
// bucketed as the Redis keys are:
//
//	ab:<kind>:<bucket> hash   abuse_counts (kind, bucket, field)
//	ab:u:<acct>:<minute> HLL  abuse_numbers (account, minute, writer): a sketch
//	                          per process, merged by the reader (hll.go)
//	ab:t:<acct> list          abuse_texts (account, writer, slot): a ring of
//	                          `keep` rows per process, newest read by (at, seq)
//	ab:epoch, ab:cfg          abuse_marks
//
// internal/abusecount buffers in memory and flushes once a second; a flush is
// at most four statements (the epoch, the counts, the number sketches, the
// texts), each 500 rows at most — never a statement a message. Each process
// writes only its own sketch and text rows, so two processes never fight
// over one row there; the counts are added in key order (no deadlock between
// two flushes).
type AbuseCounts struct {
	db *sql.DB
	// writer names this handle's own sketch and text rows (a random number:
	// a restarted process writes new rows, and the old ones end with their
	// keep time).
	writer int64

	mu       sync.Mutex
	sketches map[[2]int64]*hll // (account, minute) → the numbers this handle saw
	seq      map[int64]int64   // account → its texts written by this handle
}

// NewAbuseCounts keeps the counters on the config store's pool.
func NewAbuseCounts(db *sql.DB) *AbuseCounts {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return &AbuseCounts{db: db, writer: int64(binary.BigEndian.Uint64(b[:]) >> 1),
		sketches: map[[2]int64]*hll{}, seq: map[int64]int64{}}
}

// sketchMinutes is how many minutes back this handle keeps its sketches: a
// flush carries the minute it was noted in (this one, or the last at a
// minute's turn).
const sketchMinutes = 5

var (
	stAbuseEpochSet = counterStmts.Add("abuse.epoch_set", `INSERT IGNORE INTO abuse_marks (name, value) VALUES ('epoch', ?)`, true)
	stAbuseEpoch    = counterStmts.Add("abuse.epoch", `SELECT value FROM abuse_marks WHERE name = 'epoch'`, false)
	stAbuseMark     = counterStmts.Add("abuse.mark", `SELECT value FROM abuse_marks WHERE name = ?`, false)
	stAbuseAskSet   = counterStmts.Add("abuse.ask_set", `
		INSERT INTO abuse_marks (name, value) VALUES ('prefix_len', ?), ('since', ?)
		ON DUPLICATE KEY UPDATE value = VALUES(value)`, false)
	// abuse.count_add is written with its rows repeated (rowsOf). n is
	// assigned before expires_at, so its IF sees the old end.
	stAbuseCountAdd = counterStmts.Add("abuse.count_add", `
		INSERT INTO abuse_counts (kind, bucket, field, n, expires_at) VALUES (?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		  n = IF(expires_at <= ?, VALUES(n), n + VALUES(n)),
		  expires_at = VALUES(expires_at)`, true)
	stAbuseNumbersPut = counterStmts.Add("abuse.numbers_put", `
		INSERT INTO abuse_numbers (account, minute, writer, sketch, expires_at) VALUES (?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE sketch = VALUES(sketch), expires_at = VALUES(expires_at)`, true)
	stAbuseTextsPut = counterStmts.Add("abuse.texts_put", `
		INSERT INTO abuse_texts (account, writer, slot, at, seq, sample, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE at = VALUES(at), seq = VALUES(seq), sample = VALUES(sample), expires_at = VALUES(expires_at)`, true)
	// The reads are written with their bucket or minute list repeated.
	stAbuseBuckets = counterStmts.Add("abuse.buckets", `
		SELECT bucket, field, n FROM abuse_counts FORCE INDEX (PRIMARY) WHERE kind = ? AND bucket IN (?) AND expires_at > ?`, false)
	stAbuseNumbers = counterStmts.Add("abuse.numbers", `
		SELECT sketch FROM abuse_numbers FORCE INDEX (PRIMARY) WHERE account = ? AND minute IN (?) AND expires_at > ?`, false)
	stAbuseSamples = counterStmts.Add("abuse.samples", `
		SELECT sample FROM abuse_texts WHERE account = ? AND expires_at > ? ORDER BY at DESC, seq DESC LIMIT ?`, false)
)

// abuseKind is the kind a bucket is kept under: as Redis's abuseKey, any
// kind but hour, prefix and day is a minute.
func abuseKind(k byte) string {
	switch k {
	case dal.AbuseHour, dal.AbusePrefix, dal.AbuseDay:
		return string(k)
	}
	return string(dal.AbuseMinute)
}

// keepOf is the end of a row of kind written now.
func keepOf(f dal.AbuseFlush, kind byte, now int64) int64 {
	return now + int64(max(f.Keep[kind], time.Second)/time.Second)
}

type abuseRow struct {
	kind   string
	bucket int64
	field  string
}

// FlushAbuse writes a flush: at most four statements (more only past 500
// rows of a kind).
func (a *AbuseCounts) FlushAbuse(ctx context.Context, f dal.AbuseFlush) error {
	now := time.Now().Unix()
	// Since when the counters are whole: set once, by the first flush.
	if _, err := exec(ctx, a.db, stAbuseEpochSet, f.Since.UTC().Format(time.RFC3339)); err != nil {
		return err
	}
	if err := a.addCounts(ctx, f, now); err != nil {
		return err
	}
	if err := a.putNumbers(ctx, f, now); err != nil {
		return err
	}
	return a.putTexts(ctx, f, now)
}

func (a *AbuseCounts) addCounts(ctx context.Context, f dal.AbuseFlush, now int64) error {
	sum := map[abuseRow]int64{}
	ends := map[abuseRow]int64{}
	for _, c := range f.Counts {
		if c.Field == "" || len(c.Field) > 191 {
			continue // no field the package names is this long
		}
		k := abuseRow{abuseKind(c.Kind), c.Bucket, c.Field}
		sum[k] += c.N
		ends[k] = keepOf(f, c.Kind, now)
	}
	keys := make([]abuseRow, 0, len(sum))
	for k := range sum {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		x, y := keys[i], keys[j]
		if x.kind != y.kind {
			return x.kind < y.kind
		}
		if x.bucket != y.bucket {
			return x.bucket < y.bucket
		}
		return x.field < y.field
	})
	head, tail := splitValues(stAbuseCountAdd.SQL)
	for from := 0; from < len(keys); from += rowsPerStatement {
		part := keys[from:min(from+rowsPerStatement, len(keys))]
		args := make([]any, 0, len(part)*5+1)
		for _, k := range part {
			args = append(args, k.kind, k.bucket, k.field, sum[k], ends[k])
		}
		args = append(args, now)
		q := head + rowsOf(5, len(part)) + tail
		if err := againIfVictimN(ctx, 4, func() error {
			_, err := execText(ctx, a.db, stAbuseCountAdd, q, args...)
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}

// putNumbers adds the flush's numbers to this handle's sketches and writes
// each sketch touched whole. A flush that fails is not lost: its numbers stay
// in the sketch the next flush writes.
func (a *AbuseCounts) putNumbers(ctx context.Context, f dal.AbuseFlush, now int64) error {
	if len(f.Numbers) == 0 {
		return nil
	}
	type put struct {
		acct, minute int64
		sketch       []byte
	}
	a.mu.Lock()
	oldest := now/60 - sketchMinutes
	for k := range a.sketches {
		if k[1] < oldest {
			delete(a.sketches, k)
		}
	}
	touched := map[[2]int64]bool{}
	for _, set := range f.Numbers {
		k := [2]int64{set.Account, set.Minute}
		h := a.sketches[k]
		if h == nil {
			h = &hll{}
			a.sketches[k] = h
		}
		for _, d := range set.Numbers {
			h.add(d)
		}
		touched[k] = true
	}
	puts := make([]put, 0, len(touched))
	for k := range touched {
		puts = append(puts, put{k[0], k[1], a.sketches[k].encode()})
	}
	a.mu.Unlock()
	sort.Slice(puts, func(i, j int) bool {
		if puts[i].acct != puts[j].acct {
			return puts[i].acct < puts[j].acct
		}
		return puts[i].minute < puts[j].minute
	})
	ends := keepOf(f, dal.AbuseNumbers, now)
	head, tail := splitValues(stAbuseNumbersPut.SQL)
	for from := 0; from < len(puts); from += rowsPerStatement {
		part := puts[from:min(from+rowsPerStatement, len(puts))]
		args := make([]any, 0, len(part)*5)
		for _, p := range part {
			args = append(args, p.acct, p.minute, a.writer, p.sketch, ends)
		}
		if _, err := execText(ctx, a.db, stAbuseNumbersPut, head+rowsOf(5, len(part))+tail, args...); err != nil {
			return err
		}
	}
	return nil
}

// putTexts writes each account's texts into this handle's ring of keep
// rows: the newest keep of them, each in the slot of its number.
func (a *AbuseCounts) putTexts(ctx context.Context, f dal.AbuseFlush, now int64) error {
	keep := f.SampleKeep
	if keep < 1 || len(f.Texts) == 0 {
		return nil
	}
	ends := keepOf(f, dal.AbuseTexts, now)
	var args []any
	rows := 0
	a.mu.Lock()
	for _, set := range f.Texts {
		samples := set.Samples
		if len(samples) > keep {
			samples = samples[len(samples)-keep:]
		}
		for _, s := range samples {
			b, err := json.Marshal(s)
			if err != nil {
				continue
			}
			n := a.seq[set.Account] + 1
			a.seq[set.Account] = n
			args = append(args, set.Account, a.writer, int(n%int64(keep)), s.At, n, string(b), ends)
			rows++
		}
	}
	a.mu.Unlock()
	head, tail := splitValues(stAbuseTextsPut.SQL)
	for from := 0; from < rows; from += rowsPerStatement {
		to := min(from+rowsPerStatement, rows)
		if _, err := execText(ctx, a.db, stAbuseTextsPut, head+rowsOf(7, to-from)+tail, args[from*7:to*7]...); err != nil {
			return err
		}
	}
	return nil
}

// mark reads one row of abuse_marks; ok=false when it is not there.
func (a *AbuseCounts) mark(ctx context.Context, name string) (string, bool, error) {
	start := time.Now()
	var v string
	err := a.db.QueryRowContext(ctx, stAbuseMark.SQL, name).Scan(&v)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	stAbuseMark.Observe(time.Since(start), 1)
	return v, true, nil
}

// AbusePrefixLen reads the prefix length the monitor asked for; an error
// when it has asked nothing (as Redis's HGET answers nil).
func (a *AbuseCounts) AbusePrefixLen(ctx context.Context) (int, error) {
	v, ok, err := a.mark(ctx, "prefix_len")
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, dal.ErrNoRows
	}
	return strconv.Atoi(v)
}

// AskAbusePrefixLen sets the prefix length to count, unless it is already
// asked for, and answers since when it has been.
func (a *AbuseCounts) AskAbusePrefixLen(ctx context.Context, n int) (time.Time, error) {
	cur, _, _ := a.mark(ctx, "prefix_len")
	since, _, _ := a.mark(ctx, "since")
	if cur == strconv.Itoa(n) && since != "" {
		if t, err := time.Parse(time.RFC3339, since); err == nil {
			return t, nil
		}
	}
	now := time.Now().UTC()
	_, err := exec(ctx, a.db, stAbuseAskSet, strconv.Itoa(n), now.Format(time.RFC3339))
	return now, err
}

// AbuseEpoch reads since when the counters are whole.
func (a *AbuseCounts) AbuseEpoch(ctx context.Context) (time.Time, bool, error) {
	start := time.Now()
	var v string
	err := a.db.QueryRowContext(ctx, stAbuseEpoch.SQL).Scan(&v)
	if err == sql.ErrNoRows {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	stAbuseEpoch.Observe(time.Since(start), 1)
	t, perr := time.Parse(time.RFC3339, v)
	if perr != nil {
		return time.Time{}, false, nil
	}
	return t, true, nil
}

// inList puts n placeholders in a registered statement's "IN (?)".
func inList(q string, n int) string {
	return strings.Replace(q, "IN (?)", "IN ("+placeholders(n)+")", 1)
}

// AbuseBuckets reads buckets of one kind: one statement per 500 buckets.
func (a *AbuseCounts) AbuseBuckets(ctx context.Context, kind byte, buckets []int64) ([]map[string]int64, error) {
	out := make([]map[string]int64, len(buckets))
	at := make(map[int64][]int, len(buckets))
	for i, b := range buckets {
		out[i] = map[string]int64{}
		at[b] = append(at[b], i)
	}
	now := time.Now().Unix()
	k := abuseKind(kind)
	for from := 0; from < len(buckets); from += rowsPerStatement {
		part := buckets[from:min(from+rowsPerStatement, len(buckets))]
		args := make([]any, 0, len(part)+2)
		args = append(args, k)
		for _, b := range part {
			args = append(args, b)
		}
		args = append(args, now)
		start := time.Now()
		rows, err := a.db.QueryContext(ctx, inList(stAbuseBuckets.SQL, len(part)), args...)
		if err != nil {
			return nil, err
		}
		var n int64
		for rows.Next() {
			var b, cnt int64
			var field string
			if err := rows.Scan(&b, &field, &cnt); err != nil {
				rows.Close()
				return nil, err
			}
			for _, i := range at[b] {
				out[i][field] = cnt
			}
			n++
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
		stAbuseBuckets.Observe(time.Since(start), n)
	}
	return out, nil
}

// AbuseDistinct estimates the different numbers of an account over minutes:
// every process's sketch of those minutes, merged.
func (a *AbuseCounts) AbuseDistinct(ctx context.Context, account int64, minutes []int64) (int64, error) {
	if len(minutes) == 0 {
		return 0, nil
	}
	var all hll
	now := time.Now().Unix()
	for from := 0; from < len(minutes); from += rowsPerStatement {
		part := minutes[from:min(from+rowsPerStatement, len(minutes))]
		args := make([]any, 0, len(part)+2)
		args = append(args, account)
		for _, m := range part {
			args = append(args, m)
		}
		args = append(args, now)
		start := time.Now()
		rows, err := a.db.QueryContext(ctx, inList(stAbuseNumbers.SQL, len(part)), args...)
		if err != nil {
			return 0, err
		}
		var n int64
		for rows.Next() {
			var b []byte
			if err := rows.Scan(&b); err != nil {
				rows.Close()
				return 0, err
			}
			all.decodeInto(b)
			n++
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return 0, err
		}
		stAbuseNumbers.Observe(time.Since(start), n)
	}
	return all.count(), nil
}

// AbuseSamples reads an account's kept texts, newest first, at most keep.
func (a *AbuseCounts) AbuseSamples(ctx context.Context, account int64, keep int) ([]dal.AbuseSample, error) {
	if keep < 1 {
		return nil, nil
	}
	start := time.Now()
	rows, err := a.db.QueryContext(ctx, stAbuseSamples.SQL, account, time.Now().Unix(), keep)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]dal.AbuseSample, 0, keep)
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		var s dal.AbuseSample
		if json.Unmarshal([]byte(v), &s) != nil {
			continue
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	stAbuseSamples.Observe(time.Since(start), int64(len(out)))
	return out, nil
}
