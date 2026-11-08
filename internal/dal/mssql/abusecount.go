package mssql

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// AbuseCounts is the abuse monitor's counters in the config store
// (dal.AbuseCounts) for an install with no Redis (mirrors
// internal/dal/mysql/abusecount.go), keyed and bucketed as the Redis keys
// are:
//
//	ab:<kind>:<bucket> hash   abuse_counts (kind, bucket, field)
//	ab:u:<acct>:<minute> HLL  abuse_numbers (account, minute, writer): a sketch
//	                          per process, merged by the reader (hll.go)
//	ab:t:<acct> list          abuse_texts (account, writer, slot): a ring of
//	                          `keep` rows per process, newest read by (at, seq)
//	ab:epoch, ab:cfg          abuse_marks
//
// internal/abusecount flushes once a second; a flush is at most four
// statements (the epoch, the counts, the number sketches, the texts), each
// 500 rows at most, the rows sent as a table-valued parameter. Each process
// writes only its own sketch and text rows.
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

// abuseSketchMinutes is how many minutes back this handle keeps its
// sketches.
const abuseSketchMinutes = 5

// The rows of a flush, each a row of its table type (columns in order).
type (
	abuseCountRow struct {
		Kind      []byte
		Bucket    int64
		Field     []byte
		N         int64
		ExpiresAt int64
	}
	abuseNumberRow struct {
		Account   int64
		Minute    int64
		Writer    int64
		Sketch    []byte
		ExpiresAt int64
	}
	abuseTextRow struct {
		Account   int64
		Writer    int64
		Slot      int32
		At        int64
		Seq       int64
		Sample    string
		ExpiresAt int64
	}
)

var (
	// abuse.epoch_set is MySQL's INSERT IGNORE: set once, by the first
	// flush.
	stAbuseEpochSet = CounterStmts.Add("abuse.epoch_set", `
		INSERT INTO dbo.abuse_marks (name, value) SELECT N'epoch', @p1
		 WHERE NOT EXISTS (SELECT 1 FROM dbo.abuse_marks WITH (UPDLOCK, SERIALIZABLE) WHERE name = N'epoch')`, true)
	stAbuseEpoch  = CounterStmts.Add("abuse.epoch", `SELECT value FROM dbo.abuse_marks WHERE name = N'epoch'`, false)
	stAbuseMark   = CounterStmts.Add("abuse.mark", `SELECT value FROM dbo.abuse_marks WHERE name = @p1`, false)
	stAbuseAskSet = CounterStmts.Add("abuse.ask_set", `SET NOCOUNT ON; SET XACT_ABORT ON;
BEGIN TRAN;
UPDATE t SET value = @len FROM dbo.abuse_marks AS t WITH (UPDLOCK, SERIALIZABLE) WHERE t.name = N'prefix_len';
IF @@ROWCOUNT = 0 INSERT INTO dbo.abuse_marks (name, value) VALUES (N'prefix_len', @len);
UPDATE t SET value = @since FROM dbo.abuse_marks AS t WITH (UPDLOCK, SERIALIZABLE) WHERE t.name = N'since';
IF @@ROWCOUNT = 0 INSERT INTO dbo.abuse_marks (name, value) VALUES (N'since', @since);
COMMIT;`, false)
	// abuse.count_add: an ended row starts again from the new count.
	stAbuseCountAdd = AddUpsertBatch(CounterStmts, "abuse.count_add", UpsertSpec{
		Table: "dbo.abuse_counts", Keys: []string{"kind", "bucket", "field"},
		Set: []string{
			"n = CASE WHEN t.expires_at <= @now THEN $n ELSE t.n + $n END",
			"expires_at = $expires_at",
		},
		Type: "dbo.abuse_count_rows",
	}, true)
	stAbuseNumbersPut = AddUpsertBatch(CounterStmts, "abuse.numbers_put", UpsertSpec{
		Table: "dbo.abuse_numbers", Keys: []string{"account", "minute", "writer"},
		Set:  []string{"sketch = $sketch", "expires_at = $expires_at"},
		Type: "dbo.abuse_number_rows",
	}, true)
	stAbuseTextsPut = AddUpsertBatch(CounterStmts, "abuse.texts_put", UpsertSpec{
		Table: "dbo.abuse_texts", Keys: []string{"account", "writer", "slot"},
		Set:  []string{"[at] = $at", "seq = $seq", "sample = $sample", "expires_at = $expires_at"},
		Type: "dbo.abuse_text_rows",
	}, true)
	// The reads are written with their bucket or minute list at the call.
	stAbuseBuckets = CounterStmts.Add("abuse.buckets", `
		SELECT bucket, field, n FROM dbo.abuse_counts WHERE kind = @kind AND bucket IN (@in) AND expires_at > @now`, false)
	stAbuseNumbers = CounterStmts.Add("abuse.numbers", `
		SELECT sketch FROM dbo.abuse_numbers WHERE account = @account AND minute IN (@in) AND expires_at > @now`, false)
	stAbuseSamples = CounterStmts.Add("abuse.samples", `
		SELECT TOP (@p3) sample FROM dbo.abuse_texts WHERE account = @p1 AND expires_at > @p2 ORDER BY [at] DESC, seq DESC`, false)
)

// abuseKind is the kind a bucket is kept under: as Redis's abuseKey, any
// kind but hour, prefix and day is a minute.
func abuseKind(k byte) []byte {
	switch k {
	case dal.AbuseHour, dal.AbusePrefix, dal.AbuseDay:
		return []byte{k}
	}
	return []byte{dal.AbuseMinute}
}

// abuseKeepOf is the end of a row of kind written now.
func abuseKeepOf(f dal.AbuseFlush, kind byte, now int64) int64 {
	return now + int64(max(f.Keep[kind], time.Second)/time.Second)
}

type abuseKey struct {
	kind   byte
	bucket int64
	field  string
}

// FlushAbuse writes a flush: at most four statements (more only past 500
// rows of a kind).
func (a *AbuseCounts) FlushAbuse(ctx context.Context, f dal.AbuseFlush) error {
	now := time.Now().Unix()
	// Since when the counters are whole: set once, by the first flush.
	if _, err := counterExec(ctx, a.db, stAbuseEpochSet, f.Since.UTC().Format(time.RFC3339)); err != nil && !IsDuplicate(err) {
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
	sum := map[abuseKey]int64{}
	ends := map[abuseKey]int64{}
	for _, c := range f.Counts {
		if c.Field == "" || len(c.Field) > 191 {
			continue // no field the package names is this long
		}
		k := abuseKey{abuseKind(c.Kind)[0], c.Bucket, c.Field}
		sum[k] += c.N
		ends[k] = abuseKeepOf(f, c.Kind, now)
	}
	keys := make([]abuseKey, 0, len(sum))
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
	for from := 0; from < len(keys); from += counterRowsPerStatement {
		part := keys[from:min(from+counterRowsPerStatement, len(keys))]
		rows := make([]abuseCountRow, len(part))
		for i, k := range part {
			rows[i] = abuseCountRow{[]byte{k.kind}, k.bucket, []byte(k.field), sum[k], ends[k]}
		}
		if err := Again(ctx, counterTries, func() error {
			return upsertRows(ctx, a.db, stAbuseCountAdd, rows, sql.Named("now", now))
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
	ends := abuseKeepOf(f, dal.AbuseNumbers, now)
	a.mu.Lock()
	oldest := now/60 - abuseSketchMinutes
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
	puts := make([]abuseNumberRow, 0, len(touched))
	for k := range touched {
		puts = append(puts, abuseNumberRow{k[0], k[1], a.writer, a.sketches[k].encode(), ends})
	}
	a.mu.Unlock()
	sort.Slice(puts, func(i, j int) bool {
		if puts[i].Account != puts[j].Account {
			return puts[i].Account < puts[j].Account
		}
		return puts[i].Minute < puts[j].Minute
	})
	for from := 0; from < len(puts); from += counterRowsPerStatement {
		if err := upsertRows(ctx, a.db, stAbuseNumbersPut, puts[from:min(from+counterRowsPerStatement, len(puts))]); err != nil {
			return err
		}
	}
	return nil
}

// putTexts writes each account's texts into this handle's ring of keep
// rows: the newest keep of them, each in the slot of its number. A slot
// written twice in one flush keeps the later text, as MySQL's multi-row
// upsert does.
func (a *AbuseCounts) putTexts(ctx context.Context, f dal.AbuseFlush, now int64) error {
	keep := f.SampleKeep
	if keep < 1 || len(f.Texts) == 0 {
		return nil
	}
	ends := abuseKeepOf(f, dal.AbuseTexts, now)
	var rows []abuseTextRow
	at := map[[2]int64]int{} // (account, slot) → its row
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
			r := abuseTextRow{set.Account, a.writer, int32(n % int64(keep)), s.At, n, string(b), ends}
			k := [2]int64{set.Account, int64(r.Slot)}
			if i, ok := at[k]; ok {
				rows[i] = r
				continue
			}
			at[k] = len(rows)
			rows = append(rows, r)
		}
	}
	a.mu.Unlock()
	for from := 0; from < len(rows); from += counterRowsPerStatement {
		if err := upsertRows(ctx, a.db, stAbuseTextsPut, rows[from:min(from+counterRowsPerStatement, len(rows))]); err != nil {
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
	observe(stAbuseMark, start, 1)
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
	_, err := counterExec(ctx, a.db, stAbuseAskSet,
		sql.Named("len", strconv.Itoa(n)), sql.Named("since", now.Format(time.RFC3339)))
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
	observe(stAbuseEpoch, start, 1)
	t, perr := time.Parse(time.RFC3339, v)
	if perr != nil {
		return time.Time{}, false, nil
	}
	return t, true, nil
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
	for from := 0; from < len(buckets); from += counterRowsPerStatement {
		list, args := namedList("b", buckets[from:min(from+counterRowsPerStatement, len(buckets))])
		args = append(args, sql.Named("kind", k), sql.Named("now", now))
		start := time.Now()
		rows, err := a.db.QueryContext(ctx, withList(stAbuseBuckets.SQL, list), args...)
		if err != nil {
			return nil, err
		}
		var n int64
		for rows.Next() {
			var b, cnt int64
			var field []byte
			if err := rows.Scan(&b, &field, &cnt); err != nil {
				rows.Close()
				return nil, err
			}
			for _, i := range at[b] {
				out[i][string(field)] = cnt
			}
			n++
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
		observe(stAbuseBuckets, start, n)
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
	for from := 0; from < len(minutes); from += counterRowsPerStatement {
		list, args := namedList("m", minutes[from:min(from+counterRowsPerStatement, len(minutes))])
		args = append(args, sql.Named("account", account), sql.Named("now", now))
		start := time.Now()
		rows, err := a.db.QueryContext(ctx, withList(stAbuseNumbers.SQL, list), args...)
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
		observe(stAbuseNumbers, start, n)
	}
	return all.count(), nil
}

// AbuseSamples reads an account's kept texts, newest first, at most keep.
func (a *AbuseCounts) AbuseSamples(ctx context.Context, account int64, keep int) ([]dal.AbuseSample, error) {
	if keep < 1 {
		return nil, nil
	}
	start := time.Now()
	rows, err := a.db.QueryContext(ctx, stAbuseSamples.SQL, account, time.Now().Unix(), int64(keep))
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
	observe(stAbuseSamples, start, int64(len(out)))
	return out, nil
}
