// Package abusecount keeps, in Redis, what the abuse monitor needs to know
// about accepted traffic — so the monitor reads a few counters and not the
// message tables.
//
// Why (2026-10-13, Saleem: the abuse monitor works from counters, no SQL on
// message tables). The monitor scored every account by asking each account's
// day table for the rows of the last minutes and of the baseline hours, a
// dozen statements a table, every minute: about 9 billion rows read and
// 70,000 seconds of MySQL in 20 hours on the dev box, one connection busy
// all the time with no traffic at all.
//
// The accept path (the enqueue stage, once a message is durably queued) says
// one Event per message. Like internal/campaignwin it only adds to a map in
// memory; a loop writes the map to Redis once a second in one pipeline, so a
// slow or missing Redis never delays a message — the counts of that second
// are lost, and said.
//
// What is kept, per account:
//
//	ab:m:<minute>   hash, 4 hours    <acct>|n, |e:<encoding>, |sp, |hc, |hm, |fn, |fp, |fe, |fx, |fc, |fu
//	ab:h:<hour>     hash, 8 days     the same fields, by the hour (for baselines)
//	ab:p:<minute>   hash, 4 hours    <acct>|<prefix> = messages to numbers that begin so
//	ab:u:<acct>:<minute>  HyperLogLog, 4 hours    the numbers sent to
//	ab:t:<acct>     list, 4 hours    the newest texts (as STORED: sealed stays sealed)
//	ab:d:<yyyymmdd> hash, 35 days    <acct>|rj = parts refused for spam or a blocked number
//	ab:epoch        since when the counters are whole (set once)
//	ab:cfg          what the monitor asks for: prefix_len
//
// n = messages; e:<encoding> = messages in that encoding; sp = what they cost
// the customer, in millionths of its company's currency; hc / hm = numbers
// the HLR was asked about / did not know; f* = sums of abuse.Measure, so the
// average shape of an account's text needs no text.
//
// Reading them is reader.go. Nothing here decides anything. The keys are
// kept by the DAL (dal.AbuseCounts, moved into internal/dal/redis on 29 Oct
// 2026); the fields, the buffer and the buckets' arithmetic are here.
package abusecount

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/abuse"
	"github.com/yakugate/yakugate/internal/dal"
)

const (
	minuteKeep = 4 * time.Hour
	hourKeep   = 8*24*time.Hour + 6*time.Hour // baselines go back 168 hours and a window
	dayKeep    = 35 * 24 * time.Hour
	textKeep   = minuteKeep

	// SampleKeep is how many of an account's newest texts are kept.
	SampleKeep = 200
	// maxPrefixes is how many different prefixes one account may add to in
	// one minute from one process; the rest are counted under otherPrefix.
	maxPrefixes = 64
	otherPrefix = "*"
	// maxDests is how many numbers of one account and minute one flush
	// carries; past it the count of different numbers reads a little low.
	maxDests = 5000
	// maxHeld bounds the counts held between two flushes.
	maxHeld = 200000

	// SpendScale turns an amount into the whole number that is counted.
	SpendScale = 1_000_000
	shapeScale = 1_000_000
)

// keeps is how long each kind of count is kept after its last write.
var keeps = map[byte]time.Duration{
	dal.AbuseMinute:  minuteKeep,
	dal.AbuseHour:    hourKeep,
	dal.AbusePrefix:  minuteKeep,
	dal.AbuseDay:     dayKeep,
	dal.AbuseNumbers: minuteKeep,
	dal.AbuseTexts:   textKeep,
}

// Event is one accepted message, as the accept path says it.
type Event struct {
	AccountID int64
	// At is when the message was accepted (the time in its id), so a count
	// sits in the same minute as the record's accepted_at.
	At          time.Time
	Encoding    string
	Spend       float64 // what it costs the customer, company currency; 0 = no price
	HLRSource   string  // as on the record: db, dip, miss, dip_miss, range, …
	Destination string
	Sender      string
	// Stored is the text as the record will hold it: ciphertext for a sealed
	// message, digits hidden for a masked one. It is what the sample keeps.
	Stored string
	// Plain is the text as sent, read here for its shape only and never kept.
	Plain string
}

// Sample is one kept text: At (unix seconds), Dest, Sender, Body.
type Sample = dal.AbuseSample

type countKey struct {
	kind   byte // 'm' minute, 'h' hour, 'p' prefix, 'd' day
	bucket int64
	acct   int64
	name   string
}

type acctMinute struct {
	acct, minute int64
}

// Counter buffers and flushes. nil is a valid Counter that keeps nothing.
type Counter struct {
	a    dal.AbuseCounts
	logf func(string, ...any)

	mu       sync.Mutex
	counts   map[countKey]int64
	prefixes map[acctMinute]map[string]bool // which prefixes an account-minute has, for the cap
	dests    map[acctMinute][]string
	texts    map[int64][]Sample
	drop     int

	prefixLen int // what the monitor asked for (ab:cfg); 0 = prefixes are not counted
}

// New starts the flush loop; it stops with ctx. No store gives nil.
func New(ctx context.Context, a dal.AbuseCounts, logf func(string, ...any)) *Counter {
	if a == nil {
		return nil
	}
	k := &Counter{a: a, logf: logf}
	k.reset()
	go k.loop(ctx)
	return k
}

func (k *Counter) reset() {
	k.counts = map[countKey]int64{}
	k.prefixes = map[acctMinute]map[string]bool{}
	k.dests = map[acctMinute][]string{}
	k.texts = map[int64][]Sample{}
	k.drop = 0
}

// Note counts one accepted message. It never blocks on Redis.
func (k *Counter) Note(ev Event) {
	if k == nil || ev.AccountID <= 0 {
		return
	}
	at := ev.At
	if at.IsZero() {
		at = time.Now()
	}
	minute, hour := at.Unix()/60, at.Unix()/3600
	// Only the text as sent has a shape; the stored copy may be ciphertext.
	shape, measured := abuse.Shape{}, false
	if ev.Plain != "" {
		shape, measured = abuse.Measure(ev.Plain), true
	}

	k.mu.Lock()
	defer k.mu.Unlock()
	if len(k.counts) >= maxHeld {
		k.drop++
		return
	}
	add := func(name string, n int64) {
		if n == 0 {
			return
		}
		k.counts[countKey{'m', minute, ev.AccountID, name}] += n
		k.counts[countKey{'h', hour, ev.AccountID, name}] += n
	}
	add("n", 1)
	if ev.Encoding != "" {
		add("e:"+ev.Encoding, 1)
	}
	if ev.Spend > 0 {
		add("sp", int64(ev.Spend*SpendScale+0.5))
	}
	switch ev.HLRSource {
	case "miss", "dip_miss":
		add("hc", 1)
		add("hm", 1)
	case "db", "dip":
		add("hc", 1)
	}
	if measured {
		add("fn", 1)
		add("fp", int64(shape.Punct*shapeScale+0.5))
		add("fe", int64(shape.Emoji*shapeScale+0.5))
		add("fx", int64(shape.Padding*shapeScale+0.5))
		if shape.Cased {
			add("fc", 1)
			add("fu", int64(shape.Caps*shapeScale+0.5))
		}
	}
	am := acctMinute{ev.AccountID, minute}
	if ev.Destination != "" {
		if n := k.prefixLen; n > 0 {
			p := ev.Destination
			if len(p) > n {
				p = p[:n]
			}
			seen := k.prefixes[am]
			if seen == nil {
				seen = map[string]bool{}
				k.prefixes[am] = seen
			}
			if !seen[p] {
				if len(seen) >= maxPrefixes {
					p = otherPrefix
				} else {
					seen[p] = true
				}
			}
			k.counts[countKey{'p', minute, ev.AccountID, p}]++
		}
		if d := k.dests[am]; len(d) < maxDests {
			k.dests[am] = append(d, ev.Destination)
		}
	}
	if ev.Stored != "" {
		t := k.texts[ev.AccountID]
		if len(t) >= SampleKeep {
			t = t[1:] // the newest are kept
		}
		k.texts[ev.AccountID] = append(t, Sample{At: at.Unix(), Dest: ev.Destination, Sender: ev.Sender, Body: ev.Stored})
	}
}

// refusalCodes are the refusals the reputation score counts: a submit
// refused for spam or for a blocked number (a trap number answers the same
// code by design). The monitor's daily read of rejects_<month> counted
// exactly these.
var refusalCodes = map[string]bool{"SPAM_KEYWORD": true, "DESTINATION_BLOCKED": true}

// NoteRefusal counts a refused submit of an account, in parts (as the reject
// row has them), when its code is one the reputation score counts. It never
// blocks on Redis.
func (k *Counter) NoteRefusal(accountID int64, code string, parts int, at time.Time) {
	if k == nil || accountID <= 0 || !refusalCodes[code] {
		return
	}
	if parts < 1 {
		parts = 1
	}
	if at.IsZero() {
		at = time.Now()
	}
	day := at.UTC().Unix() / 86400
	k.mu.Lock()
	if len(k.counts) < maxHeld {
		k.counts[countKey{'d', day, accountID, "rj"}] += int64(parts)
	} else {
		k.drop++
	}
	k.mu.Unlock()
}

// Flush writes what is held to Redis now (the loop does it every second; a
// role stopping, and a test, call it themselves).
func (k *Counter) Flush(ctx context.Context) {
	if k != nil {
		k.flush(ctx)
	}
}

func (k *Counter) loop(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for n := 0; ; n++ {
		select {
		case <-ctx.Done():
			k.flush(context.Background())
			return
		case <-t.C:
			if n%30 == 0 {
				k.readConfig(ctx)
			}
			k.flush(ctx)
		}
	}
}

// readConfig takes what the monitor asks for (the prefix length of its AIT
// rule). Until the monitor has said, prefixes are not counted.
func (k *Counter) readConfig(ctx context.Context) {
	c, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	v, err := k.a.AbusePrefixLen(c)
	if err != nil || v < 0 || v > 15 {
		return
	}
	k.mu.Lock()
	k.prefixLen = v
	k.mu.Unlock()
}

func (k *Counter) flush(ctx context.Context) {
	k.mu.Lock()
	if len(k.counts) == 0 && len(k.texts) == 0 && len(k.dests) == 0 {
		k.mu.Unlock()
		return
	}
	counts, dests, texts, drop := k.counts, k.dests, k.texts, k.drop
	// The prefix cap is per minute: keep the sets of minutes still open.
	nowMin := time.Now().Unix() / 60
	keepPrefixes := map[acctMinute]map[string]bool{}
	for am, set := range k.prefixes {
		if am.minute >= nowMin-1 {
			keepPrefixes[am] = set
		}
	}
	k.reset()
	k.prefixes = keepPrefixes
	k.mu.Unlock()

	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	// Since when the counters are whole: set by the first flush of the first
	// accept process, and set again by the first flush after Redis lost
	// everything — which is what makes the monitor wait for a new baseline.
	f := dal.AbuseFlush{Since: time.Now(), Keep: keeps, SampleKeep: SampleKeep,
		Counts: make([]dal.AbuseCount, 0, len(counts))}
	for ck, n := range counts {
		field := strconv.FormatInt(ck.acct, 10) + "|" + ck.name
		f.Counts = append(f.Counts, dal.AbuseCount{Kind: ck.kind, Bucket: ck.bucket, Field: field, N: n})
	}
	for am, list := range dests {
		f.Numbers = append(f.Numbers, dal.AbuseNumberSet{Account: am.acct, Minute: am.minute, Numbers: list})
	}
	for acct, list := range texts {
		f.Texts = append(f.Texts, dal.AbuseTextSet{Account: acct, Samples: list})
	}
	if err := k.a.FlushAbuse(cctx, f); err != nil {
		if k.logf != nil {
			k.logf("abuse counters: one second of counts lost (%d counts, %d accounts' texts): %v", len(counts), len(texts), err)
		}
		return
	}
	if drop > 0 && k.logf != nil {
		k.logf("abuse counters: %d messages not counted (more than %d counts between two flushes)", drop, maxHeld)
	}
}
