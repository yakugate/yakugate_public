package monitor

// HourHist counts values into fixed buckets over the last hour, in twelve
// five-minute slots.
//
// Why buckets and not a ring of raw samples like Latency: the same account's
// traffic is spread over several processes — two http-servers, P3 and P5 both
// applying receipts — and percentiles do not add up across processes. Bucket
// counts do. Each process publishes its counts, the Admin API sums them, and
// the p50/p95 it reads off the sum is exact to a bucket's width, which is the
// honest resolution of the question anyway.
//
// Not safe for concurrent use: the owner holds its own lock.
type HourHist struct {
	bounds []float64 // upper bounds, ascending; one extra bucket catches the rest
	slots  [hourSlots]histSlot
}

const (
	hourSlots   = 12
	hourSlotSec = 300
)

type histSlot struct {
	start  int64
	counts []int64
}

// NewHourHist makes a histogram over the given ascending upper bounds.
func NewHourHist(bounds []float64) *HourHist { return &HourHist{bounds: bounds} }

// Add counts one value at unix time now.
func (h *HourHist) Add(v float64, now int64) {
	s := h.slot(now)
	i := 0
	for i < len(h.bounds) && v > h.bounds[i] {
		i++
	}
	s.counts[i]++
}

// Counts sums the slots of the last hour.
func (h *HourHist) Counts(now int64) []int64 {
	out := make([]int64, len(h.bounds)+1)
	for i := range h.slots {
		s := &h.slots[i]
		if s.counts == nil || now-s.start >= hourSlots*hourSlotSec {
			continue
		}
		for j, c := range s.counts {
			out[j] += c
		}
	}
	return out
}

func (h *HourHist) slot(now int64) *histSlot {
	start := now - now%hourSlotSec
	s := &h.slots[(now/hourSlotSec)%hourSlots]
	if s.start != start || s.counts == nil {
		s.start = start
		s.counts = make([]int64, len(h.bounds)+1)
	}
	return s
}

// HistQuantile reads the q-th quantile (0 < q <= 1) off bucket counts as the
// upper bound of the bucket it falls in. over=true means it fell past the last
// bound: "more than bounds[last]", not a value. ok=false for no samples.
func HistQuantile(bounds []float64, counts []int64, q float64) (v float64, over, ok bool) {
	var n int64
	for _, c := range counts {
		n += c
	}
	if n == 0 || len(bounds) == 0 {
		return 0, false, false
	}
	rank := int64(float64(n)*q + 0.999999) // ceil, so p95 of 20 is the 19th
	if rank < 1 {
		rank = 1
	}
	var seen int64
	for i, c := range counts {
		seen += c
		if seen >= rank {
			if i >= len(bounds) {
				return bounds[len(bounds)-1], true, true
			}
			return bounds[i], false, true
		}
	}
	return bounds[len(bounds)-1], true, true
}

// DeliveryBoundsSec buckets accept-to-delivered time: a handset reached in a
// second, a queue minutes deep, a vendor sitting on receipts for hours.
var DeliveryBoundsSec = []float64{1, 2, 5, 10, 30, 60, 120, 300, 600, 1800, 3600, 10800}

// AcceptBoundsMs buckets our own accept-path time per message (target p95
// under 50 ms).
var AcceptBoundsMs = []float64{1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000}
