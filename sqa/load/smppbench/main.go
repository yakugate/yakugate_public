// smppbench — SMPP submit_sm load & latency benchmark for Yakugate P2.
//
// Opens N concurrent transceiver binds and fires submit_sm as fast as each
// connection allows (send -> wait submit_sm_resp -> record), reporting
// throughput (submit/s and segment/s) and per-submit latency percentiles
// (p50/p90/p95/p99/max). Self-contained SMPP v3.4 client — no engine imports.
//
// Measures the P2 targets: sustained segments/sec per process and p95 latency.
// Point it at an account whose company has a high max_throughput and dedup off
// (see the `loadtest` fixture) so rate-limit / dedupe don't distort the numbers.
//
//	go run ./sqa/load/smppbench -conns 200 -dur 15s -sysid loadtest -pw loadtest
package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	bindTransceiver = 0x00000009
	submitSM        = 0x00000004
	unbind          = 0x00000006
)

// ---- minimal SMPP PDU codec ----
func cstr(s string) []byte { return append([]byte(s), 0) }

func pdu(cmd, status, seq uint32, body []byte) []byte {
	b := make([]byte, 16+len(body))
	binary.BigEndian.PutUint32(b[0:], uint32(16+len(body)))
	binary.BigEndian.PutUint32(b[4:], cmd)
	binary.BigEndian.PutUint32(b[8:], status)
	binary.BigEndian.PutUint32(b[12:], seq)
	copy(b[16:], body)
	return b
}

func readPDU(c net.Conn) (cmd, status, seq uint32, body []byte, err error) {
	h := make([]byte, 16)
	if _, err = readFull(c, h); err != nil {
		return
	}
	ln := binary.BigEndian.Uint32(h[0:])
	cmd = binary.BigEndian.Uint32(h[4:])
	status = binary.BigEndian.Uint32(h[8:])
	seq = binary.BigEndian.Uint32(h[12:])
	if ln > 16 {
		body = make([]byte, ln-16)
		_, err = readFull(c, body)
	}
	return
}

func readFull(c net.Conn, b []byte) (int, error) {
	got := 0
	for got < len(b) {
		n, err := c.Read(b[got:])
		if err != nil {
			return got, err
		}
		got += n
	}
	return got, nil
}

func bindTRX(c net.Conn, sysID, pw string) (uint32, error) {
	body := append(append(append(cstr(sysID), cstr(pw)...), cstr("")...), 0x34, 0, 0)
	body = append(body, cstr("")...)
	if _, err := c.Write(pdu(bindTransceiver, 0, 1, body)); err != nil {
		return 0, err
	}
	_, st, _, _, err := readPDU(c)
	return st, err
}

// submitSMBody builds one submit_sm with a GSM-7 body of the requested segment
// count (160 + 153*(segs-1) chars, well under the 254-octet short_message cap
// only for segs==1; multi-segment uses message_payload TLV).
func submitBody(sender, dest, text string) []byte {
	sm := []byte(text)
	var payloadTLV []byte
	if len(sm) > 254 {
		// carry long bodies in message_payload (0x0424); empty short_message
		t := make([]byte, 4+len(sm))
		binary.BigEndian.PutUint16(t[0:], 0x0424)
		binary.BigEndian.PutUint16(t[2:], uint16(len(sm)))
		copy(t[4:], sm)
		payloadTLV = t
		sm = nil
	}
	b := cstr("")                                  // service_type
	b = append(b, 5, 0)                            // source TON(alnum)/NPI
	b = append(b, cstr(sender)...)                 // source_addr
	b = append(b, 1, 1)                            // dest TON(intl)/NPI(ISDN)
	b = append(b, cstr(dest)...)                   // destination_addr
	b = append(b, 0, 0, 0)                         // esm_class, protocol_id, priority
	b = append(b, cstr("")...)                     // schedule_delivery_time
	b = append(b, cstr("")...)                     // validity_period
	b = append(b, 0, 0, 0, 0)                      // reg_delivery, replace, data_coding, sm_default
	b = append(b, byte(len(sm)))                   // sm_length
	b = append(b, sm...)                           // short_message
	b = append(b, payloadTLV...)                   // optional message_payload TLV
	return b
}

// ---- latency histogram (0.1ms buckets up to 200ms + overflow) ----
const bucketNs = 100_000 // 0.1 ms
const nBuckets = 2001    // 0..2000 (>200ms = index 2000)

type hist struct {
	b     [nBuckets]int64
	count int64
	sumNs int64
	maxNs int64
}

func (h *hist) add(ns int64) {
	h.count++
	h.sumNs += ns
	if ns > h.maxNs {
		h.maxNs = ns
	}
	i := ns / bucketNs
	if i >= nBuckets {
		i = nBuckets - 1
	}
	h.b[i]++
}
func (h *hist) merge(o *hist) {
	h.count += o.count
	h.sumNs += o.sumNs
	if o.maxNs > h.maxNs {
		h.maxNs = o.maxNs
	}
	for i := range h.b {
		h.b[i] += o.b[i]
	}
}
func (h *hist) pct(p float64) float64 {
	target := int64(float64(h.count) * p)
	var cum int64
	for i, c := range h.b {
		cum += c
		if cum >= target {
			return float64(i) * 0.1 // ms
		}
	}
	return 200.0
}
func (h *hist) meanMs() float64 {
	if h.count == 0 {
		return 0
	}
	return float64(h.sumNs) / float64(h.count) / 1e6
}

func main() {
	addr := flag.String("addr", "127.0.0.1:2775", "smpp-server address")
	sysID := flag.String("sysid", "loadtest", "bind system_id")
	pw := flag.String("pw", "loadtest", "bind password")
	conns := flag.Int("conns", 200, "concurrent transceiver binds")
	dur := flag.Duration("dur", 15*time.Second, "measurement duration")
	warmup := flag.Duration("warmup", 2*time.Second, "warmup (latencies discarded)")
	dest := flag.String("dest", "923001234567", "destination MSISDN (must route)")
	sender := flag.String("sender", "LOADTEST", "source address")
	segs := flag.Int("segs", 1, "segments per message (1 = 160 GSM-7 chars)")
	jsonOut := flag.String("json", "", "write results JSON to this path")
	flag.Parse()

	// body sized to the requested segment count
	chars := 160
	if *segs > 1 {
		chars = 160 + 153*(*segs-1)
	}
	base := strings.Repeat("A", chars)

	var submitted, accepted, failed int64
	hists := make([]*hist, *conns)
	var wg sync.WaitGroup
	start := make(chan struct{})
	deadline := time.Now().Add(*warmup + *dur)
	warmEnd := time.Now().Add(*warmup)

	for g := 0; g < *conns; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			h := &hist{}
			hists[gid] = h
			c, err := net.DialTimeout("tcp", *addr, 5*time.Second)
			if err != nil {
				return
			}
			defer c.Close()
			if st, err := bindTRX(c, *sysID, *pw); err != nil || st != 0 {
				fmt.Fprintf(os.Stderr, "conn %d bind failed st=0x%x err=%v\n", gid, st, err)
				return
			}
			<-start
			var seq uint32 = 1
			var local int64
			for time.Now().Before(deadline) {
				seq++
				local++
				text := fmt.Sprintf("%s g%d-%d", base, gid, local) // unique tail -> distinct rows
				t0 := time.Now()
				if _, err := c.Write(pdu(submitSM, 0, seq, submitBody(*sender, *dest, text))); err != nil {
					return
				}
				_, st, _, _, err := readPDU(c)
				lat := time.Since(t0)
				if err != nil {
					return
				}
				atomic.AddInt64(&submitted, 1)
				if st == 0 {
					atomic.AddInt64(&accepted, 1)
				} else {
					atomic.AddInt64(&failed, 1)
				}
				if time.Now().After(warmEnd) { // record only post-warmup
					h.add(int64(lat))
				}
			}
			c.Write(pdu(unbind, 0, seq+1, nil))
		}(g)
	}

	// let all binds establish, then start the clock
	time.Sleep(1500 * time.Millisecond)
	measureStart := time.Now().Add(*warmup)
	close(start)
	wg.Wait()
	elapsed := time.Since(measureStart).Seconds()
	if elapsed <= 0 {
		elapsed = dur.Seconds()
	}

	total := &hist{}
	for _, h := range hists {
		if h != nil {
			total.merge(h)
		}
	}
	tps := float64(total.count) / elapsed
	segps := tps * float64(*segs)

	fmt.Printf("\n==================== smppbench results ====================\n")
	fmt.Printf("target        : %s   binds=%d   segs/msg=%d   window=%.1fs (post-warmup)\n", *addr, *conns, *segs, elapsed)
	fmt.Printf("submitted     : %d   accepted=%d   failed=%d\n", submitted, accepted, failed)
	fmt.Printf("THROUGHPUT    : %.0f submit/s   %.0f segment/s\n", tps, segps)
	fmt.Printf("LATENCY (ms)  : mean=%.2f  p50=%.1f  p90=%.1f  p95=%.1f  p99=%.1f  max=%.1f\n",
		total.meanMs(), total.pct(0.50), total.pct(0.90), total.pct(0.95), total.pct(0.99), float64(total.maxNs)/1e6)
	fmt.Printf("P2 target     : 10,000 seg/s  &  p95 < 30 ms/segment  ->  %s\n", verdict(segps, total.pct(0.95)/float64(*segs)))
	fmt.Printf("===========================================================\n")

	if *jsonOut != "" {
		res := map[string]any{
			"addr": *addr, "binds": *conns, "segs_per_msg": *segs, "window_s": elapsed,
			"submitted": submitted, "accepted": accepted, "failed": failed,
			"submit_per_s": round1(tps), "segment_per_s": round1(segps),
			"mean_ms": round2(total.meanMs()), "p50_ms": total.pct(0.50), "p90_ms": total.pct(0.90),
			"p95_ms": total.pct(0.95), "p99_ms": total.pct(0.99), "max_ms": round2(float64(total.maxNs) / 1e6),
		}
		b, _ := json.MarshalIndent(res, "", "  ")
		os.WriteFile(*jsonOut, b, 0644)
		fmt.Printf("results JSON -> %s\n", *jsonOut)
	}
	// keep the analysis import honest
	_ = sort.Ints
}

func verdict(segps, p95PerSeg float64) string {
	tp := "seg/s below 10k"
	if segps >= 10000 {
		tp = "seg/s target MET"
	}
	lt := "p95 above 30ms"
	if p95PerSeg < 30 {
		lt = "p95 target MET"
	}
	return tp + "; " + lt
}
func round1(f float64) float64 { return float64(int64(f*10)) / 10 }
func round2(f float64) float64 { return float64(int64(f*100)) / 100 }
