// httpbench — native-V4 /sms/v4/send load & latency benchmark for Yakugate P1.
//
// Fires POST /sms/v4/send from N concurrent workers (JWT auth, unique body per
// submit) and reports throughput (submit/s, segment/s) and per-submit latency
// percentiles. Point it at the `loadtest` account (unlimited company throughput,
// dedup off) so rate-limit/dedupe don't distort the numbers.
//
//	go run ./sqa/load/httpbench -conns 100 -dur 15s -user loadtest
package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func jwt(user, secret string) string {
	b64 := func(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }
	h := b64([]byte(`{"alg":"HS256","typ":"JWT"}`))
	p := b64([]byte(`{"sub":"` + user + `"}`))
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(h + "." + p))
	return h + "." + p + "." + b64(mac.Sum(nil))
}

const bucketNs = 100_000 // 0.1ms
const nBuckets = 2001

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
			return float64(i) * 0.1
		}
	}
	return 200
}
func (h *hist) mean() float64 {
	if h.count == 0 {
		return 0
	}
	return float64(h.sumNs) / float64(h.count) / 1e6
}

func main() {
	addr := flag.String("addr", "http://127.0.0.1:8080", "http-server base URL")
	user := flag.String("user", "loadtest", "account (JWT sub)")
	secret := flag.String("secret", "yaku-dev-secret", "dev HS256 secret")
	conns := flag.Int("conns", 100, "concurrent workers")
	dur := flag.Duration("dur", 15*time.Second, "measurement duration")
	warmup := flag.Duration("warmup", 2*time.Second, "warmup (discarded)")
	dest := flag.String("dest", "923001234567", "destination")
	segs := flag.Int("segs", 1, "segments per message")
	jsonOut := flag.String("json", "", "results JSON path")
	flag.Parse()

	chars := 160
	if *segs > 1 {
		chars = 160 + 153*(*segs-1)
	}
	base := strings.Repeat("A", chars)
	tok := jwt(*user, *secret)

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        *conns * 2,
			MaxIdleConnsPerHost: *conns * 2,
			MaxConnsPerHost:     0,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	var submitted, accepted, failed int64
	hists := make([]*hist, *conns)
	var wg sync.WaitGroup
	deadline := time.Now().Add(*warmup + *dur)
	warmEnd := time.Now().Add(*warmup)
	measureStart := time.Now().Add(*warmup)

	for g := 0; g < *conns; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			h := &hist{}
			hists[gid] = h
			var local int64
			for time.Now().Before(deadline) {
				local++
				body, _ := json.Marshal(map[string]string{
					"to": *dest, "from": "LOADTEST", "text": fmt.Sprintf("%s g%d-%d", base, gid, local),
				})
				req, _ := http.NewRequest("POST", *addr+"/sms/v4/send", bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("yaku-sms-key", tok)
				t0 := time.Now()
				resp, err := client.Do(req)
				lat := time.Since(t0)
				atomic.AddInt64(&submitted, 1)
				if err != nil {
					atomic.AddInt64(&failed, 1)
					continue
				}
				ok := resp.StatusCode == 200
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if ok {
					atomic.AddInt64(&accepted, 1)
				} else {
					atomic.AddInt64(&failed, 1)
				}
				if time.Now().After(warmEnd) {
					h.add(int64(lat))
				}
			}
		}(g)
	}
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

	fmt.Printf("\n==================== httpbench results ====================\n")
	fmt.Printf("target        : %s   workers=%d   segs/msg=%d   window=%.1fs\n", *addr, *conns, *segs, elapsed)
	fmt.Printf("submitted     : %d   accepted=%d   failed=%d\n", submitted, accepted, failed)
	fmt.Printf("THROUGHPUT    : %.0f submit/s   %.0f segment/s\n", tps, segps)
	fmt.Printf("LATENCY (ms)  : mean=%.2f  p50=%.1f  p90=%.1f  p95=%.1f  p99=%.1f  max=%.1f\n",
		total.mean(), total.pct(0.50), total.pct(0.90), total.pct(0.95), total.pct(0.99), float64(total.maxNs)/1e6)
	fmt.Printf("P1 target     : 10,000 seg/s & p95 < 30 ms/segment  ->  %s / %s\n",
		yn(segps >= 10000, "seg/s MET", "seg/s below 10k"), yn(total.pct(0.95)/float64(*segs) < 30, "p95 MET", "p95 above 30ms"))
	fmt.Printf("==========================================================\n")

	if *jsonOut != "" {
		b, _ := json.MarshalIndent(map[string]any{
			"workers": *conns, "segs_per_msg": *segs, "window_s": elapsed,
			"submitted": submitted, "accepted": accepted, "failed": failed,
			"submit_per_s": int64(tps), "segment_per_s": int64(segps),
			"p50_ms": total.pct(0.50), "p95_ms": total.pct(0.95), "p99_ms": total.pct(0.99),
			"mean_ms": total.mean(), "max_ms": float64(total.maxNs) / 1e6,
		}, "", "  ")
		os.WriteFile(*jsonOut, b, 0644)
		fmt.Printf("results JSON -> %s\n", *jsonOut)
	}
}

func yn(b bool, y, n string) string {
	if b {
		return y
	}
	return n
}
