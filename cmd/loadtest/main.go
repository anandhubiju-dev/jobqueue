package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type submission struct {
	d      time.Duration
	status int
	id     string
	err    error
}

type jobView struct {
	Status      string     `json:"status"`
	RunAt       time.Time  `json:"run_at"`
	StartedAt   *time.Time `json:"started_at"`
	CompletedAt *time.Time `json:"completed_at"`
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)) * p)
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

func summarize(name string, d []time.Duration) {
	if len(d) == 0 {
		fmt.Printf("%s: no data\n", name)
		return
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	fmt.Printf("%s p50=%v p95=%v p99=%v max=%v\n", name,
		pct(d, 0.50).Round(time.Millisecond), pct(d, 0.95).Round(time.Millisecond),
		pct(d, 0.99).Round(time.Millisecond), d[len(d)-1].Round(time.Millisecond))
}

func main() {
	base := flag.String("url", "http://localhost:8080", "base URL")
	n := flag.Int("n", 80, "total requests")
	c := flag.Int("c", 20, "concurrent clients")
	typ := flag.String("type", "notification", "job type")
	wait := flag.Duration("wait", 3*time.Minute, "max wait for jobs to finish")
	flag.Parse()

	if *n < 1 || *c < 1 {
		fmt.Fprintln(os.Stderr, "-n and -c must both be at least 1")
		os.Exit(2)
	}
	if *c > *n {
		*c = *n
	}

	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: *c + 16},
	}

	// ---- phase 1: submit ----
	body := fmt.Appendf(nil, `{"type":%q,"payload":{}}`, *typ)
	work := make(chan struct{})
	results := make(chan submission, *n)

	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < *c; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range work {
				t := time.Now()
				s := submission{}
				resp, err := client.Post(*base+"/jobs", "application/json", bytes.NewReader(body))
				if err != nil {
					s.err = err
				} else {
					s.status = resp.StatusCode
					if resp.StatusCode == http.StatusAccepted {
						var out struct {
							ID string `json:"id"`
						}
						_ = json.NewDecoder(resp.Body).Decode(&out)
						s.id = out.ID
					}
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
				s.d = time.Since(t)
				results <- s
			}
		}()
	}
	for i := 0; i < *n; i++ {
		work <- struct{}{}
	}
	close(work)
	wg.Wait()
	close(results)
	submitTime := time.Since(start)

	var lat []time.Duration
	var ids []string
	rejected := map[int]int{}
	failed := 0
	for r := range results {
		switch {
		case r.err != nil:
			failed++
		case r.status == http.StatusAccepted && r.id != "":
			ids = append(ids, r.id)
			lat = append(lat, r.d)
		default:
			rejected[r.status]++
		}
	}

	fmt.Printf("submitted: %d requests in %v (%.0f req/s)\n", *n, submitTime.Round(time.Millisecond), float64(*n)/submitTime.Seconds())
	fmt.Printf("accepted=%d rejected=%v network_failures=%d\n", len(ids), rejected, failed)
	summarize("submit latency (accepted only)", lat)

	if len(ids) == 0 {
		fmt.Println("nothing was accepted, so there is nothing to wait for")
		return
	}

	// ---- phase 2: poll each accepted job until it is terminal ----
	type outcome struct {
		wait, exec time.Duration
		status     string
		ok         bool
	}
	outcomes := make([]outcome, len(ids))
	deadline := time.Now().Add(*wait)

	pollers := *c
	if pollers > 32 {
		pollers = 32
	}
	idx := make(chan int)
	var pwg sync.WaitGroup
	var finishedCount atomic.Int64
	stopProgress := make(chan struct{})
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				fmt.Printf("  progress: %d/%d finished after %v\n",
					finishedCount.Load(), len(ids), time.Since(start).Round(time.Second))
			case <-stopProgress:
				return
			}
		}
	}()
	for p := 0; p < pollers; p++ {
		pwg.Add(1)
		go func() {
			defer pwg.Done()
			for i := range idx {
				for time.Now().Before(deadline) {
					resp, err := client.Get(*base + "/jobs/" + ids[i])
					if err == nil {
						var v jobView
						err = json.NewDecoder(resp.Body).Decode(&v)
						resp.Body.Close()
						if err == nil && (v.Status == "completed" || v.Status == "failed") {
							o := outcome{status: v.Status, ok: true}
							if v.StartedAt != nil {
								o.wait = v.StartedAt.Sub(v.RunAt)
								if v.CompletedAt != nil {
									o.exec = v.CompletedAt.Sub(*v.StartedAt)
								}
							}
							outcomes[i] = o
							break
						}
					}
					time.Sleep(500 * time.Millisecond)
				}
			}
		}()
	}
	for i := range ids {
		idx <- i
	}
	close(idx)
	pwg.Wait()
	total := time.Since(start)

	var waits, execs []time.Duration
	byStatus := map[string]int{}
	unfinished := 0
	for _, o := range outcomes {
		if !o.ok {
			unfinished++
			continue
		}
		byStatus[o.status]++
		waits = append(waits, o.wait)
		execs = append(execs, o.exec)
	}

	fmt.Printf("finished: %v, unfinished before timeout: %d\n", byStatus, unfinished)
	fmt.Printf("end-to-end: %v for %d accepted jobs (%.1f jobs/s)\n",
		total.Round(time.Millisecond), len(ids), float64(len(ids))/total.Seconds())
	summarize("queue wait (run_at to start)", waits)
	summarize("execution (last attempt)", execs)
}
