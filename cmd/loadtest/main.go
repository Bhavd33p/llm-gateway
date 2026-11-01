// Command loadtest drives concurrent chat completion requests at the gateway (or straight at an upstream) and
// reports throughput, latency and time-to-first-token percentiles, and the observed cache hit rate.
//
// The duplicate-prompt ratio controls the hit rate: each request repeats one of a small set of "hot" prompts with
// that probability, and otherwise sends a fresh prompt made of random words that nothing else resembles.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type options struct {
	url         string
	key         string
	model       string
	concurrency int
	duration    time.Duration
	requests    int64
	dupRatio    float64
	hotPrompts  int
	stream      bool
	noCache     bool
	seed        uint64
}

type result struct {
	ok       bool
	status   int
	hit      bool
	latency  time.Duration
	ttft     time.Duration
	errorMsg string
}

func main() {
	var o options
	flag.StringVar(&o.url, "url", "http://localhost:8080", "gateway (or upstream) base URL")
	flag.StringVar(&o.key, "key", "loadtest-key", "API key sent as a Bearer token")
	flag.StringVar(&o.model, "model", "gpt-4o-mini", "model to request")
	flag.IntVar(&o.concurrency, "concurrency", 50, "concurrent clients")
	flag.DurationVar(&o.duration, "duration", 30*time.Second, "how long to keep starting requests")
	flag.Int64Var(&o.requests, "requests", 0, "stop after this many requests instead (0 = use -duration)")
	flag.Float64Var(&o.dupRatio, "dup-ratio", 0.5, "fraction of requests that repeat a hot prompt (controls cache hit rate)")
	flag.IntVar(&o.hotPrompts, "hot-prompts", 20, "number of distinct hot prompts")
	flag.BoolVar(&o.stream, "stream", true, "use streaming responses")
	flag.BoolVar(&o.noCache, "no-cache", false, "send Cache-Control: no-cache to bypass the semantic cache")
	flag.Uint64Var(&o.seed, "seed", 42, "seed for prompt generation")
	flag.Parse()

	results, elapsed := run(o)
	report(os.Stdout, o, results, elapsed)
}

func run(o options) ([]result, time.Duration) {
	client := &http.Client{Transport: &http.Transport{
		MaxIdleConns:        o.concurrency,
		MaxIdleConnsPerHost: o.concurrency,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}}
	hot := make([]string, o.hotPrompts)
	hotRand := rand.New(rand.NewPCG(o.seed, 0))
	for i := range hot {
		hot[i] = randomPrompt(hotRand)
	}

	deadline := time.Now().Add(o.duration)
	var started atomic.Int64
	perWorker := make([][]result, o.concurrency)
	var wg sync.WaitGroup
	start := time.Now()
	for w := range o.concurrency {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(o.seed, uint64(w)+1))
			for {
				if o.requests > 0 {
					if started.Add(1) > o.requests {
						return
					}
				} else if time.Now().After(deadline) {
					return
				}
				prompt := randomPrompt(rng)
				if rng.Float64() < o.dupRatio {
					prompt = hot[rng.IntN(len(hot))]
				}
				perWorker[w] = append(perWorker[w], do(client, o, prompt))
			}
		})
	}
	wg.Wait()
	return slices.Concat(perWorker...), time.Since(start)
}

func do(client *http.Client, o options, prompt string) result {
	body, _ := json.Marshal(map[string]any{
		"model":    o.model,
		"stream":   o.stream,
		"messages": []map[string]string{{"role": "user", "content": prompt}},
	})
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, strings.TrimRight(o.url, "/")+"/v1/chat/completions", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+o.key)
	if o.noCache {
		req.Header.Set("Cache-Control", "no-cache")
	}

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return result{errorMsg: err.Error()}
	}
	defer resp.Body.Close()
	r := result{status: resp.StatusCode, hit: resp.Header.Get("X-Cache") == "HIT"}
	if resp.StatusCode != http.StatusOK {
		return r
	}

	if !o.stream {
		var out struct {
			Choices []json.RawMessage `json:"choices"`
		}
		err := json.NewDecoder(resp.Body).Decode(&out)
		r.latency = time.Since(start)
		r.ttft = r.latency
		r.ok = err == nil && len(out.Choices) > 0
		return r
	}

	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64*1024), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if r.ttft == 0 && strings.Contains(line, `"content":"`) && !strings.Contains(line, `"content":""`) {
			r.ttft = time.Since(start)
		}
		if line == "data: [DONE]" {
			r.ok = true
			break
		}
	}
	r.latency = time.Since(start)
	if !r.ok {
		r.errorMsg = "stream ended without [DONE]"
	}
	return r
}

// randomPrompt builds 12 pseudo-words from random syllables, so two fresh prompts share almost no n-grams.
func randomPrompt(rng *rand.Rand) string {
	syllables := []string{"ka", "lo", "mi", "zu", "re", "ta", "vo", "ni", "shi", "pa", "qe", "dor", "lin", "mar", "vex", "tor", "bel", "cus", "fen", "gri"}
	words := make([]string, 12)
	for i := range words {
		var w strings.Builder
		for range 2 + rng.IntN(2) {
			w.WriteString(syllables[rng.IntN(len(syllables))])
		}
		words[i] = w.String()
	}
	return "Explain " + strings.Join(words, " ")
}

func report(out *os.File, o options, results []result, elapsed time.Duration) {
	var ok, hits []result
	failures := map[string]int{}
	for _, r := range results {
		if !r.ok {
			key := r.errorMsg
			if r.status != 0 {
				key = fmt.Sprintf("HTTP %d", r.status)
			}
			failures[key]++
			continue
		}
		ok = append(ok, r)
		if r.hit {
			hits = append(hits, r)
		}
	}
	misses := slices.DeleteFunc(slices.Clone(ok), func(r result) bool { return r.hit })

	fmt.Fprintf(out, "target          %s (stream=%v, model=%s, no-cache=%v)\n", o.url, o.stream, o.model, o.noCache)
	fmt.Fprintf(out, "concurrency     %d\n", o.concurrency)
	fmt.Fprintf(out, "dup ratio       %.2f over %d hot prompts\n", o.dupRatio, o.hotPrompts)
	fmt.Fprintf(out, "elapsed         %.2fs\n", elapsed.Seconds())
	fmt.Fprintf(out, "requests        %d total, %d ok, %d failed (%.2f%% success)\n", len(results), len(ok), len(results)-len(ok), pct(len(ok), len(results)))
	fmt.Fprintf(out, "throughput      %.1f req/s (successful requests / elapsed)\n", float64(len(ok))/elapsed.Seconds())
	fmt.Fprintf(out, "cache hit rate  %.2f%% (X-Cache: HIT among successful requests)\n", pct(len(hits), len(ok)))
	fmt.Fprintln(out, "                    n        p50        p95        p99        max")
	row := func(name string, rs []result, pick func(result) time.Duration) {
		if len(rs) == 0 {
			return
		}
		ds := make([]time.Duration, len(rs))
		for i, r := range rs {
			ds[i] = pick(r)
		}
		sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
		fmt.Fprintf(out, "%-14s %7d %10s %10s %10s %10s\n", name, len(ds), ms(percentile(ds, 50)), ms(percentile(ds, 95)), ms(percentile(ds, 99)), ms(ds[len(ds)-1]))
	}
	latency := func(r result) time.Duration { return r.latency }
	ttft := func(r result) time.Duration { return r.ttft }
	row("latency all", ok, latency)
	row("latency hit", hits, latency)
	row("latency miss", misses, latency)
	row("ttft all", ok, ttft)
	row("ttft hit", hits, ttft)
	row("ttft miss", misses, ttft)
	for reason, n := range failures {
		fmt.Fprintf(out, "failure         %d x %s\n", n, reason)
	}
}

// percentile uses the nearest-rank method on sorted durations.
func percentile(sorted []time.Duration, p float64) time.Duration {
	idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	return sorted[max(0, idx)]
}

func pct(a, b int) float64 {
	if b == 0 {
		return 0
	}
	return 100 * float64(a) / float64(b)
}

func ms(d time.Duration) string {
	return fmt.Sprintf("%.2fms", float64(d.Microseconds())/1000)
}
