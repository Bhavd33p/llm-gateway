# Benchmarks

Every number on this page comes from the runs recorded below, copied from the load generator's output. Nothing is
rounded up or estimated. **The upstreams are the mock provider in this repo (`cmd/mockprovider`), not real LLM
APIs**: the point is to measure what the gateway adds, with an upstream whose latency is known exactly.

## Setup

| | |
|---|---|
| Machine | Apple M5 Pro (18 cores), 24 GB RAM, macOS 26.4 (build 25E246) |
| Go | go1.26.4 darwin/arm64 |
| Redis | 8.8.0, local, `redis-server --port 6390 --save "" --appendonly no` |
| Topology | load generator, gateway, both mock providers and Redis all on the same machine, over loopback |
| Gateway config | `config.example.yaml` with `redis.addr` set to `127.0.0.1:6390`; tenant `loadtest` (rate limit 100000/s so the limiter never rejects), hash embedder (256 dims), cache threshold 0.95, `max_entries` 1000, routing `max_attempts` 3, backoff 50ms to 1s, breaker 5 failures / 10s cooldown, request logging at `info` to a file |
| Upstreams | `mock-a` (weight 3) on :9001 and `mock-b` (weight 1) on :9002 |
| Client | `cmd/loadtest`, 50 concurrent streaming clients, one prompt per request; fresh prompts are 12 random pseudo-words; "dup ratio" is the share of requests that repeat one of 20 hot prompts |

Two mock profiles:

* **Realistic**: `-latency 100ms -token-delay 10ms -tokens 50`, so one streamed answer takes about 100 ms to the first
  token and about 600 ms in total.
* **Zero latency**: `-latency 0 -token-delay 0 -tokens 50`, to find the gateway's own ceiling.

Redis was flushed (`FLUSHALL` on the benchmark instance) before each cache-enabled run so every run starts with an
empty cache. Latency is measured by the client from sending the request to reading `data: [DONE]`; TTFT is to the
first SSE line with non-empty `content`.

Commands (the gateway and mocks were restarted between profiles):

```sh
redis-server --port 6390 --save "" --appendonly no --bind 127.0.0.1
./bin/mockprovider -addr 127.0.0.1:9001 -name mock-a -latency 100ms -token-delay 10ms -tokens 50
./bin/mockprovider -addr 127.0.0.1:9002 -name mock-b -latency 100ms -token-delay 10ms -tokens 50
./bin/gateway -config bench-config.yaml        # config.example.yaml with redis at 127.0.0.1:6390

./bin/loadtest -url http://127.0.0.1:9001 -concurrency 50 -duration 30s -dup-ratio 0              # direct to mock
./bin/loadtest -url http://127.0.0.1:8080 -concurrency 50 -duration 30s -dup-ratio 0 -no-cache    # gateway, cache bypassed
./bin/loadtest -url http://127.0.0.1:8080 -concurrency 50 -duration 30s -dup-ratio 0              # gateway, 0% hits
./bin/loadtest -url http://127.0.0.1:8080 -concurrency 50 -duration 30s -dup-ratio 0.5            # gateway, ~50% hits
# zero-latency profile: same commands with -duration 20s against mocks started with -latency 0 -token-delay 0
# failover: mock-a started with -fail-rate 0.5 (then 1.0), loadtest with -dup-ratio 0 -no-cache
```

## Results

### Gateway overhead on time to first token (realistic mocks, cache bypassed)

| Path | Requests | TTFT p50 | TTFT p95 | TTFT p99 | Latency p50 | Latency p99 | Throughput |
|---|---:|---:|---:|---:|---:|---:|---:|
| Direct to `mock-a` | 2350 | 102.01 ms | 103.90 ms | 105.00 ms | 640.14 ms | 647.06 ms | 78.1 req/s |
| Through the gateway, `Cache-Control: no-cache` | 2350 | 103.64 ms | 106.50 ms | 111.33 ms | 640.99 ms | 651.77 ms | 78.0 req/s |

The gateway (auth, the Redis rate-limit script, routing, SSE passthrough with a flush per chunk) added **1.63 ms at
p50 and 6.33 ms at p99** to time to first token. Throughput is the same because with 50 clients and ~640 ms streams
it is capped by the mock's speed (50 / 0.64 s is about 78 req/s), not by the gateway.

### Semantic cache: hit latency versus upstream latency (realistic mocks, 50 concurrent streams)

| Run | Requests | Hit rate | Throughput | Latency p50 | Latency p99 | TTFT p50 | TTFT p99 |
|---|---:|---:|---:|---:|---:|---:|---:|
| Cache on, dup ratio 0 (0% hits) | 2351 | 0.00% | 76.7 req/s | 648.15 ms | 670.53 ms | 119.43 ms | 139.55 ms |
| Cache on, dup ratio 0.5 | 4792 | 49.62% | 156.4 req/s | 605.66 ms | 639.83 ms | 101.21 ms | 109.51 ms |
| of which cache hits | 2378 | | | 3.94 ms | 10.42 ms | 3.94 ms | 10.33 ms |
| of which misses (upstream) | 2414 | | | 620.92 ms | 641.39 ms | 104.03 ms | 110.10 ms |

* A cache hit returned the complete answer in **3.94 ms median / 10.42 ms p99**, against **620.92 ms / 641.39 ms**
  for an upstream stream in the same run.
* At a 49.62% hit rate, throughput doubled from 76.7 to 156.4 req/s at the same concurrency, because half the
  requests no longer wait on the upstream.
* At 0% hits the cache still costs something: every request is embedded, scanned against the tenant's cache and
  stored. TTFT p50 rose from 103.64 ms (cache bypassed) to 119.43 ms as the cache filled to its 1000-entry limit;
  the index held 931 entries at the end of the run (concurrent stores can each evict, so it settles a little under
  the limit; see the walkthrough's limitations).

### Gateway capacity (zero-latency mocks, 50 concurrent streams, 20 s)

| Run | Requests | Hit rate | Throughput | Latency p50 | Latency p99 | TTFT p50 | TTFT p99 |
|---|---:|---:|---:|---:|---:|---:|---:|
| Direct to `mock-a` | 148973 | | 7446.6 req/s | 5.87 ms | 19.56 ms | 4.03 ms | 16.32 ms |
| Gateway, cache bypassed | 70814 | 0.00% | 3538.7 req/s | 14.13 ms | 18.27 ms | 9.65 ms | 14.50 ms |
| Gateway, cache on, 0% hits | 28866 | 0.00% | 1442.4 req/s | 34.90 ms | 50.39 ms | 28.88 ms | 43.02 ms |
| Gateway, cache on, dup ratio 0.5 | 31782 | 50.07% | 1587.9 req/s | 31.33 ms | 45.60 ms | 28.58 ms | 37.95 ms |
| of which cache hits | 15914 | | | 29.43 ms | 38.55 ms | 29.42 ms | 38.53 ms |

With an upstream that answers instantly, the gateway itself sustained **3538.7 req/s with p99 latency 18.27 ms**
when the cache is bypassed. With the cache on, the per-request linear scan becomes the bottleneck: each lookup
pipelines one HMGET per cached entry (here around 900 entries of 1 KB embeddings, about 1 MB from Redis) and computes
that many cosine similarities, so throughput drops to 1442.4 to 1587.9 req/s and a hit (29.43 ms median) is no
faster than calling the zero-latency mock. This is the measured reason the next step for larger caches is a vector
index (Redis Query Engine HNSW) instead of a scan. Against a real LLM, whose first token takes hundreds of
milliseconds, the same hit is still far cheaper than a call, as the realistic run shows.

### Failover (realistic mocks, cache bypassed, 50 concurrent streams, 30 s)

| `mock-a` (weight 3) failure rate | Requests | Succeeded | Throughput | TTFT p50 | TTFT p95 | TTFT p99 | Latency p99 | Failovers a to b |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 50% (HTTP 500) | 2321 | 2321 (100.00%) | 75.4 req/s | 101.53 ms | 244.34 ms | 250.22 ms | 762.07 ms | 848 |
| 100% (hard down) | 2360 | 2360 (100.00%) | 77.2 req/s | 100.98 ms | 102.32 ms | 225.26 ms | 750.69 ms | 40 |

* At a 50% failure rate on the heavier upstream, every client request succeeded. The mock logs agree with the
  gateway's metrics: `mock-a` received 1706 requests and injected 848 failures, `mock-b` received 1463, and
  1706 + 1463 = 2321 client requests + 848 retries.
* A failed first attempt costs the failing upstream's latency plus a backoff (at most 50 ms on the first retry),
  which is the jump from 101.53 ms at p50 to 244.34 ms at p95.
* With `mock-a` hard down, its circuit breaker opened after the first consecutive failures and only cooldown probes
  reached it afterwards: `mock-a` saw 40 requests in 30 s (all failed), `mock-b` served all 2360, and the breaker
  gauge read 2 (open) at the end. p95 TTFT stayed at 102.32 ms because open-breaker upstreams are skipped without
  spending an attempt.

### Go micro-benchmarks (`make bench`)

| Benchmark | Result |
|---|---|
| `BenchmarkHashEmbed` (one prompt, 256 dims) | 924.4 ns/op, 1312 B/op, 2 allocs/op |
| `BenchmarkCosine256` (one candidate in the scan) | 164.7 ns/op, 0 allocs |
| `BenchmarkPick` (weighted round-robin + failover order, parallel) | 89.14 ns/op, 24 B/op, 1 alloc/op |

## Caveats

* Single machine: the load generator, gateway, mocks and Redis compete for the same CPU, and loopback hides network
  latency between the gateway and Redis.
* Mock upstreams have no jitter; real providers vary far more, which would widen every percentile.
* The hash embedder is local and costs about 1 µs. A remote embedding model would add a network round trip to every
  cache lookup.
* Request logging was on (`info`) during all runs.

## Raw output

<details><summary>Realistic profile</summary>

```
== R1 direct to mock-a ==
target          http://127.0.0.1:9001 (stream=true, model=gpt-4o-mini, no-cache=false)
concurrency     50
dup ratio       0.00 over 20 hot prompts
elapsed         30.10s
requests        2350 total, 2350 ok, 0 failed (100.00% success)
throughput      78.1 req/s (successful requests / elapsed)
cache hit rate  0.00% (X-Cache: HIT among successful requests)
                    n        p50        p95        p99        max
latency all       2350   640.14ms   643.13ms   647.06ms   648.90ms
latency miss      2350   640.14ms   643.13ms   647.06ms   648.90ms
ttft all          2350   102.01ms   103.90ms   105.00ms   106.30ms
ttft miss         2350   102.01ms   103.90ms   105.00ms   106.30ms
== R2 gateway no-cache ==
target          http://127.0.0.1:8080 (stream=true, model=gpt-4o-mini, no-cache=true)
concurrency     50
dup ratio       0.00 over 20 hot prompts
elapsed         30.14s
requests        2350 total, 2350 ok, 0 failed (100.00% success)
throughput      78.0 req/s (successful requests / elapsed)
cache hit rate  0.00% (X-Cache: HIT among successful requests)
                    n        p50        p95        p99        max
latency all       2350   640.99ms   645.99ms   651.77ms   653.34ms
latency miss      2350   640.99ms   645.99ms   651.77ms   653.34ms
ttft all          2350   103.64ms   106.50ms   111.33ms   112.73ms
ttft miss         2350   103.64ms   106.50ms   111.33ms   112.73ms
== R3 gateway cache on, dup 0 ==
target          http://127.0.0.1:8080 (stream=true, model=gpt-4o-mini, no-cache=false)
concurrency     50
dup ratio       0.00 over 20 hot prompts
elapsed         30.66s
requests        2351 total, 2351 ok, 0 failed (100.00% success)
throughput      76.7 req/s (successful requests / elapsed)
cache hit rate  0.00% (X-Cache: HIT among successful requests)
                    n        p50        p95        p99        max
latency all       2351   648.15ms   666.43ms   670.53ms   673.91ms
latency miss      2351   648.15ms   666.43ms   670.53ms   673.91ms
ttft all          2351   119.43ms   136.63ms   139.55ms   142.07ms
ttft miss         2351   119.43ms   136.63ms   139.55ms   142.07ms
931
== R4 gateway cache on, dup 0.5 ==
target          http://127.0.0.1:8080 (stream=true, model=gpt-4o-mini, no-cache=false)
concurrency     50
dup ratio       0.50 over 20 hot prompts
elapsed         30.63s
requests        4792 total, 4792 ok, 0 failed (100.00% success)
throughput      156.4 req/s (successful requests / elapsed)
cache hit rate  49.62% (X-Cache: HIT among successful requests)
                    n        p50        p95        p99        max
latency all       4792   605.66ms   629.29ms   639.83ms   642.13ms
latency hit       2378     3.94ms     8.30ms    10.42ms    13.52ms
latency miss      2414   620.92ms   632.69ms   641.39ms   642.13ms
ttft all          4792   101.21ms   107.20ms   109.51ms   111.50ms
ttft hit          2378     3.94ms     8.28ms    10.33ms    13.52ms
ttft miss         2414   104.03ms   108.10ms   110.10ms   111.50ms
```
The bare `931` line is `ZCARD sc:{loadtest}:lru` taken right after the 0%-hit run.
</details>

<details><summary>Zero-latency profile</summary>

```
== C1 direct to mock-a, zero latency ==
target          http://127.0.0.1:9001 (stream=true, model=gpt-4o-mini, no-cache=false)
concurrency     50
dup ratio       0.00 over 20 hot prompts
elapsed         20.01s
requests        148973 total, 148973 ok, 0 failed (100.00% success)
throughput      7446.6 req/s (successful requests / elapsed)
cache hit rate  0.00% (X-Cache: HIT among successful requests)
                    n        p50        p95        p99        max
latency all     148973     5.87ms    14.46ms    19.56ms    52.97ms
latency miss    148973     5.87ms    14.46ms    19.56ms    52.97ms
ttft all        148973     4.03ms    11.58ms    16.32ms    35.74ms
ttft miss       148973     4.03ms    11.58ms    16.32ms    35.74ms
== C2 gateway no-cache, zero latency ==
target          http://127.0.0.1:8080 (stream=true, model=gpt-4o-mini, no-cache=true)
concurrency     50
dup ratio       0.00 over 20 hot prompts
elapsed         20.01s
requests        70814 total, 70814 ok, 0 failed (100.00% success)
throughput      3538.7 req/s (successful requests / elapsed)
cache hit rate  0.00% (X-Cache: HIT among successful requests)
                    n        p50        p95        p99        max
latency all      70814    14.13ms    16.46ms    18.27ms    27.71ms
latency miss     70814    14.13ms    16.46ms    18.27ms    27.71ms
ttft all         70814     9.65ms    13.48ms    14.50ms    24.61ms
ttft miss        70814     9.65ms    13.48ms    14.50ms    24.61ms
== C3 gateway cache on dup 0, zero latency ==
target          http://127.0.0.1:8080 (stream=true, model=gpt-4o-mini, no-cache=false)
concurrency     50
dup ratio       0.00 over 20 hot prompts
elapsed         20.01s
requests        28866 total, 28866 ok, 0 failed (100.00% success)
throughput      1442.4 req/s (successful requests / elapsed)
cache hit rate  0.00% (X-Cache: HIT among successful requests)
                    n        p50        p95        p99        max
latency all      28866    34.90ms    45.67ms    50.39ms    66.03ms
latency miss     28866    34.90ms    45.67ms    50.39ms    66.03ms
ttft all         28866    28.88ms    38.90ms    43.02ms    56.19ms
ttft miss        28866    28.88ms    38.90ms    43.02ms    56.19ms
874
== C4 gateway cache on dup 0.5, zero latency ==
target          http://127.0.0.1:8080 (stream=true, model=gpt-4o-mini, no-cache=false)
concurrency     50
dup ratio       0.50 over 20 hot prompts
elapsed         20.01s
requests        31782 total, 31782 ok, 0 failed (100.00% success)
throughput      1587.9 req/s (successful requests / elapsed)
cache hit rate  50.07% (X-Cache: HIT among successful requests)
                    n        p50        p95        p99        max
latency all      31782    31.33ms    40.79ms    45.60ms    54.27ms
latency hit      15914    29.43ms    35.59ms    38.55ms    45.02ms
latency miss     15868    33.91ms    43.08ms    47.18ms    54.27ms
ttft all         31782    28.58ms    34.85ms    37.95ms    45.02ms
ttft hit         15914    29.42ms    35.59ms    38.53ms    45.02ms
ttft miss        15868    27.50ms    33.71ms    36.73ms    44.61ms
```
The bare `874` line is `ZCARD sc:{loadtest}:lru` taken right after the 0%-hit run.
</details>

<details><summary>Failover, mock-a failing 50%</summary>

```
== F1 rerun: mock-a (weight 3) fail-rate 0.5, mock-b (weight 1) healthy ==
target          http://127.0.0.1:8080 (stream=true, model=gpt-4o-mini, no-cache=true)
concurrency     50
dup ratio       0.00 over 20 hot prompts
elapsed         30.77s
requests        2321 total, 2321 ok, 0 failed (100.00% success)
throughput      75.4 req/s (successful requests / elapsed)
cache hit rate  0.00% (X-Cache: HIT among successful requests)
                    n        p50        p95        p99        max
latency all       2321   611.26ms   751.87ms   762.07ms   777.38ms
latency miss      2321   611.26ms   751.87ms   762.07ms   777.38ms
ttft all          2321   101.53ms   244.34ms   250.22ms   252.33ms
ttft miss         2321   101.53ms   244.34ms   250.22ms   252.33ms
llm_gateway_circuit_breaker_state{upstream="mock-a"} 0
llm_gateway_circuit_breaker_state{upstream="mock-b"} 0
llm_gateway_failovers_total{from="mock-a",to="mock-b"} 848
llm_gateway_requests_total{code="200",tenant="loadtest"} 2321
llm_gateway_upstream_errors_total{reason="status_5xx",upstream="mock-a"} 848
mock-a log: "requests":1706,"failures":848,"cancelled":0
mock-b log: "requests":1463,"failures":0,"cancelled":0
```
</details>

<details><summary>Failover, mock-a failing 100%</summary>

```
== F mock-a (weight 3) fail-rate 1.0, mock-b (weight 1) healthy ==
target          http://127.0.0.1:8080 (stream=true, model=gpt-4o-mini, no-cache=true)
concurrency     50
dup ratio       0.00 over 20 hot prompts
elapsed         30.58s
requests        2360 total, 2360 ok, 0 failed (100.00% success)
throughput      77.2 req/s (successful requests / elapsed)
cache hit rate  0.00% (X-Cache: HIT among successful requests)
                    n        p50        p95        p99        max
latency all       2360   637.46ms   641.96ms   750.69ms   783.02ms
latency miss      2360   637.46ms   641.96ms   750.69ms   783.02ms
ttft all          2360   100.98ms   102.32ms   225.26ms   254.91ms
ttft miss         2360   100.98ms   102.32ms   225.26ms   254.91ms
llm_gateway_circuit_breaker_state{upstream="mock-a"} 2
llm_gateway_circuit_breaker_state{upstream="mock-b"} 0
llm_gateway_failovers_total{from="mock-a",to="mock-b"} 40
llm_gateway_requests_total{code="200",tenant="loadtest"} 2360
llm_gateway_upstream_errors_total{reason="status_5xx",upstream="mock-a"} 40
mock-a log: "requests":40,"failures":40,"cancelled":0
mock-b log: "requests":2360,"failures":0,"cancelled":0
```
</details>
