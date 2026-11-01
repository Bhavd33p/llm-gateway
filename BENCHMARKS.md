# Benchmarks

Every number on this page comes from the runs recorded below, copied from the load generator's output. Nothing is
rounded up or estimated. **The upstreams are the mock provider in this repo (`cmd/mockprovider`), not real LLM
APIs**: the point is to measure what the gateway adds, with an upstream whose latency is known exactly.

## Setup

All runs used the code at commit `502200f`.

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
  token and, in these runs, 640 to 690 ms in total.
* **Zero latency**: `-latency 0 -token-delay 0 -tokens 50`, to find the gateway's own ceiling.

Redis was flushed (`FLUSHALL` on the benchmark instance) before each cache-enabled run, and the gateway and mocks were
restarted before each profile and each failover run, so every run starts cold. Latency is measured by the client
from sending the request to reading `data: [DONE]`; TTFT is to the first SSE line with non-empty `content`.

Commands:

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
make bench                                     # Go micro-benchmarks
```

## Results

### Gateway overhead on time to first token (realistic mocks, cache bypassed)

| Path | Requests | TTFT p50 | TTFT p95 | TTFT p99 | Latency p50 | Latency p99 | Throughput |
|---|---:|---:|---:|---:|---:|---:|---:|
| Direct to `mock-a` | 2350 | 101.99 ms | 103.87 ms | 104.46 ms | 640.73 ms | 685.62 ms | 77.4 req/s |
| Through the gateway, `Cache-Control: no-cache` | 2250 | 104.05 ms | 106.75 ms | 111.09 ms | 677.63 ms | 688.42 ms | 73.8 req/s |

The gateway (auth, the Redis rate-limit script, routing, SSE passthrough with a flush per chunk) added **2.06 ms at
p50 and 6.63 ms at p99** to time to first token. TTFT is the clean comparison. Total stream time is not: the mock
sleeps 49 times for 10 ms per answer and the operating system's timers stretch those sleeps by varying amounts, so even
the direct run spread from 640.73 ms at p50 to 678.46 ms at p95. Throughput follows
stream time (50 clients divided by the time per stream), so it measures the mock, not the gateway.

### Semantic cache: hit latency versus upstream latency (realistic mocks, 50 concurrent streams)

| Run | Requests | Hit rate | Throughput | Latency p50 | Latency p99 | TTFT p50 | TTFT p99 |
|---|---:|---:|---:|---:|---:|---:|---:|
| Cache on, dup ratio 0 (0% hits) | 2256 | 0.00% | 73.6 req/s | 675.75 ms | 699.11 ms | 113.17 ms | 135.18 ms |
| Cache on, dup ratio 0.5 | 4583 | 49.81% | 150.9 req/s | 624.02 ms | 684.43 ms | 101.15 ms | 110.91 ms |
| of which cache hits | 2283 | | | 4.26 ms | 12.03 ms | 4.25 ms | 12.03 ms |
| of which misses (upstream) | 2300 | | | 651.63 ms | 685.95 ms | 104.52 ms | 111.97 ms |

* A cache hit returned the complete answer in **4.26 ms median / 12.03 ms p99**, against
  **651.63 ms / 685.95 ms** for an upstream stream in the same run.
* At a 49.81% hit rate, throughput went from 73.6 to 150.9 req/s at the same concurrency, because half
  the requests no longer wait on the upstream.
* At 0% hits the cache still costs something: every request is embedded, scanned against the tenant's cache and
  stored. TTFT p50 rose from 104.05 ms (cache bypassed) to 113.17 ms as the cache filled to its
  1000-entry limit; the index held 1000 entries at the end of the run.

### Gateway capacity (zero-latency mocks, 50 concurrent streams, 20 s)

| Run | Requests | Hit rate | Throughput | Latency p50 | Latency p99 | TTFT p50 | TTFT p99 |
|---|---:|---:|---:|---:|---:|---:|---:|
| Direct to `mock-a` | 149375 |  | 7466.3 req/s | 5.84 ms | 19.83 ms | 4.00 ms | 16.39 ms |
| Gateway, cache bypassed | 71178 | 0.00% | 3556.9 req/s | 14.06 ms | 18.45 ms | 9.68 ms | 14.41 ms |
| Gateway, cache on, 0% hits | 26340 | 0.00% | 1316.3 req/s | 38.27 ms | 57.90 ms | 32.24 ms | 49.81 ms |
| Gateway, cache on, dup ratio 0.5 | 26298 | 50.22% | 1313.7 req/s | 36.49 ms | 75.75 ms | 33.26 ms | 69.24 ms |
| of which cache hits | 13206 | | | 34.20 ms | 70.39 ms | 34.20 ms | 70.39 ms |

With an upstream that answers instantly, the gateway itself sustained **3556.9 req/s with p99 latency
18.45 ms** when the cache was bypassed. With the cache on, the per-request linear scan becomes the
bottleneck: each lookup pipelines one HMGET per cached entry (here up to 1000 entries, each a 1 KB embedding plus model
and parameter fields, about 1 MB from Redis) and computes that many cosine similarities. Throughput drops to
1316.3 req/s at 0% hits and 1313.7 req/s at 50.22% hits, and a hit (34.20 ms median) is slower
than a cache-bypassed request (14.06 ms). After the 0%-hit run the index held 956 entries: concurrent
stores can each evict, so it sits a little under the limit under load. This is the measured reason the next step for
larger caches is a vector index (Redis Query Engine HNSW) instead of a scan. Against a real LLM, whose first token
takes hundreds of milliseconds, a hit is still far cheaper than a call, as the realistic run shows.

### Failover (realistic mocks, cache bypassed, 50 concurrent streams, 30 s)

| `mock-a` (weight 3) failure rate | Requests | Succeeded | Throughput | TTFT p50 | TTFT p95 | TTFT p99 | Latency p99 | Failovers a to b |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 50% (HTTP 500) | 2250 | 2250 (100.00%) | 73.5 req/s | 102.78 ms | 105.12 ms | 229.80 ms | 789.11 ms | 43 |
| 100% (hard down) | 2250 | 2250 (100.00%) | 73.4 req/s | 102.61 ms | 104.33 ms | 232.34 ms | 788.78 ms | 40 |

* Every client request succeeded in both runs.
* At a 50% failure rate, the heavier upstream quickly hit five consecutive failures and its circuit breaker opened.
  From then on it only received what each cooldown let through. Over 30 s `mock-a` received 78 requests and
  injected 43 failures, and `mock-b` served the rest (2215 requests). The mock logs agree with the gateway's
  metrics: 78 + 2215 = 2250 client requests + 43 retries. The breaker gauge read 2 (open)
  at the end.
* Only requests whose first attempt failed paid for failover: the failing attempt (about 100 ms here) plus a backoff
  of at most 50 ms. That shows at p99 (229.80 ms) but not at p95 (105.12 ms).
* With `mock-a` hard down, the result is the same shape: it saw 40 requests in 30 s (all failed),
  `mock-b` served all 2250, and upstreams with an open breaker are skipped without spending an attempt.

### Redis outage

The fail-fast change was measured separately from the runs above, on the same machine: "before" is commit `9980b08`,
"after" is commit `8016890` plus one metric help-text change committed afterwards as `52f255f` (later commits only
change docs, the dashboard and that help text). The gateway used `config.example.yaml`
with `redis.addr` set to `127.0.0.1:6489` and both mocks at zero latency, so request time is the gateway's own time.
"Redis down" means nothing listening on that port, so every connection is refused.

| Scenario | Code | Requests | Succeeded | Latency p50 | Latency p99 | TTFT p50 | Other |
|---|---|---:|---:|---:|---:|---:|---|
| Redis down, 10 sequential streaming requests | before | 10 | 10 | 5485.04 ms | 5528.61 ms | 3483.73 ms | |
| Redis down, 10 sequential streaming requests | after | 10 | 10 | 0.92 ms | 4.08 ms | 0.36 ms | |
| Redis down, 50 concurrent streams, 20 s | after | 82411 | 82411 | 11.97 ms | 17.38 ms | 10.58 ms | 4118.2 req/s |
| Redis killed at about 5 s and restarted at about 12 s, 50 streams, 20 s, dup ratio 0.5 | after | 54570 | 54570 | 13.24 ms | 46.12 ms | 12.03 ms | max latency 62.23 ms |
| Healthy Redis, heaviest cache load (0% hits, 1000-entry scans), 50 streams, 20 s | after | 26397 | 26397 | 38.83 ms | 54.03 ms | 32.98 ms | 0 Redis failures, breaker stayed closed |

* **Before:** every request made four Redis calls (rate limit, cache lookup, cache store, ledger). With go-redis
  defaults each call made 5 dial attempts 100 ms apart and was retried up to 3 times, so a request took 5485.04 ms
  at p50. 3483.73 ms of that came before the first token (rate limit and cache lookup); the rest came after the
  answer (store and ledger).
* **After:** the first calls fail in milliseconds (one dial attempt, no retries). After 5 consecutive failures the
  breaker opens and calls are refused locally without touching the network, so a request took 0.92 ms at p50. Under
  50 concurrent streams the gateway served 4118.2 req/s with Redis down. That is faster than with a healthy Redis,
  because it skips the cache scan.
* **Kill and restart under load:** no request failed, and the slowest took 62.23 ms. The breaker log shows the
  recovery:
  * opened at 21:35:21, when Redis was killed;
  * probed at 21:35:26 and reopened, because Redis was still down;
  * probed at 21:35:31 and closed, 3 s after Redis was restarted at 21:35:28.
  * The hit rate was 13.75% because the cache was unavailable during the outage and empty after the restart
    (benchmark Redis runs without persistence).
* **Healthy Redis:** the 50 ms timeouts do not misfire. The heaviest cache workload ran without a single Redis
  failure, at the same throughput as the capacity run above.
* **Not measured:** a network partition, where packets are dropped instead of refused. There, each failure before the
  breaker opens waits for the 50 ms dial timeout, so roughly the first two requests would be slowed by up to about
  200 ms each. After that the breaker skips Redis in the same way.

### Go micro-benchmarks (`make bench`)

| Benchmark | Result |
|---|---|
| `BenchmarkHashEmbed` (one prompt, 256 dims) | 1180 ns/op, 1312 B/op, 2 allocs/op |
| `BenchmarkCosine256` (one candidate in the scan) | 172.6 ns/op, 0 B/op, 0 allocs/op |
| `BenchmarkPick` (weighted round-robin + failover order, parallel) | 91.03 ns/op, 24 B/op, 1 allocs/op |

## Caveats

* Single machine: the load generator, gateway, mocks and Redis compete for the same CPU, and loopback hides network
  latency between the gateway and Redis.
* Mock upstreams have no deliberate jitter (the timer stretch noted above is the only variation); real providers vary
  far more, which would widen every percentile.
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
elapsed         30.35s
requests        2350 total, 2350 ok, 0 failed (100.00% success)
throughput      77.4 req/s (successful requests / elapsed)
cache hit rate  0.00% (X-Cache: HIT among successful requests)
                    n        p50        p95        p99        max
latency all       2350   640.73ms   678.46ms   685.62ms   687.54ms
latency miss      2350   640.73ms   678.46ms   685.62ms   687.54ms
ttft all          2350   101.99ms   103.87ms   104.46ms   104.90ms
ttft miss         2350   101.99ms   103.87ms   104.46ms   104.90ms
== R2 gateway no-cache ==
target          http://127.0.0.1:8080 (stream=true, model=gpt-4o-mini, no-cache=true)
concurrency     50
dup ratio       0.00 over 20 hot prompts
elapsed         30.51s
requests        2250 total, 2250 ok, 0 failed (100.00% success)
throughput      73.8 req/s (successful requests / elapsed)
cache hit rate  0.00% (X-Cache: HIT among successful requests)
                    n        p50        p95        p99        max
latency all       2250   677.63ms   685.47ms   688.42ms   690.28ms
latency miss      2250   677.63ms   685.47ms   688.42ms   690.28ms
ttft all          2250   104.05ms   106.75ms   111.09ms   111.67ms
ttft miss         2250   104.05ms   106.75ms   111.09ms   111.67ms
== R3 gateway cache on, dup 0 ==
target          http://127.0.0.1:8080 (stream=true, model=gpt-4o-mini, no-cache=false)
concurrency     50
dup ratio       0.00 over 20 hot prompts
elapsed         30.63s
requests        2256 total, 2256 ok, 0 failed (100.00% success)
throughput      73.6 req/s (successful requests / elapsed)
cache hit rate  0.00% (X-Cache: HIT among successful requests)
                    n        p50        p95        p99        max
latency all       2256   675.75ms   696.39ms   699.11ms   705.28ms
latency miss      2256   675.75ms   696.39ms   699.11ms   705.28ms
ttft all          2256   113.17ms   127.55ms   135.18ms   136.77ms
ttft miss         2256   113.17ms   127.55ms   135.18ms   136.77ms
1000
== R4 gateway cache on, dup 0.5 ==
target          http://127.0.0.1:8080 (stream=true, model=gpt-4o-mini, no-cache=false)
concurrency     50
dup ratio       0.50 over 20 hot prompts
elapsed         30.38s
requests        4583 total, 4583 ok, 0 failed (100.00% success)
throughput      150.9 req/s (successful requests / elapsed)
cache hit rate  49.81% (X-Cache: HIT among successful requests)
                    n        p50        p95        p99        max
latency all       4583   624.02ms   668.10ms   684.43ms   690.84ms
latency hit       2283     4.26ms     9.62ms    12.03ms    14.29ms
latency miss      2300   651.63ms   678.63ms   685.95ms   690.84ms
ttft all          4583   101.15ms   108.16ms   110.91ms   114.17ms
ttft hit          2283     4.25ms     9.62ms    12.03ms    14.29ms
ttft miss         2300   104.52ms   109.52ms   111.97ms   114.17ms
```
The bare `1000` line is `ZCARD sc:{loadtest}:lru` taken right after the 0%-hit run.
</details>

<details><summary>Zero-latency profile</summary>

```
== C1 direct to mock-a, zero latency ==
target          http://127.0.0.1:9001 (stream=true, model=gpt-4o-mini, no-cache=false)
concurrency     50
dup ratio       0.00 over 20 hot prompts
elapsed         20.01s
requests        149375 total, 149375 ok, 0 failed (100.00% success)
throughput      7466.3 req/s (successful requests / elapsed)
cache hit rate  0.00% (X-Cache: HIT among successful requests)
                    n        p50        p95        p99        max
latency all     149375     5.84ms    14.52ms    19.83ms    42.40ms
latency miss    149375     5.84ms    14.52ms    19.83ms    42.40ms
ttft all        149375     4.00ms    11.66ms    16.39ms    38.49ms
ttft miss       149375     4.00ms    11.66ms    16.39ms    38.49ms
== C2 gateway no-cache, zero latency ==
target          http://127.0.0.1:8080 (stream=true, model=gpt-4o-mini, no-cache=true)
concurrency     50
dup ratio       0.00 over 20 hot prompts
elapsed         20.01s
requests        71178 total, 71178 ok, 0 failed (100.00% success)
throughput      3556.9 req/s (successful requests / elapsed)
cache hit rate  0.00% (X-Cache: HIT among successful requests)
                    n        p50        p95        p99        max
latency all      71178    14.06ms    16.46ms    18.45ms    30.08ms
latency miss     71178    14.06ms    16.46ms    18.45ms    30.08ms
ttft all         71178     9.68ms    13.40ms    14.41ms    26.82ms
ttft miss        71178     9.68ms    13.40ms    14.41ms    26.82ms
== C3 gateway cache on dup 0, zero latency ==
target          http://127.0.0.1:8080 (stream=true, model=gpt-4o-mini, no-cache=false)
concurrency     50
dup ratio       0.00 over 20 hot prompts
elapsed         20.01s
requests        26340 total, 26340 ok, 0 failed (100.00% success)
throughput      1316.3 req/s (successful requests / elapsed)
cache hit rate  0.00% (X-Cache: HIT among successful requests)
                    n        p50        p95        p99        max
latency all      26340    38.27ms    50.73ms    57.90ms    88.10ms
latency miss     26340    38.27ms    50.73ms    57.90ms    88.10ms
ttft all         26340    32.24ms    43.56ms    49.81ms    67.85ms
ttft miss        26340    32.24ms    43.56ms    49.81ms    67.85ms
956
== C4 gateway cache on dup 0.5, zero latency ==
target          http://127.0.0.1:8080 (stream=true, model=gpt-4o-mini, no-cache=false)
concurrency     50
dup ratio       0.50 over 20 hot prompts
elapsed         20.02s
requests        26298 total, 26298 ok, 0 failed (100.00% success)
throughput      1313.7 req/s (successful requests / elapsed)
cache hit rate  50.22% (X-Cache: HIT among successful requests)
                    n        p50        p95        p99        max
latency all      26298    36.49ms    54.58ms    75.75ms   175.31ms
latency hit      13206    34.20ms    49.71ms    70.39ms   162.44ms
latency miss     13092    39.48ms    57.45ms    82.18ms   175.31ms
ttft all         26298    33.26ms    48.20ms    69.24ms   162.44ms
ttft hit         13206    34.20ms    49.69ms    70.39ms   162.44ms
ttft miss        13092    32.15ms    46.87ms    67.36ms   155.83ms
```
The bare `956` line is `ZCARD sc:{loadtest}:lru` taken right after the 0%-hit run.
</details>

<details><summary>Failover, mock-a failing 50%</summary>

```
== F mock-a (weight 3) fail-rate 0.5, mock-b (weight 1) healthy ==
target          http://127.0.0.1:8080 (stream=true, model=gpt-4o-mini, no-cache=true)
concurrency     50
dup ratio       0.00 over 20 hot prompts
elapsed         30.60s
requests        2250 total, 2250 ok, 0 failed (100.00% success)
throughput      73.5 req/s (successful requests / elapsed)
cache hit rate  0.00% (X-Cache: HIT among successful requests)
                    n        p50        p95        p99        max
latency all       2250   673.95ms   683.33ms   789.11ms   821.83ms
latency miss      2250   673.95ms   683.33ms   789.11ms   821.83ms
ttft all          2250   102.78ms   105.12ms   229.80ms   256.54ms
ttft miss         2250   102.78ms   105.12ms   229.80ms   256.54ms
llm_gateway_circuit_breaker_state{upstream="mock-a"} 2
llm_gateway_circuit_breaker_state{upstream="mock-b"} 0
llm_gateway_failovers_total{from="mock-a",to="mock-b"} 43
llm_gateway_requests_total{code="200",tenant="loadtest"} 2250
llm_gateway_upstream_errors_total{reason="status_5xx",upstream="mock-a"} 43
mock-a log: "requests":78,"failures":43,"cancelled":0
mock-b log: "requests":2215,"failures":0,"cancelled":0
```
</details>

<details><summary>Failover, mock-a failing 100%</summary>

```
== F mock-a (weight 3) fail-rate 1.0, mock-b (weight 1) healthy ==
target          http://127.0.0.1:8080 (stream=true, model=gpt-4o-mini, no-cache=true)
concurrency     50
dup ratio       0.00 over 20 hot prompts
elapsed         30.66s
requests        2250 total, 2250 ok, 0 failed (100.00% success)
throughput      73.4 req/s (successful requests / elapsed)
cache hit rate  0.00% (X-Cache: HIT among successful requests)
                    n        p50        p95        p99        max
latency all       2250   678.84ms   684.29ms   788.78ms   819.90ms
latency miss      2250   678.84ms   684.29ms   788.78ms   819.90ms
ttft all          2250   102.61ms   104.33ms   232.34ms   253.44ms
ttft miss         2250   102.61ms   104.33ms   232.34ms   253.44ms
llm_gateway_circuit_breaker_state{upstream="mock-a"} 2
llm_gateway_circuit_breaker_state{upstream="mock-b"} 0
llm_gateway_failovers_total{from="mock-a",to="mock-b"} 40
llm_gateway_requests_total{code="200",tenant="loadtest"} 2250
llm_gateway_upstream_errors_total{reason="status_5xx",upstream="mock-a"} 40
mock-a log: "requests":40,"failures":40,"cancelled":0
mock-b log: "requests":2250,"failures":0,"cancelled":0
```
</details>

<details><summary>Go micro-benchmarks</summary>

```
BenchmarkHashEmbed-18    	 1000000	      1180 ns/op	    1312 B/op	       2 allocs/op
BenchmarkCosine256-18    	 6437846	       172.6 ns/op	       0 B/op	       0 allocs/op
BenchmarkPick-18    	12689125	        91.03 ns/op	      24 B/op	       1 allocs/op
```
</details>

<details><summary>Redis outage: before (9980b08)</summary>

```
== BEFORE (9980b08): Redis down, nothing listening on 127.0.0.1:6489; zero-latency mocks; 10 sequential streaming requests ==
target          http://127.0.0.1:28471 (stream=true, model=gpt-4o-mini, no-cache=false)
concurrency     1
dup ratio       0.00 over 20 hot prompts
elapsed         54.86s
requests        10 total, 10 ok, 0 failed (100.00% success)
throughput      0.2 req/s (successful requests / elapsed)
cache hit rate  0.00% (X-Cache: HIT among successful requests)
                    n        p50        p95        p99        max
latency all         10  5485.04ms  5528.61ms  5528.61ms  5528.61ms
latency miss        10  5485.04ms  5528.61ms  5528.61ms  5528.61ms
ttft all            10  3483.73ms  3526.24ms  3526.24ms  3526.24ms
ttft miss           10  3483.73ms  3526.24ms  3526.24ms  3526.24ms
llm_gateway_dependency_errors_total{component="cache"} 20
llm_gateway_dependency_errors_total{component="ledger"} 10
llm_gateway_dependency_errors_total{component="ratelimit"} 10
llm_gateway_requests_total{code="200",tenant="loadtest"} 10
```
</details>

<details><summary>Redis outage: after (8016890), Redis down</summary>

```
== AFTER (8016890): Redis down, nothing listening on 127.0.0.1:6489; zero-latency mocks; 10 sequential streaming requests ==
target          http://127.0.0.1:28471 (stream=true, model=gpt-4o-mini, no-cache=false)
concurrency     1
dup ratio       0.00 over 20 hot prompts
elapsed         0.01s
requests        10 total, 10 ok, 0 failed (100.00% success)
throughput      817.0 req/s (successful requests / elapsed)
cache hit rate  0.00% (X-Cache: HIT among successful requests)
                    n        p50        p95        p99        max
latency all         10     0.92ms     4.08ms     4.08ms     4.08ms
latency miss        10     0.92ms     4.08ms     4.08ms     4.08ms
ttft all            10     0.36ms     3.33ms     3.33ms     3.33ms
ttft miss           10     0.36ms     3.33ms     3.33ms     3.33ms
llm_gateway_redis_breaker_state 2
llm_gateway_redis_unavailable_total{component="cache_lookup"} 10
llm_gateway_redis_unavailable_total{component="ledger"} 10
llm_gateway_redis_unavailable_total{component="ratelimit"} 10
llm_gateway_requests_total{code="200",tenant="loadtest"} 10
== AFTER (8016890): Redis still down, 50 concurrent streams, 20 s ==
target          http://127.0.0.1:28471 (stream=true, model=gpt-4o-mini, no-cache=false)
concurrency     50
dup ratio       0.00 over 20 hot prompts
elapsed         20.01s
requests        82411 total, 82411 ok, 0 failed (100.00% success)
throughput      4118.2 req/s (successful requests / elapsed)
cache hit rate  0.00% (X-Cache: HIT among successful requests)
                    n        p50        p95        p99        max
latency all      82411    11.97ms    15.17ms    17.38ms    29.98ms
latency miss     82411    11.97ms    15.17ms    17.38ms    29.98ms
ttft all         82411    10.58ms    13.10ms    14.92ms    28.45ms
ttft miss        82411    10.58ms    13.10ms    14.92ms    28.45ms
llm_gateway_redis_breaker_state 2
llm_gateway_redis_unavailable_total{component="cache_lookup"} 82421
llm_gateway_redis_unavailable_total{component="ledger"} 82421
llm_gateway_redis_unavailable_total{component="ratelimit"} 82421
llm_gateway_requests_total{code="200",tenant="loadtest"} 82421
redis: 2026/11/01 21:34:38 pool.go:770: redis: connection pool: failed to dial after 1 attempts: dial tcp 127.0.0.1:6489: connect: connection refused
redis: 2026/11/01 21:34:38 pool.go:770: redis: connection pool: failed to dial after 1 attempts: dial tcp 127.0.0.1:6489: connect: connection refused
redis: 2026/11/01 21:34:38 pool.go:770: redis: connection pool: failed to dial after 1 attempts: dial tcp 127.0.0.1:6489: connect: connection refused
redis: 2026/11/01 21:34:38 pool.go:770: redis: connection pool: failed to dial after 1 attempts: dial tcp 127.0.0.1:6489: connect: connection refused
redis: 2026/11/01 21:34:38 pool.go:770: redis: connection pool: failed to dial after 1 attempts: dial tcp 127.0.0.1:6489: connect: connection refused
redis: 2026/11/01 21:34:38 pool.go:770: redis: connection pool: failed to dial after 1 attempts: dial tcp 127.0.0.1:6489: connect: connection refused
{"time":"2026-11-01T21:34:38.397697+05:30","level":"WARN","msg":"redis circuit breaker changed state","state":"open"}
{"time":"2026-11-01T21:34:43.397761+05:30","level":"WARN","msg":"redis circuit breaker changed state","state":"half-open"}
```
</details>

<details><summary>Redis outage: after (8016890), kill and restart under load</summary>

```
== AFTER (8016890): Redis up, killed at ~5 s, restarted at ~12 s; zero-latency mocks, 50 streams, 20 s ==
kill 21:35:21
restart 21:35:28
target          http://127.0.0.1:28471 (stream=true, model=gpt-4o-mini, no-cache=false)
concurrency     50
dup ratio       0.50 over 20 hot prompts
elapsed         20.02s
requests        54570 total, 54570 ok, 0 failed (100.00% success)
throughput      2726.1 req/s (successful requests / elapsed)
cache hit rate  13.75% (X-Cache: HIT among successful requests)
                    n        p50        p95        p99        max
latency all      54570    13.24ms    39.99ms    46.12ms    62.23ms
latency hit       7506    33.09ms    39.41ms    43.34ms    46.26ms
latency miss     47064    12.93ms    40.17ms    46.49ms    62.23ms
ttft all         54570    12.03ms    35.13ms    39.42ms    48.70ms
ttft hit          7506    33.08ms    39.40ms    43.34ms    46.25ms
ttft miss        47064    11.74ms    32.53ms    37.00ms    48.70ms
llm_gateway_redis_breaker_state 0
llm_gateway_redis_unavailable_total{component="cache_lookup"} 39460
llm_gateway_redis_unavailable_total{component="ledger"} 39440
llm_gateway_redis_unavailable_total{component="ratelimit"} 39414
llm_gateway_requests_total{code="200",tenant="loadtest"} 54570
breaker 21:35:21 open
breaker 21:35:26 half-open
breaker 21:35:26 open
breaker 21:35:31 half-open
breaker 21:35:31 closed
```
</details>

<details><summary>Redis outage: after (8016890), healthy Redis under the heaviest cache load</summary>

```
== Healthy Redis, after (8016890): zero-latency mocks, cache on, dup ratio 0, 50 streams, 20 s ==
requests        26397 total, 26397 ok, 0 failed (100.00% success)
throughput      1318.3 req/s (successful requests / elapsed)
cache hit rate  0.00% (X-Cache: HIT among successful requests)
                    n        p50        p95        p99        max
latency all      26397    38.83ms    49.58ms    54.03ms    66.53ms
latency miss     26397    38.83ms    49.58ms    54.03ms    66.53ms
ttft all         26397    32.98ms    43.06ms    46.74ms    56.66ms
ttft miss        26397    32.98ms    43.06ms    46.74ms    56.66ms
llm_gateway_redis_breaker_state 0
(no llm_gateway_redis_unavailable_total or llm_gateway_dependency_errors_total series: zero Redis failures)
```
</details>
