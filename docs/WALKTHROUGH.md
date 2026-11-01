# Code Walkthrough

This guide is for someone who knows backend basics (HTTP, Redis, goroutines) but has not seen this code. It covers
where to start reading, where each feature lives, what happens to one request from start to finish, why the design
is the way it is, what it does not do, likely interview questions, and a five-minute demo.

## 1. Reading order

Read in this order; each step only depends on the ones before it. The git history follows the same order, one
concept per commit (`git log --reverse --oneline`).

| # | File | What to look for |
|---|---|---|
| 1 | `internal/config/config.go` | The shape of the system: upstreams, tenants, cache, pricing. `Parse` applies defaults, reads secrets from env, validates |
| 2 | `internal/provider/provider.go` | The OpenAI wire types everything speaks, the `Provider` and `Stream` interfaces, `StatusError` |
| 3 | `internal/provider/transport.go` | `NewHTTPClient`: the per-upstream connection pool |
| 4 | `internal/provider/sse.go`, `openai.go` | Reading server-sent events; the OpenAI-compatible adapter |
| 5 | `internal/provider/anthropic.go` | Translating to and from the Anthropic Messages API, including its typed stream events |
| 6 | `internal/router/backoff.go`, `internal/breaker/breaker.go` | Full-jitter backoff; the circuit breaker state machine |
| 7 | `internal/router/router.go` | Weighted pick, failover loop, retry-before-first-token |
| 8 | `internal/ratelimit/tokenbucket.lua`, `ratelimit.go` | The token bucket as one atomic Redis script |
| 9 | `internal/cache/vector.go`, `embedder.go` | Cosine similarity, vector encoding, the two embedders |
| 10 | `internal/cache/semantic.go` | Redis key layout, `Lookup` and `Store` with LRU eviction |
| 11 | `internal/cost/cost.go` | Integer pricing, the ledger, usage estimation |
| 12 | `internal/metrics/metrics.go` | Every Prometheus series in one place, with the PromQL for each derived number |
| 13 | `internal/redisguard/redisguard.go` | The Redis client: tight timeouts, no retries, a breaker hook so an outage costs milliseconds |
| 14 | `internal/server/server.go`, `sse.go`, `chat.go` | The request pipeline that ties it all together |
| 15 | `cmd/gateway/main.go` | Wiring and graceful shutdown |
| 16 | `internal/mockllm`, `cmd/mockprovider`, `cmd/loadtest` | How the tests, demos and benchmarks get a controllable upstream and load |

Tests sit next to each file. The most instructive ones are `internal/router/router_test.go` (failover and
retry-before-first-token as executable examples) and `internal/server/server_test.go` (end-to-end through the real
HTTP stack to mock upstreams).

## 2. Resume claims mapped to code

### "high-throughput proxy service in Go fronting multiple upstream providers"

* `internal/server/chat.go`, `chatCompletions`: the handler for `POST /v1/chat/completions`.
* `internal/provider/openai.go` (`OpenAI`) and `internal/provider/anthropic.go` (`Anthropic`): adapters behind the
  `Provider` interface in `provider.go`. The rest of the gateway only sees OpenAI-format types, so adding a provider
  means writing one adapter.
* Throughput, measured (BENCHMARKS.md): with zero-latency mock upstreams the gateway served 3556.9 req/s at 50
  concurrent streams with p99 18.45 ms when the cache was bypassed, and about 1314 to 1316 req/s with the cache on. Its
  added time to first token was 2.06 ms at p50 (6.63 ms at p99) against a mock with a 100 ms first token.

### "weighted routing"

* `internal/router/router.go`, `Router.pick`: nginx's smooth weighted round-robin. Every pick adds each upstream's
  weight to its running score, chooses the highest score and subtracts the total weight from the winner. With
  weights 5:1:1 the sequence is `a a b a c a a`: exact proportions over every 7 picks, and interleaved instead of
  `a a a a a b c`. `TestSmoothWeightedRoundRobin` checks both properties.

### "automatic failover"

* `Router.candidates`: the weighted pick first, then the other upstreams by descending weight.
* `Router.try`: walks the candidates. An upstream whose breaker is open is skipped without spending an attempt.
  A retryable failure (connection error, timeout, 429, 5xx, and 401/403, which mean this upstream rejected the
  gateway's own key) records the error, marks the breaker and moves to the next candidate after a backoff. Any other
  4xx is returned to the client immediately: every upstream would reject the same request.
* `classify`: turns an error into a metrics reason and a retry decision.
* `internal/breaker/breaker.go`, `Breaker`: closed, then open after N consecutive failures, then half-open after a
  cooldown, admitting one probe whose result closes or reopens it.

### "streaming response passthrough"

* `internal/server/chat.go`, `Server.stream`: reads chunks from the routed stream and writes each one immediately.
* `internal/server/sse.go`, `sseWriter.event`: writes one `data: ...\n\n` event and calls `http.Flusher.Flush`, so
  the chunk leaves the process now instead of sitting in a buffer. It also sets `X-Accel-Buffering: no` for nginx-style
  proxies.
* `internal/provider/openai.go`, `openAIStream.Recv`: keeps the upstream's original bytes in `Chunk.Raw`, so
  OpenAI-compatible chunks are forwarded byte for byte. `anthropicStream.Recv` translates Anthropic events into
  OpenAI chunks.
* Client cancellation: the handler passes `r.Context()` down to the upstream request. When the client disconnects,
  Go cancels that context, the upstream HTTP request is aborted and the upstream stops generating.
  `TestClientDisconnectCancelsUpstream` proves it from the mock's side.

### "connection pooling"

* `internal/provider/transport.go`, `NewHTTPClient`: one `http.Transport` per upstream with `MaxIdleConnsPerHost`
  raised from Go's default of 2 to 256, keep-alives, and dial and TLS timeouts. There is no `Client.Timeout` (it
  would cut long streams off) and no `ResponseHeaderTimeout` (a non-streaming answer only sends headers when the whole
  generation is done); per-request contexts carry the deadlines instead.
* Connections only go back to the pool if the body is read to EOF and closed: `openAIStream.Recv` drains the body on
  `[DONE]`, and `readStatusError` reads and closes error bodies.

### "retry with exponential backoff and jitter"

* `internal/router/backoff.go`, `Backoff.Delay`: full jitter, `rand(0, min(max, base * 2^n))`.
* `Router.try` sleeps that delay before every retry, whether to the same upstream or the next one.
* `Router.openStream`: the retry window for streams. It opens the stream and reads ahead to the first chunk that
  carries content. Nothing has been written to the client yet, so a failure up to that point can still be retried
  elsewhere. Once `Router.Stream` returns, the server writes chunks and the router never retries again
  (`TestStreamDoesNotRetryAfterFirstToken`).

### "semantic caching: embedding incoming requests and serving cached responses above a similarity threshold"

* `internal/cache/semantic.go`, `PromptText`: the canonical text of a request: the model plus every message,
  lower-cased with whitespace collapsed.
* `Cache.Embed` calls an `Embedder` (`embedder.go`): `OpenAIEmbedder` for any `/v1/embeddings` endpoint, or
  `HashEmbedder`, a deterministic local embedder that hashes words and word pairs into a fixed-size signed vector.
* `Cache.Lookup`: reads the tenant's entry ids from the LRU sorted set, pipelines one `HMGET` (embedding, model and
  parameter fingerprint) per entry in a single round trip, computes `Cosine` in Go for entries with the same model
  and parameters (`ParamsKey`: `max_tokens`, `temperature`, `top_p`, `stop`), and on a hit fetches the answer and
  bumps its LRU score in one more round trip.
* `internal/server/chat.go`, `Server.lookup` and `Server.serveHit`: apply the per-tenant threshold, return the cached
  answer as a stream or JSON, and set `X-Cache` and `X-Cache-Similarity`.

### "with an LRU eviction policy in Redis"

* `internal/cache/semantic.go`, `Cache.Store`: writes the entry hash with a TTL and adds its id to the sorted set
  `sc:{tenant}:lru` scored by time, in one `MULTI/EXEC`. If the set is larger than `max_entries`, `ZPOPMIN` removes
  the least-recently-used ids and `DEL` removes their hashes.
* `Cache.Lookup` refreshes the score of the entry it serves with `ZADD XX`, which is what makes the order
  least-recently-used rather than oldest-first. `TestLRUEviction` shows an older but recently read entry surviving.
* `docker-compose.yml` also runs Redis with `maxmemory-policy volatile-lru` as a memory safety net that only evicts
  keys with a TTL, so the ledger is never evicted.

### "per-tenant token-bucket rate limiting"

* `internal/ratelimit/tokenbucket.lua`: reads the bucket, refills it for the time elapsed since the last call (using
  Redis `TIME`), takes a token or computes the wait, writes the bucket back and sets its expiry, all in one script
  that Redis runs atomically.
* `internal/ratelimit/ratelimit.go`, `Limiter.Allow`: runs the script (EVALSHA with EVAL fallback) for the key
  `rl:{tenant}`.
* `internal/server/chat.go`, `Server.allow`: returns `429` with `Retry-After` in whole seconds, and fails open if Redis
  is down.

### "cost accounting"

* `internal/cost/cost.go`, `NewPricer`: converts the config's USD per million tokens into integer micro-dollars per
  million tokens, which equals picodollars per token, so a request's cost is an exact integer product.
* `Pricer.MicroUSD`: prompt and completion tokens times their prices, rounded to whole micro-dollars.
* `Ledger.Record`: `HINCRBY` on `ledger:{tenant}:YYYY-MM` for requests, tokens, spend in total and per model, cache
  hits and the money they saved, and how many requests had estimated token counts.
* `EstimateUsage`: about four characters per token, used only when a stream ends without a usage report.
* `internal/server/chat.go`, `Server.finish`: takes token counts from the provider's usage fields (the gateway always
  asks OpenAI-compatible upstreams for `stream_options.include_usage`), estimates when missing, and records cost.
  `GET /v1/usage` returns the ledger.

### "Instrumented end to end with Prometheus: cache hit rate, time-to-first-token, per-upstream latency percentiles and per-tenant spend"

All series are defined in `internal/metrics/metrics.go`, `New`.

* Cache hit rate: `llm_gateway_cache_hits_total` and `llm_gateway_cache_misses_total`, incremented in
  `Cache.Lookup`; the rate is computed in PromQL.
* Time to first token: `llm_gateway_ttft_seconds`, observed in `Server.stream` when the first chunk with content
  is flushed (label `source` = upstream), and in `Server.serveHit` for cache hits (`source="cache"`).
* Per-upstream latency: `llm_gateway_upstream_request_duration_seconds{upstream,outcome}`, observed in
  `Router.Complete`, `Router.try` (failed attempts) and `routedStream.finish` (streams, at their last byte).
  Percentiles come from `histogram_quantile` over the buckets.
* Per-tenant spend: `llm_gateway_tenant_spend_usd_total{tenant,model}`, incremented in `Ledger.Record`.
* Also: failovers, upstream errors by reason, breaker state, rate-limit rejections, in-flight requests, tokens.
* `deploy/prometheus.yml` scrapes the gateway; `deploy/grafana/dashboards/llm-gateway.json` has a panel for each.

### "load-tested to establish throughput and p99 under concurrency"

* `cmd/loadtest/main.go`: `run` starts N workers (default 50); `do` sends one streaming request and records latency,
  TTFT and `X-Cache`; `report` prints throughput and p50/p95/p99 (nearest rank) for all requests, hits and misses.
  `-dup-ratio` sets the share of repeated prompts, which controls the hit rate.
* `cmd/mockprovider` gives an upstream with exact latency, per-token delay and failure rates.
* Results: `BENCHMARKS.md`.

### "Stack: Go, Redis, Prometheus, Docker"

Go 1.26 with the standard library router; Redis via go-redis v9; Prometheus via client_golang; a multi-stage
`Dockerfile` (distroless, non-root) and `docker-compose.yml` with Redis, two mocks, Prometheus and Grafana. CI builds
the image on every push.

## 3. The life of one request

A client sends `POST /v1/chat/completions` with `"stream": true`. In `internal/server/chat.go`:

1. **Metrics bookkeeping.** `chatCompletions` wraps the response writer in `statusWriter` (to learn the status code
   later), increments the in-flight gauge, and defers the request counter and duration histogram.
2. **Authenticate.** `Server.authenticate` takes the Bearer token (or `X-API-Key`), hashes it with SHA-256 and looks
   the digest up in a map built from config. No match: `401`.
3. **Rate limit.** `Server.allow` runs the token-bucket script for `rl:{tenant}`. Empty bucket: `429` with
   `Retry-After`. Redis unavailable: count `llm_gateway_redis_unavailable_total{component="ratelimit"}` and let the
   request through (see "What if Redis is down?" below).
4. **Decode.** The body is capped with `http.MaxBytesReader` and decoded into `provider.ChatRequest`.
5. **Cache lookup.** Unless the tenant disabled the cache or the client sent `Cache-Control: no-cache`,
   `Server.lookup` embeds `PromptText(model, messages)` once and calls `Cache.Lookup`. On a hit, `serveHit` writes the
   cached answer (one content chunk, a finish chunk, `[DONE]`), observes TTFT with `source="cache"`, records a cache
   hit and the money saved in the ledger, and returns. On a miss the embedding is kept for later.
6. **Route.** `Router.Stream` asks `candidates` for an order (weighted pick, then by weight) and `try` walks it. For
   each attempt, `openStream` sends the upstream request with a context that is cancelled if no token arrives
   within `first_token_timeout`, then reads ahead to the first content chunk. Failures before that point are retried
   on the next upstream after a full-jitter backoff; open breakers are skipped.
7. **Stream.** `Server.stream` sets `X-Cache: MISS` and `X-Upstream`, writes the `200` header, and loops: receive a
   chunk, remember content, finish reason and usage, drop the usage-only chunk if this client did not ask for usage,
   write and flush the chunk. The first content chunk observes TTFT. A mid-stream failure becomes an in-band error
   event and the stream ends without `[DONE]`; a client disconnect cancels the context, which aborts the upstream.
8. **Cost.** `Server.finish` takes token counts from the upstream's usage chunk, or estimates them and flags the
   record, and calls `Ledger.Record`, which `HINCRBY`s the tenant's monthly hash and adds to the spend counter.
9. **Cache store.** If the answer completed with `finish_reason: "stop"`, `Cache.Store` saves it under a new id with a
   TTL, adds it to the LRU set and evicts beyond `max_entries`. This happens before `[DONE]` is written, so a client
   that repeats the question immediately gets a hit.
10. **Done.** `[DONE]` is written, the routed stream is closed (which records the per-upstream latency for the whole
    attempt), the request log line is written, and the deferred metrics run.

Non-streaming requests take the same path with `Router.Complete` and one JSON response instead of steps 6 and 7.

## 4. Design trade-offs and alternatives

**Smooth weighted round-robin instead of weighted random.** Both give the configured shares on average. Weighted
random has variance: with weights 3:1, a run of eight picks to the small upstream is possible and bursts hurt a
small provider quota. SWRR is deterministic, exact over every `sum(weights)` picks and interleaved. It needs a mutex
around the scores; a whole routing decision (pick plus failover order) measured 91.03 ns (`BenchmarkPick`). Across several gateway replicas each replica
interleaves independently, which still gives the right shares overall.

**Retry only before the first byte.** An LLM answer is not idempotent: a second upstream produces a different
answer. Once part of answer A has reached the client, appending answer B, or restarting, would hand the client a
corrupted or duplicated message, and HTTP has no way to take back bytes already sent (the `200` header is gone too).
So the gateway only retries while it has sent nothing, which includes upstreams that accept the request and then die
before the first token. After that, a failure is reported in-band and the client decides.

**Full jitter.** Exponential backoff without jitter makes every client that failed at the same moment retry at the
same moment, recreating the spike. Full jitter (`rand(0, cap)`) spreads retries over the whole window; AWS's
"Exponential Backoff and Jitter" analysis found it does the least total work among the common variants. Equal jitter
keeps a minimum wait; decorrelated jitter uses the previous delay. With at most three attempts the difference is
small; full jitter is the simplest correct choice.

**A simple consecutive-failure breaker.** Error-rate windows (for example "50% of the last 20 requests") react better
to partial failures but need a sliding window per upstream. Consecutive failures is easy to reason about, and the
benchmark shows it works: with an upstream hard down it received 40 requests in 30 seconds instead of thousands.

**Per-upstream connection pools.** A single shared transport would work, but per-upstream pools isolate a slow
provider (its connections cannot crowd out others) and allow per-provider limits such as `max_conns`.

**Semantic cache as a linear scan.** Simple, exact (no approximate-nearest-neighbour recall loss) and fast enough for
hundreds of entries. Its cost grows linearly: at 1000 entries of 1 KB, a lookup moves about 1 MB from Redis, and
the zero-latency benchmark shows that capping the gateway at about 1300 req/s. The next step is a vector index:
Redis Query Engine with an HNSW index answers top-k inside Redis, with a tag filter on tenant and model.

**Threshold choice.** Too low serves wrong answers ("capital of France" vs "capital of Germany" are lexically close);
too high rarely hits. 0.95 is conservative. `llm_gateway_cache_best_similarity` records the best similarity of every
lookup so the threshold can be tuned from data, per tenant.

**What the cache key includes.** The embedding covers the model and every message, so a cached answer is only reused
for the same conversation. The model and a fingerprint of `max_tokens`, `temperature`, `top_p` and `stop` are also
compared exactly: two requests to different models must never share an answer, and an answer generated for 500 tokens
must not be served to a request for 5. Clients that want a fresh sample every time should send
`Cache-Control: no-cache`.

**LRU in a sorted set.** The ZSET gives `O(log n)` updates and `ZPOPMIN` returns the least recently used ids
directly. Relying only on Redis `maxmemory-policy allkeys-lru` would be per-instance rather than per-tenant, approximate,
and could evict the ledger; it is used only as a safety net (`volatile-lru`).

**Hash tags.** `sc:{acme}:lru`, `sc:{acme}:e:<id>`, `rl:{acme}` and `ledger:{acme}:2026-11` all hash on `acme`, so they
live in one Redis Cluster slot. That is what allows the `MULTI/EXEC` in `Cache.Store` (and any future Lua touching
several keys) on a cluster, and it spreads different tenants across slots.

**Token bucket in Lua with Redis time.** Check-then-write from the application would race between replicas. A Lua
script runs atomically, makes one round trip, and with `TIME` every replica uses the same clock. A token bucket
allows bursts up to the capacity while holding the long-run rate; a fixed window allows double bursts at window edges,
and a sliding log stores every request.

**What if Redis is down? Fail open, and fail fast.** Redis backs three optional features, so the gateway keeps serving
without it. Each feature has an explicit outage behaviour, counted in
`llm_gateway_redis_unavailable_total{component}`:

| Feature | While Redis is unavailable | Trade-off |
|---|---|---|
| Rate limiting (`ratelimit`) | The request is allowed unchecked | Availability over enforcement: a tenant can exceed its limit during the outage. A deployment where limits protect billing or a fragile upstream could fail closed instead (return 503). |
| Cache lookup (`cache_lookup`) | Served as a miss, and the answer is not stored | Every request goes upstream: correct answers, higher cost and latency until Redis returns. |
| Cost ledger (`ledger`) | The ledger write is skipped and counted | The monthly ledger has a gap for the outage. The Prometheus spend and token counters still increase, so spend stays visible and can be reconciled from them. |

Failing open only helps if failing is quick. With go-redis defaults (5 dial attempts 100 ms apart, then 3 command
retries) each Redis call spent over a second failing, and a request made four of them: the measured request time with
Redis down was 5454.05 ms. `internal/redisguard` fixes this in two layers. Tight timeouts (50 ms dial, read, write and
pool wait, one dial attempt, no command retries) bound the first failures. A circuit breaker hook around every
command and pipeline then opens after 5 consecutive failures and refuses calls locally (no network at all) for a 5 s
cooldown, then lets one probe through and closes when Redis answers. Measured request time with Redis down after the
fix: 0.78 ms at p50 (BENCHMARKS.md). The breaker logs each state change once instead of every request logging an error.

The timeouts are tight because the slowest healthy call is small: the heaviest benchmark (1000-entry cache scans at
1246.2 req/s) ran with zero Redis timeouts at 50 ms. `/readyz` still pings Redis through the breaker, so an orchestrator
sees the gateway as not ready while Redis is down, even though requests are served.

**Integer money.** Floats cannot represent 0.1 exactly and errors accumulate over millions of additions. Prices
become integers once at startup, costs are exact integer products in picodollars, and the ledger stores whole
micro-dollars with atomic `HINCRBY`.

**Accounting before `[DONE]`.** Writing the ledger and the cache entry before the final event costs a Redis round trip
at the end of the stream (never on the first token) and gives read-your-writes: a repeat question right after the
answer is a hit.

## 5. Known limitations

* Text chat only. Tool calls, function calling, images, `n > 1`, `logprobs` and `response_format` are not translated,
  and fields the gateway does not model are dropped. The Anthropic adapter rejects the `tool` role, clamps
  temperature to Anthropic's 0 to 1 range, and merges consecutive messages from the same role.
* The semantic cache scans linearly; cost grows with `max_entries` (see section 4). Under concurrent stores the
  eviction can remove a few more entries than necessary: after the zero-latency 0%-hit benchmark at 50 concurrent
  clients the index held 956 of 1000. A Lua script doing `ZCARD`, `ZPOPMIN` and `DEL` atomically would make it exact.
* The hash embedder is lexical. It matches rewordings that share words, not synonyms or paraphrases. Production
  should use `embedder.type: openai` with a real embedding model, which adds a network round trip to each lookup.
* Matching ignores letter case and extra whitespace. That suits prose but can be wrong for code, where `Print` and
  `print` differ. The cache also serves the answer cached for the requested model name, whichever upstream produced it
  under `model_map` (savings are priced at the model that actually served it).
* Breaker state and round-robin scores live in each gateway process; replicas do not share them.
* Rate limits count requests, not LLM tokens.
* Token counts are estimated (about four characters per token) when the upstream reports no usage, for example when a
  client disconnects before the usage chunk. Such requests are flagged in the ledger and in
  `llm_gateway_estimated_usage_total`.
* Prices are matched on the exact model name sent upstream. Unknown models are recorded at zero cost and counted in
  `llm_gateway_unpriced_requests_total`. The prices in the example config are illustrative.
* Each request's cost is rounded to whole micro-dollars before it is added to the ledger; token counts are stored too,
  so exact totals can be recomputed.
* Tenants come from the config file; changing them needs a restart. There is no TLS termination; run behind a proxy or
  load balancer for that.
* During a Redis outage there is no rate limiting, no caching and no ledger (see section 4), and recovery takes up to
  one breaker cooldown (5 s) after Redis returns. A probe can also reach a Redis that is back but not yet warm; the
  first failed call simply reopens the breaker for another cooldown.
* Tested against single-node Redis only. The key layout is cluster-ready but has not been run on a cluster.
* Benchmarks ran on one laptop with mock upstreams (see BENCHMARKS.md for caveats). The Docker image is built and
  checked in CI; the compose stack was not run locally while writing this.

## 6. Likely interview questions

**1. Walk me through a request.** Auth by hashed API key, token bucket in Redis, embed and look up the semantic cache,
on a miss route with weighted round-robin and failover, stream chunks back with a flush per chunk, record cost from
the usage fields, store the answer in the cache, and Prometheus metrics along the way (section 3).

**2. Why smooth weighted round-robin and not weighted random?** Deterministic and exact over each cycle, and it
interleaves picks, so a small upstream never gets a burst. Random is right on average but has variance.

**3. Why do you only retry before the first byte?** LLM answers are not idempotent. After part of one answer has
been sent, a retry elsewhere would splice two different answers, and the 200 status is already committed. Before
the first byte a retry is invisible to the client.

**4. What do you retry, and what not?** Connection errors, timeouts (including no first token in time), 429 and 5xx
are retried on the next upstream, and so are 401 and 403, because they mean that upstream rejected the gateway's own
key. Other 4xx (400, 404, 413, 422) go back to the client as-is, because any upstream would reject the same request.
Client cancellation stops everything and is not counted against the upstream.

**5. Why full jitter?** Without jitter, clients that failed together retry together and re-create the overload. Full
jitter spreads them uniformly over the backoff window.

**6. Explain the circuit breaker.** Closed until five consecutive failures, then open: the router skips that upstream
without spending an attempt. After a 10 s cooldown it is half-open and lets exactly one probe through; success closes
it, failure reopens it. A success reported while it is open (a slow request that started before it tripped) is
ignored, so only a probe can close it. A probe whose result never arrives stops blocking after another cooldown.

**7. How does streaming passthrough work?** The adapter reads SSE events, the server writes each one as
`data: ...\n\n` and calls `Flush`. OpenAI-format chunks are forwarded byte for byte; Anthropic events are translated.
`X-Accel-Buffering: no` asks reverse proxies not to buffer.

**8. What happens when the client disconnects?** `r.Context()` is cancelled, and since that context is the parent of
the upstream request, the upstream connection is closed and the provider stops generating. Cost is still recorded for
what was produced.

**9. What did you tune for connection pooling and why?** One `http.Transport` per upstream, `MaxIdleConnsPerHost` from
the default 2 to 256 so concurrent streams reuse warm TCP/TLS connections instead of redialling, dial and TLS
timeouts, no `Client.Timeout` or `ResponseHeaderTimeout` (they would kill long streams and long non-streaming
generations; contexts carry deadlines instead), and draining bodies so connections return to the pool.

**10. Why is the rate limiter a Lua script, and why Redis `TIME`?** Atomicity: read, refill, take and write happen as
one step, so concurrent replicas cannot both take the last token (`TestConcurrentCallsNeverExceedBurst`). One round
trip. Redis time means replicas with skewed clocks still agree on how much the bucket refilled.

**11. Token bucket vs fixed window vs sliding window?** Token bucket gives a long-run rate with bounded bursts and
needs two numbers per tenant. Fixed windows allow 2x bursts at the boundary. Sliding logs are exact but store every
request.

**12. How does the semantic cache decide a hit?** Embed the normalised request (model plus messages), compare against
every cached entry with the same model and parameters by cosine similarity, and serve the best one if it is at or above the tenant's
threshold (0.95 by default).

**13. How would you scale the cache past thousands of entries?** Move from the linear scan to a vector index such as
Redis Query Engine HNSW (top-k in Redis, filtered by tenant and model), or a dedicated vector store. The benchmark shows
where the scan stops being cheap.

**14. Why hash tags in the keys?** All of a tenant's keys land in one Redis Cluster slot, so multi-key transactions
(entry plus LRU index in one `MULTI/EXEC`) work on a cluster.

**15. How is LRU implemented?** A sorted set per tenant, member = entry id, score = last access time. Reads bump the
score with `ZADD XX`; when the set exceeds `max_entries`, `ZPOPMIN` takes the least recently used ids and `DEL` removes
their hashes. Entries also have TTLs, and stale ids are pruned on lookup.

**16. Why not floats for money?** Binary floats cannot represent most decimal fractions and errors accumulate. Prices
become integer picodollars per token at startup; the ledger adds whole micro-dollars with `HINCRBY`.

**17. Where do token counts come from when streaming?** The gateway always asks OpenAI-compatible upstreams for
`stream_options.include_usage`, and Anthropic reports usage in `message_start` and `message_delta`. If no usage
arrives (say the client hung up first), it estimates about four characters per token and flags the record as
estimated.

**18. How do you get p99 from Prometheus?** Latencies are histograms;
`histogram_quantile(0.99, sum by (le) (rate(..._bucket[5m])))` interpolates within the bucket that contains the 99th
percentile, so accuracy depends on the bucket boundaries. Averages hide tails, which is why TTFT and upstream latency
are histograms, not gauges.

**19. Why measure TTFT separately from total latency?** For streaming UIs, the user perceives the first token; total
time mostly reflects answer length. A gateway should add almost nothing to TTFT; the benchmark measured 2.06 ms at p50.

**20. What if Redis is down?** Requests are still served, and quickly. Rate limiting fails open, the cache serves a
miss and skips the store, and the ledger write is skipped (Prometheus still counts spend); each is counted in
`llm_gateway_redis_unavailable_total`. Fast matters as much as open: with go-redis defaults a request waited about
5.5 s on dial retries, so the client has 50 ms timeouts, no retries and a circuit breaker that skips Redis entirely
for 5 s after 5 consecutive failures. Measured: 5454.05 ms before, 0.78 ms after at p50, and no failed requests when
Redis was killed and restarted under load. `/readyz` reports not ready so an orchestrator can react.

**21. How does graceful shutdown work with long streams?** On SIGTERM, `http.Server.Shutdown` stops accepting
connections and waits up to 30 s for in-flight requests, including open streams. At the deadline, `Close` drops the
remaining connections, which cancels their contexts and the upstream calls.

**22. How did you load test and what did you find?** A Go load generator with 50 concurrent streaming clients against
mock upstreams with known latency, on one machine. Gateway TTFT overhead 2.06 ms p50 / 6.63 ms p99; cache hits 4.26 ms
p50 vs 651.63 ms for upstream answers; throughput roughly doubled (73.6 to 150.9 req/s) at a 49.81% hit rate; 100%
success with the heavier upstream failing half its requests, because its breaker took it out of rotation; the cache's
linear scan is the first bottleneck at about 1300 req/s.

**23. What would you add for production?** A vector index for the cache, per-model route groups, tool-call
translation, token-based rate limits, shared breaker state or outlier detection, OpenTelemetry tracing per attempt,
tenant management from a database, and fail-closed options for billing-critical limits.

## 7. Five-minute demo

Before the demo: `make build`, and have Redis running on 6379. Use three terminals.

**Minute 0 to 1: start it.**

```sh
make mocks                                   # terminal 1: mock-a :9001, mock-b :9002
./bin/gateway -config config.example.yaml    # terminal 2
```

Say: two OpenAI-compatible upstreams, weighted 3:1; tenants and prices come from the YAML file; provider keys would
come from the environment.

**Minute 1 to 2: streaming and routing.**

```sh
curl -N -D - http://localhost:8080/v1/chat/completions -H "Authorization: Bearer demo-key-acme" \
  -d '{"model":"gpt-4o-mini","stream":true,"messages":[{"role":"user","content":"What is the capital of France?"}]}'
```

Point at tokens arriving one by one, `X-Upstream`, and `X-Cache: MISS`.

**Minute 2 to 3: the semantic cache.**

```sh
curl -s -o /dev/null -D - -w "%{time_total}s\n" http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer demo-key-acme" \
  -d '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"what is the capital of  FRANCE?"}]}'
```

`X-Cache: HIT`, `X-Cache-Similarity`, and a time in milliseconds instead of about 0.6 s. Mention that `globex` has a
0.9 threshold, and that `llm_gateway_cache_best_similarity` is how the threshold gets tuned.

**Minute 3 to 4: rate limiting and failover.**

```sh
for i in $(seq 1 15); do curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer demo-key-globex" -H "Cache-Control: no-cache" \
  -d '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}]}' & done | sort | uniq -c
```

Fifteen requests at once against a burst of 10: ten `200`s and five `429`s. (Sent one after another they would all
pass, because each takes about 0.6 s and the bucket refills 5 tokens per second.) For failover, stop the mocks in terminal 1 and restart mock-a broken:

```sh
./bin/mockprovider -addr :9001 -name mock-a -fail-rate 1 & ./bin/mockprovider -addr :9002 -name mock-b & wait
```

Send a few requests: all succeed with `X-Upstream: mock-b`.

**Minute 4 to 5: observability and load.**

```sh
curl -s localhost:8080/metrics | grep -E "llm_gateway_(failovers|circuit_breaker_state|cache_hits|tenant_spend)"
curl -s localhost:8080/v1/usage -H "Authorization: Bearer demo-key-acme"
./bin/loadtest -concurrency 50 -duration 10s -dup-ratio 0.5    # terminal 3
```

Show failovers, the open breaker on mock-a, spend per tenant and the ledger in micro-dollars, then the load test's
hit and miss percentiles. With Docker, `docker compose up --build` shows the same in Grafana at http://localhost:3300.
