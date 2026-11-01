package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Bhavd33p/llm-gateway/internal/cache"
	"github.com/Bhavd33p/llm-gateway/internal/config"
	"github.com/Bhavd33p/llm-gateway/internal/cost"
	"github.com/Bhavd33p/llm-gateway/internal/provider"
	"github.com/Bhavd33p/llm-gateway/internal/redisguard"
	"github.com/Bhavd33p/llm-gateway/internal/router"
)

// Accounting and cache writes happen after the client has its answer and must not be cut short by a client that
// hangs up right after [DONE], so they run on a detached context with this budget.
const afterResponseTimeout = 2 * time.Second

// chatRequest carries one request's state through the pipeline.
type chatRequest struct {
	start  time.Time
	tenant *config.Tenant
	body   provider.ChatRequest
	// cacheState is "hit", "miss" or "bypass"; vec is the request embedding and params its parameter fingerprint,
	// both reused to store the answer on a miss.
	cacheState string
	vec        []float32
	params     string
}

// chatCompletions is the request pipeline:
// auth -> rate limit -> decode -> cache lookup -> route (with failover) -> stream or respond -> cost -> cache store.
func (s *Server) chatCompletions(rw http.ResponseWriter, r *http.Request) {
	w := &statusWriter{ResponseWriter: rw}
	req := &chatRequest{start: time.Now(), cacheState: "bypass"}
	s.Metrics.InFlight.Inc()
	defer func() {
		s.Metrics.InFlight.Dec()
		tenant := "unknown"
		if req.tenant != nil {
			tenant = req.tenant.Name
		}
		status := w.status
		if status == 0 {
			status = 499 // client went away before we wrote anything (nginx's convention)
		}
		s.Metrics.Requests.WithLabelValues(tenant, strconv.Itoa(status)).Inc()
		s.Metrics.RequestDuration.WithLabelValues(req.cacheState).Observe(time.Since(req.start).Seconds())
	}()

	if req.tenant = s.authenticate(r); req.tenant == nil {
		writeError(w, http.StatusUnauthorized, "invalid_api_key", "missing or invalid API key")
		return
	}
	if !s.allow(w, r, req.tenant) {
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, s.MaxBodyBytes)
	if err := json.NewDecoder(r.Body).Decode(&req.body); err != nil {
		if _, tooLarge := errors.AsType[*http.MaxBytesError](err); tooLarge {
			writeError(w, http.StatusRequestEntityTooLarge, "invalid_request_error", "request body is too large")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid_request_error", "invalid JSON body: "+err.Error())
		return
	}
	if req.body.Model == "" || len(req.body.Messages) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "model and messages are required")
		return
	}

	if hit := s.lookup(r, req); hit != nil {
		s.serveHit(w, req, hit)
		return
	}
	if req.body.Stream {
		s.stream(w, r, req)
	} else {
		s.complete(w, r, req)
	}
}

// allow applies the tenant's token bucket. If Redis is unreachable the request is let through (fail open): a
// rate limiter outage should not become a gateway outage, at the price of not enforcing limits until Redis is back.
func (s *Server) allow(w http.ResponseWriter, r *http.Request, tenant *config.Tenant) bool {
	res, err := s.Limiter.Allow(r.Context(), tenant.Name, tenant.RateLimit)
	if err != nil {
		s.redisFailed("ratelimit", err, "tenant", tenant.Name)
		return true
	}
	if res.Allowed {
		return true
	}
	s.Metrics.RateLimited.WithLabelValues(tenant.Name).Inc()
	w.Header().Set("Retry-After", strconv.Itoa(max(1, int(math.Ceil(res.RetryAfter.Seconds())))))
	writeError(w, http.StatusTooManyRequests, "rate_limit_exceeded", "rate limit exceeded for tenant "+tenant.Name)
	return false
}

// lookup embeds the request and searches the tenant's cache. "Cache-Control: no-cache" skips the cache entirely.
// Any embedder or Redis failure degrades to a miss; if Redis is unreachable the answer is not stored either.
func (s *Server) lookup(r *http.Request, req *chatRequest) *cache.Hit {
	if !req.tenant.Cache.IsEnabled() || strings.Contains(r.Header.Get("Cache-Control"), "no-cache") {
		return nil
	}
	vec, err := s.Cache.Embed(r.Context(), req.body.Model, req.body.Messages)
	if err != nil {
		s.Metrics.DependencyErrors.WithLabelValues("embedder").Inc()
		s.Logger.Warn("embedding failed, skipping cache", "tenant", req.tenant.Name, "err", err)
		return nil
	}
	req.vec, req.params, req.cacheState = vec, cache.ParamsKey(&req.body), "miss"
	hit, err := s.Cache.Lookup(r.Context(), req.tenant.Name, req.body.Model, req.params, vec, req.tenant.Cache)
	if err != nil {
		s.redisFailed("cache_lookup", err, "tenant", req.tenant.Name)
		if redisguard.IsUnavailable(err) {
			req.vec = nil // don't spend another Redis call trying to store the answer
		}
		return nil
	}
	if hit != nil {
		req.cacheState = "hit"
	}
	return hit
}

func (s *Server) serveHit(w http.ResponseWriter, req *chatRequest, hit *cache.Hit) {
	w.Header().Set("X-Cache", "HIT")
	w.Header().Set("X-Cache-Similarity", strconv.FormatFloat(hit.Similarity, 'f', 4, 64))
	id, model, created := "chatcmpl-cache-"+hit.ID, req.body.Model, time.Now().Unix()
	usage := hit.Entry.Usage

	if req.body.Stream {
		sse := newSSEWriter(w)
		chunk := func(delta provider.Delta, finish *string) *provider.Chunk {
			return &provider.Chunk{ID: id, Object: "chat.completion.chunk", Created: created, Model: model,
				Choices: []provider.ChunkChoice{{Delta: delta, FinishReason: finish}}}
		}
		// The whole cached answer goes out as one content chunk: there is nothing to wait for.
		if sse.chunk(chunk(provider.Delta{Role: "assistant", Content: hit.Entry.Content}, nil)) == nil {
			s.Metrics.TTFT.WithLabelValues("cache").Observe(time.Since(req.start).Seconds())
		}
		finish := hit.Entry.FinishReason
		sse.chunk(chunk(provider.Delta{}, &finish))
		if wantsUsage(&req.body) {
			sse.chunk(&provider.Chunk{ID: id, Object: "chat.completion.chunk", Created: created, Model: model, Choices: []provider.ChunkChoice{}, Usage: &usage})
		}
		sse.done()
	} else {
		writeJSON(w, http.StatusOK, provider.ChatResponse{
			ID: id, Object: "chat.completion", Created: created, Model: model,
			Choices: []provider.Choice{{Message: provider.Message{Role: "assistant", Content: hit.Entry.Content}, FinishReason: hit.Entry.FinishReason}},
			Usage:   &usage,
		})
	}

	// Savings are priced like the original call: at the model the upstream actually served, which can differ from
	// the requested name under model_map.
	pricedModel := hit.Entry.ServedModel
	if pricedModel == "" {
		pricedModel = model
	}
	ctx, cancel := context.WithTimeout(context.Background(), afterResponseTimeout)
	defer cancel()
	s.record(ctx, cost.Record{Tenant: req.tenant.Name, Model: pricedModel, Usage: usage, CacheHit: true})
	s.Logger.Info("request", "tenant", req.tenant.Name, "model", model, "stream", req.body.Stream, "cache", "hit",
		"similarity", hit.Similarity, "duration_ms", time.Since(req.start).Milliseconds())
}

// stream proxies an SSE stream from the chosen upstream, flushing every chunk as it arrives. The router has
// already retried and failed over as needed to obtain the first token; from here on, a failure ends the stream.
func (s *Server) stream(w http.ResponseWriter, r *http.Request, req *chatRequest) {
	ctx := r.Context() // cancelled when the client disconnects, which aborts the upstream request too
	upstream, served, err := s.Router.Stream(ctx, &req.body)
	if err != nil {
		s.writeRouteError(w, req, err)
		return
	}
	defer upstream.Close()

	w.Header().Set("X-Cache", strings.ToUpper(req.cacheState))
	w.Header().Set("X-Upstream", served.Upstream)
	sse := newSSEWriter(w)
	w.WriteHeader(http.StatusOK)

	var (
		content   strings.Builder
		usage     *provider.Usage
		finish    string
		streamErr error
		ttft      time.Duration
	)
	for {
		c, err := upstream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			streamErr = err
			if ctx.Err() == nil {
				sse.streamError("upstream stream interrupted")
			}
			break
		}
		if c.Usage != nil {
			usage = c.Usage
		}
		content.WriteString(c.Content())
		if fr := c.FinishReason(); fr != "" {
			finish = fr
		}
		if len(c.Choices) == 0 && c.Usage != nil && !wantsUsage(&req.body) {
			continue // we asked the upstream for usage on our own behalf; this client did not
		}
		if err := sse.chunk(c); err != nil {
			streamErr = err // client connection is gone
			break
		}
		if ttft == 0 && c.Content() != "" {
			ttft = time.Since(req.start)
			s.Metrics.TTFT.WithLabelValues(served.Upstream).Observe(ttft.Seconds())
		}
	}
	// Account and cache before [DONE], so a client that repeats the question right away already gets a hit.
	s.finish(req, served, usage, content.String(), finish, streamErr == nil)
	if streamErr == nil {
		sse.done()
	}
	s.Logger.Info("request", "tenant", req.tenant.Name, "model", req.body.Model, "stream", true, "cache", req.cacheState,
		"upstream", served.Upstream, "attempts", served.Attempts, "ttft_ms", ttft.Milliseconds(),
		"duration_ms", time.Since(req.start).Milliseconds(), "stream_error", errString(streamErr))
}

func (s *Server) complete(w http.ResponseWriter, r *http.Request, req *chatRequest) {
	resp, served, err := s.Router.Complete(r.Context(), &req.body)
	if err != nil {
		s.writeRouteError(w, req, err)
		return
	}
	w.Header().Set("X-Cache", strings.ToUpper(req.cacheState))
	w.Header().Set("X-Upstream", served.Upstream)
	writeJSON(w, http.StatusOK, resp)

	var content, finish string
	if len(resp.Choices) > 0 {
		content, finish = resp.Choices[0].Message.Content, resp.Choices[0].FinishReason
	}
	s.finish(req, served, resp.Usage, content, finish, true)
	s.Logger.Info("request", "tenant", req.tenant.Name, "model", req.body.Model, "stream", false, "cache", req.cacheState,
		"upstream", served.Upstream, "attempts", served.Attempts, "duration_ms", time.Since(req.start).Milliseconds())
}

// finish records cost and, for complete answers, stores them in the cache. Truncated ("length") or broken answers
// are not cached, because serving half an answer to the next caller would be worse than a miss.
func (s *Server) finish(req *chatRequest, served router.Served, usage *provider.Usage, content, finish string, complete bool) {
	ctx, cancel := context.WithTimeout(context.Background(), afterResponseTimeout)
	defer cancel()

	estimated := usage == nil
	if estimated {
		u := cost.EstimateUsage(req.body.Messages, content)
		usage = &u
	}
	s.record(ctx, cost.Record{Tenant: req.tenant.Name, Model: served.Model, Upstream: served.Upstream, Usage: *usage, Estimated: estimated})

	if req.vec == nil || !complete || finish != "stop" {
		return
	}
	entry := cache.Entry{Content: content, FinishReason: finish, Usage: *usage, ServedModel: served.Model}
	if err := s.Cache.Store(ctx, req.tenant.Name, req.body.Model, req.params, req.vec, entry, req.tenant.Cache); err != nil {
		s.redisFailed("cache_store", err, "tenant", req.tenant.Name)
	}
}

// record writes the request to the cost ledger. If Redis is unreachable the ledger entry is skipped and counted in
// llm_gateway_redis_unavailable_total{component="ledger"}; the Prometheus spend and token counters still increase.
func (s *Server) record(ctx context.Context, rec cost.Record) {
	if _, err := s.Ledger.Record(ctx, rec); err != nil {
		s.redisFailed("ledger", err, "tenant", rec.Tenant, "model", rec.Model)
	}
}

// redisFailed counts a Redis-backed feature that had to go without Redis. An outage (unreachable, or breaker open)
// repeats on every request, so it is only counted; the Redis client logs each breaker state change once. Any other
// error is unexpected and is also logged.
func (s *Server) redisFailed(component string, err error, attrs ...any) {
	if redisguard.IsUnavailable(err) {
		s.Metrics.RedisUnavailable.WithLabelValues(component).Inc()
		return
	}
	s.Metrics.DependencyErrors.WithLabelValues(component).Inc()
	s.Logger.Warn(component+" failed", append(attrs, "err", err)...)
}

// writeRouteError maps routing failures to client-facing statuses. Only errors about the request itself are passed
// through; an upstream 401 or 403 means the gateway's own provider key is wrong, which is a 502 for the client, not
// a hint that the client's key is bad.
func (s *Server) writeRouteError(w http.ResponseWriter, req *chatRequest, err error) {
	statusErr, isStatus := errors.AsType[*provider.StatusError](err)
	switch {
	case errors.Is(err, context.Canceled):
		return // the client is gone; nobody is listening
	case errors.Is(err, router.ErrNoHealthyUpstream):
		writeError(w, http.StatusServiceUnavailable, "upstream_unavailable", err.Error())
	case isStatus && passThroughStatus[statusErr.StatusCode]:
		writeError(w, statusErr.StatusCode, "invalid_request_error", statusErr.Body)
	default:
		writeError(w, http.StatusBadGateway, "upstream_error", "all upstream attempts failed")
	}
	s.Logger.Warn("routing failed", "tenant", req.tenant.Name, "model", req.body.Model, "err", err)
}

// passThroughStatus lists upstream statuses that describe the client's request rather than the upstream's health.
var passThroughStatus = map[int]bool{
	http.StatusBadRequest:            true,
	http.StatusNotFound:              true,
	http.StatusRequestEntityTooLarge: true,
	http.StatusUnprocessableEntity:   true,
}

func wantsUsage(req *provider.ChatRequest) bool {
	return req.StreamOptions != nil && req.StreamOptions.IncludeUsage
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprint(err)
}
