// Package server is the gateway's HTTP layer: authentication, rate limiting, the semantic cache, routing,
// streaming passthrough and cost accounting for POST /v1/chat/completions.
package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Bhavd33p/llm-gateway/internal/cache"
	"github.com/Bhavd33p/llm-gateway/internal/config"
	"github.com/Bhavd33p/llm-gateway/internal/cost"
	"github.com/Bhavd33p/llm-gateway/internal/metrics"
	"github.com/Bhavd33p/llm-gateway/internal/ratelimit"
	"github.com/Bhavd33p/llm-gateway/internal/router"
)

type Deps struct {
	Tenants      []config.Tenant
	Router       *router.Router
	Cache        *cache.Cache
	Limiter      *ratelimit.Limiter
	Ledger       *cost.Ledger
	Metrics      *metrics.Metrics
	Gatherer     prometheus.Gatherer
	Ready        func(context.Context) error // readiness check, e.g. a Redis PING
	Logger       *slog.Logger
	MaxBodyBytes int64
}

type Server struct {
	Deps
	tenantsByKeyHash map[string]*config.Tenant
}

func New(d Deps) *Server {
	s := &Server{Deps: d, tenantsByKeyHash: map[string]*config.Tenant{}}
	for i := range d.Tenants {
		t := &d.Tenants[i]
		for _, h := range t.APIKeySHA256 {
			s.tenantsByKeyHash[h] = t
		}
	}
	return s
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", s.chatCompletions)
	mux.HandleFunc("GET /v1/usage", s.usage)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", s.ready)
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.Gatherer, promhttp.HandlerOpts{}))
	return mux
}

// authenticate maps the API key (OpenAI-style "Authorization: Bearer <key>", or X-API-Key) to a tenant. Only the
// key's SHA-256 is compared, so config never holds plaintext keys.
func (s *Server) authenticate(r *http.Request) *config.Tenant {
	key := r.Header.Get("X-API-Key")
	if auth := r.Header.Get("Authorization"); key == "" && strings.HasPrefix(auth, "Bearer ") {
		key = strings.TrimPrefix(auth, "Bearer ")
	}
	if key == "" {
		return nil
	}
	sum := sha256.Sum256([]byte(key))
	return s.tenantsByKeyHash[hex.EncodeToString(sum[:])]
}

// usage reports the calling tenant's ledger for the current month.
func (s *Server) usage(w http.ResponseWriter, r *http.Request) {
	tenant := s.authenticate(r)
	if tenant == nil {
		writeError(w, http.StatusUnauthorized, "invalid_api_key", "missing or invalid API key")
		return
	}
	fields, err := s.Ledger.Month(r.Context(), tenant.Name)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "ledger_unavailable", "could not read the ledger")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"tenant":    tenant.Name,
		"month":     time.Now().UTC().Format("2006-01"),
		"spend_usd": fmt.Sprintf("%.6f", float64(fields["spend_micro_usd"])/1e6),
		"ledger":    fields,
	})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.Ready(ctx); err != nil {
		http.Error(w, "not ready: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

type apiError struct {
	Error struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error"`
}

// writeError writes an OpenAI-shaped error so existing client SDKs surface it properly.
func writeError(w http.ResponseWriter, status int, errType, message string) {
	var body apiError
	body.Error.Message, body.Error.Type = message, errType
	writeJSON(w, status, body)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// statusWriter remembers the status code for metrics and keeps http.Flusher available for streaming.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
