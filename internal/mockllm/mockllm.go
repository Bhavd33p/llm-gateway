// Package mockllm is a fake OpenAI-compatible upstream with tunable latency and failures. It backs the
// mockprovider binary (demos, load tests) and the gateway's end-to-end tests.
package mockllm

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Bhavd33p/llm-gateway/internal/provider"
)

type Config struct {
	Name string
	// Latency is the wait before the response starts, standing in for queueing and prompt processing.
	Latency time.Duration
	// TokenDelay is the wait between streamed tokens.
	TokenDelay time.Duration
	// Tokens is the completion length; each word counts as one token.
	Tokens int
	// FailureRate is the fraction of requests answered with HTTP 500 before any byte is sent.
	FailureRate float64
	// MidStreamFailureRate is the fraction of streams cut off after the first token.
	MidStreamFailureRate float64
}

type Server struct {
	cfg       Config
	requests  atomic.Int64
	failures  atomic.Int64
	cancelled atomic.Int64
}

func New(cfg Config) *Server {
	if cfg.Tokens <= 0 {
		cfg.Tokens = 20
	}
	return &Server{cfg: cfg}
}

// Requests, Failures and Cancelled are counters that tests and the demo use to observe the upstream's view.
func (s *Server) Requests() int64  { return s.requests.Load() }
func (s *Server) Failures() int64  { return s.failures.Load() }
func (s *Server) Cancelled() int64 { return s.cancelled.Load() }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", s.chatCompletions)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}

func (s *Server) chatCompletions(w http.ResponseWriter, r *http.Request) {
	s.requests.Add(1)
	var req provider.ChatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":{"message":"invalid JSON"}}`, http.StatusBadRequest)
		return
	}

	if !sleep(r.Context(), s.cfg.Latency) {
		s.cancelled.Add(1)
		return
	}
	if rand.Float64() < s.cfg.FailureRate {
		s.failures.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"error":{"message":"%s: injected failure","type":"server_error"}}`, s.cfg.Name)
		return
	}

	words := s.answer(&req)
	usage := &provider.Usage{PromptTokens: promptTokens(req.Messages), CompletionTokens: len(words)}
	usage.TotalTokens = usage.PromptTokens + usage.CompletionTokens
	id := fmt.Sprintf("mock-%d", rand.Int64())

	if req.Stream {
		s.stream(w, r, &req, id, words, usage)
		return
	}
	if !sleep(r.Context(), time.Duration(len(words))*s.cfg.TokenDelay) {
		s.cancelled.Add(1)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(provider.ChatResponse{
		ID: id, Object: "chat.completion", Created: time.Now().Unix(), Model: req.Model,
		Choices: []provider.Choice{{Message: provider.Message{Role: "assistant", Content: strings.Join(words, " ")}, FinishReason: "stop"}},
		Usage:   usage,
	})
}

func (s *Server) stream(w http.ResponseWriter, r *http.Request, req *provider.ChatRequest, id string, words []string, usage *provider.Usage) {
	flusher := w.(http.Flusher)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	created := time.Now().Unix()
	send := func(c provider.Chunk) {
		c.ID, c.Object, c.Created, c.Model = id, "chat.completion.chunk", created, req.Model
		b, _ := json.Marshal(c)
		fmt.Fprintf(w, "data: %s\n\n", b)
		flusher.Flush()
	}

	send(provider.Chunk{Choices: []provider.ChunkChoice{{Delta: provider.Delta{Role: "assistant"}}}})
	failMidStream := rand.Float64() < s.cfg.MidStreamFailureRate
	for i, word := range words {
		if i > 0 && !sleep(r.Context(), s.cfg.TokenDelay) {
			s.cancelled.Add(1)
			return
		}
		if i > 0 {
			word = " " + word
		}
		send(provider.Chunk{Choices: []provider.ChunkChoice{{Delta: provider.Delta{Content: word}}}})
		if failMidStream {
			s.failures.Add(1)
			panic(http.ErrAbortHandler) // drop the connection without finishing the response
		}
	}
	stop := "stop"
	send(provider.Chunk{Choices: []provider.ChunkChoice{{FinishReason: &stop}}})
	if req.StreamOptions != nil && req.StreamOptions.IncludeUsage {
		send(provider.Chunk{Choices: []provider.ChunkChoice{}, Usage: usage})
	}
	fmt.Fprint(w, "data: [DONE]\n\n")
	flusher.Flush()
}

// answer builds a deterministic reply of cfg.Tokens words that echoes the start of the last user message.
func (s *Server) answer(req *provider.ChatRequest) []string {
	var last string
	for _, m := range req.Messages {
		if m.Role == "user" {
			last = m.Content
		}
	}
	words := append([]string{s.cfg.Name, "answers:"}, strings.Fields(last)...)
	filler := strings.Fields("the quick brown fox jumps over the lazy dog while the gateway streams every token")
	for i := 0; len(words) < s.cfg.Tokens; i++ {
		words = append(words, filler[i%len(filler)])
	}
	return words[:s.cfg.Tokens]
}

func promptTokens(messages []provider.Message) int {
	n := 0
	for _, m := range messages {
		n += len(strings.Fields(m.Content)) + 4 // per-message overhead, roughly like OpenAI's accounting
	}
	return n
}

// sleep waits for d or until ctx is cancelled, and reports whether the full duration elapsed.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
