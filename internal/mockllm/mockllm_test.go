package mockllm

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Bhavd33p/llm-gateway/internal/config"
	"github.com/Bhavd33p/llm-gateway/internal/provider"
)

func newProvider(t *testing.T, cfg Config) (*Server, provider.Provider) {
	t.Helper()
	mock := New(cfg)
	srv := httptest.NewServer(mock.Handler())
	t.Cleanup(srv.Close)
	return mock, provider.NewOpenAI(config.Upstream{Name: cfg.Name, BaseURL: srv.URL + "/v1"}, srv.Client())
}

func TestMockSpeaksOpenAIStreaming(t *testing.T) {
	_, p := newProvider(t, Config{Name: "mock-a", Tokens: 6})
	stream, err := p.Stream(context.Background(), &provider.ChatRequest{Model: "m", Messages: []provider.Message{{Role: "user", Content: "hello world"}}})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer stream.Close()

	var text strings.Builder
	var usage *provider.Usage
	for {
		c, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		text.WriteString(c.Content())
		if c.Usage != nil {
			usage = c.Usage
		}
	}
	if got := text.String(); got != "mock-a answers: hello world the quick" {
		t.Errorf("text = %q", got)
	}
	if usage == nil || usage.CompletionTokens != 6 {
		t.Errorf("usage = %+v, want 6 completion tokens", usage)
	}
}

func TestMockFailureRate(t *testing.T) {
	mock, p := newProvider(t, Config{Name: "broken", FailureRate: 1})
	_, err := p.Complete(context.Background(), &provider.ChatRequest{Model: "m"})
	var se *provider.StatusError
	if !errors.As(err, &se) || se.StatusCode != 500 || mock.Failures() != 1 {
		t.Fatalf("err = %v failures = %d, want one injected 500", err, mock.Failures())
	}
}

func TestMockMidStreamFailure(t *testing.T) {
	_, p := newProvider(t, Config{Name: "flaky", Tokens: 5, MidStreamFailureRate: 1})
	stream, err := p.Stream(context.Background(), &provider.ChatRequest{Model: "m"})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer stream.Close()
	var lastErr error
	for i := 0; i < 10 && lastErr == nil; i++ {
		_, lastErr = stream.Recv()
	}
	if lastErr == nil || lastErr == io.EOF {
		t.Fatalf("stream should break mid-way, got %v", lastErr)
	}
}
