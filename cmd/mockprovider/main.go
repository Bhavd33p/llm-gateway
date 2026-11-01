// Command mockprovider runs a fake OpenAI-compatible LLM API with configurable latency and failure rates,
// for demos, failover drills and load tests that should not spend real provider money.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Bhavd33p/llm-gateway/internal/mockllm"
)

func main() {
	addr := flag.String("addr", ":9001", "listen address")
	name := flag.String("name", "mock", "name echoed in responses")
	latency := flag.Duration("latency", 100*time.Millisecond, "delay before the response starts")
	tokenDelay := flag.Duration("token-delay", 10*time.Millisecond, "delay between streamed tokens")
	tokens := flag.Int("tokens", 50, "completion length in tokens")
	failRate := flag.Float64("fail-rate", 0, "fraction of requests answered with HTTP 500")
	midStreamFailRate := flag.Float64("midstream-fail-rate", 0, "fraction of streams cut off after the first token")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	mock := mockllm.New(mockllm.Config{
		Name:                 *name,
		Latency:              *latency,
		TokenDelay:           *tokenDelay,
		Tokens:               *tokens,
		FailureRate:          *failRate,
		MidStreamFailureRate: *midStreamFailRate,
	})
	srv := &http.Server{Addr: *addr, Handler: mock.Handler(), ReadHeaderTimeout: 10 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		logger.Info("mock provider listening", "addr", *addr, "name", *name, "latency", latency.String(),
			"token_delay", tokenDelay.String(), "tokens", *tokens, "fail_rate", *failRate)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("listen failed", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	logger.Info("mock provider stopped", "requests", mock.Requests(), "failures", mock.Failures(), "cancelled", mock.Cancelled())
}
