// Command gateway runs the LLM gateway: an OpenAI-compatible proxy with weighted routing, failover, a semantic
// cache, per-tenant rate limiting and cost accounting.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/Bhavd33p/llm-gateway/internal/cache"
	"github.com/Bhavd33p/llm-gateway/internal/config"
	"github.com/Bhavd33p/llm-gateway/internal/cost"
	"github.com/Bhavd33p/llm-gateway/internal/metrics"
	"github.com/Bhavd33p/llm-gateway/internal/provider"
	"github.com/Bhavd33p/llm-gateway/internal/ratelimit"
	"github.com/Bhavd33p/llm-gateway/internal/router"
	"github.com/Bhavd33p/llm-gateway/internal/server"
)

func main() {
	configPath := flag.String("config", "config.example.yaml", "path to the YAML config file")
	logLevel := flag.String("log-level", "info", "debug, info, warn or error")
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	if err := run(*configPath, logger); err != nil {
		logger.Error("gateway stopped", "err", err)
		os.Exit(1)
	}
}

func run(configPath string, logger *slog.Logger) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
		PoolSize: cfg.Redis.PoolSize, // 0 = go-redis default of 10 per CPU
	})
	defer rdb.Close()
	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		// Not fatal: every Redis-backed feature fails open, and /readyz reports the problem.
		logger.Warn("redis is not reachable at startup", "addr", cfg.Redis.Addr, "err", err)
	}

	registry := prometheus.NewRegistry()
	m := metrics.New(registry)

	var upstreams []router.Upstream
	for _, u := range cfg.Upstreams {
		client := provider.NewHTTPClient(u)
		var p provider.Provider
		switch u.Type {
		case config.UpstreamAnthropic:
			p = provider.NewAnthropic(u, client)
		default:
			p = provider.NewOpenAI(u, client)
		}
		upstreams = append(upstreams, router.Upstream{Provider: p, Weight: u.Weight, Timeout: u.Timeout, FirstTokenTimeout: u.FirstTokenTimeout})
	}
	rt := router.New(upstreams, router.Options{
		MaxAttempts:      cfg.Routing.MaxAttempts,
		Backoff:          router.Backoff{Base: cfg.Routing.BackoffBase, Max: cfg.Routing.BackoffMax},
		BreakerThreshold: cfg.Routing.BreakerFailureThreshold,
		BreakerCooldown:  cfg.Routing.BreakerCooldown,
	}, m)

	gateway := server.New(server.Deps{
		Tenants:      cfg.Tenants,
		Router:       rt,
		Cache:        cache.New(rdb, cache.NewEmbedder(cfg.Embedder), m),
		Limiter:      ratelimit.New(rdb),
		Ledger:       cost.NewLedger(rdb, cost.NewPricer(cfg.Pricing), m),
		Metrics:      m,
		Gatherer:     registry,
		Ready:        func(ctx context.Context) error { return rdb.Ping(ctx).Err() },
		Logger:       logger,
		MaxBodyBytes: cfg.Server.MaxBodyBytes,
	})

	srv := &http.Server{
		Addr:              cfg.Server.Addr,
		Handler:           gateway.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		// No WriteTimeout: a long generation can legitimately stream for minutes. Upstream timeouts and client
		// cancellation bound each request instead.
		IdleTimeout: 120 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("gateway listening", "addr", cfg.Server.Addr, "upstreams", len(upstreams), "tenants", len(cfg.Tenants), "embedder", cfg.Embedder.Type)
		serveErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
	}

	// Stop accepting connections and let in-flight requests, including open streams, finish within the deadline.
	logger.Info("shutting down", "deadline", cfg.Server.ShutdownTimeout.String())
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer cancelShutdown()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		// Streams still running at the deadline are cut off; closing their connections cancels their upstreams.
		logger.Warn("shutdown deadline reached, closing remaining connections", "err", err)
		_ = srv.Close()
	}
	if err := <-serveErr; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	logger.Info("shutdown complete")
	return nil
}
