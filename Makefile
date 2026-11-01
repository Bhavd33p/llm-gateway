GO ?= go
CONFIG ?= config.example.yaml

.PHONY: build test test-redis vet fmt run mocks loadtest bench compose-up compose-down

build:
	$(GO) build -o bin/ ./cmd/...

# Unit and end-to-end tests; Redis-backed tests use in-process miniredis.
test:
	$(GO) test -race -count=1 ./...

# The same tests against a real Redis, as CI runs them: make test-redis REDIS=localhost:6379
test-redis:
	LLMGW_TEST_REDIS_ADDR=$(REDIS) $(GO) test -race -count=1 ./...

vet:
	$(GO) vet ./...

fmt:
	gofmt -l -w .

# Needs Redis on localhost:6379 and the mocks from `make mocks`.
run: build
	./bin/gateway -config $(CONFIG)

# Two mock upstreams on :9001 and :9002 (Ctrl-C stops both).
mocks: build
	./bin/mockprovider -addr :9001 -name mock-a & ./bin/mockprovider -addr :9002 -name mock-b & wait

# 50 concurrent streaming clients for 30s with half the prompts repeated; see BENCHMARKS.md for the recorded runs.
loadtest: build
	./bin/loadtest -url http://localhost:8080 -concurrency 50 -duration 30s -dup-ratio 0.5

# Go micro-benchmarks for the hot in-process paths.
bench:
	$(GO) test -run '^$$' -bench . -benchmem ./internal/cache/ ./internal/router/

compose-up:
	docker compose up --build -d

compose-down:
	docker compose down -v
