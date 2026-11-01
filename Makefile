GO ?= go

.PHONY: build test vet fmt

build:
	$(GO) build -o bin/ ./cmd/...

test:
	$(GO) test -race -count=1 ./...

vet:
	$(GO) vet ./...

fmt:
	gofmt -l -w .
