# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS build
WORKDIR /src
# Bind-mount the source and cache modules/build output between builds instead of COPYing.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=bind,target=. \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

# Static binaries need no libc, so distroless/static is enough; :nonroot runs as uid 65532.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/gateway"]
