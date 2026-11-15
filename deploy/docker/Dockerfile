# syntax=docker/dockerfile:1.7
#
# One Dockerfile builds all four binaries:
#   docker build -f deploy/docker/Dockerfile --build-arg APP=worker -t jobscheduler/worker .
# APP is one of: api, worker, scheduler, jobctl.
#
# Stage 1 compiles a static binary. Stage 2 is distroless/static: no shell, no package
# manager, no libc, so there is almost nothing to patch or exploit, and it runs as a
# non-root user (uid 65532) by default.

# Build with the newest supported Go toolchain. The binary embeds the compiler's standard library,
# so an older toolchain ships its unpatched net/http, crypto/tls, etc. (Trivy flags exactly that:
# building with Go 1.23 produced dozens of fixable HIGH/CRITICAL findings). go.mod's `go` line is
# only the minimum version the module needs.
ARG GO_VERSION=1.27

FROM golang:${GO_VERSION}-bookworm AS build
ARG APP
WORKDIR /src
# Copy the module files first so dependency downloads are cached until go.mod changes.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
# CGO off => fully static binary (required for distroless/static). -trimpath removes local
# paths and -s -w strips debug info: smaller image, reproducible builds.
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    test -n "${APP}" && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${APP}

FROM gcr.io/distroless/static-debian12:nonroot
ARG APP
LABEL org.opencontainers.image.title="jobscheduler-${APP}" \
      org.opencontainers.image.source="https://github.com/tobi6903/Distributed-jobschedular"
COPY --from=build /out/app /app
# Numeric uid so Kubernetes can verify runAsNonRoot without resolving a name.
USER 65532:65532
# Exec form so the process is PID 1 and receives SIGTERM directly (graceful shutdown).
ENTRYPOINT ["/app"]
