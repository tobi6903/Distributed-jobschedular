// Package telemetry sets up structured logging and (optional) OpenTelemetry tracing.
package telemetry

import (
	"context"
	"io"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// NewLogger returns a JSON logger tagged with the service name. Per-job fields (job_id,
// tenant_id, trace_id) are added by the worker and API where the context is known.
func NewLogger(w io.Writer, level slog.Level, service string) *slog.Logger {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})
	return slog.New(h).With("service", service)
}

// SetupTracing installs a global tracer provider that exports over OTLP/HTTP when
// OTEL_EXPORTER_OTLP_ENDPOINT (or the traces-specific variant) is set. Otherwise tracing is
// left as a no-op: spans cost nothing and no exporter goroutines run. The returned func
// flushes buffered spans and must be called on shutdown.
func SetupTracing(ctx context.Context, service string) (func(context.Context) error, error) {
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" && os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") == "" {
		return func(context.Context) error { return nil }, nil
	}
	// The exporter reads endpoint/headers/insecure settings from the standard OTEL_* variables.
	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	res := resource.NewSchemaless(semconv.ServiceName(service))
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}
