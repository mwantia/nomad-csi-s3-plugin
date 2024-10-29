package otel

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

func SetupOpentelemetry(ctx context.Context, cfg OpenTelemtryConfig) (shutdown func(context.Context) error, err error) {
	var shutdowns []func(context.Context) error
	shutdown = func(ctx context.Context) error {
		var err error
		for _, sd := range shutdowns {
			err = errors.Join(err, sd(ctx))
		}
		shutdowns = nil
		return err
	}

	handle := func(in error) {
		err = errors.Join(in, shutdown(ctx))
	}

	prop := propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
	otel.SetTextMapPropagator(prop)

	httpexporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(cfg.Endpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		handle(err)
		return
	}

	tracerprovider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(httpexporter,
			sdktrace.WithBatchTimeout(cfg.BatchTimeout),
			sdktrace.WithMaxExportBatchSize(cfg.BatchSize),
		),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(cfg.ServiceName),
			semconv.TelemetrySDKNameKey.String("opentelemetry"),
			semconv.TelemetrySDKLanguageKey.String("go"),
			attribute.String("hostname", cfg.Hostname),
		)),
	)

	otel.SetTracerProvider(tracerprovider)

	shutdowns = append(shutdowns, tracerprovider.Shutdown)
	shutdowns = append(shutdowns, httpexporter.Shutdown)

	return
}
