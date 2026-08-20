// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// loadgen pushes OTLP trace spans to a collector's gRPC endpoint.
// Each span is ~2KB to simulate realistic queue item sizes.
//
// Usage:
//
//	loadgen --endpoint=localhost:4317 --spans=10000000 --workers=10
package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	endpoint := flag.String("endpoint", "localhost:4317", "OTLP gRPC endpoint")
	totalSpans := flag.Int64("spans", 1000000, "total spans to send")
	workers := flag.Int("workers", 10, "concurrent workers")
	batchSize := flag.Int("batch", 100, "spans per batch")
	queueSize := flag.Int("queue-size", 50000, "SDK in-memory queue size (spans)")
	flag.Parse()

	fmt.Printf("Load generator: endpoint=%s spans=%d workers=%d batch=%d queue_size=%d\n",
		*endpoint, *totalSpans, *workers, *batchSize, *queueSize)

	ctx := context.Background()

	conn, err := grpc.NewClient(*endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial: %v\n", err)
		os.Exit(1)
	}

	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		fmt.Fprintf(os.Stderr, "exporter: %v\n", err)
		os.Exit(1)
	}

	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String("loadgen"),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter,
			sdktrace.WithMaxExportBatchSize(*batchSize),
			sdktrace.WithBatchTimeout(50*time.Millisecond),
			sdktrace.WithMaxQueueSize(*queueSize),
		),
		sdktrace.WithResource(res),
	)
	defer tp.Shutdown(ctx)

	tracer := tp.Tracer("loadgen")

	// ~2KB payload per span via attributes.
	payload := make([]byte, 1536)
	_, _ = rand.Read(payload)
	payloadStr := fmt.Sprintf("%x", payload[:768]) // 1536 hex chars ≈ 1.5KB + span overhead ≈ 2KB

	var sent atomic.Int64
	start := time.Now()

	// Progress reporter.
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			n := sent.Load()
			elapsed := time.Since(start)
			rate := float64(n) / elapsed.Seconds()
			pct := float64(n) / float64(*totalSpans) * 100
			fmt.Printf("  [%.1f%%] sent=%d rate=%.0f spans/s elapsed=%v\n", pct, n, rate, elapsed.Round(time.Second))
			if n >= *totalSpans {
				return
			}
		}
	}()

	// Worker pool.
	var wg sync.WaitGroup
	spansPerWorker := *totalSpans / int64(*workers)

	for w := range *workers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			count := spansPerWorker
			if workerID == *workers-1 {
				count = *totalSpans - spansPerWorker*int64(*workers-1)
			}
			for i := range count {
				_, span := tracer.Start(ctx, "loadgen-op",
					trace.WithAttributes(
						semconv.HTTPRequestMethodKey.String("POST"),
						semconv.URLPathKey.String("/api/v1/data"),
					),
				)
				span.SetAttributes(
					semconv.HTTPResponseStatusCodeKey.Int(200),
				)
				span.AddEvent("payload", trace.WithAttributes(
					semconv.ExceptionMessageKey.String(payloadStr),
				))
				span.End()
				sent.Add(1)

				// Throttle slightly to avoid overwhelming the batcher.
				if i%1000 == 0 {
					time.Sleep(time.Millisecond)
				}
			}
		}(w)
	}

	wg.Wait()

	// Flush remaining.
	fmt.Println("Flushing...")
	if err := tp.ForceFlush(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "flush error: %v\n", err)
	}

	elapsed := time.Since(start)
	fmt.Printf("Done: %d spans in %v (%.0f spans/s)\n", sent.Load(), elapsed, float64(sent.Load())/elapsed.Seconds())
}
