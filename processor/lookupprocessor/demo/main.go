// Demo program to test DNS lookup processor functionality
package main

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/lookupprocessor/internal/source/dns"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/lookupprocessor/lookupsource"
)

func main() {
	logger, _ := zap.NewDevelopment()
	
	fmt.Println("=== DNS Lookup Processor Demo ===\n")
	
	// Test 1: Forward lookup without cache
	fmt.Println("Test 1: Forward DNS Lookup (hostname -> IP) without cache")
	testForwardLookup(logger, false)
	
	fmt.Println("\n==================================================\n")
	
	// Test 2: Forward lookup with cache
	fmt.Println("Test 2: Forward DNS Lookup with caching enabled")
	testForwardLookupWithCache(logger)
	
	fmt.Println("\n==================================================\n")
	
	// Test 3: Reverse lookup
	fmt.Println("Test 3: Reverse DNS Lookup (IP -> hostname)")
	testReverseLookup(logger)
}

func testForwardLookup(logger *zap.Logger, enableCache bool) {
	factory := dns.NewFactory()
	cfg := &dns.Config{
		Mode:    dns.ModeForward,
		Timeout: 5 * time.Second,
	}
	
	settings := lookupsource.CreateSettings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
		Cache: lookupsource.CacheConfig{
			Enabled: enableCache,
			Size:    100,
			TTL:     5 * time.Minute,
		},
	}
	
	source, err := factory.CreateSource(context.Background(), settings, cfg)
	if err != nil {
		fmt.Printf("Error creating source: %v\n", err)
		return
	}
	
	// Test some hostnames
	testHosts := []string{"localhost", "google.com", "github.com"}
	
	for _, hostname := range testHosts {
		result, found, err := source.Lookup(context.Background(), hostname)
		if err != nil {
			fmt.Printf("  ❌ %s: Error - %v\n", hostname, err)
		} else if found {
			fmt.Printf("  ✓ %s -> %v\n", hostname, result)
		} else {
			fmt.Printf("  ⚠ %s: Not found\n", hostname)
		}
	}
}

func testForwardLookupWithCache(logger *zap.Logger) {
	factory := dns.NewFactory()
	cfg := &dns.Config{
		Mode:    dns.ModeForward,
		Timeout: 5 * time.Second,
	}
	
	settings := lookupsource.CreateSettings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
		Cache: lookupsource.CacheConfig{
			Enabled: true,
			Size:    100,
			TTL:     5 * time.Minute,
		},
	}
	
	source, err := factory.CreateSource(context.Background(), settings, cfg)
	if err != nil {
		fmt.Printf("Error creating source: %v\n", err)
		return
	}
	
	hostname := "localhost"
	
	// First lookup (cache miss)
	fmt.Printf("First lookup (should miss cache):\n")
	start := time.Now()
	result1, found1, err := source.Lookup(context.Background(), hostname)
	duration1 := time.Since(start)
	if err != nil {
		fmt.Printf("  ❌ Error: %v\n", err)
		return
	}
	if found1 {
		fmt.Printf("  ✓ %s -> %v (took %v)\n", hostname, result1, duration1)
	}
	
	// Second lookup (cache hit)
	fmt.Printf("\nSecond lookup (should hit cache):\n")
	start = time.Now()
	result2, found2, err := source.Lookup(context.Background(), hostname)
	duration2 := time.Since(start)
	if err != nil {
		fmt.Printf("  ❌ Error: %v\n", err)
		return
	}
	if found2 {
		fmt.Printf("  ✓ %s -> %v (took %v)\n", hostname, result2, duration2)
	}
	
	fmt.Printf("\nPerformance improvement: %.2fx faster\n", float64(duration1)/float64(duration2))
}

func testReverseLookup(logger *zap.Logger) {
	factory := dns.NewFactory()
	cfg := &dns.Config{
		Mode:    dns.ModeReverse,
		Timeout: 5 * time.Second,
	}
	
	settings := lookupsource.CreateSettings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
		Cache: lookupsource.CacheConfig{
			Enabled: true,
			Size:    100,
			TTL:     10 * time.Minute,
		},
	}
	
	source, err := factory.CreateSource(context.Background(), settings, cfg)
	if err != nil {
		fmt.Printf("Error creating source: %v\n", err)
		return
	}
	
	// Test some IPs
	testIPs := []string{"127.0.0.1", "8.8.8.8", "1.1.1.1"}
	
	for _, ip := range testIPs {
		result, found, err := source.Lookup(context.Background(), ip)
		if err != nil {
			fmt.Printf("  ❌ %s: Error - %v\n", ip, err)
		} else if found {
			fmt.Printf("  ✓ %s -> %v\n", ip, result)
		} else {
			fmt.Printf("  ⚠ %s: Not found\n", ip)
		}
	}
}
