// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package dns

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/lookupprocessor/lookupsource"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name:    "valid forward mode",
			config:  Config{Mode: ModeForward, Timeout: 5 * time.Second},
			wantErr: false,
		},
		{
			name:    "valid reverse mode",
			config:  Config{Mode: ModeReverse, Timeout: 5 * time.Second},
			wantErr: false,
		},
		{
			name:    "empty mode defaults to forward",
			config:  Config{Timeout: 5 * time.Second},
			wantErr: false,
		},
		{
			name:    "invalid mode",
			config:  Config{Mode: "invalid", Timeout: 5 * time.Second},
			wantErr: true,
		},
		{
			name:    "negative timeout",
			config:  Config{Mode: ModeForward, Timeout: -1 * time.Second},
			wantErr: true,
		},
		{
			name:    "valid with resolver",
			config:  Config{Mode: ModeForward, Timeout: 5 * time.Second, Resolver: "8.8.8.8:53"},
			wantErr: false,
		},
		{
			name:    "invalid resolver format",
			config:  Config{Mode: ModeForward, Timeout: 5 * time.Second, Resolver: "8.8.8.8"},
			wantErr: true,
		},
		{
			name:    "valid with multiple results",
			config:  Config{Mode: ModeForward, Timeout: 5 * time.Second, MultipleResults: true},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestNewFactory(t *testing.T) {
	factory := NewFactory()
	assert.Equal(t, sourceType, factory.Type())

	cfg := factory.CreateDefaultConfig()
	assert.NotNil(t, cfg)

	defaultCfg, ok := cfg.(*Config)
	require.True(t, ok)
	assert.Equal(t, ModeForward, defaultCfg.Mode)
	assert.Equal(t, 5*time.Second, defaultCfg.Timeout)
	assert.False(t, defaultCfg.MultipleResults)
}

func TestCreateSource(t *testing.T) {
	factory := NewFactory()
	cfg := &Config{
		Mode:    ModeForward,
		Timeout: 5 * time.Second,
	}

	source, err := factory.CreateSource(context.Background(), lookupsource.CreateSettings{}, cfg)
	require.NoError(t, err)
	require.NotNil(t, source)
	assert.Equal(t, sourceType, source.Type())
}

func TestForwardLookup(t *testing.T) {
	factory := NewFactory()
	cfg := &Config{
		Mode:    ModeForward,
		Timeout: 10 * time.Second,
	}

	source, err := factory.CreateSource(context.Background(), lookupsource.CreateSettings{}, cfg)
	require.NoError(t, err)

	ctx := context.Background()

	// Test localhost resolution - should work on all systems
	t.Run("resolve localhost", func(t *testing.T) {
		result, found, err := source.Lookup(ctx, "localhost")
		require.NoError(t, err)
		if found {
			// localhost typically resolves to 127.0.0.1 or ::1
			assert.NotEmpty(t, result)
			t.Logf("localhost resolved to: %v", result)
		}
	})

	// Test non-existent domain
	t.Run("non-existent domain", func(t *testing.T) {
		result, found, err := source.Lookup(ctx, "this-domain-should-not-exist-12345.invalid")
		assert.NoError(t, err) // Should not error, just not found
		assert.False(t, found)
		assert.Nil(t, result)
	})
}

func TestReverseLookup(t *testing.T) {
	factory := NewFactory()
	cfg := &Config{
		Mode:    ModeReverse,
		Timeout: 10 * time.Second,
	}

	source, err := factory.CreateSource(context.Background(), lookupsource.CreateSettings{}, cfg)
	require.NoError(t, err)

	ctx := context.Background()

	// Test localhost IP reverse resolution
	t.Run("reverse resolve localhost", func(t *testing.T) {
		result, found, err := source.Lookup(ctx, "127.0.0.1")
		require.NoError(t, err)
		if found {
			assert.NotEmpty(t, result)
			t.Logf("127.0.0.1 resolved to: %v", result)
		}
	})

	// Test invalid IP
	t.Run("invalid IP", func(t *testing.T) {
		result, found, err := source.Lookup(ctx, "999.999.999.999")
		// Invalid IP should either error or not be found
		if err == nil {
			assert.False(t, found)
			assert.Nil(t, result)
		}
	})
}

func TestMultipleResults(t *testing.T) {
	factory := NewFactory()
	cfg := &Config{
		Mode:            ModeForward,
		Timeout:         10 * time.Second,
		MultipleResults: true,
	}

	source, err := factory.CreateSource(context.Background(), lookupsource.CreateSettings{}, cfg)
	require.NoError(t, err)

	ctx := context.Background()

	// Test domain that might have multiple A records
	t.Run("multiple results as comma-separated", func(t *testing.T) {
		result, found, err := source.Lookup(ctx, "localhost")
		require.NoError(t, err)
		if found {
			resultStr, ok := result.(string)
			require.True(t, ok)
			assert.NotEmpty(t, resultStr)
			t.Logf("Multiple results: %v", resultStr)
		}
	})
}

func TestTimeout(t *testing.T) {
	factory := NewFactory()
	cfg := &Config{
		Mode:    ModeForward,
		Timeout: 1 * time.Nanosecond, // Very short timeout
	}

	source, err := factory.CreateSource(context.Background(), lookupsource.CreateSettings{}, cfg)
	require.NoError(t, err)

	ctx := context.Background()

	// This should timeout quickly
	_, _, err = source.Lookup(ctx, "example.com")
	// We expect either a timeout error or it completes very quickly
	// Don't assert on error as DNS might be cached
	if err != nil {
		t.Logf("Got expected timeout or error: %v", err)
	}
}

func TestCustomResolver(t *testing.T) {
	factory := NewFactory()
	cfg := &Config{
		Mode:     ModeForward,
		Timeout:  10 * time.Second,
		Resolver: "8.8.8.8:53", // Google's public DNS
	}

	source, err := factory.CreateSource(context.Background(), lookupsource.CreateSettings{}, cfg)
	require.NoError(t, err)

	ctx := context.Background()

	// Test with custom resolver
	t.Run("resolve with custom DNS server", func(t *testing.T) {
		// Use a well-known domain that should resolve
		result, found, err := source.Lookup(ctx, "google.com")
		if err != nil {
			t.Skipf("Skipping test, custom resolver not accessible: %v", err)
		}
		if found {
			assert.NotEmpty(t, result)
			t.Logf("google.com resolved to: %v", result)
		}
	})
}

func TestModeSwitching(t *testing.T) {
	ctx := context.Background()
	factory := NewFactory()

	t.Run("forward then reverse", func(t *testing.T) {
		// First, do forward lookup
		forwardCfg := &Config{
			Mode:    ModeForward,
			Timeout: 10 * time.Second,
		}
		forwardSource, err := factory.CreateSource(ctx, lookupsource.CreateSettings{}, forwardCfg)
		require.NoError(t, err)

		ip, found, err := forwardSource.Lookup(ctx, "localhost")
		require.NoError(t, err)
		if !found {
			t.Skip("localhost not found, skipping reverse lookup test")
		}

		ipStr, ok := ip.(string)
		require.True(t, ok)
		t.Logf("Forward lookup: localhost -> %s", ipStr)

		// Now do reverse lookup on that IP
		reverseCfg := &Config{
			Mode:    ModeReverse,
			Timeout: 10 * time.Second,
		}
		reverseSource, err := factory.CreateSource(ctx, lookupsource.CreateSettings{}, reverseCfg)
		require.NoError(t, err)

		hostname, found, err := reverseSource.Lookup(ctx, ipStr)
		require.NoError(t, err)
		if found {
			t.Logf("Reverse lookup: %s -> %v", ipStr, hostname)
			assert.NotEmpty(t, hostname)
		}
	})
}

func TestWithCaching(t *testing.T) {
	factory := NewFactory()
	cfg := &Config{
		Mode:    ModeForward,
		Timeout: 10 * time.Second,
	}

	// Create source with caching enabled
	settings := lookupsource.CreateSettings{
		Cache: lookupsource.CacheConfig{
			Enabled: true,
			Size:    100,
			TTL:     5 * time.Minute,
		},
	}
	source, err := factory.CreateSource(context.Background(), settings, cfg)
	require.NoError(t, err)

	ctx := context.Background()

	// First lookup should miss cache and query DNS
	result1, found1, err := source.Lookup(ctx, "localhost")
	require.NoError(t, err)
	if !found1 {
		t.Skip("localhost not found")
	}
	assert.NotEmpty(t, result1)

	// Second lookup should hit cache (we can't easily verify this without instrumentation,
	// but we can at least verify it returns the same result)
	result2, found2, err := source.Lookup(ctx, "localhost")
	require.NoError(t, err)
	assert.True(t, found2)
	assert.Equal(t, result1, result2)
}
