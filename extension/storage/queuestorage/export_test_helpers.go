// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queuestorage // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/queuestorage"

import (
	"go.etcd.io/bbolt"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.uber.org/zap"
)

// NewQueueClientForBench creates a queueClient for use in benchmarks and
// external test programs. It uses default bbolt options suitable for benchmarks.
func NewQueueClientForBench(logger *zap.Logger, dir string, segSize int64, maxOpen int) (*QueueClientExported, error) {
	opts := bbolt.Options{
		NoSync:         true,
		NoFreelistSync: true,
		FreelistType:   bbolt.FreelistMapType,
	}
	c, err := newQueueClient(logger, dir, segSize, maxOpen, opts)
	if err != nil {
		return nil, err
	}
	return &QueueClientExported{c}, nil
}

// QueueClientExported wraps queueClient with exported methods for external use.
type QueueClientExported struct {
	*queueClient
}

// Ensure it satisfies storage.Client.
var _ storage.Client = (*QueueClientExported)(nil)
