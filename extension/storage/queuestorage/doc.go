// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package queuestorage implements a storage extension optimized for persistent
// queue workloads. It uses multiple small bbolt segment files instead of a single
// large file, bounding memory usage and startup time regardless of total queue size.
package queuestorage // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/queuestorage"
