// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package queuestorage // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/queuestorage"

func getDefaultDirectory() string {
	return "/var/lib/otelcol/queue_storage"
}
