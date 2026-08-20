// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package queuestorage // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/queuestorage"

import (
	"io"
	"os"
)

func prefetchFile(path string, _ int64) {
	if prefetchDisabled {
		return
	}
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	buf := make([]byte, 1<<20)
	_, _ = io.CopyBuffer(io.Discard, f, buf)
}
