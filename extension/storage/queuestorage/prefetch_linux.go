// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queuestorage // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/queuestorage"

import (
	"io"
	"os"

	"golang.org/x/sys/unix"
)

func prefetchFile(path string, size int64) {
	if prefetchDisabled {
		return
	}
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	fd := int(f.Fd())
	_ = unix.Fadvise(fd, 0, size, unix.FADV_SEQUENTIAL)
	// Synchronous read forces the entire file into page cache.
	// Using a 1MB buffer ensures large merged I/Os regardless of
	// the kernel's read_ahead_kb setting.
	buf := make([]byte, 1<<20)
	_, _ = io.CopyBuffer(io.Discard, f, buf)
}


