// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queuestorage // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/queuestorage"

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"time"
)

const (
	defaultSegmentSize     = 64 * 1024 * 1024 // 64 MiB
	defaultMaxOpenSegments = 3
	defaultTimeout         = time.Second
)

// Config defines configuration for the queue storage extension.
type Config struct {
	// Directory is the base path where segment files are stored.
	Directory string `mapstructure:"directory,omitempty"`

	// Timeout is the bbolt lock-acquire timeout per segment file.
	Timeout time.Duration `mapstructure:"timeout,omitempty"`

	// FSync controls whether fsync is called after every write.
	FSync bool `mapstructure:"fsync,omitempty"`

	// SegmentSize is the maximum bbolt file size (bytes) before rolling to a new segment.
	SegmentSize int64 `mapstructure:"segment_size,omitempty"`

	// MaxOpenSegments is the maximum number of bbolt files kept open simultaneously.
	// Minimum 2 (write segment + at least one read segment).
	MaxOpenSegments int `mapstructure:"max_open_segments,omitempty"`

	// CreateDirectory auto-creates the directory on start.
	CreateDirectory bool `mapstructure:"create_directory,omitempty"`

	// DirectoryPermissions is an octal string for directory creation mode.
	DirectoryPermissions       string `mapstructure:"directory_permissions,omitempty"`
	directoryPermissionsParsed int64  `mapstructure:"-"`
}

func (cfg *Config) Validate() error {
	if info, err := os.Stat(cfg.Directory); err != nil {
		if !cfg.CreateDirectory && os.IsNotExist(err) {
			return fmt.Errorf("directory must exist: %w. Enable create_directory to auto-create", err)
		}
		fsErr := &fs.PathError{}
		if errors.As(err, &fsErr) && !os.IsNotExist(err) {
			return fmt.Errorf("problem accessing directory: %s, err: %w", cfg.Directory, fsErr)
		}
	} else if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", cfg.Directory)
	}

	if cfg.SegmentSize <= 0 {
		return errors.New("segment_size must be positive")
	}

	if cfg.MaxOpenSegments < 2 {
		return errors.New("max_open_segments must be at least 2")
	}

	if cfg.CreateDirectory {
		permissions, err := strconv.ParseInt(cfg.DirectoryPermissions, 8, 32)
		if err != nil {
			return errors.New("directory_permissions must be a valid octal value")
		}
		if permissions&int64(os.ModePerm) != permissions {
			return errors.New("directory_permissions contains invalid bits")
		}
		cfg.directoryPermissionsParsed = permissions
	}

	return nil
}
