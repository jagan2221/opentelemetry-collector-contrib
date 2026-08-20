// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queuestorage // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/queuestorage"

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"go.etcd.io/bbolt"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.uber.org/zap"
)

type queueStorageExtension struct {
	cfg    *Config
	logger *zap.Logger
}

var _ storage.Extension = (*queueStorageExtension)(nil)

func newQueueStorageExtension(logger *zap.Logger, cfg *Config) (extension.Extension, error) {
	if cfg.CreateDirectory {
		if err := os.MkdirAll(cfg.Directory, os.FileMode(cfg.directoryPermissionsParsed)); err != nil {
			return nil, err
		}
	}
	return &queueStorageExtension{cfg: cfg, logger: logger}, nil
}

func (e *queueStorageExtension) Start(context.Context, component.Host) error {
	return nil
}

func (*queueStorageExtension) Shutdown(context.Context) error {
	return nil
}

func (e *queueStorageExtension) GetClient(_ context.Context, kind component.Kind, ent component.ID, name string) (storage.Client, error) {
	var rawName string
	if name == "" {
		rawName = fmt.Sprintf("%s_%s_%s", kindString(kind), ent.Type(), ent.Name())
	} else {
		rawName = fmt.Sprintf("%s_%s_%s_%s", kindString(kind), ent.Type(), ent.Name(), name)
	}

	dirName := sanitize(rawName)
	clientDir := filepath.Join(e.cfg.Directory, dirName)

	_, err := os.Stat(clientDir)
	if errors.Is(err, syscall.ENAMETOOLONG) {
		clientDir = filepath.Join(e.cfg.Directory, hashName(rawName))
	}

	if err := os.MkdirAll(clientDir, 0o700); err != nil {
		return nil, fmt.Errorf("queue_storage: cannot create client dir %s: %w", clientDir, err)
	}

	opts := bbolt.Options{
		Timeout:        e.cfg.Timeout,
		NoSync:         !e.cfg.FSync,
		NoFreelistSync: true,
		FreelistType:   bbolt.FreelistMapType,
	}

	return newQueueClient(e.logger, clientDir, e.cfg.SegmentSize, e.cfg.MaxOpenSegments, opts)
}

func kindString(k component.Kind) string {
	switch k {
	case component.KindReceiver:
		return "receiver"
	case component.KindProcessor:
		return "processor"
	case component.KindExporter:
		return "exporter"
	case component.KindExtension:
		return "extension"
	case component.KindConnector:
		return "connector"
	default:
		return "other"
	}
}

func sanitize(name string) string {
	var sanitized strings.Builder
	for _, character := range name {
		if isSafe(character) {
			sanitized.WriteRune(character)
		} else {
			fmt.Fprintf(&sanitized, "~%04X", character)
		}
	}
	return sanitized.String()
}

func isSafe(character rune) bool {
	switch {
	case character >= 'a' && character <= 'z',
		character >= 'A' && character <= 'Z',
		character >= '0' && character <= '9',
		character == '.',
		character == '-',
		character == '_':
		return true
	}
	return false
}

func hashName(name string) string {
	h := sha256.Sum256([]byte(name))
	return hex.EncodeToString(h[:])
}
