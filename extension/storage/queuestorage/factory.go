// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queuestorage // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/queuestorage"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/queuestorage/internal/metadata"
)

func NewFactory() extension.Factory {
	return extension.NewFactory(
		metadata.Type,
		createDefaultConfig,
		createExtension,
		metadata.ExtensionStability,
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Directory:            getDefaultDirectory(),
		Timeout:              defaultTimeout,
		SegmentSize:          defaultSegmentSize,
		MaxOpenSegments:      defaultMaxOpenSegments,
		FSync:                false,
		CreateDirectory:      false,
		DirectoryPermissions: "0750",
	}
}

func createExtension(
	_ context.Context,
	params extension.Settings,
	cfg component.Config,
) (extension.Extension, error) {
	return newQueueStorageExtension(params.Logger, cfg.(*Config))
}
