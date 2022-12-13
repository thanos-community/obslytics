// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package factory

import (
	"path"
	"strings"

	"github.com/efficientgo/core/errors"
	"github.com/go-kit/log"
	"github.com/thanos-io/objstore/client"
	"gopkg.in/yaml.v2"

	"github.com/thanos-community/obslytics/pkg/exporter"
	"github.com/thanos-community/obslytics/pkg/exporter/parquet"
	"github.com/thanos-community/obslytics/pkg/version"
)

// NewExporter returns exporter based on configuration file.
func NewExporter(logger log.Logger, cfg exporter.Config) (*exporter.Exporter, error) {
	storageConf, err := yaml.Marshal(cfg.Storage)
	if err != nil {
		return nil, errors.Wrap(err, "storage configuration")
	}
	bkt, err := client.NewBucket(logger, storageConf, nil, path.Join("obslytics", version.Version))
	if err != nil {
		return nil, errors.Wrap(err, "creating storage")
	}

	var e exporter.Encoder
	switch exporter.Type(strings.ToUpper(string(cfg.Type))) {
	case exporter.PARQUET:
		e = parquet.NewEncoder()
	default:
		return nil, errors.Newf("unsupported export type %v", cfg.Type)
	}

	return exporter.New(e, cfg.Path, bkt), nil
}
