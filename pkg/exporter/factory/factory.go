package factory

import (
	"path"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/thanos-community/obslytics/pkg/exporter"
	"github.com/thanos-community/obslytics/pkg/exporter/parquet"
	"github.com/thanos-community/obslytics/pkg/version"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"gopkg.in/yaml.v2"
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
		return nil, errors.Errorf("unsupported export type %v", cfg.Type)
	}

	return exporter.New(e, cfg.Path, bkt), nil
}
