package factory

import (
	"strings"

	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/thanos-community/obslytics/pkg/version"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"gopkg.in/yaml.v2"

	"github.com/thanos-community/obslytics/pkg/output"
	"github.com/thanos-community/obslytics/pkg/output/parquet"
)

func Parse(logger log.Logger, confYaml []byte) (objstore.Bucket, output.Output, error) {

	// Set Default configuration values.
	// Default output type is Parquet.
	cfg := output.OutputConfig{
		OutputType: output.PARQUET,
	}

	// Default Storage Type is Filesystem.
	cfg.BucketConfig = client.BucketConfig{Type: client.FILESYSTEM}

	if err := yaml.UnmarshalStrict(confYaml, &cfg); err != nil {
		return nil, nil, err
	}

	// Extract Storage Configuration from Bucket Config.
	storageConf, err := yaml.Marshal(cfg.BucketConfig)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Storage configuration.")
	}

	// Create Object Storage Bucket.
	storageBucket, err := client.NewBucket(logger, storageConf, nil, "Obslytics/"+version.Version)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Creating storage bucket")
	}

	// Selecting Output Type.
	var out output.Output
	switch strings.ToUpper(string(cfg.OutputType)) {
	case string(output.PARQUET):
		out = parquet.Output{}
	default:
		return nil, nil, errors.Errorf("Unsupported Output Type: " + string(cfg.OutputType))
	}

	return storageBucket, out, nil
}
