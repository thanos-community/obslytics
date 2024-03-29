// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package exporter

import (
	"context"
	"io"

	"github.com/efficientgo/core/errors"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/client"

	"github.com/thanos-community/obslytics/pkg/dataframe"
)

type Type string

const (
	PARQUET Type = "PARQUET"
)

// Config contains the options determining the object storage where files will be uploaded to.
type Config struct {
	Type    Type                `yaml:"type"`
	Path    string              `yaml:"path"`
	Storage client.BucketConfig `yaml:"storage"`
}

// An Encoder writes serialized type to an output stream.
type Encoder interface {
	// TODO(bwplotka): Consider more generic option with interface{} if we have more types than Dataframe.
	Encode(io.Writer, dataframe.Dataframe) (err error)
}

type Exporter struct {
	enc Encoder

	path string
	bkt  objstore.Bucket
}

func New(c Encoder, path string, bkt objstore.Bucket) *Exporter {
	return &Exporter{
		enc:  c,
		path: path,
		bkt:  bkt,
	}
}

// Export encodes and streams the dataframe to given bucket. On error partial result might occur.
// It's caller responsibility to clean after error.
func (e *Exporter) Export(ctx context.Context, df dataframe.Dataframe) (err error) {
	r, w := io.Pipe()

	errch := make(chan error, 1)
	go func() {
		// TODO(bwplotka): Log error from close (e.g using runutil.Close... package).
		defer w.Close()
		if err := e.enc.Encode(w, df); err != nil {
			errch <- errors.Wrap(err, "encode")
			return
		}
		errch <- nil
	}()
	defer func() {
		// TODO(bwplotka): Log error from close (e.g using runutil.Close... package).
		_ = r.Close()
		if cerr := <-errch; cerr != nil && err == nil {
			err = cerr
		}
	}()

	if err := e.bkt.Upload(ctx, e.path, r); err != nil {
		return errors.Wrap(err, "upload")
	}
	return nil
}
