package output

import (
	"context"
	"io"

	"github.com/thanos-community/obslytics/pkg/dataframe"
	"github.com/thanos-io/thanos/pkg/objstore/client"
)

type Type string

const (
	PARQUET Type = "PARQUET"
)

// OutputConfig contains the options determining the object storage where files will be uploaded to.
type OutputConfig struct {
	client.BucketConfig `yaml:"storage"`
	OutputType          Type `yaml:"type"`
}

type nopWriteCloser struct {
	io.Writer
}

func (w *nopWriteCloser) Close() error { return nil }

// NopWriteCloser returns a nopWriteCloser.
func NopWriteCloser(w io.Writer) io.WriteCloser {
	return &nopWriteCloser{w}
}

// Params represent options that change more often than configs.
type Params struct {
	Out io.WriteCloser
}

type Output interface {
	Open(context.Context, Params) (Writer, error)
}

type Writer interface {
	Write(dataframe.Dataframe) error
	Close() error
}
