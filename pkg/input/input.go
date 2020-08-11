package input

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	http_util "github.com/thanos-io/thanos/pkg/http"
)

// InputConfig contains the options determining the endpoint to talk to.
type InputConfig struct {
	Endpoint string `yaml:"endpoint"`
	TLSConfig http_util.TLSConfig `yaml:"tls_config"`
}

// SeriesParams determines what data should be loaded from the input
type SeriesParams struct {
	Metric string
	MinTime time.Time
	MaxTime time.Time
}

type Input interface {
	Open(context.Context, SeriesParams) (SeriesIterator, error)
}

// SeriesIterator iterates through all series in tn the input.
type SeriesIterator interface {
	Next() bool
	At() Series
	Close() error
}

// Series exposes data for a single series (determined by a label)
type Series interface {
	Labels() labels.Labels
	// The earliest time of all chunks in the series. Zero time if no chunks present.
	MinTime() time.Time
	// To iterate through all the ts within the series.
	ChunkIterator() (ChunkIterator, error)
}

// ChunkIterator iterates through all the ts within the series.
type ChunkIterator chunkenc.Iterator
