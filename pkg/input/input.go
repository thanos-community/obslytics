package input

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type Input interface {
	Read(ctx context.Context) (SeriesIterator, error)
}

// SeriesIterator iterates through all series in tn the input.
type SeriesIterator interface {
	Next() bool
	At() Series
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
