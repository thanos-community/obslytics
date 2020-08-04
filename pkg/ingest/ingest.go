package ingest

import (
	"time"

	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/store/storepb"

	"github.com/thanos-community/obslytics/pkg/dataframe"
	"github.com/thanos-community/obslytics/pkg/output"
)

type Ingestor interface {
	Ingest(*storepb.Series) error
}

// Options for a single aggregation.
type AggrOption struct {
	// Should the aggregation be used?
	Enabled bool
	// Column to store the aggregation at
	Column string
}

// Collections of aggregations-related options. Determines what aggregations are enabled etc..
type aggrsOptions struct {
	// If true (default), the samples should repeat the same time for every day.
	// This puts additional requirement for 24h to be divisible by the resolution.
	MidnightAligned bool

	Sum   AggrOption
	Count AggrOption
	Min   AggrOption
	Max   AggrOption
}

// By default, all aggregations are disabled and target columns set with `_` prefix.
func NewAggregationsOptions() (ao aggrsOptions) {
	ao.MidnightAligned = true

	ao.Sum.Column = "_sum"
	ao.Count.Column = "_count"
	ao.Min.Column = "_min"
	ao.Max.Column = "_max"
	return ao
}

// Ingests the data and produces aggregations at the specified resolution.
type AggregatedIngestor struct {
	Resolution  time.Duration
	aggrOptions aggrsOptions
}

// Ingest the data to an aggregated set.
func (i AggregatedIngestor) Ingest(s *storepb.Series) error {
	// TODO(inecas): implement
	return nil
}

// Return already aggregated data from previous samples while cleaning
// the buffer.
func (i AggregatedIngestor) Flush() dataframe.Dataframe {
	// TODO(inecas): implement
	return dataframe.Dataframe{}
}

// Ingests the data while periodically flushing the aggregated results to the writer
type ContinuousIngestor struct {
	aggr AggregatedIngestor
	w    output.Writer
}

// Ingests the data and flushes the aggregated results to the writer when appropriate.
func (ci ContinuousIngestor) Ingest(s *storepb.Series) error {
	err := ci.aggr.Ingest(s)
	if err != nil {
		return errors.Wrap(err, "Error while aggregating the series")
	}

	if ci.shouldFlush() {
		err = ci.w.Write(ci.aggr.Flush())
		if err != nil {
			return errors.Wrap(err, "Error while writing the series")
		}
	}
	return nil
}

func (ci ContinuousIngestor) Finish() error {
	err := ci.w.Write(ci.aggr.Flush())
	if err != nil {
		return errors.Wrap(err, "Error while writing on finish")
	}
	return nil
}

// Decides whether it's the right time to do the flushing right now
func (ci ContinuousIngestor) shouldFlush() bool {
	// TODO(inecas): implement
	return false
}
