package ingest

import (
	"github.com/pkg/errors"

	"github.com/thanos-community/obslytics/pkg/dataframe"
	"github.com/thanos-community/obslytics/pkg/input"
	"github.com/thanos-community/obslytics/pkg/output"
)

type Ingestor interface {
	// Ingest a single series. It expects the series' chunks to be sorted
	// by timestamp.
	Ingest(input.Series) error

	// Return already ingested data from previous samples while cleaning
	// the buffer. The second return value determines, if any data was returned.
	Flush() (df dataframe.Dataframe, ok bool)

	// Indicate no more Ingest calls to be done. It allows finalizing the active
	// data even when not reaching next sample time.
	Finalize() error
}

// Ingests the data while periodically flushing the ingested results to the writer
type ContinuousIngestor struct {
	in Ingestor
	w  output.Writer
}

// Ingests the data and flushes the ingested results to the writer when appropriate.
func (ci ContinuousIngestor) Ingest(s input.Series) error {
	err := ci.in.Ingest(s)
	if err != nil {
		return errors.Wrap(err, "Error while ingesting the series")
	}

	df, ok := ci.in.Flush()
	if !ok {
		return nil
	}

	err = ci.w.Write(df)
	if err != nil {
		return errors.Wrap(err, "Error while writing the series")
	}

	return nil
}

func (ci ContinuousIngestor) Finalize() error {
	err := ci.in.Finalize()
	if err != nil {
		return errors.Wrap(err, "Error while finalizing the underlying ingestor")
	}

	df, ok := ci.in.Flush()
	if !ok {
		return nil
	}

	err = ci.w.Write(df)
	if err != nil {
		return errors.Wrap(err, "Error while writing on finish")
	}

	return nil
}
