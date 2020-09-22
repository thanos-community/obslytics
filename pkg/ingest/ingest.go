package ingest

import (
	"github.com/thanos-community/obslytics/pkg/dataframe"
	"github.com/thanos-community/obslytics/pkg/input"
)

type Ingestor interface {
	// Ingest a single series. It expects the series' chunks to be sorted
	// by timestamp.
	Ingest(input.Series) error

	// Indicate no more Ingest calls to be done. It allows finalizing the active
	// data even when not reaching next sample time.
	Finalize() error

	// Return already ingested data from previous samples while cleaning
	// the buffer. The second return value determines, if any data was returned.
	Flush() (df dataframe.Dataframe, ok bool)
}
