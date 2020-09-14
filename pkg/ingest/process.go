package ingest

// Implements logic around putting ingested results into the writer.

import (
	"github.com/pkg/errors"

	"github.com/thanos-community/obslytics/pkg/input"
	"github.com/thanos-community/obslytics/pkg/output"
)

// Process a single series via an ingestor and write results to writer if available.
func Process(s input.Series, in Ingestor, w output.Writer) error {
	if err := in.Ingest(s); err != nil {
		return errors.Wrap(err, "error while ingesting the series")
	}

	df, ok := in.Flush()
	if !ok {
		return nil
	}

	if err := w.Write(df); err != nil {
		return errors.Wrap(err, "error while writing the series")
	}

	return nil
}

func ProcessFinalize(in Ingestor, w output.Writer) error {
	if err := in.Finalize(); err != nil {
		return errors.Wrap(err, "error while finalizing the ingestor")
	}

	df, ok := in.Flush()
	if !ok {
		return nil
	}

	if err := w.Write(df); err != nil {
		return errors.Wrap(err, "error while writing on finish")
	}

	return nil
}

// Ingest all series from the SeriesIterator.
func ProcessAll(si input.SeriesIterator, in Ingestor, w output.Writer) error {
	for si.Next() {
		s := si.At()
		if err := Process(s, in, w); err != nil {
			return err
		}
	}

	if err := ProcessFinalize(in, w); err != nil {
		return err
	}

	return nil
}
