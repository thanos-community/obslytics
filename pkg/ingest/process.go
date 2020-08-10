package ingest

// Implements logic around putting ingested results into the writer.

import (
	"github.com/pkg/errors"

	"github.com/thanos-community/obslytics/pkg/input"
	"github.com/thanos-community/obslytics/pkg/output"
)

// Process a single series via an ingestor and write results to writer if available.
func Process(s input.Series, in Ingestor, w output.Writer) error {
	err := in.Ingest(s)
	if err != nil {
		return errors.Wrap(err, "Error while ingesting the series")
	}

	df, ok := in.Flush()
	if !ok {
		return nil
	}

	err = w.Write(df)
	if err != nil {
		return errors.Wrap(err, "Error while writing the series")
	}

	return nil
}

// Ingest all series from the SeriesIterator.
func ProcessAll(si input.SeriesIterator, in Ingestor, w output.Writer) error {
	for si.Next() {
		s := si.At()
		err := Process(s, in, w)
		if err != nil {
			return err
		}
	}

	err := ProcessFinalize(in, w)
	if err != nil {
		return err
	}

	return nil
}

func ProcessFinalize(in Ingestor, w output.Writer) error {
	err := in.Finalize()
	if err != nil {
		return errors.Wrap(err, "Error while finalizing the ingestor")
	}

	df, ok := in.Flush()
	if !ok {
		return nil
	}

	err = w.Write(df)
	if err != nil {
		return errors.Wrap(err, "Error while writing on finish")
	}

	return nil
}
