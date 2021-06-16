package exporter

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/thanos-community/obslytics/pkg/dataframe"
	"github.com/thanos-community/obslytics/pkg/series"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/client"
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

// Export encodes and streams the dataframe to given bucket. On error partial result might occur.
// It's caller responsibility to clean after error.
func (e *Exporter) ExportStream(ctx context.Context, in series.Reader, matchers []*labels.Matcher, minTime model.TimeOrDurationValue, maxTime model.TimeOrDurationValue, resolution time.Duration, dbgOut bool, chunks int) (err error) {

	readPipe, writePipe := io.Pipe()

	errch := make(chan error, 1)

	go func() {
		// TODO(bwplotka): Log error from close (e.g using runutil.Close... package).
		defer writePipe.Close()

		chunkStart := minTime.PrometheusTimestamp()
		chunkSize := (maxTime.PrometheusTimestamp() - chunkStart) / int64(chunks)

		for i := 0; i < chunks; i++ {
			ser, err := in.Read(ctx, series.Params{
				Matchers: matchers,
				MinTime:  timestamp.Time(chunkStart),
				MaxTime:  timestamp.Time(chunkStart + chunkSize),
			})
			chunkStart += chunkSize

			if err != nil {
				errch <- err
				return
			}

			df, err := dataframe.FromSeries(ser, resolution, func(o *dataframe.AggrsOptions) {
				o.Count.Enabled = false
				o.Sum.Enabled = false
				o.Min.Enabled = false
				o.Max.Enabled = false
			})

			if err != nil {
				errch <- errors.Wrap(err, "dataframe creation")
				return
			}

			if dbgOut {
				dataframe.Print(os.Stdout, df)
			}

			if err := e.enc.Encode(writePipe, df); err != nil {
				errch <- errors.Wrap(err, "encode")
				return
			}

		}
		errch <- nil
	}()
	defer func() {
		// TODO(bwplotka): Log error from close (e.g using runutil.Close... package).
		_ = readPipe.Close()
		if cerr := <-errch; cerr != nil && err == nil {
			err = cerr
		}
	}()

	if err := e.bkt.Upload(ctx, e.path, readPipe); err != nil {
		return errors.Wrap(err, "upload")
	}

	return nil
}
