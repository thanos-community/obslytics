package promread

import (
	"context"
	"net/url"
	"path"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-community/obslytics/pkg/series"
	"github.com/thanos-community/obslytics/pkg/version"
)

type Series struct {
	logger log.Logger
	conf   series.Config
}

func NewSeries(logger log.Logger, conf series.Config) (Series, error) {
	return Series{logger: logger, conf: conf}, nil
}

// TranslatePromMatchers returns proto matchers (prompb) from Prometheus matchers.
// NOTE: It allocates memory.
func TranslatePromMatchers(ms ...*labels.Matcher) ([]*prompb.LabelMatcher, error) {
	res := make([]*prompb.LabelMatcher, 0, len(ms))
	for _, m := range ms {
		var t prompb.LabelMatcher_Type

		switch m.Type {
		case labels.MatchEqual:
			t = prompb.LabelMatcher_EQ
		case labels.MatchNotEqual:
			t = prompb.LabelMatcher_NEQ
		case labels.MatchRegexp:
			t = prompb.LabelMatcher_RE
		case labels.MatchNotRegexp:
			t = prompb.LabelMatcher_NRE
		default:
			return nil, errors.Errorf("unrecognized matcher type %d", m.Type)
		}
		res = append(res, &prompb.LabelMatcher{Type: t, Name: m.Name, Value: m.Value})
	}
	return res, nil
}

func (i Series) Read(ctx context.Context, params series.Params) (series.Set, error) {
	tlsConfig := config_util.TLSConfig{
		CAFile:             i.conf.TLSConfig.CAFile,
		CertFile:           i.conf.TLSConfig.CertFile,
		KeyFile:            i.conf.TLSConfig.KeyFile,
		ServerName:         i.conf.TLSConfig.ServerName,
		InsecureSkipVerify: i.conf.TLSConfig.InsecureSkipVerify,
	}

	httpConfig := config_util.HTTPClientConfig{
		TLSConfig: tlsConfig,
	}

	parsedUrl, err := url.Parse(i.conf.Endpoint)
	if err != nil {
		return nil, err
	}
	timeoutDuration, err := model.ParseDuration("10s")
	if err != nil {
		return nil, err
	}

	clientConfig := &remote.ClientConfig{
		URL:              &config_util.URL{URL: parsedUrl},
		Timeout:          timeoutDuration,
		HTTPClientConfig: httpConfig,
	}

	client, err := remote.NewReadClient(path.Join("obslytics", version.Version), clientConfig)
	if err != nil {
		return nil, err
	}

	promLabelMatchers, err := TranslatePromMatchers(params.Matchers...)
	if err != nil {
		return nil, err
	}

	// Construct Query.
	query := &prompb.Query{
		StartTimestampMs: timestamp.FromTime(params.MinTime),
		EndTimestampMs:   timestamp.FromTime(params.MaxTime),
		Matchers:         promLabelMatchers,
	}
	// TODO: Move to streaming remote read version when available.
	readResponse, err := client.Read(ctx, query)
	if err != nil {
		return nil, err
	}

	readSeriesList := make([]ReadSeries, 0, len(readResponse.Timeseries))

	// Convert Timeseries List to a Read Series List.
	for index := range readResponse.Timeseries {
		readSeriesList = append(readSeriesList, ReadSeries{
			timeseries: *readResponse.Timeseries[index],
		})
	}

	return &iterator{
		ctx:                ctx,
		client:             client,
		seriesList:         readSeriesList,
		currentSeriesIndex: -1,
	}, nil
}

// iterator implements input.Set.
type iterator struct {
	ctx                context.Context
	client             remote.ReadClient
	seriesList         []ReadSeries
	currentSeriesIndex int
}

func (i *iterator) Next() bool {
	// Return false if the last index is already reached.
	if i.currentSeriesIndex+1 > len(i.seriesList)-1 {
		return false
	}
	i.currentSeriesIndex++
	return true

}

func (i *iterator) At() storage.Series {
	return i.seriesList[i.currentSeriesIndex]
}

func (i *iterator) Warnings() storage.Warnings { return nil }
func (i *iterator) Err() error                 { return nil }
func (i *iterator) Close() error               { return nil }

// ReadSeries implements storage.Series.
type ReadSeries struct {
	timeseries prompb.TimeSeries
}

func (r ReadSeries) Labels() labels.Labels {
	var labelList []labels.Label
	for i := range r.timeseries.Labels {
		labelList = append(labelList, labels.Label{
			Name:  r.timeseries.Labels[i].Name,
			Value: r.timeseries.Labels[i].Value,
		})

	}
	return labels.New(labelList...)
}

func (r ReadSeries) Iterator() chunkenc.Iterator {
	return readChunk{series: r.timeseries}.Iterator()
}

// readChunkIterator implements input.ChunkIterator.
type readChunkIterator struct {
	Chunk              readChunk
	currentSampleIndex int
}

func (c *readChunkIterator) Next() bool {
	// Return false if the last index is reached.
	if c.currentSampleIndex+1 > len(c.Chunk.series.Samples)-1 {
		return false
	}
	c.currentSampleIndex++
	return true
}

// Seek advances the iterator forward to the first sample with the timestamp equal or greater than t.
// If current sample found by previous `Next` or `Seek` operation already has this property, Seek has no effect.
// Seek returns true, if such sample exists, false otherwise.
// Set is exhausted when the Seek returns false.
func (c *readChunkIterator) Seek(t int64) bool {
	if c.currentSampleIndex < 0 {
		c.currentSampleIndex = 0
	}
	for c.Chunk.series.Samples[c.currentSampleIndex].Timestamp < t {
		if !c.Next() {
			return false
		}
	}
	return true
}

// At returns the current timestamp/value pair.
// Before the iterator has advanced At behavior is unspecified.
func (c *readChunkIterator) At() (int64, float64) {
	return c.Chunk.series.Samples[c.currentSampleIndex].Timestamp, c.Chunk.series.Samples[c.currentSampleIndex].Value
}

// Err returns the current error. It should be used only after iterator is
// exhausted, that is `Next` or `Seek` returns false.
func (c *readChunkIterator) Err() error {
	return nil
}

type readChunk struct {
	chunkenc.Chunk
	series prompb.TimeSeries
}

func (c readChunk) Iterator() chunkenc.Iterator {
	return &readChunkIterator{Chunk: c, currentSampleIndex: -1}
}
