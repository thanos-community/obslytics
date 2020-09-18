package remoteread

import (
	"context"
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-community/obslytics/pkg/input"
	"github.com/thanos-community/obslytics/pkg/version"
	"net/url"

	"time"
)

type remoteReadInput struct {
	logger log.Logger
	conf   input.InputConfig
}

func NewRemoteReadInput(logger log.Logger, conf input.InputConfig) (remoteReadInput, error) {
	return remoteReadInput{logger: logger, conf: conf}, nil
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

func (i remoteReadInput) Open(ctx context.Context, params input.SeriesParams) (input.SeriesIterator, error) {

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
	endpointUrl := &config_util.URL{URL: parsedUrl}
	clientConfig := &remote.ClientConfig{
		URL:              endpointUrl,
		Timeout:          timeoutDuration,
		HTTPClientConfig: httpConfig,
	}

	clientName := "Obslytics/" + version.Version
	client, err := remote.NewReadClient(clientName, clientConfig)
	if err != nil {
		return nil, err
	}

	promLabelMatchers, err := TranslatePromMatchers(params.Matchers...)
	if err != nil {
		return nil, err
	}

	// Contruct Query
	query := &prompb.Query{
		StartTimestampMs: timestamp.FromTime(params.MinTime),
		EndTimestampMs:   timestamp.FromTime(params.MaxTime),
		Matchers:         promLabelMatchers,
	}

	readResponse, err := client.Read(ctx, query)
	if err != nil {
		return nil, err
	}

	var readSeriesList = make([]ReadSeries, 0, len(readResponse.Timeseries))

	// Convert Timeseries List to a Read Series List
	for index := range readResponse.Timeseries {
		readSeriesList = append(readSeriesList, ReadSeries{
			timeseries: *readResponse.Timeseries[index],
		})
	}

	return &readSeriesIterator{
		logger:             i.logger,
		ctx:                ctx,
		client:             client,
		seriesList:         readSeriesList,
		currentSeriesIndex: -1,
		maxSeriesIndex:     len(readSeriesList) - 1,
	}, nil
}

// readSeriesIterator implements input.SeriesIterator
type readSeriesIterator struct {
	logger             log.Logger
	ctx                context.Context
	client             remote.ReadClient
	seriesList         []ReadSeries
	currentSeriesIndex int
	maxSeriesIndex     int
}

func (i *readSeriesIterator) Next() bool {

	// return false if the last index is already reached
	if i.currentSeriesIndex+1 > i.maxSeriesIndex {
		return false
	} else {
		i.currentSeriesIndex++
		return true
	}
}

func (i *readSeriesIterator) At() input.Series {
	return i.seriesList[i.currentSeriesIndex]
}

func (i *readSeriesIterator) Close() error {
	return nil
}

// ReadSeries implements input.Series
type ReadSeries struct {
	input.Series
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

func (r ReadSeries) MinTime() time.Time {

	seconds := r.timeseries.Samples[0].Timestamp / 1000
	nanoseconds := r.timeseries.Samples[0].Timestamp % 1000 * 1000000

	return time.Unix(seconds, nanoseconds)
}

func (r ReadSeries) ChunkIterator() (input.ChunkIterator, error) {
	chunk := ReadChunk{
		series: r.timeseries,
	}
	iterator := chunk.Iterator()
	return iterator, nil
}

// ReadChunkIterator implements input.ChunkIterator
type ReadChunkIterator struct {
	input.ChunkIterator
	Chunk              ReadChunk
	currentSampleIndex int
	maxSampleIndex     int
}

func (c *ReadChunkIterator) Next() bool {
	// return false if the last index is reached
	if c.currentSampleIndex+1 > c.maxSampleIndex {
		return false
	} else {
		c.currentSampleIndex++
		return true
	}
}

// Seek advances the iterator forward to the first sample with the timestamp equal or greater than t.
// If current sample found by previous `Next` or `Seek` operation already has this property, Seek has no effect.
// Seek returns true, if such sample exists, false otherwise.
// Iterator is exhausted when the Seek returns false.
func (c *ReadChunkIterator) Seek(t int64) bool {
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
// Before the iterator has advanced At behaviour is unspecified.
func (c *ReadChunkIterator) At() (int64, float64) {
	return c.Chunk.series.Samples[c.currentSampleIndex].Timestamp, c.Chunk.series.Samples[c.currentSampleIndex].Value
}

// Err returns the current error. It should be used only after iterator is
// exhausted, that is `Next` or `Seek` returns false.
func (c *ReadChunkIterator) Err() error {
	return nil
}

type ReadChunk struct {
	chunkenc.Chunk
	series prompb.TimeSeries
}

func (c ReadChunk) Iterator() input.ChunkIterator {
	return &ReadChunkIterator{Chunk: c, currentSampleIndex: -1, maxSampleIndex: len(c.series.Samples) - 1}
}
