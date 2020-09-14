package storeapi

// Implements input interfaces on top of Thanos Store API endpoints.

import (
	"context"
	"io"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/extgrpc"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	tracing "github.com/thanos-io/thanos/pkg/tracing/client"
	"google.golang.org/grpc"

	"github.com/thanos-community/obslytics/pkg/input"
)

// storeAPIInput implements input.Input
type storeAPIInput struct {
	logger log.Logger
	conf   input.InputConfig
}

func NewStoreAPIInput(logger log.Logger, conf input.InputConfig) (storeAPIInput, error) {
	return storeAPIInput{logger: logger, conf: conf}, nil
}

func parseStoreMatchers(matcherStr string) (storeMatchers []storepb.LabelMatcher, err error) {
	matchers, err := parser.ParseMetricSelector(matcherStr)
	if err != nil {
		return nil, err
	}
	stm, err := storepb.TranslatePromMatchers(matchers...)
	if err != nil {
		return nil, err
	}

	return stm, nil
}

func (i storeAPIInput) Open(ctx context.Context, params input.SeriesParams) (input.SeriesIterator, error) {
	dialOpts, err := extgrpc.StoreClientGRPCOpts(i.logger, nil, tracing.NoopTracer(),
		!i.conf.TLSConfig.InsecureSkipVerify,
		i.conf.TLSConfig.CertFile,
		i.conf.TLSConfig.KeyFile,
		i.conf.TLSConfig.CAFile,
		i.conf.Endpoint)

	if err != nil {
		return nil, errors.Wrap(err, "error initializing GRPC options")
	}

	conn, err := grpc.DialContext(ctx, i.conf.Endpoint, dialOpts...)
	if err != nil {
		return nil, errors.Wrap(err, "error initializing GRPC dial context")
	}

	matchers, err := parseStoreMatchers(params.Matcher)
	if err != nil {
		return nil, errors.Wrap(err, "error parsing provided matchers")
	}

	client := storepb.NewStoreClient(conn)
	seriesClient, err := client.Series(ctx, &storepb.SeriesRequest{
		MinTime:                 params.MinTime.PrometheusTimestamp(),
		MaxTime:                 params.MaxTime.PrometheusTimestamp(),
		Matchers:                matchers,
		PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
	})
	return &storeSeriesIterator{
		logger: i.logger,
		ctx:    ctx,
		conn:   conn,
		client: seriesClient}, nil
}

// storeSeriesIterator implements input.SeriesIterator.
type storeSeriesIterator struct {
	logger        log.Logger
	ctx           context.Context
	conn          *grpc.ClientConn
	client        storepb.Store_SeriesClient
	currentSeries *storepb.Series
}

func (i *storeSeriesIterator) Next() bool {
	seriesResp, err := i.client.Recv()
	if err == io.EOF {
		return false
	}
	if err != nil {
		i.logger.Log("msg", "Couldn't ready series", "err", err)
		return false
	}

	i.currentSeries = seriesResp.GetSeries()
	return true
}

func (i *storeSeriesIterator) At() input.Series {
	return StoreSeries{StoreS: i.currentSeries}
}

func (i *storeSeriesIterator) Close() error {
	err := i.client.CloseSend()
	if err != nil {
		return err
	}

	err = i.conn.Close()
	if err != nil {
		return err
	}

	return nil
}

// StoreSeries implements input.Series.
type StoreSeries struct{ StoreS *storepb.Series }

func (s StoreSeries) Labels() labels.Labels {
	return storepb.LabelsToPromLabels(s.StoreS.Labels)
}

func (s StoreSeries) MinTime() time.Time {
	if len(s.StoreS.Chunks) == 0 {
		return time.Time{}
	}
	return timestamp.Time(s.StoreS.Chunks[0].MinTime)
}

func (s StoreSeries) ChunkIterator() (input.ChunkIterator, error) {
	return NewStoreChunkIterator(s.StoreS.Chunks)
}

// storeChunkIterator implements dataframe.ChunkIterator.
type storeChunkIterator struct {
	chunks        []storepb.AggrChunk
	decodedChunks []chunkenc.Chunk
	chunkPos      int
	currentIt     chunkenc.Iterator
}

func NewStoreChunkIterator(chunks []storepb.AggrChunk) (*storeChunkIterator, error) {
	decodedChunks := make([]chunkenc.Chunk, 0, len(chunks))
	for _, c := range chunks {
		ce, err := chunkenc.FromData(chunkenc.EncXOR, c.Raw.Data)
		if err != nil {
			return nil, err
		}
		decodedChunks = append(decodedChunks, ce)
	}
	return &storeChunkIterator{chunks: chunks, decodedChunks: decodedChunks, chunkPos: -1}, nil
}

func (i *storeChunkIterator) At() (int64, float64) {
	return i.currentIt.At()
}

func (i *storeChunkIterator) Err() error {
	return i.currentIt.Err()
}

func (i *storeChunkIterator) Next() bool {
	if !i.ensureCurrentIt() {
		return false
	}
	// try finding next chunk that return true
	for {
		ret := i.currentIt.Next()
		if ret {
			// there is still something in the current iterator
			return true
		}

		if !i.nextChunk() {
			// no more chunks to try
			return false
		}
	}
}

func (i *storeChunkIterator) Seek(t int64) bool {
	if !i.ensureCurrentIt() {
		return false
	}
	for i.chunks[i.chunkPos].MaxTime < t {
		// move until we get to a chunk with MaxTime >= t
		if !i.nextChunk() {
			// no more chunks to try
			return false
		}
	}
	// seek in the chunk that matches MaxTime >= t (or it's the last one)
	return i.currentIt.Seek(t)
}

// Moves the position to the next chunk if available. Returns false if no more
// chunks to try.
func (i *storeChunkIterator) nextChunk() bool {
	if i.chunkPos == len(i.chunks)-1 {
		// no more chunks to try
		return false
	}

	i.chunkPos++
	i.currentIt = i.decodedChunks[i.chunkPos].Iterator(nil)
	return true
}

// At the beginning, the currentIt it not set. This method initializes
// it if missing and possible (not empty chunks).
// Returns true if the iterator is successfully initialized, false otherwise.
func (i *storeChunkIterator) ensureCurrentIt() bool {
	if i.currentIt == nil {
		return i.nextChunk()
	}
	return true
}
