package storeapi

import (
	"context"
	"io"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-community/obslytics/pkg/series"
	"github.com/thanos-io/thanos/pkg/extgrpc"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	tracing "github.com/thanos-io/thanos/pkg/tracing/client"
	"google.golang.org/grpc"
)

// Series implements input.Reader.
type Series struct {
	logger log.Logger
	conf   series.Config
}

func NewSeries(logger log.Logger, conf series.Config) (Series, error) {
	return Series{logger: logger, conf: conf}, nil
}

func (i Series) Read(ctx context.Context, params series.Params) (series.Set, error) {
	// set as true for authenticated connection if cert, key and/or ca are defined.
	secure := i.conf.TLSConfig.CertFile != "" ||
		i.conf.TLSConfig.KeyFile != "" ||
		i.conf.TLSConfig.CAFile != ""
	dialOpts, err := extgrpc.StoreClientGRPCOpts(i.logger, nil, tracing.NoopTracer(),
		secure,
		i.conf.TLSConfig.InsecureSkipVerify,
		i.conf.TLSConfig.CertFile,
		i.conf.TLSConfig.KeyFile,
		i.conf.TLSConfig.CAFile,
		i.conf.Endpoint,
	)

	if err != nil {
		return nil, errors.Wrap(err, "error initializing GRPC options")
	}

	conn, err := grpc.DialContext(ctx, i.conf.Endpoint, dialOpts...)
	if err != nil {
		return nil, errors.Wrap(err, "error initializing GRPC dial context")
	}

	matchers, err := storepb.PromMatchersToMatchers(params.Matchers...)
	if err != nil {
		return nil, err
	}

	client := storepb.NewStoreClient(conn)
	seriesClient, err := client.Series(ctx, &storepb.SeriesRequest{
		MinTime:                 timestamp.FromTime(params.MinTime),
		MaxTime:                 timestamp.FromTime(params.MaxTime),
		Matchers:                matchers,
		PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "storepb.Series against %v", i.conf.Endpoint)
	}

	return &iterator{
		ctx:    ctx,
		conn:   conn,
		client: seriesClient,
		mint:   timestamp.FromTime(params.MinTime),
		maxt:   timestamp.FromTime(params.MaxTime),
	}, nil
}

// iterator implements input.Set.
type iterator struct {
	ctx           context.Context
	conn          *grpc.ClientConn
	client        storepb.Store_SeriesClient
	currentSeries *storepb.Series

	mint, maxt int64

	err error
}

func (i *iterator) Next() bool {
	seriesResp, err := i.client.Recv()
	if err == io.EOF {
		return false
	}
	if err != nil {
		i.err = err
		return false
	}

	i.currentSeries = seriesResp.GetSeries()
	return true
}

func (i *iterator) At() storage.Series {
	// We support only raw data for now.
	return newChunkSeries(
		labelpb.ZLabelsToPromLabels(i.currentSeries.Labels),
		i.currentSeries.Chunks,
		i.mint, i.maxt,
		[]storepb.Aggr{storepb.Aggr_COUNT, storepb.Aggr_SUM},
	)
}

func (i *iterator) Warnings() storage.Warnings { return nil }

func (i *iterator) Err() error {
	return i.err
}

func (i *iterator) Close() error {
	if err := i.client.CloseSend(); err != nil {
		return err
	}

	return i.conn.Close()
}
