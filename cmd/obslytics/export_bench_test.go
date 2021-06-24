package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-community/obslytics/pkg/exporter"
	"github.com/thanos-community/obslytics/pkg/series"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/objstore/filesystem"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
	"google.golang.org/grpc"
)

func BenchmarkExport(b *testing.B) {
	// Generate large enough input case.
	b.Run("10000 series with 100 labels with one sample", func(b *testing.B) {
		b.Skip("FOR NOW")
		resps := make([]*storepb.SeriesResponse, 0, 10000)
		for ser := 0; ser < cap(resps); ser++ {
			lset := make(labels.Labels, 0, 100)
			for lab := 0; lab < cap(lset); lab++ {
				lset = append(lset, labels.Label{Name: fmt.Sprintf("label-%v", lab), Value: "something"})
			}
			resps = append(resps, storeSeriesResponse(b, append(lset, labels.Label{Name: "cluster", Value: fmt.Sprintf("value-%v", ser)}), []sample{{t: 0, v: 0}}))
		}
		benchExport(b, resps)
	})
	b.Run("1000 series with 100 labels with 1000 samples", func(b *testing.B) {
		resps := make([]*storepb.SeriesResponse, 0, 1000)
		for ser := 0; ser < cap(resps); ser++ {
			lset := make(labels.Labels, 0, 100)
			for lab := 0; lab < cap(lset); lab++ {
				lset = append(lset, labels.Label{Name: fmt.Sprintf("label-%v", lab), Value: "something"})
			}

			samples := make([]sample, 0, 1000)
			for s := 0; s < cap(samples); s++ {
				samples = append(samples, sample{t: int64(s), v: float64(s)})
			}
			resps = append(resps, storeSeriesResponse(b, append(lset, labels.Label{Name: "cluster", Value: fmt.Sprintf("value-%v", ser)}), samples))
		}
		benchExport(b, resps)
	})
	b.Run("1 series with 100 labels with millions sample", func(b *testing.B) {
		b.Skip("FOR NOW")
		resps := make([]*storepb.SeriesResponse, 0, 1)
		lset := make(labels.Labels, 0, 100)
		for lab := 0; lab < cap(lset); lab++ {
			lset = append(lset, labels.Label{Name: fmt.Sprintf("label-%v", lab), Value: "something"})
		}

		samples := make([]sample, 0, 10e6)
		for s := 0; s < cap(samples); s++ {
			samples = append(samples, sample{t: int64(s), v: float64(s)})
		}
		resps = append(resps, storeSeriesResponse(b, append(lset, labels.Label{Name: "cluster", Value: "value-0"}), samples))
		benchExport(b, resps)
	})

}

func benchExport(b *testing.B, resp []*storepb.SeriesResponse) {
	tmpDir, err := ioutil.TempDir("", "export-bench")
	testutil.Ok(b, err)
	defer testutil.Ok(b, os.RemoveAll(tmpDir))

	// Create local grpc service.
	srv := grpc.NewServer()
	testServer := &testThanosSeriesServer{resps: resp}
	storepb.RegisterStoreServer(srv, testServer)

	list, err := net.Listen("tcp", "localhost:0")
	testutil.Ok(b, err)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = srv.Serve(list)
		wg.Done()
	}()

	var (
		ctx      = context.Background()
		logger   = log.NewNopLogger()
		matchers = "{something=\"doesnotmatter\"}"
	)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testutil.Ok(b, export(
			ctx,
			logger,
			matchers,
			series.Config{
				Type:     series.STOREAPI,
				Endpoint: list.Addr().String(),
			}, exporter.Config{
				Type: exporter.PARQUET,
				Storage: client.BucketConfig{
					Type: client.FILESYSTEM,
					Config: filesystem.Config{
						Directory: filepath.Join(tmpDir, fmt.Sprintf("%v", i)),
					},
				},
			},
			model.TimeOrDurationValue{},
			model.TimeOrDurationValue{},
			5*time.Minute,
			false,
		))
	}

	srv.GracefulStop()
	srv.Stop()
	wg.Wait()
}

type sample struct {
	t int64
	v float64
}

type testThanosSeriesServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	storepb.StoreServer

	resps []*storepb.SeriesResponse
}

func (s *testThanosSeriesServer) Series(_ *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	for _, resp := range s.resps {
		if err := srv.Send(resp); err != nil {
			return err
		}
	}
	return nil
}

// storeSeriesResponse creates test storepb.SeriesResponse that includes series with single chunk that stores all the given samples.
func storeSeriesResponse(t testing.TB, lset labels.Labels, smplChunks ...[]sample) *storepb.SeriesResponse {
	var s storepb.Series

	for _, l := range lset {
		s.Labels = append(s.Labels, labelpb.ZLabel{Name: l.Name, Value: l.Value})
	}

	for _, smpls := range smplChunks {
		c := chunkenc.NewXORChunk()
		a, err := c.Appender()
		testutil.Ok(t, err)

		for _, smpl := range smpls {
			a.Append(smpl.t, smpl.v)
		}

		ch := storepb.AggrChunk{
			MinTime: smpls[0].t,
			MaxTime: smpls[len(smpls)-1].t,
			Raw:     &storepb.Chunk{Type: storepb.Chunk_XOR, Data: c.Bytes()},
		}

		s.Chunks = append(s.Chunks, ch)
	}
	return storepb.NewSeriesResponse(&s)
}
