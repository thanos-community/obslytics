// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/efficientgo/e2e"
	e2emon "github.com/efficientgo/e2e/monitoring"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/exthttp"

	"github.com/thanos-community/obslytics/pkg/dataframe"
	"github.com/thanos-community/obslytics/pkg/exporter"
	"github.com/thanos-community/obslytics/pkg/exporter/parquet"
	"github.com/thanos-community/obslytics/pkg/series"
	"github.com/thanos-community/obslytics/pkg/series/promread"
	"github.com/thanos-community/obslytics/pkg/series/storeapi"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

// defaultPromConfig returns Prometheus config that sets Prometheus to:
// * expose 2 external labels, source and replica.
// * scrape fake target. This will produce up == 0 metric which we can assert on.
// * optionally remote write endpoint to write into.
func defaultPromConfig(name string, replica int, remoteWriteEndpoint, ruleFile string) string {
	config := fmt.Sprintf(`
global:
  external_labels:
    prometheus: %v
    replica: %v
scrape_configs:
- job_name: 'myself'
  # Quick scrapes for test purposes.
  scrape_interval: 1s
  scrape_timeout: 1s
  static_configs:
  - targets: ['localhost:9090']
`, name, replica)

	if remoteWriteEndpoint != "" {
		config = fmt.Sprintf(`
%s
remote_write:
- url: "%s"
  # Don't spam receiver on mistake.
  queue_config:
    min_backoff: 2s
    max_backoff: 10s
`, config, remoteWriteEndpoint)
	}

	if ruleFile != "" {
		config = fmt.Sprintf(`
%s
rule_files:
-  "%s"
`, config, ruleFile)
	}

	return config
}

func exportToParquet(t *testing.T, ctx context.Context, r series.Reader, bkt objstore.Bucket, mint, maxt time.Time, fileName string) {
	s, err := r.Read(ctx, series.Params{
		Matchers: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "__name__", "prometheus_tsdb_head_series"),
		},
		MinTime: mint,
		MaxTime: maxt,
	})
	testutil.Ok(t, err)

	df, err := dataframe.FromSeries(s, 3*time.Second, func(o *dataframe.AggrsOptions) {
		// TODO(inecas): Expose the enabled aggregations via flag.
		o.Count.Enabled = true
		o.Sum.Enabled = true
		o.Min.Enabled = true
		o.Max.Enabled = true
	})
	testutil.Ok(t, err)

	t.Log("Dataframe:", dataframe.ToString(df))
	testutil.Ok(t, exporter.New(parquet.NewEncoder(), fileName, bkt).Export(ctx, df))
}

func TestRemoteReadAndThanos_Parquet_e2e(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("export")
	testutil.Ok(t, err)
	t.Cleanup(e.Close)

	mint := time.Now()
	testutil.Ok(t, os.Setenv("THANOS_IMAGE", "quay.io/thanos/thanos:v0.29.0"))

	prom, sidecar := e2ethanos.NewPrometheusWithSidecar(e, "prom", defaultPromConfig("test", 0, "", ""), "", e2ethanos.DefaultPrometheusImage(), "")
	testutil.Ok(t, e2e.StartAndWaitReady(prom, sidecar))

	testutil.Ok(t, prom.WaitSumMetricsWithOptions(e2emon.Greater(512), []string{"prometheus_tsdb_head_samples_appended_total"}, e2emon.WaitMissingMetrics()))
	maxt := time.Now()

	logger := log.NewLogfmtLogger(os.Stderr)
	bkt := objstore.NewInMemBucket()

	t.Run("export metric from RemoteRead to parquet file", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		api, err := promread.NewSeries(logger, series.Config{
			Endpoint:  "http://" + prom.Endpoint("http") + "/api/v1/read",
			TLSConfig: exthttp.TLSConfig{InsecureSkipVerify: true},
		})
		testutil.Ok(t, err)

		exportToParquet(t, ctx, api, bkt, mint, maxt, "something/yolo.parquet")
	})

	t.Run("export metric from StoreAPI to parquet file", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		api, err := storeapi.NewSeries(logger, series.Config{
			Endpoint:  sidecar.Endpoint("grpc"),
			TLSConfig: exthttp.TLSConfig{InsecureSkipVerify: true},
		})
		testutil.Ok(t, err)

		exportToParquet(t, ctx, api, bkt, mint, maxt, "something/yolo2.parquet")
	})

	result, err := bkt.Get(context.Background(), "something/yolo.parquet")
	testutil.Ok(t, err)

	resultBytes1, err := io.ReadAll(result)
	testutil.Ok(t, err)
	testutil.Ok(t, result.Close())

	// TODO(bwplotka): Assert properly the actual result, vs what metric actually gives.
	testutil.Assert(t, 1610 <= len(resultBytes1)) // Output varies from 2197 to 1610, debug shows me sometimes two rows, is this expected?

	result, err = bkt.Get(context.Background(), "something/yolo2.parquet")
	testutil.Ok(t, err)

	resultBytes2, err := io.ReadAll(result)
	testutil.Ok(t, err)
	testutil.Ok(t, result.Close())

	// TODO(bwplotka): Assert properly the actual result, vs what metric actually gives.
	testutil.Assert(t, 1610 <= len(resultBytes2)) // Output varies from 2197 to 1610, debug shows me sometimes two rows, is this expected?

	// Data from both StoreAPI and Remote Read should be the same.
	testutil.Equals(t, resultBytes1, resultBytes2)
}
