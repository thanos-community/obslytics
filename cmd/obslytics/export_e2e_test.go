// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/thanos-community/obslytics/pkg/input/promread"

	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/thanos-community/obslytics/pkg/input"
	"github.com/thanos-community/obslytics/pkg/input/storeapi"
	"github.com/thanos-community/obslytics/pkg/output"
	"github.com/thanos-community/obslytics/pkg/output/parquet"
	"github.com/thanos-io/thanos/pkg/http"

	"github.com/thanos-io/thanos/pkg/testutil"
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

func TestThanos_Parquet_e2e_storeapi(t *testing.T) {
	t.Parallel()

	s, err := e2e.NewScenario("e2e_test_thanos_parquet_storeapi")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, s))

	now := time.Now()

	// TODO(bwplotka): Allow clients to specify image directly via function args.
	testutil.Ok(t, os.Setenv("THANOS_IMAGE", "quay.io/thanos/thanos:v0.15.0"))
	prom, sidecar, err := e2ethanos.NewPrometheusWithSidecar(s.SharedDir(), s.NetworkName(), "1", defaultPromConfig("test", 0, "", ""), e2ethanos.DefaultPrometheusImage())
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(prom, sidecar))

	testutil.Ok(t, prom.WaitSumMetricsWithOptions(e2e.Greater(512), []string{"prometheus_tsdb_head_samples_appended_total"}, e2e.WaitMissingMetrics))

	logger := log.NewLogfmtLogger(os.Stderr)
	t.Run("export up metric to parquet file using thanos storeapi", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		api, err := storeapi.NewStoreAPIInput(logger, input.InputConfig{
			Endpoint:  sidecar.GRPCEndpoint(),
			TLSConfig: http.TLSConfig{InsecureSkipVerify: true},
		})
		testutil.Ok(t, err)

		b := bytes.Buffer{}
		testutil.Ok(t, export(
			ctx,
			logger,
			api,
			input.SeriesParams{
				Matchers: []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, "__name__", "prometheus_tsdb_head_series"),
				},
				MinTime: now,
				MaxTime: timestamp.Time(math.MaxInt64),
			},
			3*time.Second,
			parquet.NewOutput(),
			output.Params{Out: output.NopWriteCloser(&b)},
			true,
		))

		// TODO(bwplotka): Assert properly the actual result, vs what metric actually gives.
		testutil.Assert(t, 1610 <= b.Len()) // Output varies from 2197 to 1610, debug shows me sometimes two rows, is this expected?
		/*
			| instance        job     prometheus  replica  _sample_start  _sample_end  _min_time  _max_time  _count  _sum  _min  _max  |
			| localhost:9090  myself  test        0        10:30:33       10:30:36     10:30:35   10:30:35   1       0     0     0     |
			| localhost:9090  myself  test        0        10:30:36       10:30:39     10:30:36   10:30:36   1       377   377   377   |
		*/
	})
}

func TestProm_Parquet_e2e_promread(t *testing.T) {
	t.Parallel()

	s, err := e2e.NewScenario("e2e_test_prom_parquet_promread")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, s))

	prom, _, err := e2ethanos.NewPrometheus(s.SharedDir(), s.NetworkName(), defaultPromConfig("test", 0, "", ""), e2ethanos.DefaultPrometheusImage())
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(prom))

	testutil.Ok(t, prom.WaitSumMetricsWithOptions(e2e.Greater(512), []string{"prometheus_tsdb_head_samples_appended_total"}, e2e.WaitMissingMetrics))

	logger := log.NewLogfmtLogger(os.Stderr)

	t.Run("export up metric to parquet file using prometheus remoteread api", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		api, err := promread.NewRemoteReadInput(logger, input.InputConfig{
			Endpoint: "http://" + prom.HTTPEndpoint() + "/api/v1/read",
			TLSConfig: http.TLSConfig{
				InsecureSkipVerify: true,
			},
		},
		)

		testutil.Ok(t, err)

		b := bytes.Buffer{}
		testutil.Ok(t, export(
			ctx,
			logger,
			api,
			input.SeriesParams{
				Matchers: []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, "__name__", "prometheus_tsdb_head_samples_appended_total"),
				},
				MinTime: time.Now().Add(-6 * time.Hour),
				MaxTime: timestamp.Time(math.MaxInt64),
			},
			1*time.Second,
			parquet.NewOutput(),
			output.Params{Out: output.NopWriteCloser(&b)},
			true,
		))

		// TODO(bwplotka): Assert properly the actual result, vs what metric actually gives.
		testutil.Assert(t, 1610 <= b.Len()) // Output varies from 2197 to 1610, debug shows me sometimes two rows, is this expected?
		/*
			| instance        job     prometheus  replica  _sample_start  _sample_end  _min_time  _max_time  _count  _sum  _min  _max  |
			| localhost:9090  myself  test        0        00:30:21       00:30:22     04:30:21   04:30:21   1       0     0     0     |
			| localhost:9090  myself  test        0        00:30:22       00:30:23     04:30:22   04:30:22   1       353   353   353   |
		*/
	})
}
