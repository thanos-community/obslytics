// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package promread

import (
	"context"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/efficientgo/e2e"
	e2emon "github.com/efficientgo/e2e/monitoring"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	http_util "github.com/thanos-io/thanos/pkg/exthttp"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"

	"github.com/thanos-community/obslytics/pkg/series"
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

func TestRemoteReadInput_Open(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("remote-read")
	testutil.Ok(t, err)
	t.Cleanup(e.Close)

	testutil.Ok(t, os.Setenv("THANOS_IMAGE", "quay.io/thanos/thanos:v0.29.0"))

	prom, sidecar := e2ethanos.NewPrometheusWithSidecar(e, "prom", defaultPromConfig("test", 0, "", ""), "", e2ethanos.DefaultPrometheusImage(), "")
	testutil.Ok(t, e2e.StartAndWaitReady(prom, sidecar))

	testutil.Ok(t, prom.WaitSumMetricsWithOptions(e2emon.Greater(512), []string{"prometheus_tsdb_head_samples_appended_total"}, e2emon.WaitMissingMetrics()))

	t.Run("test remote read input", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		inConfig := series.Config{
			Endpoint: "http://" + prom.Endpoint("http") + "/api/v1/read",
			TLSConfig: http_util.TLSConfig{
				InsecureSkipVerify: true,
			},
		}
		remoteReadInput, err := NewSeries(nil, inConfig)
		testutil.Ok(t, err)

		minT := time.Now().Add(-6 * time.Hour)
		maxT := timestamp.Time(math.MaxInt64)

		inSeriesParams := series.Params{
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "prometheus_tsdb_head_series"),
			},
			MinTime: minT,
			MaxTime: maxT,
		}

		inSeriesIter, err := remoteReadInput.Read(ctx, inSeriesParams)
		testutil.Ok(t, err)

		// Go to the first series.
		testutil.Assert(t, inSeriesIter.Next() == true)

		currentSeries := inSeriesIter.At()
		currentSeriesChunkIter := currentSeries.Iterator()

		// Test for "__name__" label value.
		testutil.Assert(t, currentSeries.Labels().Get("__name__") == "prometheus_tsdb_head_series")

		// Go to the first sample.
		testutil.Assert(t, currentSeriesChunkIter.Next() == true)

		// Current Sample.
		metricTimestamp, metricValue := currentSeriesChunkIter.At()
		// Check if metric timestamp is valid.
		testutil.Assert(t, metricTimestamp > 0)
		// The first metric value is 0.
		testutil.Assert(t, metricValue == 0)

		// Test if iteration inside chunk works, go to next sample.
		testutil.Assert(t, currentSeriesChunkIter.Next())
		metricTimestamp, metricValue = currentSeriesChunkIter.At()
		// Check if metric timestamp is valid.
		testutil.Assert(t, metricTimestamp > 0)
		// The second metric value is non 0.
		testutil.Assert(t, metricValue > 0)

		// There are only two metric values inside the chunk, so this should return false.
		testutil.Assert(t, !currentSeriesChunkIter.Next())

		// There is only one series, so this should return false.
		testutil.Assert(t, !inSeriesIter.Next())

	})
}
