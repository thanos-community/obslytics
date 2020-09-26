package promread

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/thanos-community/obslytics/pkg/series"
	http_util "github.com/thanos-io/thanos/pkg/http"
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

func TestRemoteReadInput_Open(t *testing.T) {
	t.Parallel()

	s, err := e2e.NewScenario("unit_test_remoteread")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, s))

	prom, _, err := e2ethanos.NewPrometheus(s.SharedDir(), s.NetworkName(), defaultPromConfig("test", 0, "", ""), e2ethanos.DefaultPrometheusImage())
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(prom))

	testutil.Ok(t, prom.WaitSumMetricsWithOptions(e2e.Greater(512), []string{"prometheus_tsdb_head_samples_appended_total"}, e2e.WaitMissingMetrics))

	t.Run("test remote read input", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		inConfig := series.Config{
			Endpoint: "http://" + prom.HTTPEndpoint() + "/api/v1/read",
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
		testutil.Assert(t, "prometheus_tsdb_head_series" == currentSeries.Labels().Get("__name__"))

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
