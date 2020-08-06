package ingest

import (
	"fmt"
	"log"
	"time"

	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/store/storepb"

	"github.com/thanos-community/obslytics/pkg/dataframe"
)

func ExampleAggregatedIngestor() {
	opts := NewAggregationsOptions()
	opts.Sum.Enabled = true
	opts.Count.Enabled = true
	opts.Min.Enabled = true
	opts.Max.Enabled = true

	ai := AggregatedIngestor{
		Resolution:  30 * time.Minute,
		aggrOptions: opts,
	}
	for _, s := range sampleSeries() {
		ai.Ingest(s)
	}
	df := ai.Flush()

	fmt.Printf("Dataframe aggregated over %s: %+v", ai.Resolution, df)
	// Output: Dataframe aggregated over 30m0s: {}
}

func ExampleContinuousIngestor() {
	ai := AggregatedIngestor{Resolution: 30 * time.Minute}
	w := dummyWriter{}

	ci := ContinuousIngestor{
		aggr: ai,
		w: w,
	}

	for _, s := range sampleSeries() {
		ci.Ingest(s)
	}

	fmt.Printf("Dataframe aggregated over %s and sent to writer: %+v", ai.Resolution, w)
	// Output: Dataframe aggregated over 30m0s and sent to writer: {}
}

type dummyWriter struct{}

func (_ dummyWriter) Write(df dataframe.Dataframe) error {
	return nil
}

func (_ dummyWriter) Close() error {
	return nil
}

type sample struct {
	t int64
	v float64
}

func sampleSeries() []*storepb.Series {
	baseT := timestamp.FromTime(time.Date(2020, 5, 4, 10, 42, 00, 0, time.UTC))
	metricLabelsBase := []storepb.Label{{Name: "__name__", Value: "net_conntrack_dialer_conn_attempted_total"}}

	series1Labels := append(metricLabelsBase, storepb.Label{Name: "dialer_name", Value: "prometheus"})
	minute := int64(time.Minute)
	series1Samples := []sample{{baseT, 0}, {baseT + 15*minute, 1}, {baseT + 30*minute, 2}, {baseT + 45*minute, 2}}

	series2Labels := append(metricLabelsBase, storepb.Label{Name: "dialer_name", Value: "default"})
	series2Samples := []sample{{baseT, 0}, {baseT + 15*minute, 1}, {baseT + 60*minute, 1}, {baseT + 75*minute, 2}}

	series3Labels := append(metricLabelsBase, storepb.Label{Name: "dialer_name", Value: "alertmanager"})
	series3Samples := []sample{{baseT, 0}, {baseT + 30*minute, 1}, {baseT + 60*minute, 2}, {baseT + 75*minute, 2}}

	seriesSlice := []*storepb.Series{
		buildSampleSeries(series1Labels, series1Samples),
		buildSampleSeries(series2Labels, series2Samples),
		buildSampleSeries(series3Labels, series3Samples),
	}
	return seriesSlice
}

func buildSampleSeries(lset []storepb.Label, smplChunks ...[]sample) *storepb.Series {
	var s storepb.Series

	s.Labels = lset

	for _, smpls := range smplChunks {
		c := chunkenc.NewXORChunk()
		a, err := c.Appender()
		if err != nil {
			log.Fatal(err)
		}

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
	return &s
}
