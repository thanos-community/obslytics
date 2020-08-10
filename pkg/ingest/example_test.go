package ingest

import (
	"fmt"
	"os"
	"time"

	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/store/storepb"

	"github.com/thanos-community/obslytics/pkg/input"
	"github.com/thanos-community/obslytics/pkg/input/storeapi"
	"github.com/thanos-community/obslytics/pkg/output/example"
)

func ExampleAggregator() {
	opts := NewAggregationsOptions()
	opts.Sum.Enabled = true
	opts.Count.Enabled = true
	opts.Min.Enabled = true
	opts.Max.Enabled = true

	a := NewAggregator(30*time.Minute, opts)

	for _, s := range sampleSeries() {
		err := a.Ingest(s)
		if err != nil {
			fmt.Print(err)
			return
		}
	}
	a.Finalize()
	df, _ := a.Flush()

	w := example.NewExampleWriter(os.Stdout)
	defer w.Close()

	w.Write(df)
	// Unordered output:
	// | dialer_name   _sample_start  _sample_end  _min_time  _max_time  _count  _sum  _min  _max  |
	// | default       10:30:00       11:00:00     11:04:02   11:04:02   1       2     2     2     |
	// | default       11:00:00       11:30:00     11:19:02   11:19:02   1       2     2     2     |
	// | alertmanager  10:30:00       11:00:00     10:34:02   10:34:02   1       1     1     1     |
	// | alertmanager  11:00:00       11:30:00     11:04:02   11:19:02   2       4     2     2     |
	// | prometheus    10:30:00       11:00:00     10:34:02   10:49:02   2       4     2     2     |

}

func ExampleProcess() {
	opts := NewAggregationsOptions()
	opts.Max.Enabled = true
	a := NewAggregator(30*time.Minute, opts)
	w := example.NewExampleWriter(os.Stdout)
	defer w.Close()

	for _, s := range sampleSeries() {
		err := Process(s, a, w)
		if err != nil {
			fmt.Print(err)
			return
		}
	}
	err := ProcessFinalize(a, w)
	if err != nil {
		fmt.Print(err)
		return
	}
	// Unordered output:
	// | dialer_name   _sample_start  _sample_end  _min_time  _max_time  _max  |
	// | default       10:30:00       11:00:00     11:04:02   11:04:02   2     |
	// | alertmanager  10:30:00       11:00:00     10:34:02   10:34:02   1     |
	// | prometheus    10:30:00       11:00:00     10:34:02   10:49:02   2     |
	// | default       11:00:00       11:30:00     11:19:02   11:19:02   2     |
	// | alertmanager  11:00:00       11:30:00     11:04:02   11:19:02   2     |
}

func ExampleProcessAll() {
	opts := NewAggregationsOptions()
	opts.Max.Enabled = true
	a := NewAggregator(30*time.Minute, opts)
	w := example.NewExampleWriter(os.Stdout)
	defer w.Close()

	i := NewSeriesIterator(sampleSeries())
	err := ProcessAll(i, a, w)
	if err != nil {
		fmt.Print(err)
		return
	}
	// Unordered output:
	// | dialer_name   _sample_start  _sample_end  _min_time  _max_time  _max  |
	// | default       10:30:00       11:00:00     11:04:02   11:04:02   2     |
	// | alertmanager  10:30:00       11:00:00     10:34:02   10:34:02   1     |
	// | prometheus    10:30:00       11:00:00     10:34:02   10:49:02   2     |
	// | default       11:00:00       11:30:00     11:19:02   11:19:02   2     |
	// | alertmanager  11:00:00       11:30:00     11:04:02   11:19:02   2     |
}

type seriesIterator struct {
	series []input.Series
	pos    int
}

func NewSeriesIterator(series []input.Series) *seriesIterator {
	return &seriesIterator{series: series, pos: -1}
}

func (i *seriesIterator) Next() bool {
	if i.pos == len(i.series)-1 {
		return false
	}
	i.pos++
	return true
}

func (i *seriesIterator) At() input.Series {
	return i.series[i.pos]
}

type sample struct {
	t time.Time
	v float64
}

func sampleSeries() []input.Series {
	baseT := time.Date(2020, 5, 4, 10, 4, 2, 0, time.UTC)
	metricLabelsBase := []storepb.Label{{Name: "__name__", Value: "net_conntrack_dialer_conn_attempted_total"}}

	series1Labels := append(metricLabelsBase, storepb.Label{Name: "dialer_name", Value: "prometheus"})
	minute := time.Minute
	series1Samples := []sample{
		{baseT, 0}, {baseT.Add(15 * minute), 1}, {baseT.Add(30 * minute), 2}, {baseT.Add(45 * minute), 2},
	}

	series2Labels := append(metricLabelsBase, storepb.Label{Name: "dialer_name", Value: "default"})
	series2Samples := []sample{
		{baseT, 0}, {baseT.Add(15 * minute), 1}, {baseT.Add(60 * minute), 2}, {baseT.Add(75 * minute), 2},
	}

	series3Labels := append(metricLabelsBase, storepb.Label{Name: "dialer_name", Value: "alertmanager"})
	series3Samples := []sample{
		{baseT, 0}, {baseT.Add(30 * minute), 1}, {baseT.Add(60 * minute), 2}, {baseT.Add(75 * minute), 2},
	}

	seriesSlice := []input.Series{
		buildSampleSeries(series1Labels, series1Samples),
		buildSampleSeries(series2Labels, series2Samples),
		buildSampleSeries(series3Labels, series3Samples),
	}
	return seriesSlice
}

func buildSampleSeries(lset []storepb.Label, smplChunks ...[]sample) input.Series {
	var s storepb.Series

	s.Labels = lset

	for _, smpls := range smplChunks {
		c := chunkenc.NewXORChunk()
		a, err := c.Appender()
		if err != nil {
			fmt.Print("Error building a sample series", err)
			return nil
		}

		for _, smpl := range smpls {
			a.Append(timestamp.FromTime(smpl.t), smpl.v)
		}

		ch := storepb.AggrChunk{
			MinTime: timestamp.FromTime(smpls[0].t),
			MaxTime: timestamp.FromTime(smpls[len(smpls)-1].t),
			Raw:     &storepb.Chunk{Type: storepb.Chunk_XOR, Data: c.Bytes()},
		}

		s.Chunks = append(s.Chunks, ch)
	}
	return storeapi.StoreSeries{StoreS: &s}
}
