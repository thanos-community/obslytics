package ingest

import (
	"fmt"
	"io"
	"os"
	"text/tabwriter"
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

	a := NewAggregator(30*time.Minute, opts)

	for _, s := range sampleSeries() {
		err := a.Ingest(s)
		if err != nil {
			fmt.Print(err)
			return
		}
	}
	a.Finalize()
	df := a.Flush()

	w := newExampleWriter(os.Stdout)
	defer w.Close()

	w.Write(df)
	// Output:
	// | sample_start  sample_end  min_time  max_time  _count  _sum  _min  _max  |
	// | 10:30:00      11:00:00    11:04:02  11:04:02  1       2     2     2     |
	// | 11:00:00      11:30:00    11:19:02  11:19:02  1       2     2     2     |
	// | 10:30:00      11:00:00    10:34:02  10:34:02  1       1     1     1     |
	// | 11:00:00      11:30:00    11:04:02  11:19:02  2       4     2     2     |
	// | 10:30:00      11:00:00    10:34:02  10:49:02  2       4     2     2     |
}

func ExampleContinuousIngestor() {
	opts := NewAggregationsOptions()
	opts.Max.Enabled = true
	a := NewAggregator(30*time.Minute, opts)
	w := newExampleWriter(os.Stdout)
	defer w.Close()

	ci := ContinuousIngestor{
		aggr: a,
		w:    w,
	}

	for _, s := range sampleSeries() {
		err := ci.Ingest(s)
		if err != nil {
			fmt.Print(err)
			return
		}
	}
	ci.Finalize()
	// Output:
	// | sample_start  sample_end  min_time  max_time  _max  |
	// | 10:30:00      11:00:00    11:04:02  11:04:02  2     |
	// | 10:30:00      11:00:00    10:34:02  10:34:02  1     |
	// | 10:30:00      11:00:00    10:34:02  10:49:02  2     |
	// | 11:00:00      11:30:00    11:19:02  11:19:02  2     |
	// | 11:00:00      11:30:00    11:04:02  11:19:02  2     |
}

type sample struct {
	t time.Time
	v float64
}

func sampleSeries() []*storepb.Series {
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
			fmt.Print("Error building a sample series", err)
			return &s
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
	return &s
}

// Implements output.Writer
type exampleWriter struct {
	w       *tabwriter.Writer
	started bool
}

func newExampleWriter(w io.Writer) *exampleWriter {
	tabw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	return &exampleWriter{w: tabw}
}

func (w *exampleWriter) Write(df dataframe.Dataframe) error {
	if !w.started {
		w.PrintHeader(df)
		w.started = true
	}
	i := df.RowsIterator()
	for i.Next() {
		w.PrintRow(df.Schema(), i.At())
	}
	return nil
}

func (w *exampleWriter) PrintHeader(df dataframe.Dataframe) {
	// Adding | <-   -> | around the lines to avoid dealing with training spaces
	// in example output checking
	fmt.Fprint(w.w, "| ")
	for _, c := range df.Schema() {
		fmt.Fprintf(w.w, "%s\t", c.Name)
	}
	fmt.Fprint(w.w, "|\n")
}

func (w *exampleWriter) PrintRow(s dataframe.Schema, r dataframe.Row) {
	fmt.Fprint(w.w, "| ")
	for i, cell := range r {
		c := s[i]
		switch c.Type {
		case dataframe.TypeString:
			fmt.Fprintf(w.w, "%s\t", cell)
		case dataframe.TypeFloat:
			v := cell.(float64)
			fmt.Fprintf(w.w, "%.0f\t", v)
		case dataframe.TypeUint:
			v := cell.(uint64)
			fmt.Fprintf(w.w, "%d\t", v)
		case dataframe.TypeTime:
			v := cell.(time.Time)
			fmt.Fprintf(w.w, "%s\t", v.Format("15:04:05"))
		default:
			fmt.Fprintf(w.w, "%s\t", cell)
		}
	}
	fmt.Fprint(w.w, "|\n")
}

func (w exampleWriter) Close() error {
	err := w.w.Flush()
	if err != nil {
		return err
	}
	return nil
}
