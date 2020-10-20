package dataframe

import (
	"sort"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-community/obslytics/pkg/series"
)

// AggrOption defines options for a single aggregation.
type AggrOption struct {
	// Should the aggregation be used?
	Enabled bool
	// Column to store the aggregation at.
	Column string
}

// AggrsOptions ia a collections of aggregations-related options. Determines
// what aggregations are enabled etc..
type AggrsOptions struct {
	// Function to suggest the time of the first sample based on the resolution
	// and the initial time of a series. By default, it uses time.Truncate(resolution)
	// to normalize against the beginning of epoch.
	initSampleTimeFunc func(time.Duration, time.Time) time.Time

	Sum   AggrOption
	Count AggrOption
	Min   AggrOption
	Max   AggrOption
}

// By default, all aggregations are disabled and target columns set with `_` prefix.
func defaultSeriesAggrsOptions() AggrsOptions {
	return AggrsOptions{
		initSampleTimeFunc: func(res time.Duration, t time.Time) time.Time {
			return t.Truncate(res)
		},

		Sum:   AggrOption{Column: "_sum"},
		Count: AggrOption{Column: "_count"},
		Min:   AggrOption{Column: "_min"},
		Max:   AggrOption{Column: "_max"},
	}
}

type AggrOptionFunc func(*AggrsOptions)

func evalOptions(optFuncs []AggrOptionFunc) *AggrsOptions {
	opt := defaultSeriesAggrsOptions()
	for _, o := range optFuncs {
		o(&opt)
	}
	return &opt
}

type aggregatedSeries struct {
	labels      labels.Labels
	hash        uint64
	sampleStart time.Time
	sampleEnd   time.Time
	minTime     time.Time
	maxTime     time.Time
	count       uint64
	min         float64
	max         float64
	sum         float64
}

type seriesAggregator struct {
	df         *seriesDataframe
	resolution time.Duration
	options    AggrsOptions
}

// IteratorFromSeries returns iterator that produce dataframe for every series.
// TODO(bwplotka): Dataframe allows us to do bit more streaming approach. Consider this.
func FromSeries(r series.Set, resolution time.Duration, opts ...AggrOptionFunc) (Dataframe, error) {
	defer r.Close()

	// TODO(bwplotka): What if resolution is 0?
	a := &seriesAggregator{
		resolution: resolution,
		options:    *evalOptions(opts),
		df:         &seriesDataframe{seriesRecordSets: make(map[uint64]*seriesRecordSet)},
	}

	var activeSeries *aggregatedSeries
	var currentHash uint64
	for r.Next() {
		s := r.At()
		ls := s.Labels()
		seriesHash := ls.Hash()

		i := s.Iterator()
		if !i.Next() {
			// Series without samples.
			if err := i.Err(); err != nil {
				return nil, err
			}
			continue
		}

		if currentHash != seriesHash {
			if activeSeries != nil {
				_ = a.finalizeSample(activeSeries, activeSeries.sampleEnd)
			}

			mint, _ := i.At()
			sampleStart := a.options.initSampleTimeFunc(resolution, timestamp.Time(mint))
			sampleEnd := sampleStart.Add(resolution)

			activeSeries = &aggregatedSeries{labels: ls, hash: seriesHash, sampleStart: sampleStart, sampleEnd: sampleEnd}
			currentHash = seriesHash
		}

		if !i.Seek(timestamp.FromTime(activeSeries.sampleStart)) {
			// No chunks after the sampleStart to process.
			if err := i.Err(); err != nil {
				return nil, err
			}
			continue
		}

		if err := a.ingestSamples(activeSeries, i); err != nil {
			return nil, errors.Wrap(err, "aggregating samples")
		}
	}

	if activeSeries != nil {
		_ = a.finalizeSample(activeSeries, activeSeries.sampleEnd)
	}

	// We postpone the schema calculation to the time just before sending the df out
	// so that we can use the ingested data to determine the labels to be exported.
	a.df.schema = a.getSchema()
	return a.df, r.Err()
}

// ingestSamples ingests samples provided via an iterator for single series. We
// assume the iterator returns values ordered by the timestamp.
// The iterator is expected to already be at the point of the first sample after as.sampleStart.
func (a *seriesAggregator) ingestSamples(as *aggregatedSeries, i chunkenc.Iterator) error {
	var (
		ts int64
		v  float64
		t  time.Time
	)
	for {
		ts, v = i.At()
		t = timestamp.Time(ts)
		if t.Before(as.sampleStart) {
			return errors.Errorf("Chunk timestamp %s is less than the sampleStart %s", t, as.sampleStart)
		}
		if t.After(as.sampleEnd) {
			as = a.finalizeSample(as, t)
		}

		if as.count == 0 {
			as.minTime = t
			as.maxTime = t
			as.min = v
			as.max = v
		}
		if as.maxTime.After(t) {
			return errors.Errorf("Incoming chunks are not sorted by timestamp: expected %s after %s", t, as.maxTime)
		}
		as.maxTime = t
		as.count += 1
		as.sum += v
		if as.max < v {
			as.max = v
		}
		if as.min > v {
			as.min = v
		}
		if !i.Next() {
			return i.Err()
		}
	}
}

// finalizeSample adds the active aggregated series into the final dataframe when we've reached the
// sample end time. Returns pointer to a new instance of the aggregatedSeries.
func (a *seriesAggregator) finalizeSample(as *aggregatedSeries, nextT time.Time) *aggregatedSeries {
	if as.count > 0 {
		a.df.addSeries(as, a.options)
	}

	// calculate the next sample cycle to contain the nextT time. First calculate how many
	// whole resolution cycles are between current sampleStart and nextT and then add
	// those cycles to the current sampleStart.
	nextSampleCycle := (nextT.Unix() - as.sampleStart.Unix()) / (int64)(a.resolution/time.Second)
	nextSampleStart := as.sampleStart.Add((time.Duration(nextSampleCycle)) * a.resolution)

	return &aggregatedSeries{
		labels:      as.labels,
		hash:        as.hash,
		sampleStart: nextSampleStart,
		sampleEnd:   nextSampleStart.Add(a.resolution),
	}
}

// getLabelNames assumes all series having the same labels and just takes the first
// series to get the label names.
// The returned strings are always sorted alphabetically.
func (a *seriesAggregator) getLabelNames() []string {
	// TODO(inecas): The labels can be changing over time: to workaround
	// this problem, it could have to add an option to explicitly provide the list
	// of labels we want to export and fill in NULLs in case the label would be missing.
	var (
		ls  labels.Labels
		ret []string
	)
	// Create a union set (map) of all the label names.
	lsMap := make(map[string]string)
	for _, s := range a.df.seriesRecordSets {
		for labelName, labelValue := range s.Labels.Map() {
			lsMap[labelName] = labelValue
		}
	}
	ls = labels.FromMap(lsMap)

	for _, l := range ls {
		if l.Name == "__name__" {
			continue
		}
		ret = append(ret, l.Name)
	}
	sort.Strings(ret)
	return ret
}

func (a *seriesAggregator) getSchema() Schema {
	ao := a.options
	schema := Schema{}

	for _, l := range a.getLabelNames() {
		schema = append(schema, Column{Name: l, Type: TypeString})
	}

	timeColumns := []Column{
		{Name: "_sample_start", Type: TypeTime},
		{Name: "_sample_end", Type: TypeTime},
		{Name: "_min_time", Type: TypeTime},
		{Name: "_max_time", Type: TypeTime},
	}
	schema = append(schema, timeColumns...)

	if ao.Count.Enabled {
		schema = append(schema, Column{Name: ao.Count.Column, Type: TypeUint})
	}
	if ao.Sum.Enabled {
		schema = append(schema, Column{Name: ao.Sum.Column, Type: TypeFloat})
	}
	if ao.Min.Enabled {
		schema = append(schema, Column{Name: ao.Min.Column, Type: TypeFloat})
	}
	if ao.Max.Enabled {
		schema = append(schema, Column{Name: ao.Max.Column, Type: TypeFloat})
	}

	return schema
}

// seriesDataframe implements dataframe.Dataframe.
type seriesDataframe struct {
	schema           Schema
	seriesRecordSets map[uint64]*seriesRecordSet
	seriesOrder      []uint64
}

func (df *seriesDataframe) addSeries(as *aggregatedSeries, opts AggrsOptions) {
	rs, ok := df.seriesRecordSets[as.hash]
	if !ok {
		rs = df.addRecordSet(as.labels)
	}

	vals := map[string]interface{}{
		"_sample_start": as.sampleStart,
		"_sample_end":   as.sampleEnd,
		"_min_time":     as.minTime,
		"_max_time":     as.maxTime,
	}

	for _, l := range as.labels {
		if l.Name == "__name__" {
			continue
		}
		vals[l.Name] = l.Value
	}

	if opts.Count.Enabled {
		vals[opts.Count.Column] = as.count
	}
	if opts.Sum.Enabled {
		vals[opts.Sum.Column] = as.sum
	}
	if opts.Min.Enabled {
		vals[opts.Min.Column] = as.min
	}
	if opts.Max.Enabled {
		vals[opts.Max.Column] = as.max
	}
	rs.Records = append(rs.Records, Record{Values: vals})
}

// Initiate new recordset for specific label.
func (df *seriesDataframe) addRecordSet(ls labels.Labels) *seriesRecordSet {
	rs := &seriesRecordSet{Labels: ls, Records: make([]Record, 0)}
	hash := ls.Hash()
	df.seriesRecordSets[hash] = rs
	df.seriesOrder = append(df.seriesOrder, hash)
	return rs
}

func (df seriesDataframe) Schema() Schema {
	return df.schema
}

func (df seriesDataframe) RowsIterator() RowsIterator {
	rs := make([]seriesRecordSet, 0, len(df.seriesRecordSets))
	for _, v := range df.seriesOrder {
		rs = append(rs, *df.seriesRecordSets[v])
	}
	return &seriesDataframeRowIterator{seriesRecordSets: rs, schema: df.schema, seriesPos: 0, recordPos: -1}
}

// seriesDataframeRowIterator implements dataframe.RowIterator.
type seriesDataframeRowIterator struct {
	seriesRecordSets []seriesRecordSet
	schema           Schema
	seriesPos        int
	recordPos        int
}

func (i *seriesDataframeRowIterator) Next() bool {
	if len(i.seriesRecordSets) == 0 {
		return false
	}
	s := i.seriesRecordSets[i.seriesPos]

	if i.recordPos < len(s.Records)-1 {
		i.recordPos += 1
		return true
	}

	if i.seriesPos < len(i.seriesRecordSets)-1 {
		i.seriesPos += 1
		i.recordPos = 0
		return true
	}

	return false
}

func (i *seriesDataframeRowIterator) At() Row {
	s := i.seriesRecordSets[i.seriesPos]
	ret := make([]interface{}, 0, len(i.schema))
	vals := s.Records[i.recordPos].Values
	for _, c := range i.schema {
		ret = append(ret, vals[c.Name])
	}
	return ret
}

// seriesRecordSet is a set of records for specific labels values.
type seriesRecordSet struct {
	Labels  labels.Labels
	Records []Record
}

// Record is a single instance of values for specific sample.
type Record struct {
	Values map[string]interface{}
}
