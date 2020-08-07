// This file implements dataframe-related interfaces to expose the ingested data
// to the writers.
package ingest

import (
	"github.com/prometheus/prometheus/pkg/labels"

	"github.com/thanos-community/obslytics/pkg/dataframe"
)

// implements Dataframe
type AggrDf struct {
	schema           dataframe.Schema
	seriesRecordSets map[uint64]*SeriesRecordSet
	seriesOrder      []uint64
}

// Initiate new recordset for specific label.
func (df *AggrDf) addRecordSet(ls labels.Labels) *SeriesRecordSet {
	rs := &SeriesRecordSet{Labels: ls, Records: make([]Record, 0)}
	hash := ls.Hash()
	df.seriesRecordSets[hash] = rs
	df.seriesOrder = append(df.seriesOrder, hash)
	return rs
}

func (df AggrDf) Schema() dataframe.Schema {
	return df.schema
}

func (df AggrDf) RowsIterator() dataframe.RowsIterator {
	rs := make([]SeriesRecordSet, 0, len(df.seriesRecordSets))
	for _, v := range df.seriesOrder {
		rs = append(rs, *df.seriesRecordSets[v])
	}
	return &aggrDfIterator{seriesRecordSets: rs, schema: df.schema, seriesPos: 0, recordPos: -1}
}

// implements dataframe.RowIterator
type aggrDfIterator struct {
	seriesRecordSets []SeriesRecordSet
	schema           dataframe.Schema
	seriesPos        int
	recordPos        int
}

func (i *aggrDfIterator) Next() bool {
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

func (i *aggrDfIterator) At() dataframe.Row {
	s := i.seriesRecordSets[i.seriesPos]
	ret := make([]interface{}, 0, len(i.schema))
	vals := s.Records[i.recordPos].Values
	for _, c := range i.schema {
		ret = append(ret, vals[c.Name])
	}
	return ret
}

// Set of records for specific labels values.
type SeriesRecordSet struct {
	Labels  labels.Labels
	Records []Record
}

// A single instance of values for specific sample
type Record struct {
	// // what time the sample starts at
	// SampleStart time.Time
	// // what time the sample ends at
	// SampleEnd time.Time
	// // what was the minimal time in the timeseries withing the sample
	// MinTime time.Time
	// // what was the maximal time in the timeseries withing the sample
	// MaxTime time.Time
	// // mapping of aggregated values
	Values map[string]interface{}
}
