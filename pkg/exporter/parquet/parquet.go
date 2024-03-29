// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package parquet

import (
	"fmt"
	"io"
	"time"

	"github.com/efficientgo/core/errors"
	parquetwriter "github.com/xitongsys/parquet-go-source/writerfile"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"

	"github.com/thanos-community/obslytics/pkg/dataframe"
	"github.com/thanos-community/obslytics/pkg/exporter"
)

// Compile-time check if parquet Encoder implements exporter.Encoder interface.
var _ exporter.Encoder = &Encoder{}

type Encoder struct{}

func NewEncoder() *Encoder {
	return &Encoder{}
}

func (e *Encoder) Encode(w io.Writer, df dataframe.Dataframe) (err error) {
	parqf := parquetwriter.NewWriterFile(w)
	parqw, err := initCSVWriter(parqf, df)
	if err != nil {
		return errors.Wrap(err, "initializing the schema")
	}
	defer func() {
		if serr := parqw.WriteStop(); serr != nil && err == nil {
			err = serr
		}
	}()

	i := df.RowsIterator()
	s := df.Schema()
	for i.Next() {
		r := i.At()
		d := make([]interface{}, 0, len(r))
		for i, cell := range r {
			c := s[i]
			switch c.Type {
			case dataframe.TypeString:
				d = append(d, cell)
			case dataframe.TypeFloat:
				d = append(d, cell)
			case dataframe.TypeUint:
				v := cell.(uint64)
				// There has been some issue with uint and parquet-go, typecasting to int64 instead.
				d = append(d, int64(v))
			case dataframe.TypeTime:
				v := cell.(time.Time)
				d = append(d, v.Unix()*1000)
			default:
				d = append(d, cell)
			}
		}
		if err := parqw.Write(d); err != nil {
			return errors.Wrap(err, "writing a row")
		}
	}
	return nil
}

func initCSVWriter(parqf source.ParquetFile, df dataframe.Dataframe) (*writer.CSVWriter, error) {
	schema := df.Schema()
	pqSchema := make([]string, 0, len(schema))
	for _, c := range schema {
		var pqType string
		switch c.Type {
		case dataframe.TypeString:
			pqType = "BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"
		case dataframe.TypeFloat:
			pqType = "DOUBLE"
		case dataframe.TypeUint:
			pqType = "INT64, convertedtype=UINT_64"
		case dataframe.TypeTime:
			pqType = "INT64, convertedtype=TIMESTAMP_MILLIS"
		}
		pqSchema = append(pqSchema, fmt.Sprintf("name=%s, type=%s", c.Name, pqType))
	}

	parqw, err := writer.NewCSVWriter(pqSchema, parqf, 4)
	if err != nil {
		return nil, err
	}
	parqw.CompressionType = parquet.CompressionCodec_SNAPPY

	return parqw, nil
}
