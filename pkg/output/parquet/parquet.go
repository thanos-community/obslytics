package parquet

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/thanos-community/obslytics/pkg/dataframe"
	"github.com/thanos-community/obslytics/pkg/output"
	parquetwriter "github.com/xitongsys/parquet-go-source/writerfile"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

// Output implements output.Output interface.
type Output struct{}

func NewOutput() Output {
	return Output{}
}

func (o Output) Open(_ context.Context, params output.Params) (output.Writer, error) {
	return newParquetWriter(params.Out), nil
}

// parquetWriter Implements output.Writer.
type parquetWriter struct {
	w     io.WriteCloser
	parqf source.ParquetFile
	parqw *writer.CSVWriter
}

func newParquetWriter(w io.WriteCloser) *parquetWriter {
	return &parquetWriter{w: w, parqf: parquetwriter.NewWriterFile(w)}
}

func (w *parquetWriter) Write(df dataframe.Dataframe) error {
	if w.parqw == nil {
		parqw, err := initCSVWriter(w.parqf, df)
		if err != nil {
			return errors.Wrap(err, "error initializing the schema")
		}
		w.parqw = parqw
	}
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
		err := w.parqw.Write(d)
		if err != nil {
			return errors.Wrap(err, "error writing a row")
		}
	}
	return nil
}

func (w *parquetWriter) Close() error {
	if w.parqw != nil {
		err := w.parqw.WriteStop()
		if err != nil {
			return errors.Wrap(err, "error closing parquet writer")
		}
	}
	err := w.w.Close()
	if err != nil {
		return errors.Wrap(err, "error closing output file")
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
			pqType = "UTF8, encoding=PLAIN_DICTIONARY"
		case dataframe.TypeFloat:
			pqType = "DOUBLE"
		case dataframe.TypeUint:
			pqType = "UINT_64"
		case dataframe.TypeTime:
			pqType = "TIMESTAMP_MILLIS"
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
