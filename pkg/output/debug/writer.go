package debug

import (
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"github.com/thanos-community/obslytics/pkg/dataframe"
	"github.com/thanos-community/obslytics/pkg/output"
)

// DebugWriter formats the dataframe into format usable for debugging and testing purposes (e.g. in
// examples). Uses tabwriter to produce the table in readable format and shortens
// fields when possible (such as using only time part of a timestamp) so it fits
// nicer into the output.
//
// If nextW present, it forwards the data there as well.
// Implements output.Writer.
type DebugWriter struct {
	w       *tabwriter.Writer
	nextW   output.Writer
	started bool
}

func NewDebugWriter(w io.Writer, nextW output.Writer) *DebugWriter {
	tabw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	return &DebugWriter{w: tabw, nextW: nextW}
}

func (w *DebugWriter) Write(df dataframe.Dataframe) error {
	if !w.started {
		w.PrintHeader(df)
		w.started = true
	}
	i := df.RowsIterator()
	for i.Next() {
		w.PrintRow(df.Schema(), i.At())
	}
	if w.nextW != nil {
		err := w.nextW.Write(df)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *DebugWriter) PrintHeader(df dataframe.Dataframe) {
	// Adding | <-   -> | around the lines to avoid dealing with training spaces
	// in example output checking
	fmt.Fprint(w.w, "| ")
	for _, c := range df.Schema() {
		fmt.Fprintf(w.w, "%s\t", c.Name)
	}
	fmt.Fprint(w.w, "|\n")
}

func (w *DebugWriter) PrintRow(s dataframe.Schema, r dataframe.Row) {
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

func (w *DebugWriter) Close() error {
	err := w.w.Flush()
	if err != nil {
		return err
	}
	return nil
}
