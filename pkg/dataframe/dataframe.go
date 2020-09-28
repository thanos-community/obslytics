package dataframe

import (
	"bytes"
	"fmt"
	"io"
	"text/tabwriter"
	"time"
)

type Type string

const (
	TypeString Type = "string"
	TypeFloat  Type = "float"
	TypeUint   Type = "uint"
	TypeTime   Type = "time"
)

type Column struct {
	Name string
	Type Type
}

// Schema defines columns to be exposed by the dataframe.
type Schema []Column

// RowsIterator exposes the rows of the dataframe.
type RowsIterator interface {
	Next() bool
	At() Row
}

// Row stores a single line of a table - the order of columns is defined by the Schema.
type Row []interface{}

// Dataframe exposes ingested data to be used to turn into tabular format.
type Dataframe interface {
	Schema() Schema
	RowsIterator() RowsIterator
}

// Print formats the dataframe into format usable for debugging and testing purposes (e.g. in
// examples). Uses tabwriter to produce the table in readable format and shortens
// fields when possible (such as using only time part of a timestamp) so it fits
// nicer into the output.
func Print(w io.Writer, df Dataframe) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)

	printHeader(tw, df)
	i := df.RowsIterator()
	for i.Next() {
		printRow(tw, df.Schema(), i.At())
	}
	_ = tw.Flush()
}

func ToString(df Dataframe) string {
	b := &bytes.Buffer{}
	Print(b, df)
	return b.String()
}

func printHeader(w io.Writer, df Dataframe) {
	// Adding | <-   -> | around the lines to avoid dealing with training spaces
	// in example output checking.
	fmt.Fprint(w, "| ")
	for _, c := range df.Schema() {
		fmt.Fprintf(w, "%s\t", c.Name)
	}
	fmt.Fprint(w, "|\n")
}

func printRow(w io.Writer, s Schema, r Row) {
	fmt.Fprint(w, "| ")
	for i, cell := range r {
		c := s[i]
		switch c.Type {
		case TypeString:
			fmt.Fprintf(w, "%s\t", cell)
		case TypeFloat:
			v := cell.(float64)
			fmt.Fprintf(w, "%.0f\t", v)
		case TypeUint:
			v := cell.(uint64)
			fmt.Fprintf(w, "%d\t", v)
		case TypeTime:
			v := cell.(time.Time)
			fmt.Fprintf(w, "%s\t", v.Format("15:04:05"))
		default:
			fmt.Fprintf(w, "%s\t", cell)
		}
	}
	fmt.Fprint(w, "|\n")
}
