package dataframe

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
