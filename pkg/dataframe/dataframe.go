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

type Schema []Column

type RowsIterator interface {
	Next() bool
	At() Row
}

type Row []interface{}

// Stores ingested data to be used to turn into tabular format.
type Dataframe interface {
	Schema() Schema
	RowsIterator() RowsIterator
}
