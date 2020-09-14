package output

import (
	"context"

	"github.com/thanos-community/obslytics/pkg/dataframe"
)

// OutputParams represent options that change more often than configs.
type OutputParams struct {
	OutFile string
}

type Output interface {
	Open(context.Context, OutputParams) (Writer, error)
}

type Writer interface {
	Write(dataframe.Dataframe) error
	Close() error
}
