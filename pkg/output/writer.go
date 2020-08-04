package output

import (
	"github.com/thanos-community/obslytics/pkg/dataframe"
)

type Writer interface {
	Write(dataframe.Dataframe) error
	Close() error
}
