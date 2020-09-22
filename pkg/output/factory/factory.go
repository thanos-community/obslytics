package factory

import (
	"github.com/thanos-community/obslytics/pkg/output"
	"github.com/thanos-community/obslytics/pkg/output/parquet"
)

func Parse(output []byte) (output.Output, error) {
	// TODO(inecas): Add output configuration support.
	out := parquet.Output{}
	return out, nil
}
