package output

import (
	"context"

	"github.com/thanos-community/obslytics/pkg/input"
)

type Output interface {
	// TODO(bwplotka): TBD
	Handle(ctx context.Context, in input.Input) error
}
