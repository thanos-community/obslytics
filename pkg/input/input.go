package input

import "context"

type Input interface {
	// TODO(bwplotka): TBD params & response
	Read(ctx context.Context) error
}
