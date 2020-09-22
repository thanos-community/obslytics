package output

import (
	"context"
	"io"

	"github.com/thanos-community/obslytics/pkg/dataframe"
)

type nopWriteCloser struct {
	io.Writer
}

func (w *nopWriteCloser) Close() error { return nil }

// NopWriteCloser returns a nopWriteCloser.
func NopWriteCloser(w io.Writer) io.WriteCloser {
	return &nopWriteCloser{w}
}

// Params represent options that change more often than configs.
type Params struct {
	Out io.WriteCloser
}

type Output interface {
	Open(context.Context, Params) (Writer, error)
}

type Writer interface {
	Write(dataframe.Dataframe) error
	Close() error
}
