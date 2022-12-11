// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package series

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	http_util "github.com/thanos-io/thanos/pkg/exthttp"
)

type Type string

const (
	REMOTEREAD Type = "REMOTEREAD"
	STOREAPI   Type = "STOREAPI"
)

// Config contains the options determining the endpoint to talk to.
type Config struct {
	Endpoint  string              `yaml:"endpoint"`
	TLSConfig http_util.TLSConfig `yaml:"tls_config"`
	Type      Type                `yaml:"type"`
}

// Params determines what data should be loaded from the input.
type Params struct {
	Matchers []*labels.Matcher
	MinTime  time.Time
	MaxTime  time.Time
}

type Reader interface {
	Read(context.Context, Params) (Set, error)
}

// Set allows iterating through all series in tn the input.
// The set is expected to iterate series by series. The same series can be partitioned between multiple iterations.
type Set interface {
	storage.SeriesSet
	Close() error
}
