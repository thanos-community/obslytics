package factory

import (
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/thanos-community/obslytics/pkg/series"
	"github.com/thanos-community/obslytics/pkg/series/promread"
	"github.com/thanos-community/obslytics/pkg/series/storeapi"
)

// NewSeriesReader creates series.Reader based on configuration file.
func NewSeriesReader(logger log.Logger, cfg series.Config) (series.Reader, error) {
	switch series.Type(strings.ToUpper(string(cfg.Type))) {
	case series.REMOTEREAD:
		return promread.NewSeries(logger, cfg)
	case series.STOREAPI:
		return storeapi.NewSeries(logger, cfg)
	default:
		return nil, errors.Errorf("unsupported Reader type %s", cfg.Type)
	}
}
