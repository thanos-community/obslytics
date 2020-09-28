package factory

import (
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/thanos-community/obslytics/pkg/series"
	"github.com/thanos-community/obslytics/pkg/series/promread"
	"github.com/thanos-community/obslytics/pkg/series/storeapi"
	"gopkg.in/yaml.v2"
)

// NewSeriesReader creates series.Reader based on configuration file.
func NewSeriesReader(logger log.Logger, confYaml []byte) (series.Reader, error) {
	cfg := series.Config{}
	if err := yaml.UnmarshalStrict(confYaml, &cfg); err != nil {
		return nil, err
	}

	switch series.Type(strings.ToUpper(string(cfg.Type))) {
	case series.REMOTEREAD:
		return promread.NewSeries(logger, cfg)
	case series.STOREAPI:
		return storeapi.NewSeries(logger, cfg)
	default:
		return nil, errors.Errorf("unsupported Reader type %s", cfg.Type)
	}
}
