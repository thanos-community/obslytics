package factory

import (
	"github.com/go-kit/kit/log"
	"gopkg.in/yaml.v2"

	"github.com/thanos-community/obslytics/pkg/input"
	"github.com/thanos-community/obslytics/pkg/input/storeapi"
)

func Parse(logger log.Logger, confYaml []byte) (input.Input, error) {
	var cfg input.InputConfig
	if err := yaml.UnmarshalStrict(confYaml, &cfg); err != nil {
		return nil, err
	}

	// TODO(inecas): determine the implementation from config options.
	in, err := storeapi.NewStoreAPIInput(logger, cfg)
	return in, err
}
