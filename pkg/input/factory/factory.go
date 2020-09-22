package factory

import (
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/thanos-community/obslytics/pkg/input/storeapi"
	"gopkg.in/yaml.v2"

	"github.com/thanos-community/obslytics/pkg/input"
	"github.com/thanos-community/obslytics/pkg/input/promread"
)

func Parse(logger log.Logger, confYaml []byte) (input.Input, error) {
	// Set Default Configuration values.
	cfg := input.InputConfig{
		Type: input.REMOTEREAD,
	}

	if err := yaml.UnmarshalStrict(confYaml, &cfg); err != nil {
		return nil, err
	}

	var in input.Input
	var err error
	switch strings.ToUpper(string(cfg.Type)) {
	case string(input.REMOTEREAD):
		in, err = promread.NewRemoteReadInput(logger, cfg)
	case string(input.STOREAPI):
		in, err = storeapi.NewStoreAPIInput(logger, cfg)
	default:
		return nil, errors.Errorf("Unsupported Input type %s", string(cfg.Type))
	}

	return in, err
}
