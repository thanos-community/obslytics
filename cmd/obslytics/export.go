package main

import (
	"context"

	"github.com/go-kit/kit/log"
	"github.com/oklog/run"
	infactory "github.com/thanos-community/obslytics/pkg/input/factory"
	outfactory "github.com/thanos-community/obslytics/pkg/output/factory"
	"github.com/thanos-io/thanos/pkg/extflag"
	"gopkg.in/alecthomas/kingpin.v2"
)

func registerExport(m map[string]setupFunc, app *kingpin.Application) {
	cmd := app.Command("export", "Export Observability Data into popular analytics formats.")
	inputFlag := extflag.RegisterPathOrContent(cmd, "input", "YAML for input configuration.", true)
	outputFlag := extflag.RegisterPathOrContent(cmd, "ouput", "YAML for output configuration.", true)

	m["export"] = func(g *run.Group, logger log.Logger) error {
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			inputConfig, err := inputFlag.Content()
			if err != nil {
				return err
			}
			in, err := infactory.Parse(inputConfig)
			if err != nil {
				return err
			}

			outputConfig, err := outputFlag.Content()
			if err != nil {
				return err
			}
			out, err := outfactory.Parse(outputConfig)
			if err != nil {
				return err
			}
			return out.Handle(ctx, in)
		}, func(error) { cancel() })
		return nil
	}
}
