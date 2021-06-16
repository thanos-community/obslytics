package main

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/run"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/thanos/pkg/model"

	"github.com/thanos-io/thanos/pkg/extflag"
	"gopkg.in/alecthomas/kingpin.v2"

	exporterfactory "github.com/thanos-community/obslytics/pkg/exporter/factory"
	infactory "github.com/thanos-community/obslytics/pkg/series/factory"
)

func registerExport(m map[string]setupFunc, app *kingpin.Application) {
	cmd := app.Command("export", "Export observability series data into popular analytics formats.")
	inputFlag := extflag.RegisterPathOrContent(cmd, "input-config", "YAML for input, series configuration.", true)
	outputFlag := extflag.RegisterPathOrContent(cmd, "output-config", "YAML for dataframe export configuration.", false)

	// TODO(bwplotka): Describe more how the format looks like.
	matchersStr := cmd.Flag("match", "Metric matcher for metrics to export (e.g up{a=\"1\"}").Required().String()
	timeFmt := time.RFC3339

	var mint, maxt model.TimeOrDurationValue
	cmd.Flag("min-time", fmt.Sprintf("The lower boundary of the time series in %s or duration format", timeFmt)).
		Required().SetValue(&mint)

	cmd.Flag("max-time", fmt.Sprintf("The upper boundary of the time series in %s or duration format", timeFmt)).
		Required().SetValue(&maxt)

	resolution := cmd.Flag("resolution", "Sample resolution (e.g. 30m)").Required().Duration()
	dbgOut := cmd.Flag("debug", "Show additional debug info (such as produced table)").Bool()

	m["export"] = func(g *run.Group, logger log.Logger) error {
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			matchers, err := parser.ParseMetricSelector(*matchersStr)
			if err != nil {
				return errors.Wrap(err, "parsing provided matchers")
			}

			inputCfg, err := inputFlag.Content()
			if err != nil {
				return err
			}
			in, err := infactory.NewSeriesReader(logger, inputCfg)
			if err != nil {
				return err
			}
			outputCfg, err := outputFlag.Content()
			if err != nil {
				return err
			}

			exp, err := exporterfactory.NewExporter(logger, outputCfg)
			if err != nil {
				return err
			}

			if err := exp.ExportStream(ctx, in, matchers, mint, maxt, *resolution, *dbgOut, 4); err != nil {
				return errors.Wrapf(err, "export dataframe")
			}
			return nil
		}, func(error) { cancel() })
		return nil
	}
}
