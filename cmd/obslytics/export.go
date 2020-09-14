package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/run"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-community/obslytics/pkg/input"
	"github.com/thanos-community/obslytics/pkg/output"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/runutil"

	"github.com/thanos-community/obslytics/pkg/output/debug"
	"github.com/thanos-io/thanos/pkg/extflag"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/thanos-community/obslytics/pkg/ingest"
	infactory "github.com/thanos-community/obslytics/pkg/input/factory"
	outfactory "github.com/thanos-community/obslytics/pkg/output/factory"
)

func registerExport(m map[string]setupFunc, app *kingpin.Application) {
	cmd := app.Command("export", "Export observability data into popular analytics formats.")
	inputFlag := extflag.RegisterPathOrContent(cmd, "input-config", "YAML for input configuration.", true)
	// TODO(inecas): add support for output configuration.
	outputFlag := extflag.RegisterPathOrContent(cmd, "output-config", "YAML for output configuration.", false)

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
	outFile := cmd.Flag("out", "Output file").Required().String()

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
			in, err := infactory.Parse(logger, inputCfg)
			if err != nil {
				return err
			}
			outputCfg, err := outputFlag.Content()
			if err != nil {
				return err
			}
			out, err := outfactory.Parse(outputCfg)
			if err != nil {
				return err
			}

			w, err := os.Create(*outFile)
			if err != nil {
				return errors.Wrapf(err, "opening file %v", *outFile)
			}
			return export(ctx, logger, in, input.SeriesParams{
				Matchers: matchers,
				MinTime:  timestamp.Time(mint.PrometheusTimestamp()),
				MaxTime:  timestamp.Time(maxt.PrometheusTimestamp()),
			}, *resolution, out, output.Params{Out: w}, *dbgOut)
		}, func(error) { cancel() })
		return nil
	}
}

func export(ctx context.Context, logger log.Logger, in input.Input, seriesParams input.SeriesParams, resolution time.Duration, out output.Output, outParams output.Params, dbgOut bool) error {
	ser, err := in.Open(ctx, seriesParams)
	if err != nil {
		return err
	}

	a := ingest.NewAggregator(resolution, func(o *ingest.AggrsOptions) {
		// TODO(inecas): Expose the enabled aggregations via flag.
		o.Count.Enabled = true
		o.Sum.Enabled = true
		o.Min.Enabled = true
		o.Max.Enabled = true
	})

	w, err := out.Open(ctx, outParams)
	if err != nil {
		return err
	}
	if dbgOut {
		w = debug.NewWriter(os.Stdout, w)
	}
	defer runutil.CloseWithLogOnErr(logger, w, "close output")
	return ingest.ProcessAll(ser, a, w)
}
