package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/run"
	"github.com/thanos-community/obslytics/pkg/input"
	"github.com/thanos-community/obslytics/pkg/output"

	"github.com/thanos-community/obslytics/pkg/output/debug"
	"github.com/thanos-io/thanos/pkg/extflag"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/thanos-community/obslytics/pkg/ingest"
	infactory "github.com/thanos-community/obslytics/pkg/input/factory"
	outfactory "github.com/thanos-community/obslytics/pkg/output/factory"
)

func registerExport(m map[string]setupFunc, app *kingpin.Application) {
	cmd := app.Command("export", "Export Observability Data into popular analytics formats.")
	inputFlag := extflag.RegisterPathOrContent(cmd, "input-config", "YAML for input configuration.", true)
	// TODO(inecas): add support for output configuration.
	outputFlag := extflag.RegisterPathOrContent(cmd, "output-config", "YAML for output configuration.", false)

	seriesParams := input.SeriesParams{}
	cmd.Flag("match", "Name of the metric to export").Required().StringVar(&seriesParams.Matcher)
	timeFmt := time.RFC3339

	cmd.Flag("min-time", fmt.Sprintf("The lower boundary of the time series in %s or duration format", timeFmt)).
		Required().SetValue(&seriesParams.MinTime)

	cmd.Flag("max-time", fmt.Sprintf("The upper boundary of the time series in %s or duration format", timeFmt)).
		Required().SetValue(&seriesParams.MaxTime)

	var resolution time.Duration
	cmd.Flag("resolution", "Sample resolution (e.g. 30m)").Required().DurationVar(&resolution)

	dbgOut := false
	cmd.Flag("debug", "Show additional debug info (such as produced table)").BoolVar(&dbgOut)

	outParams := output.OutputParams{}
	cmd.Flag("out", "Output file").Required().StringVar(&outParams.OutFile)

	m["export"] = func(g *run.Group, logger log.Logger) error {
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			inputCfg, err := inputFlag.Content()
			if err != nil {
				return err
			}
			in, err := infactory.Parse(logger, inputCfg)
			if err != nil {
				return err
			}

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

			outputCfg, err := outputFlag.Content()
			if err != nil {
				return err
			}
			out, err := outfactory.Parse(outputCfg)
			if err != nil {
				return err
			}

			w, err := out.Open(ctx, outParams)
			if err != nil {
				return err
			}
			defer w.Close()

			if dbgOut {
				w = debug.NewDebugWriter(os.Stdout, w)
				defer w.Close()
			}

			return ingest.ProcessAll(ser, a, w)
		}, func(error) { cancel() })
		return nil
	}
}
