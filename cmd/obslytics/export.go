package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/run"
	"github.com/thanos-community/obslytics/pkg/input"
	"github.com/thanos-io/thanos/pkg/extflag"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/thanos-community/obslytics/pkg/ingest"
	infactory "github.com/thanos-community/obslytics/pkg/input/factory"
	"github.com/thanos-community/obslytics/pkg/output/example"
	// outfactory "github.com/thanos-community/obslytics/pkg/output/factory"
)

func registerExport(m map[string]setupFunc, app *kingpin.Application) {
	cmd := app.Command("export", "Export Observability Data into popular analytics formats.")
	inputFlag := extflag.RegisterPathOrContent(cmd, "input", "YAML for input configuration.", true)
	// outputFlag := extflag.RegisterPathOrContent(cmd, "ouput", "YAML for output configuration.", true)

	serParams := input.SeriesParams{}
	cmd.Flag("metric", "Name of the metric to export").Required().StringVar(&serParams.Metric)
	timeFmt := time.RFC3339
	minTimeStr := cmd.Flag("min-time", fmt.Sprintf("The lower boundary of the time series in %s format", timeFmt)).
		Required().String()
	maxTimeStr := cmd.Flag("max-time", fmt.Sprintf("The upper boundary of the time series in %s format", timeFmt)).
		Required().String()
	resolution := cmd.Flag("resolution", "Sample resolution (e.g. 30m)").
		Required().Duration()

	m["export"] = func(g *run.Group, logger log.Logger) error {
		minTime, err := time.Parse(timeFmt, *minTimeStr)
		if err != nil {
			return err
		}
		serParams.MinTime = minTime
		maxTime, err := time.Parse(timeFmt, *maxTimeStr)
		if err != nil {
			return err
		}
		serParams.MaxTime = maxTime

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			inputConfig, err := inputFlag.Content()
			if err != nil {
				return err
			}
			in, err := infactory.Parse(logger, inputConfig)
			if err != nil {
				return err
			}

			ser, err := in.Open(ctx, serParams)
			if err != nil {
				return err
			}

			a := ingest.NewAggregator(*resolution, func(o *ingest.AggrsOptions) {
				// TODO(inecas): expose the enabled aggregations via flag
				o.Count.Enabled = true
				o.Sum.Enabled = true
				o.Min.Enabled = true
				o.Max.Enabled = true
			})

			// TODO(inecas): connect the output configuration
			// outputConfig, err := outputFlag.Content()
			// if err != nil {
			// 	return err
			// }
			// out, err := outfactory.Parse(outputConfig)
			// if err != nil {
			// 	return err
			// }

			w := example.NewExampleWriter(os.Stdout)
			defer w.Close()

			err = ingest.ProcessAll(ser, a, w)
			if err != nil {
				return err
			}
			return nil
		}, func(error) { cancel() })
		return nil
	}
}
