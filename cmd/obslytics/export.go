package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/run"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-community/obslytics/pkg/dataframe"
	"github.com/thanos-community/obslytics/pkg/series"
	"github.com/thanos-io/thanos/pkg/model"

	"github.com/thanos-io/thanos/pkg/extflag"
	"gopkg.in/alecthomas/kingpin.v2"

	exportertfactory "github.com/thanos-community/obslytics/pkg/exporter/factory"
	infactory "github.com/thanos-community/obslytics/pkg/series/factory"
)

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}
func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

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
			fmt.Println("Before Parsing Matchers")
			PrintMemUsage()
			matchers, err := parser.ParseMetricSelector(*matchersStr)
			if err != nil {
				return errors.Wrap(err, "parsing provided matchers")
			}

			fmt.Println("Matchers Parsed")
			PrintMemUsage()
			inputCfg, err := inputFlag.Content()
			if err != nil {
				return err
			}

			fmt.Println("Input config determined")
			PrintMemUsage()
			in, err := infactory.NewSeriesReader(logger, inputCfg)
			if err != nil {
				return err
			}

			fmt.Println("Series reader created")
			PrintMemUsage()
			outputCfg, err := outputFlag.Content()
			if err != nil {
				return err
			}

			fmt.Println("Output config determined")
			PrintMemUsage()
			exp, err := exportertfactory.NewExporter(logger, outputCfg)
			if err != nil {
				return err
			}

			fmt.Println("After exporter is created")
			PrintMemUsage()
			ser, err := in.Read(ctx, series.Params{
				Matchers: matchers,
				MinTime:  timestamp.Time(mint.PrometheusTimestamp()),
				MaxTime:  timestamp.Time(maxt.PrometheusTimestamp()),
			})
			if err != nil {
				return err
			}

			fmt.Println("After series reader iterator is created")
			PrintMemUsage()
			df, err := dataframe.FromSeries(ser, *resolution, func(o *dataframe.AggrsOptions) {
				// TODO(inecas): Expose the enabled aggregations via flag.
				o.Count.Enabled = true
				o.Sum.Enabled = true
				o.Min.Enabled = true
				o.Max.Enabled = true
			})

			fmt.Println("after dataframe iterator is created")
			PrintMemUsage()
			if err != nil {
				return errors.Wrap(err, "dataframe creation")
			}

			if *dbgOut {
				dataframe.Print(os.Stdout, df)
			}
			fmt.Println("After printing and errors")
			PrintMemUsage()

			if err := exp.Export(ctx, df); err != nil {
				return errors.Wrapf(err, "export dataframe")
			}
			fmt.Println("After export")
			PrintMemUsage()
			return nil
		}, func(error) { cancel() })
		return nil
	}
}
