package main

import (
	"context"
	"flag"
	"os"
	"time"

	"github.com/golang/glog"
	"github.com/mwantia/nomad-csi-s3-plugin/pkg/driver"
	"github.com/mwantia/nomad-csi-s3-plugin/pkg/otel"
)

func init() {
	flag.Set("logtostderr", "true")
}

var (
	Endpoint     = flag.String("endpoint", "unix://tmp/csi.sock", "CSI Endpoint")
	NodeID       = flag.String("nodeid", "", "Node ID")
	OtelEnable   = flag.Bool("otel-enable", false, "OpenTelemetry Enable")
	OtelEndpoint = flag.String("otel-endpoint", "localhost:4318", "OpenTelemetry Endpoint")
)

func main() {
	flag.Parse()

	d, err := driver.New(*NodeID, *Endpoint)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	var shutdown func(ctx context.Context) error
	if *OtelEnable {
		shutdown, err = otel.SetupOpentelemetry(ctx, otel.OpenTelemtryConfig{
			Endpoint:     *OtelEndpoint,
			ServiceName:  driver.DriverName,
			Hostname:     *NodeID,
			BatchTimeout: 5 * time.Second,
			BatchSize:    10,
		})
		if err != nil {
			glog.Errorln("Unable to setup opentelemetry: %v", err)
		}
	}

	if err = d.Run(ctx); err != nil {
		glog.Errorln("Unable to start driver: %v", err)
	}

	if *OtelEnable {
		if shutdown != nil {
			shutdown(ctx)
		}
	}
	os.Exit(0)
}
