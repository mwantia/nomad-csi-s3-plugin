package main

import (
	"context"
	"flag"
	"log"
	"os"

	"github.com/mwantia/nomad-csi-s3-plugin/pkg/driver"
)

func init() {
	flag.Set("logtostderr", "true")
}

var (
	Endpoint = flag.String("endpoint", "unix://tmp/csi.sock", "CSI Endpoint")
	NodeID   = flag.String("nodeid", "", "Node ID")
)

func main() {
	flag.Parse()

	d, err := driver.New(*NodeID, *Endpoint)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	if err = d.Run(ctx); err != nil {
		log.Printf("Unable to start driver: %v", err)
	}

	os.Exit(0)
}
