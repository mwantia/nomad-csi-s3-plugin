package main

import (
	"context"
	"flag"
	"log"
	"os"
	"strings"

	"github.com/mwantia/nomad-csi-s3-plugin/pkg/common/config"
	"github.com/mwantia/nomad-csi-s3-plugin/pkg/driver"
)

func init() {
	flag.Set("logtostderr", "true")
}

var (
	Endpoint = flag.String("endpoint", "unix://tmp/csi.sock", "CSI Endpoint")
	NodeID   = flag.String("nodeid", "", "Node ID")
	Config   = flag.String("config", "", "Configuration Path")
)

func main() {
	flag.Parse()

	d, err := driver.New(*NodeID, *Endpoint)
	if err != nil {
		panic(err)
	}

	if strings.TrimSpace(*Config) != "" {
		data, err := os.ReadFile(*Config)
		if err != nil {
			log.Printf("unable to load config from '%s': %v", *Config, err)
		}

		cfg, err := config.LoadDriverConfig(data)
		if err != nil {
			log.Printf("failed to load config: %v", err)
		}

		d.Cfg = cfg
	}

	ctx := context.Background()

	if err = d.Run(ctx); err != nil {
		log.Printf("Unable to start driver: %v", err)
	}

	os.Exit(0)
}
