package otel

import "time"

type OpenTelemtryConfig struct {
	Endpoint     string
	ServiceName  string
	Hostname     string
	BatchTimeout time.Duration
	BatchSize    int
}
