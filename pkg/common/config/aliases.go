package config

import (
	"fmt"
	"strings"
)

type Alias struct {
	Name            string `yaml:"name"`
	Endpoint        string `yaml:"endpoint"`
	Region          string `yaml:"region"`
	AccessKeyID     string `yaml:"accessKeyID"`
	SecretAccessKey string `yaml:"secretAccessKey"`
}

func (c *DriverConfig) GetAlias(name string) (*Alias, bool) {
	for i := range c.Aliases {
		if c.Aliases[i].Name == name {
			return &c.Aliases[i], true
		}
	}

	return nil, false
}

func (a *Alias) Validate() error {
	if strings.TrimSpace(a.Name) == "" {
		return fmt.Errorf("name cannot be empty")
	}

	if strings.TrimSpace(a.Endpoint) == "" {
		return fmt.Errorf("endpoint cannot be empty")
	}

	if !strings.HasPrefix(a.Endpoint, "http://") && !strings.HasPrefix(a.Endpoint, "https://") {
		return fmt.Errorf("endpoint must start with http:// or https://")
	}

	if strings.TrimSpace(a.AccessKeyID) == "" {
		return fmt.Errorf("accessKeyID cannot be empty")
	}

	if strings.TrimSpace(a.SecretAccessKey) == "" {
		return fmt.Errorf("secretAccessKey cannot be empty")
	}

	return nil
}
