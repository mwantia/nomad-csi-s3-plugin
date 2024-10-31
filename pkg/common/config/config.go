package config

import (
	"fmt"

	"gopkg.in/yaml.v2"
)

type DriverConfig struct {
	Aliases []Alias `yaml:"aliases"`
}

func LoadDriverConfig(data []byte) (*DriverConfig, error) {
	cfg := &DriverConfig{}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return cfg, fmt.Errorf("failed to parse config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return cfg, fmt.Errorf("invalid config: %w", err)
	}

	return cfg, nil
}

func (c *DriverConfig) Validate() error {
	if len(c.Aliases) > 0 {
		uniques := make(map[string]bool)
		for i, alias := range c.Aliases {
			if err := alias.Validate(); err != nil {
				return fmt.Errorf("invalid alias '%s' at index '%d': %w", alias.Name, i, err)
			}

			if uniques[alias.Name] {
				return fmt.Errorf("duplicate alias name found: %s", alias.Name)
			}
			uniques[alias.Name] = true
		}
	}

	return nil
}
