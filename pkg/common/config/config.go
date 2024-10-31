package config

import (
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/spf13/viper"
)

type DriverConfig struct {
	Aliases []Alias `mapstructure:"aliases"`
}

func LoadDriverConfig(path string) (*DriverConfig, error) {
	v := viper.New()

	v.SetConfigFile(path)
	v.SetEnvPrefix("")
	v.AutomaticEnv()
	v.AllowEmptyEnv(true)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.SetConfigType("yaml")

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	cfg := &DriverConfig{}

	if err := v.Unmarshal(cfg, viper.DecodeHook(func(src, dst reflect.Type, data interface{}) (interface{}, error) {
		if src.Kind() != reflect.String {
			return data, nil
		}

		str := data.(string)
		if strings.Contains(str, "${") || strings.HasPrefix(str, "$") {

			envVar := strings.Trim(str, "${}")
			envVar = strings.TrimPrefix(envVar, "$")

			if value, exists := os.LookupEnv(envVar); exists {
				return value, nil
			}
		}
		return data, nil
	})); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
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
